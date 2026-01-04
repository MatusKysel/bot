mod arbitrage;
mod config;
mod market_selection;
mod polymarket;
mod ws_orderbook;

use crate::arbitrage::{evaluate_market, evaluate_overheat, OutcomeBook, OverheatSignal};
use crate::config::Config;
use crate::market_selection::score_market;
use crate::polymarket::PolymarketClient;
use crate::ws_orderbook::WsOrderbookHandle;
use anyhow::{Context, Result};
use clap::Parser;
use futures::stream::{self, StreamExt};
use serde_json;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};
use tracing_subscriber::fmt::writer::{BoxMakeWriter, MakeWriterExt};

#[derive(Parser, Debug)]
#[command(author, version, about = "Polymarket crypto arbitrage scanner")]
struct Cli {
    #[arg(long, default_value = "config.toml")]
    config: PathBuf,
}

const OVERHEAT_FLIP_WINDOW_SECS: u64 = 60;
static SCAN_COUNTER: AtomicU64 = AtomicU64::new(1);
static OPP_COUNTER: AtomicU64 = AtomicU64::new(1);

struct RunContext {
    config_hash: String,
    start: Instant,
}

struct MarketScanResult {
    market_id: String,
    market_question: String,
    market_slug: Option<String>,
    opportunities: Vec<arbitrage::ArbOpportunity>,
    overheat: Option<OverheatSignal>,
    book_age_ms: Option<u64>,
    skip_reason: Option<SkipReason>,
    best_candidate: Option<arbitrage::BestCandidate>,
    depth_samples: Vec<arbitrage::DepthSample>,
    selected_outcomes: Option<SelectedOutcomes>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SkipReason {
    NotBinary,
    MissingToken,
    MissingBook,
    StaleBook,
    NoDepth,
    NoEdge,
    BelowBuffer,
    BelowMinNotional,
    AboveMaxNotional,
    NoOpportunity,
}

impl SkipReason {
    fn as_str(&self) -> &'static str {
        match self {
            SkipReason::NotBinary => "not_binary",
            SkipReason::MissingToken => "missing_token",
            SkipReason::MissingBook => "missing_book",
            SkipReason::StaleBook => "stale_book",
            SkipReason::NoDepth => "no_depth",
            SkipReason::NoEdge => "no_edge",
            SkipReason::BelowBuffer => "below_buffer",
            SkipReason::BelowMinNotional => "below_min_notional",
            SkipReason::AboveMaxNotional => "above_max_notional",
            SkipReason::NoOpportunity => "no_opportunity",
        }
    }
}

#[derive(Debug, Clone)]
struct SelectedOutcomes {
    yes: polymarket::Outcome,
    no: polymarket::Outcome,
    fallback: bool,
}

struct OverheatState {
    last_seen: Instant,
    bid_sum: f64,
}

struct OverheatTracker {
    states: HashMap<String, OverheatState>,
}

impl OverheatTracker {
    fn new() -> Self {
        Self {
            states: HashMap::new(),
        }
    }

    fn record(&mut self, signal: &OverheatSignal, now: Instant) {
        self.states.insert(
            signal.market_id.clone(),
            OverheatState {
                last_seen: now,
                bid_sum: signal.bid_sum,
            },
        );
    }

    fn recent(&self, market_id: &str, window: Duration, now: Instant) -> Option<(Duration, f64)> {
        let state = self.states.get(market_id)?;
        let age = now.duration_since(state.last_seen);
        if age <= window {
            Some((age, state.bid_sum))
        } else {
            None
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let (mut config, raw_config) = Config::from_path_with_hash(&cli.config)
        .with_context(|| format!("load config from {}", cli.config.display()))?;
    if let Some(path) = config.logging.file_path.as_ref() {
        if path.trim().is_empty() {
            config.logging.file_path = None;
        }
    }
    config.validate()?;

    let config_hash = hex::encode(Sha256::digest(raw_config.as_bytes()));

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(config.logging.level.clone()));
    let mut file_guard = None;
    let writer: BoxMakeWriter = if let Some(path) = config.logging.file_path.clone() {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("open log file {}", path))?;
        let (file_writer, guard) = tracing_appender::non_blocking(file);
        file_guard = Some(guard);
        BoxMakeWriter::new(std::io::stdout.and(file_writer))
    } else {
        BoxMakeWriter::new(std::io::stdout)
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .json()
        .with_current_span(true)
        .with_writer(writer)
        .init();

    let run_context = RunContext {
        config_hash,
        start: Instant::now(),
    };

    let client = PolymarketClient::new(&config.polymarket)?;
    let ws_handle = if config.polymarket.use_websocket {
        Some(ws_orderbook::spawn_ws_orderbook(&config.polymarket)?)
    } else {
        None
    };
    if ws_handle.is_some() {
        info!(event = "startup", orderbook_source = "websocket");
    } else {
        info!(event = "startup", orderbook_source = "rest");
    }
    info!(
        event = "startup",
        config_hash = %run_context.config_hash,
        ws_url = %config.polymarket.ws_url,
        scan_interval_secs = config.polymarket.scan_interval_secs,
        market_limit = config.polymarket.market_limit
    );
    let interval = config.polymarket.scan_interval_secs;
    let mut overheat_tracker = OverheatTracker::new();
    let mut last_subscribed_tokens: HashSet<String> = HashSet::new();
    loop {
        if let Err(err) = run_scan(
            &client,
            &config,
            &run_context,
            ws_handle.as_ref(),
            &mut overheat_tracker,
            &mut last_subscribed_tokens,
        )
        .await
        {
            warn!(event = "scan_failed", error = %err);
        }
        if interval == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(interval)).await;
    }

    drop(file_guard);
    Ok(())
}

async fn run_scan(
    client: &PolymarketClient,
    config: &Config,
    run_context: &RunContext,
    ws_handle: Option<&WsOrderbookHandle>,
    overheat_tracker: &mut OverheatTracker,
    last_subscribed_tokens: &mut HashSet<String>,
) -> Result<()> {
    let scan_id = SCAN_COUNTER.fetch_add(1, Ordering::Relaxed);
    let scan_started_at = Instant::now();
    let markets = client
        .fetch_markets(config.polymarket.market_limit)
        .await
        .context("fetch markets")?;

    let total = markets.len();
    let filtered = if config.polymarket.market_category.trim().is_empty() {
        markets
    } else {
        markets
            .into_iter()
            .filter(|market| {
                market.matches_category(
                    &config.polymarket.market_category,
                    config.polymarket.market_subcategory.as_deref(),
                )
            })
            .collect()
    };

    info!(
        event = "scan_started",
        scan_id,
        config_hash = %run_context.config_hash,
        total_markets = total,
        matched_markets = filtered.len(),
        category = %config.polymarket.market_category,
        elapsed_ms = scan_started_at.elapsed().as_millis() as u64,
        monotonic_ms = run_context.start.elapsed().as_millis() as u64
    );

    let mut selected = Vec::new();
    let mut skipped = 0usize;
    for market in filtered {
        let score = score_market(&market, &config.selection);
        if score.score >= config.selection.min_score {
            selected.push(market);
        } else {
            skipped += 1;
            let reasons = if score.reasons.is_empty() {
                "no_signals".to_string()
            } else {
                score.reasons.join(",")
            };
            debug!(
                event = "market_skipped",
                scan_id,
                market_id = %market.id,
                score = score.score,
                reasons = reasons
            );
        }
    }

    info!(
        event = "market_selection",
        scan_id,
        selected = selected.len(),
        skipped,
        min_score = config.selection.min_score
    );

    let max_concurrency = config.polymarket.max_concurrent_orderbook_requests.max(1);
    let selected_count = selected.len();
    let results = if let Some(ws) = ws_handle {
        let token_ids = collect_token_ids(&selected, &config.arbitrage, scan_id);
        if !token_ids.is_empty() {
            ws.subscribe_tokens(token_ids.clone()).await?;
        }
        let desired_tokens: HashSet<String> = token_ids.into_iter().collect();
        let to_unsubscribe: Vec<String> = last_subscribed_tokens
            .difference(&desired_tokens)
            .cloned()
            .collect();
        if !to_unsubscribe.is_empty() {
            ws.unsubscribe_tokens(to_unsubscribe.clone()).await?;
            ws.evict_tokens(&to_unsubscribe).await;
        }
        *last_subscribed_tokens = desired_tokens;
        let cache = ws.cache();
        let max_quote_age = Duration::from_secs(config.polymarket.max_quote_age_secs);
        stream::iter(selected.into_iter().map(|market| {
            let cache = cache.clone();
            let arb_config = config.arbitrage.clone();
            async move {
                process_market_from_cache(&cache, &arb_config, market, max_quote_age).await
            }
        }))
        .buffer_unordered(max_concurrency)
        .collect::<Vec<_>>()
        .await
    } else {
        stream::iter(selected.into_iter().map(|market| {
            let client = client.clone();
            let arb_config = config.arbitrage.clone();
            async move { process_market(&client, &arb_config, market).await }
        }))
        .buffer_unordered(max_concurrency)
        .collect::<Vec<_>>()
        .await
    };

    let now = Instant::now();
    let flip_window = Duration::from_secs(OVERHEAT_FLIP_WINDOW_SECS);
    let mut opportunities = 0usize;
    let mut books_present = 0usize;
    let mut max_book_age_ms = 0u64;
    let mut skip_counts: HashMap<String, usize> = HashMap::new();
    for result in results {
        match result {
            Ok(found) => {
                if let Some(age_ms) = found.book_age_ms {
                    books_present += 1;
                    if age_ms > max_book_age_ms {
                        max_book_age_ms = age_ms;
                    }
                }
                if let Some(reason) = found.skip_reason {
                    *skip_counts
                        .entry(reason.as_str().to_string())
                        .or_insert(0) += 1;
                    if config.logging.log_opportunity_skipped {
                        log_opportunity_skipped(&found, scan_id, run_context, config);
                    }
                }
                for opportunity in found.opportunities {
                    let opp_id = OPP_COUNTER.fetch_add(1, Ordering::Relaxed);
                    let span = tracing::info_span!(
                        "opportunity",
                        opp_id,
                        scan_id,
                        market_id = %opportunity.market_id,
                        market_question = %opportunity.market_question
                    );
                    let _enter = span.enter();
                    if let Some((age, bid_sum)) =
                        overheat_tracker.recent(&opportunity.market_id, flip_window, now)
                    {
                        info!(
                            event = "overheat_flip",
                            age_secs = age.as_secs(),
                            bid_sum = bid_sum,
                            config_hash = %run_context.config_hash,
                            monotonic_ms = run_context.start.elapsed().as_millis() as u64
                        );
                    }
                    opportunities += 1;
                    log_opportunity(&opportunity, scan_id, run_context, found.book_age_ms);
                }
                if let Some(overheat) = found.overheat {
                    log_overheat(&overheat, scan_id, run_context, found.book_age_ms);
                    overheat_tracker.record(&overheat, now);
                }
            }
            Err(err) => {
                warn!(event = "market_scan_error", error = %err);
            }
        }
    }

    if config.logging.log_health
        && scan_id % config.logging.health_log_every_scans.max(1) == 0
    {
        let skip_counts_json = serde_json::to_string(&skip_counts).unwrap_or_default();
        let (cache_size, ws_stats) = if let Some(ws) = ws_handle {
            (ws.cache_len(), Some(ws.stats_snapshot()))
        } else {
            (0, None)
        };
        info!(
            event = "health",
            scan_id,
            config_hash = %run_context.config_hash,
            markets_selected = selected_count,
            books_present,
            max_book_age_ms,
            cache_size,
            skipped = %skip_counts_json,
            ws_dropped_updates = ws_stats.as_ref().map(|s| s.dropped_updates).unwrap_or(0),
            ws_resyncs = ws_stats.as_ref().map(|s| s.resyncs).unwrap_or(0),
            ws_reconnects = ws_stats.as_ref().map(|s| s.reconnects).unwrap_or(0),
            monotonic_ms = run_context.start.elapsed().as_millis() as u64
        );
    }

    info!(
        event = "scan_complete",
        scan_id,
        opportunities,
        elapsed_ms = scan_started_at.elapsed().as_millis() as u64,
        monotonic_ms = run_context.start.elapsed().as_millis() as u64
    );
    Ok(())
}

async fn process_market_from_cache(
    cache: &ws_orderbook::OrderbookCache,
    config: &config::ArbitrageConfig,
    market: polymarket::Market,
    max_quote_age: Duration,
) -> Result<MarketScanResult> {
    let selected = match select_binary_outcomes(&market, config) {
        Some(outcomes) => outcomes,
        None => {
            return Ok(MarketScanResult {
                market_id: market.id.clone(),
                market_question: market.question.clone(),
                market_slug: market.slug.clone(),
                opportunities: Vec::new(),
                overheat: None,
                book_age_ms: None,
                skip_reason: Some(SkipReason::NotBinary),
                best_candidate: None,
                depth_samples: Vec::new(),
                selected_outcomes: None,
            })
        }
    };
    let outcomes = vec![selected.yes.clone(), selected.no.clone()];

    let enforce_staleness = config.require_orderbook && max_quote_age.as_secs() > 0;
    let mut books = Vec::with_capacity(outcomes.len());
    let mut max_age_ms: Option<u64> = None;
    for outcome in &outcomes {
        if outcome.token_id.trim().is_empty() {
            debug!("missing token id for market {}", market.id);
            return Ok(MarketScanResult {
                market_id: market.id.clone(),
                market_question: market.question.clone(),
                market_slug: market.slug.clone(),
                opportunities: Vec::new(),
                overheat: None,
                book_age_ms: None,
                skip_reason: Some(SkipReason::MissingToken),
                best_candidate: None,
                depth_samples: Vec::new(),
                selected_outcomes: Some(selected.clone()),
            });
        }

        if let Some(state) = cache.get(&outcome.token_id) {
            if enforce_staleness && state.updated_at.elapsed() > max_quote_age {
                debug!("stale quote for token {}", outcome.token_id);
                return Ok(MarketScanResult {
                    market_id: market.id.clone(),
                    market_question: market.question.clone(),
                    market_slug: market.slug.clone(),
                    opportunities: Vec::new(),
                    overheat: None,
                    book_age_ms: None,
                    skip_reason: Some(SkipReason::StaleBook),
                    best_candidate: None,
                    depth_samples: Vec::new(),
                    selected_outcomes: Some(selected.clone()),
                });
            }
            let age_ms = state.updated_at.elapsed().as_millis() as u64;
            max_age_ms = Some(max_age_ms.map_or(age_ms, |current| current.max(age_ms)));
            books.push(OutcomeBook {
                name: outcome.name.clone(),
                token_id: outcome.token_id.clone(),
                asks: state.book.asks.clone(),
                bids: state.book.bids.clone(),
            });
            continue;
        }

        debug!("missing cached book for token {}", outcome.token_id);
        return Ok(MarketScanResult {
            market_id: market.id.clone(),
            market_question: market.question.clone(),
            market_slug: market.slug.clone(),
            opportunities: Vec::new(),
            overheat: None,
            book_age_ms: None,
            skip_reason: Some(SkipReason::MissingBook),
            best_candidate: None,
            depth_samples: Vec::new(),
            selected_outcomes: Some(selected.clone()),
        });
    }

    let evaluation = evaluate_market(&market, &books, config);
    let skip_reason = if evaluation.opportunities.is_empty() {
        determine_skip_reason(config, evaluation.best_buy_candidate.as_ref())
    } else {
        None
    };
    Ok(MarketScanResult {
        market_id: market.id.clone(),
        market_question: market.question.clone(),
        market_slug: market.slug.clone(),
        opportunities: evaluation.opportunities,
        overheat: evaluate_overheat(&market, &books, config),
        book_age_ms: max_age_ms,
        skip_reason,
        best_candidate: evaluation.best_buy_candidate,
        depth_samples: evaluation.depth_samples,
        selected_outcomes: Some(selected),
    })
}

fn collect_token_ids(
    markets: &[polymarket::Market],
    config: &config::ArbitrageConfig,
    scan_id: u64,
) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut tokens = Vec::new();
    for market in markets {
        let selected = match select_binary_outcomes(market, config) {
            Some(outcomes) => outcomes,
            None => continue,
        };
        if selected.fallback {
            warn!(
                event = "binary_mapping_fallback",
                scan_id,
                market_id = %market.id,
                yes_name = %selected.yes.name,
                no_name = %selected.no.name,
                yes_token_id = %selected.yes.token_id,
                no_token_id = %selected.no.token_id
            );
        }
        for outcome in [selected.yes, selected.no] {
            if outcome.token_id.trim().is_empty() {
                continue;
            }
            if seen.insert(outcome.token_id.clone()) {
                tokens.push(outcome.token_id);
            }
        }
    }
    tokens
}

async fn process_market(
    client: &PolymarketClient,
    config: &config::ArbitrageConfig,
    market: polymarket::Market,
) -> Result<MarketScanResult> {
    let selected = match select_binary_outcomes(&market, config) {
        Some(outcomes) => outcomes,
        None => {
            return Ok(MarketScanResult {
                market_id: market.id.clone(),
                market_question: market.question.clone(),
                market_slug: market.slug.clone(),
                opportunities: Vec::new(),
                overheat: None,
                book_age_ms: None,
                skip_reason: Some(SkipReason::NotBinary),
                best_candidate: None,
                depth_samples: Vec::new(),
                selected_outcomes: None,
            })
        }
    };

    let mut books = Vec::with_capacity(2);
    for outcome in [selected.yes.clone(), selected.no.clone()] {
        if outcome.token_id.trim().is_empty() {
            debug!("missing token id for market {}", market.id);
            return Ok(MarketScanResult {
                market_id: market.id.clone(),
                market_question: market.question.clone(),
                market_slug: market.slug.clone(),
                opportunities: Vec::new(),
                overheat: None,
                book_age_ms: None,
                skip_reason: Some(SkipReason::MissingToken),
                best_candidate: None,
                depth_samples: Vec::new(),
                selected_outcomes: Some(selected.clone()),
            });
        }

        let book = client.fetch_orderbook(&outcome.token_id).await?;
        books.push(OutcomeBook {
            name: outcome.name.clone(),
            token_id: outcome.token_id,
            asks: book.asks,
            bids: book.bids,
        });
    }

    let evaluation = evaluate_market(&market, &books, config);
    let skip_reason = if evaluation.opportunities.is_empty() {
        determine_skip_reason(config, evaluation.best_buy_candidate.as_ref())
    } else {
        None
    };
    Ok(MarketScanResult {
        market_id: market.id.clone(),
        market_question: market.question.clone(),
        market_slug: market.slug.clone(),
        opportunities: evaluation.opportunities,
        overheat: evaluate_overheat(&market, &books, config),
        book_age_ms: None,
        skip_reason,
        best_candidate: evaluation.best_buy_candidate,
        depth_samples: evaluation.depth_samples,
        selected_outcomes: Some(selected),
    })
}

fn log_opportunity(
    opportunity: &arbitrage::ArbOpportunity,
    scan_id: u64,
    run_context: &RunContext,
    book_age_ms: Option<u64>,
) {
    let side = match opportunity.side {
        arbitrage::ArbSide::Buy => "buy",
        arbitrage::ArbSide::Sell => "sell",
    };
    let edge = match opportunity.side {
        arbitrage::ArbSide::Buy => 1.0 - opportunity.bundle_price_after_fees,
        arbitrage::ArbSide::Sell => opportunity.bundle_price_after_fees - 1.0,
    };
    let edge_bps = edge * 10_000.0;
    let legs_json = serde_json::to_string(&opportunity.legs).unwrap_or_default();
    let depth_samples_json = serde_json::to_string(&opportunity.depth_samples).unwrap_or_default();
    let unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let monotonic_ms = run_context.start.elapsed().as_millis() as u64;

    info!(
        event = "opportunity_detected",
        scan_id,
        config_hash = %run_context.config_hash,
        market_id = %opportunity.market_id,
        market_question = %opportunity.market_question,
        market_slug = opportunity.market_slug.as_deref().unwrap_or(""),
        side,
        size = opportunity.size,
        probe_size = opportunity.probe_size,
        probe_max_wait_ms = opportunity.probe_max_wait_ms,
        bundle_price = opportunity.bundle_price,
        bundle_price_after_fees = opportunity.bundle_price_after_fees,
        total_notional = opportunity.total_notional,
        total_after_fees = opportunity.total_after_fees,
        profit = opportunity.profit,
        margin = opportunity.margin,
        margin_bps = opportunity.margin_bps,
        edge,
        edge_bps,
        fee_bps = opportunity.fee_bps,
        buffer_bps = opportunity.buffer_bps,
        required_margin_bps = opportunity.required_margin_bps,
        book_age_ms = book_age_ms.unwrap_or(0),
        decision = "take",
        depth_samples = %depth_samples_json,
        legs = %legs_json,
        ts_unix_ms = unix_ms,
        monotonic_ms
    );

    info!(
        event = "paper_orders_submitted",
        scan_id,
        config_hash = %run_context.config_hash,
        side,
        size = opportunity.size,
        probe_size = opportunity.probe_size,
        legs = %legs_json,
        submit_latency_ms = 0u64,
        decision_latency_ms = 0u64,
        ts_unix_ms = unix_ms,
        monotonic_ms
    );

    let roi = if opportunity.total_notional > 0.0 {
        opportunity.profit / opportunity.total_notional
    } else {
        0.0
    };
    info!(
        event = "opp_closed",
        scan_id,
        config_hash = %run_context.config_hash,
        status = "paper_only",
        realized_pnl_usd = opportunity.profit,
        capital_locked_usd = opportunity.total_notional,
        lock_duration_s = 0u64,
        roi,
        ts_unix_ms = unix_ms,
        monotonic_ms
    );
}

fn log_overheat(
    signal: &OverheatSignal,
    scan_id: u64,
    run_context: &RunContext,
    book_age_ms: Option<u64>,
) {
    let legs_json = serde_json::to_string(&signal.legs).unwrap_or_default();
    let unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let monotonic_ms = run_context.start.elapsed().as_millis() as u64;

    info!(
        event = "overheat_detected",
        scan_id,
        config_hash = %run_context.config_hash,
        market_id = %signal.market_id,
        market_question = %signal.market_question,
        market_slug = signal.market_slug.as_deref().unwrap_or(""),
        bid_sum = signal.bid_sum,
        excess = signal.excess,
        min_size = signal.min_size,
        book_age_ms = book_age_ms.unwrap_or(0),
        legs = %legs_json,
        ts_unix_ms = unix_ms,
        monotonic_ms
    );
}

fn log_opportunity_skipped(
    result: &MarketScanResult,
    scan_id: u64,
    run_context: &RunContext,
    config: &Config,
) {
    let reason = result
        .skip_reason
        .map(|value| value.as_str())
        .unwrap_or("unknown");
    let best_candidate_json = serde_json::to_string(&result.best_candidate).unwrap_or_default();
    let depth_samples_json = serde_json::to_string(&result.depth_samples).unwrap_or_default();
    let unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let monotonic_ms = run_context.start.elapsed().as_millis() as u64;

    let (yes_name, no_name, yes_token, no_token, fallback) = result
        .selected_outcomes
        .as_ref()
        .map(|selected| {
            (
                selected.yes.name.as_str(),
                selected.no.name.as_str(),
                selected.yes.token_id.as_str(),
                selected.no.token_id.as_str(),
                selected.fallback,
            )
        })
        .unwrap_or(("", "", "", "", false));

    let required_margin_bps =
        (config.arbitrage.min_profit + config.arbitrage.buffer_bps / 10_000.0) * 10_000.0;

    info!(
        event = "opportunity_skipped",
        scan_id,
        config_hash = %run_context.config_hash,
        market_id = %result.market_id,
        market_question = %result.market_question,
        market_slug = result.market_slug.as_deref().unwrap_or(""),
        skip_reason = reason,
        yes_name,
        no_name,
        yes_token_id = yes_token,
        no_token_id = no_token,
        mapping_fallback = fallback,
        buffer_bps = config.arbitrage.buffer_bps,
        required_margin_bps = required_margin_bps,
        min_notional = config.arbitrage.min_notional,
        max_notional = config.arbitrage.max_notional,
        book_age_ms = result.book_age_ms.unwrap_or(0),
        best_candidate = %best_candidate_json,
        depth_samples = %depth_samples_json,
        decision = "skip",
        ts_unix_ms = unix_ms,
        monotonic_ms
    );
}

fn determine_skip_reason(
    config: &config::ArbitrageConfig,
    best_candidate: Option<&arbitrage::BestCandidate>,
) -> Option<SkipReason> {
    let required_margin_bps =
        (config.min_profit + config.buffer_bps / 10_000.0) * 10_000.0;
    match best_candidate {
        None => Some(SkipReason::NoDepth),
        Some(candidate) => {
            if candidate.edge_bps <= 0.0 {
                Some(SkipReason::NoEdge)
            } else if candidate.edge_bps < required_margin_bps {
                Some(SkipReason::BelowBuffer)
            } else if config.min_notional > 0.0 && candidate.total_notional < config.min_notional {
                Some(SkipReason::BelowMinNotional)
            } else if config.max_notional > 0.0 && candidate.total_notional > config.max_notional {
                Some(SkipReason::AboveMaxNotional)
            } else {
                Some(SkipReason::NoOpportunity)
            }
        }
    }
}

fn select_binary_outcomes(
    market: &polymarket::Market,
    config: &config::ArbitrageConfig,
) -> Option<SelectedOutcomes> {
    if market.outcomes.len() < config.min_outcomes || market.outcomes.len() > config.max_outcomes {
        return None;
    }
    if market.outcomes.len() != 2 {
        return None;
    }

    let mut yes_idx = None;
    let mut no_idx = None;
    for (idx, outcome) in market.outcomes.iter().enumerate() {
        let name = normalize_outcome_name(&outcome.name);
        if name == "yes" {
            yes_idx = Some(idx);
        } else if name == "no" {
            no_idx = Some(idx);
        }
    }

    if let (Some(yes), Some(no)) = (yes_idx, no_idx) {
        if yes != no {
            return Some(SelectedOutcomes {
                yes: market.outcomes[yes].clone(),
                no: market.outcomes[no].clone(),
                fallback: false,
            });
        }
    }

    Some(SelectedOutcomes {
        yes: market.outcomes[0].clone(),
        no: market.outcomes[1].clone(),
        fallback: true,
    })
}

fn normalize_outcome_name(name: &str) -> String {
    name.trim().to_lowercase()
}

mod arbitrage;
mod config;
mod market_selection;
mod polymarket;
mod ws_orderbook;

use crate::arbitrage::{evaluate_market, OutcomeBook};
use crate::config::Config;
use crate::market_selection::score_market;
use crate::polymarket::PolymarketClient;
use crate::ws_orderbook::WsOrderbookHandle;
use anyhow::{Context, Result};
use clap::Parser;
use futures::stream::{self, StreamExt};
use serde_json;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
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

static SCAN_COUNTER: AtomicU64 = AtomicU64::new(1);
static OPP_COUNTER: AtomicU64 = AtomicU64::new(1);
static SKIP_COUNTER: AtomicU64 = AtomicU64::new(1);

struct RunContext {
    config_hash: String,
    start: Instant,
}

struct MarketScanResult {
    market_id: String,
    market_question: String,
    market_slug: Option<String>,
    opportunities: Vec<arbitrage::ArbOpportunity>,
    book_age_ms: Option<u64>,
    skip_reason: Option<SkipReason>,
    best_candidate: Option<arbitrage::BestCandidate>,
    depth_samples: Vec<arbitrage::DepthSample>,
    selected_outcomes: Option<SelectedOutcomes>,
    updates_per_sec: Option<f64>,
    best_ask_flips_per_sec: Option<f64>,
    best_bid_flips_per_sec: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SkipReason {
    NotBinary,
    MissingToken,
    MissingBook,
    StaleBook,
    ChurnUpdates,
    ChurnBestAsk,
    ChurnBestBid,
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
            SkipReason::ChurnUpdates => "churn_updates",
            SkipReason::ChurnBestAsk => "churn_best_ask",
            SkipReason::ChurnBestBid => "churn_best_bid",
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

#[derive(Default)]
struct SkipCounters {
    not_binary: usize,
    missing_token: usize,
    missing_book: usize,
    stale_book: usize,
    churn_updates: usize,
    churn_best_ask: usize,
    churn_best_bid: usize,
    no_depth: usize,
    no_edge: usize,
    below_buffer: usize,
    below_min_notional: usize,
    above_max_notional: usize,
    no_opportunity: usize,
}

impl SkipCounters {
    fn increment(&mut self, reason: SkipReason) {
        match reason {
            SkipReason::NotBinary => self.not_binary += 1,
            SkipReason::MissingToken => self.missing_token += 1,
            SkipReason::MissingBook => self.missing_book += 1,
            SkipReason::StaleBook => self.stale_book += 1,
            SkipReason::ChurnUpdates => self.churn_updates += 1,
            SkipReason::ChurnBestAsk => self.churn_best_ask += 1,
            SkipReason::ChurnBestBid => self.churn_best_bid += 1,
            SkipReason::NoDepth => self.no_depth += 1,
            SkipReason::NoEdge => self.no_edge += 1,
            SkipReason::BelowBuffer => self.below_buffer += 1,
            SkipReason::BelowMinNotional => self.below_min_notional += 1,
            SkipReason::AboveMaxNotional => self.above_max_notional += 1,
            SkipReason::NoOpportunity => self.no_opportunity += 1,
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

    log_config_summary(&config, &run_context.config_hash);

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
    let mut last_subscribed_tokens: HashSet<String> = HashSet::new();
    loop {
        if let Err(err) = run_scan(
            &client,
            &config,
            &run_context,
            ws_handle.as_ref(),
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

    let mut opportunities = 0usize;
    let mut books_present = 0usize;
    let mut max_book_age_ms = 0u64;
    let mut skip_counts = SkipCounters::default();
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
                    skip_counts.increment(reason);
                    log_opportunity_skipped(&found, scan_id, run_context, config);
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
                    opportunities += 1;
                    log_opportunity(
                        &opportunity,
                        scan_id,
                        run_context,
                        found.book_age_ms,
                        found.updates_per_sec,
                        found.best_ask_flips_per_sec,
                        found.best_bid_flips_per_sec,
                    );
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
            skipped_not_binary = skip_counts.not_binary,
            skipped_missing_token = skip_counts.missing_token,
            skipped_missing_book = skip_counts.missing_book,
            skipped_stale_book = skip_counts.stale_book,
            skipped_no_depth = skip_counts.no_depth,
            skipped_no_edge = skip_counts.no_edge,
            skipped_below_buffer = skip_counts.below_buffer,
            skipped_below_min_notional = skip_counts.below_min_notional,
            skipped_above_max_notional = skip_counts.above_max_notional,
            skipped_churn_updates = skip_counts.churn_updates,
            skipped_churn_best_ask = skip_counts.churn_best_ask,
            skipped_churn_best_bid = skip_counts.churn_best_bid,
            skipped_no_opportunity = skip_counts.no_opportunity,
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
                book_age_ms: None,
                skip_reason: Some(SkipReason::NotBinary),
                best_candidate: None,
                depth_samples: Vec::new(),
                selected_outcomes: None,
                updates_per_sec: None,
                best_ask_flips_per_sec: None,
                best_bid_flips_per_sec: None,
            })
        }
    };
    let outcomes = vec![selected.yes.clone(), selected.no.clone()];

    let enforce_staleness = config.require_orderbook && max_quote_age.as_secs() > 0;
    let mut books = Vec::with_capacity(outcomes.len());
    let mut max_age_ms: Option<u64> = None;
    let mut updates_per_sec: Option<f64> = None;
    let mut best_ask_flips_per_sec: Option<f64> = None;
    let mut best_bid_flips_per_sec: Option<f64> = None;
    for outcome in &outcomes {
        if outcome.token_id.trim().is_empty() {
            debug!("missing token id for market {}", market.id);
            return Ok(MarketScanResult {
                market_id: market.id.clone(),
                market_question: market.question.clone(),
                market_slug: market.slug.clone(),
                opportunities: Vec::new(),
                book_age_ms: None,
                skip_reason: Some(SkipReason::MissingToken),
                best_candidate: None,
                depth_samples: Vec::new(),
                selected_outcomes: Some(selected.clone()),
                updates_per_sec: None,
                best_ask_flips_per_sec: None,
                best_bid_flips_per_sec: None,
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
                    book_age_ms: None,
                    skip_reason: Some(SkipReason::StaleBook),
                    best_candidate: None,
                    depth_samples: Vec::new(),
                    selected_outcomes: Some(selected.clone()),
                    updates_per_sec: None,
                    best_ask_flips_per_sec: None,
                    best_bid_flips_per_sec: None,
                });
            }
            let age_ms = state.updated_at.elapsed().as_millis() as u64;
            max_age_ms = Some(max_age_ms.map_or(age_ms, |current| current.max(age_ms)));
            let churn = churn_metrics_from_state(&state);
            updates_per_sec = Some(updates_per_sec.map_or(churn.updates_per_sec, |current| {
                current.max(churn.updates_per_sec)
            }));
            best_ask_flips_per_sec =
                Some(best_ask_flips_per_sec.map_or(churn.best_ask_flips_per_sec, |current| {
                    current.max(churn.best_ask_flips_per_sec)
                }));
            best_bid_flips_per_sec =
                Some(best_bid_flips_per_sec.map_or(churn.best_bid_flips_per_sec, |current| {
                    current.max(churn.best_bid_flips_per_sec)
                }));

            let max_levels = config.max_depth_levels;
            books.push(OutcomeBook {
                name: outcome.name.clone(),
                token_id: outcome.token_id.clone(),
                asks: truncate_levels(&state.book.asks, max_levels),
                bids: truncate_levels(&state.book.bids, max_levels),
            });
            continue;
        }

        debug!("missing cached book for token {}", outcome.token_id);
        return Ok(MarketScanResult {
            market_id: market.id.clone(),
            market_question: market.question.clone(),
            market_slug: market.slug.clone(),
            opportunities: Vec::new(),
            book_age_ms: None,
            skip_reason: Some(SkipReason::MissingBook),
            best_candidate: None,
            depth_samples: Vec::new(),
            selected_outcomes: Some(selected.clone()),
            updates_per_sec: None,
            best_ask_flips_per_sec: None,
            best_bid_flips_per_sec: None,
        });
    }

    if let Some(reason) = churn_skip_reason(config, updates_per_sec, best_ask_flips_per_sec, best_bid_flips_per_sec) {
        return Ok(MarketScanResult {
            market_id: market.id.clone(),
            market_question: market.question.clone(),
            market_slug: market.slug.clone(),
            opportunities: Vec::new(),
            book_age_ms: max_age_ms,
            skip_reason: Some(reason),
            best_candidate: None,
            depth_samples: Vec::new(),
            selected_outcomes: Some(selected),
            updates_per_sec,
            best_ask_flips_per_sec,
            best_bid_flips_per_sec,
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
        book_age_ms: max_age_ms,
        skip_reason,
        best_candidate: evaluation.best_buy_candidate,
        depth_samples: evaluation.depth_samples,
        selected_outcomes: Some(selected),
        updates_per_sec,
        best_ask_flips_per_sec,
        best_bid_flips_per_sec,
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
                book_age_ms: None,
                skip_reason: Some(SkipReason::NotBinary),
                best_candidate: None,
                depth_samples: Vec::new(),
                selected_outcomes: None,
                updates_per_sec: None,
                best_ask_flips_per_sec: None,
                best_bid_flips_per_sec: None,
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
                book_age_ms: None,
                skip_reason: Some(SkipReason::MissingToken),
                best_candidate: None,
                depth_samples: Vec::new(),
                selected_outcomes: Some(selected.clone()),
                updates_per_sec: None,
                best_ask_flips_per_sec: None,
                best_bid_flips_per_sec: None,
            });
        }

        let book = client.fetch_orderbook(&outcome.token_id).await?;
        let max_levels = config.max_depth_levels;
        books.push(OutcomeBook {
            name: outcome.name.clone(),
            token_id: outcome.token_id,
            asks: truncate_levels(&book.asks, max_levels),
            bids: truncate_levels(&book.bids, max_levels),
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
        book_age_ms: None,
        skip_reason,
        best_candidate: evaluation.best_buy_candidate,
        depth_samples: evaluation.depth_samples,
        selected_outcomes: Some(selected),
        updates_per_sec: None,
        best_ask_flips_per_sec: None,
        best_bid_flips_per_sec: None,
    })
}

fn log_opportunity(
    opportunity: &arbitrage::ArbOpportunity,
    scan_id: u64,
    run_context: &RunContext,
    book_age_ms: Option<u64>,
    updates_per_sec: Option<f64>,
    best_ask_flips_per_sec: Option<f64>,
    best_bid_flips_per_sec: Option<f64>,
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
        updates_per_sec = updates_per_sec.unwrap_or(0.0),
        best_ask_flips_per_sec = best_ask_flips_per_sec.unwrap_or(0.0),
        best_bid_flips_per_sec = best_bid_flips_per_sec.unwrap_or(0.0),
        decision = "take",
        ts_unix_ms = unix_ms,
        monotonic_ms
    );

    for (idx, leg) in opportunity.legs.iter().enumerate() {
        info!(
            event = "opportunity_leg",
            scan_id,
            market_id = %opportunity.market_id,
            side,
            leg_index = idx,
            name = %leg.name,
            token_id = %leg.token_id,
            avg_price = leg.avg_price,
            limit_price = leg.limit_price,
            unwind_price = leg.unwind_price,
            size = leg.size,
            levels_used = leg.levels_used,
            ts_unix_ms = unix_ms,
            monotonic_ms
        );
    }

    for sample in &opportunity.depth_samples {
        info!(
            event = "opportunity_depth_sample",
            scan_id,
            market_id = %opportunity.market_id,
            decision = "take",
            target_notional = sample.target_notional,
            size = sample.size,
            total_notional = sample.total_notional,
            bundle_price = sample.bundle_price,
            bundle_price_after_fees = sample.bundle_price_after_fees,
            edge_bps = sample.edge_bps,
            ts_unix_ms = unix_ms,
            monotonic_ms
        );
    }

    info!(
        event = "paper_orders_submitted",
        scan_id,
        config_hash = %run_context.config_hash,
        side,
        size = opportunity.size,
        probe_size = opportunity.probe_size,
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

    let (candidate_size, candidate_total_notional, candidate_edge_bps, candidate_margin_bps) =
        result
            .best_candidate
            .as_ref()
            .map(|candidate| {
                (
                    candidate.size,
                    candidate.total_notional,
                    candidate.edge_bps,
                    candidate.margin_bps,
                )
            })
            .unwrap_or((0.0, 0.0, 0.0, 0.0));
    let skip_id = SKIP_COUNTER.fetch_add(1, Ordering::Relaxed);

    info!(
        event = "opportunity_skipped",
        scan_id,
        skip_id,
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
        candidate_size,
        candidate_total_notional,
        candidate_edge_bps,
        candidate_margin_bps,
        book_age_ms = result.book_age_ms.unwrap_or(0),
        updates_per_sec = result.updates_per_sec.unwrap_or(0.0),
        best_ask_flips_per_sec = result.best_ask_flips_per_sec.unwrap_or(0.0),
        best_bid_flips_per_sec = result.best_bid_flips_per_sec.unwrap_or(0.0),
        decision = "skip",
        ts_unix_ms = unix_ms,
        monotonic_ms
    );

    for sample in &result.depth_samples {
        info!(
            event = "opportunity_depth_sample",
            scan_id,
            skip_id,
            market_id = %result.market_id,
            decision = "skip",
            skip_reason = reason,
            target_notional = sample.target_notional,
            size = sample.size,
            total_notional = sample.total_notional,
            bundle_price = sample.bundle_price,
            bundle_price_after_fees = sample.bundle_price_after_fees,
            edge_bps = sample.edge_bps,
            ts_unix_ms = unix_ms,
            monotonic_ms
        );
    }
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

fn log_config_summary(config: &Config, config_hash: &str) {
    let gamma_query = serde_json::to_string(&config.polymarket.gamma_query).unwrap_or_default();
    let target_notionals =
        serde_json::to_string(&config.arbitrage.target_notionals).unwrap_or_default();
    info!(
        event = "config_loaded",
        config_hash = %config_hash,
        ws_url = %config.polymarket.ws_url,
        market_category = %config.polymarket.market_category,
        market_limit = config.polymarket.market_limit,
        scan_interval_secs = config.polymarket.scan_interval_secs,
        max_quote_age_secs = config.polymarket.max_quote_age_secs,
        max_concurrent_orderbook_requests = config.polymarket.max_concurrent_orderbook_requests,
        gamma_query = %gamma_query,
        min_profit = config.arbitrage.min_profit,
        buffer_bps = config.arbitrage.buffer_bps,
        fee_bps = config.arbitrage.fee_bps,
        min_notional = config.arbitrage.min_notional,
        max_notional = config.arbitrage.max_notional,
        max_depth_levels = config.arbitrage.max_depth_levels,
        max_updates_per_sec = config.arbitrage.max_updates_per_sec,
        max_best_ask_flips_per_sec = config.arbitrage.max_best_ask_flips_per_sec,
        max_best_bid_flips_per_sec = config.arbitrage.max_best_bid_flips_per_sec,
        target_notionals = %target_notionals,
        log_opportunity_skipped = config.logging.log_opportunity_skipped,
        log_health = config.logging.log_health,
        health_log_every_scans = config.logging.health_log_every_scans
    );
}

struct BookChurn {
    updates_per_sec: f64,
    best_ask_flips_per_sec: f64,
    best_bid_flips_per_sec: f64,
}

fn churn_metrics_from_state(state: &ws_orderbook::BookState) -> BookChurn {
    let window_age = state
        .updated_at
        .duration_since(state.window_start)
        .as_secs_f64()
        .max(0.001);
    BookChurn {
        updates_per_sec: state.updates_in_window as f64 / window_age,
        best_ask_flips_per_sec: state.best_ask_flips as f64 / window_age,
        best_bid_flips_per_sec: state.best_bid_flips as f64 / window_age,
    }
}

fn churn_skip_reason(
    config: &config::ArbitrageConfig,
    updates_per_sec: Option<f64>,
    best_ask_flips_per_sec: Option<f64>,
    best_bid_flips_per_sec: Option<f64>,
) -> Option<SkipReason> {
    if let Some(rate) = updates_per_sec {
        if config.max_updates_per_sec > 0 && rate > config.max_updates_per_sec as f64 {
            return Some(SkipReason::ChurnUpdates);
        }
    }
    if let Some(rate) = best_ask_flips_per_sec {
        if config.max_best_ask_flips_per_sec > 0 && rate > config.max_best_ask_flips_per_sec as f64
        {
            return Some(SkipReason::ChurnBestAsk);
        }
    }
    if let Some(rate) = best_bid_flips_per_sec {
        if config.max_best_bid_flips_per_sec > 0 && rate > config.max_best_bid_flips_per_sec as f64
        {
            return Some(SkipReason::ChurnBestBid);
        }
    }
    None
}

fn truncate_levels(levels: &[polymarket::PriceLevel], max_levels: usize) -> Vec<polymarket::PriceLevel> {
    if max_levels == 0 || levels.len() <= max_levels {
        return levels.to_vec();
    }
    levels[..max_levels].to_vec()
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

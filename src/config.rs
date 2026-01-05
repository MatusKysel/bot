use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub polymarket: PolymarketConfig,
    #[serde(default)]
    pub arbitrage: ArbitrageConfig,
    #[serde(default)]
    pub selection: MarketSelectionConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PolymarketConfig {
    #[serde(default = "default_gamma_base_url")]
    pub gamma_base_url: String,
    #[serde(default = "default_gamma_markets_path")]
    pub gamma_markets_path: String,
    #[serde(default)]
    pub gamma_query: HashMap<String, String>,
    #[serde(default)]
    pub gamma_end_date_max_hours: Option<u64>,
    #[serde(default)]
    pub gamma_end_date_min_hours: Option<u64>,
    #[serde(default = "default_clob_base_url")]
    pub clob_base_url: String,
    #[serde(default = "default_orderbook_path")]
    pub orderbook_path: String,
    #[serde(default = "default_orderbook_token_param")]
    pub orderbook_token_param: String,
    #[serde(default = "default_use_websocket")]
    pub use_websocket: bool,
    #[serde(default = "default_ws_url")]
    pub ws_url: String,
    #[serde(default = "default_ws_message_type")]
    pub ws_message_type: String,
    #[serde(default = "default_ws_asset_ids_field")]
    pub ws_asset_ids_field: String,
    #[serde(default = "default_ws_operation_field")]
    pub ws_operation_field: String,
    #[serde(default = "default_ws_subscribe_operation")]
    pub ws_subscribe_operation: String,
    #[serde(default = "default_ws_unsubscribe_operation")]
    pub ws_unsubscribe_operation: String,
    #[serde(default = "default_ws_use_operation_on_connect")]
    pub ws_use_operation_on_connect: bool,
    #[serde(default)]
    pub ws_subscribe_extra: HashMap<String, String>,
    #[serde(default = "default_ws_subscribe_batch_size")]
    pub ws_subscribe_batch_size: usize,
    #[serde(default = "default_ws_subscribe_delay_ms")]
    pub ws_subscribe_delay_ms: u64,
    #[serde(default = "default_ws_reconnect_delay_secs")]
    pub ws_reconnect_delay_secs: u64,
    #[serde(default = "default_ws_reconnect_backoff_min_ms")]
    pub ws_reconnect_backoff_min_ms: u64,
    #[serde(default = "default_ws_reconnect_backoff_max_ms")]
    pub ws_reconnect_backoff_max_ms: u64,
    #[serde(default = "default_ws_reconnect_jitter_ms")]
    pub ws_reconnect_jitter_ms: u64,
    #[serde(default = "default_ws_ping_interval_secs")]
    pub ws_ping_interval_secs: u64,
    #[serde(default = "default_ws_update_queue_capacity")]
    pub ws_update_queue_capacity: usize,
    #[serde(default = "default_max_quote_age_secs")]
    pub max_quote_age_secs: u64,
    #[serde(default = "default_rest_snapshot_on_missing")]
    pub rest_snapshot_on_missing: bool,
    #[serde(default = "default_rest_snapshot_min_interval_secs")]
    pub rest_snapshot_min_interval_secs: u64,
    #[serde(default = "default_market_category")]
    pub market_category: String,
    #[serde(default)]
    pub market_category_keywords: Vec<String>,
    #[serde(default)]
    pub market_subcategory: Option<String>,
    #[serde(default = "default_market_limit")]
    pub market_limit: usize,
    #[serde(default = "default_skip_restricted")]
    pub skip_restricted: bool,
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
    #[serde(default = "default_scan_interval_secs")]
    pub scan_interval_secs: u64,
    #[serde(default = "default_max_concurrency")]
    pub max_concurrent_orderbook_requests: usize,
    #[serde(default = "default_user_agent")]
    pub user_agent: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ArbitrageConfig {
    #[serde(default = "default_min_profit")]
    pub min_profit: f64,
    #[serde(default = "default_buffer_bps")]
    pub buffer_bps: f64,
    #[serde(default = "default_fee_bps")]
    pub fee_bps: f64,
    #[serde(default = "default_min_outcomes")]
    pub min_outcomes: usize,
    #[serde(default = "default_max_outcomes")]
    pub max_outcomes: usize,
    #[serde(default = "default_min_price")]
    pub min_price: f64,
    #[serde(default = "default_max_price")]
    pub max_price: f64,
    #[serde(default = "default_min_size")]
    pub min_size: f64,
    #[serde(default = "default_max_depth_levels")]
    pub max_depth_levels: usize,
    #[serde(default = "default_max_updates_per_sec")]
    pub max_updates_per_sec: u64,
    #[serde(default = "default_max_best_ask_flips_per_sec")]
    pub max_best_ask_flips_per_sec: u64,
    #[serde(default = "default_max_best_bid_flips_per_sec")]
    pub max_best_bid_flips_per_sec: u64,
    #[serde(default = "default_target_notionals")]
    pub target_notionals: Vec<f64>,
    #[serde(default = "default_min_notional")]
    pub min_notional: f64,
    #[serde(default = "default_max_notional")]
    pub max_notional: f64,
    #[serde(default = "default_require_orderbook")]
    pub require_orderbook: bool,
    #[serde(default = "default_probe_fraction")]
    pub probe_fraction: f64,
    #[serde(default = "default_probe_min_size")]
    pub probe_min_size: f64,
    #[serde(default = "default_probe_max_wait_ms")]
    pub probe_max_wait_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarketSelectionConfig {
    #[serde(default = "default_selection_min_score")]
    pub min_score: f64,
    #[serde(default = "default_selection_min_volume")]
    pub min_volume: f64,
    #[serde(default = "default_selection_min_volume_24h")]
    pub min_volume_24h: f64,
    #[serde(default = "default_selection_min_liquidity")]
    pub min_liquidity: f64,
    #[serde(default = "default_selection_min_traders")]
    pub min_traders: usize,
    #[serde(default = "default_selection_consensus_threshold")]
    pub consensus_threshold: f64,
    #[serde(default = "default_selection_bonus_volume")]
    pub bonus_volume: f64,
    #[serde(default = "default_selection_bonus_volume_24h")]
    pub bonus_volume_24h: f64,
    #[serde(default = "default_selection_bonus_liquidity")]
    pub bonus_liquidity: f64,
    #[serde(default = "default_selection_bonus_retail")]
    pub bonus_retail: f64,
    #[serde(default = "default_selection_bonus_boring")]
    pub bonus_boring: f64,
    #[serde(default = "default_selection_bonus_consensus")]
    pub bonus_consensus: f64,
    #[serde(default = "default_selection_penalty_breaking")]
    pub penalty_breaking: f64,
    #[serde(default = "default_selection_penalty_legal")]
    pub penalty_legal: f64,
    #[serde(default = "default_selection_penalty_election")]
    pub penalty_election: f64,
    #[serde(default = "default_selection_penalty_niche")]
    pub penalty_niche: f64,
    #[serde(default)]
    pub boring_keywords: Vec<String>,
    #[serde(default)]
    pub breaking_keywords: Vec<String>,
    #[serde(default)]
    pub legal_keywords: Vec<String>,
    #[serde(default)]
    pub election_keywords: Vec<String>,
    #[serde(default)]
    pub niche_keywords: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default)]
    pub file_path: Option<String>,
    #[serde(default = "default_log_opportunity_skipped")]
    pub log_opportunity_skipped: bool,
    #[serde(default = "default_log_health")]
    pub log_health: bool,
    #[serde(default = "default_health_log_every_scans")]
    pub health_log_every_scans: u64,
}

impl Config {
    pub fn from_path_with_hash(path: &Path) -> Result<(Self, String)> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("read config file {}", path.display()))?;
        let config: Config = toml::from_str(&contents)
            .with_context(|| format!("parse config file {}", path.display()))?;
        Ok((config, contents))
    }

    pub fn validate(&self) -> Result<()> {
        if self.polymarket.ws_reconnect_backoff_min_ms == 0
            || self.polymarket.ws_reconnect_backoff_max_ms == 0
        {
            return Err(anyhow!("ws reconnect backoff must be > 0"));
        }
        if self.polymarket.ws_reconnect_backoff_min_ms
            > self.polymarket.ws_reconnect_backoff_max_ms
        {
            return Err(anyhow!(
                "ws reconnect backoff min > max ({} > {})",
                self.polymarket.ws_reconnect_backoff_min_ms,
                self.polymarket.ws_reconnect_backoff_max_ms
            ));
        }
        if self.polymarket.ws_update_queue_capacity == 0 {
            return Err(anyhow!("ws update queue capacity must be > 0"));
        }
        if self.polymarket.max_quote_age_secs == 0 && self.polymarket.use_websocket {
            return Err(anyhow!("max_quote_age_secs must be > 0 when websocket is enabled"));
        }
        if self.polymarket.rest_snapshot_min_interval_secs == 0 {
            return Err(anyhow!("rest_snapshot_min_interval_secs must be > 0"));
        }
        if self.arbitrage.min_outcomes == 0 {
            return Err(anyhow!("min_outcomes must be >= 1"));
        }
        if self.arbitrage.min_outcomes > self.arbitrage.max_outcomes {
            return Err(anyhow!(
                "min_outcomes {} > max_outcomes {}",
                self.arbitrage.min_outcomes,
                self.arbitrage.max_outcomes
            ));
        }
        if self.arbitrage.min_price >= self.arbitrage.max_price {
            return Err(anyhow!(
                "min_price {} >= max_price {}",
                self.arbitrage.min_price,
                self.arbitrage.max_price
            ));
        }
        if self.arbitrage.buffer_bps < 0.0 {
            return Err(anyhow!("buffer_bps must be >= 0"));
        }
        if self.arbitrage.min_notional < 0.0 {
            return Err(anyhow!("min_notional must be >= 0"));
        }
        if self.arbitrage.max_notional < 0.0 {
            return Err(anyhow!("max_notional must be >= 0"));
        }
        if self.arbitrage.max_notional > 0.0
            && self.arbitrage.min_notional > self.arbitrage.max_notional
        {
            return Err(anyhow!(
                "min_notional {} > max_notional {}",
                self.arbitrage.min_notional,
                self.arbitrage.max_notional
            ));
        }
        if self.logging.health_log_every_scans == 0 {
            return Err(anyhow!("health_log_every_scans must be >= 1"));
        }
        if !self.logging.log_opportunity_skipped {
            return Err(anyhow!(
                "log_opportunity_skipped must be true to satisfy required logging"
            ));
        }
        Ok(())
    }
}

impl Default for PolymarketConfig {
    fn default() -> Self {
        Self {
            gamma_base_url: default_gamma_base_url(),
            gamma_markets_path: default_gamma_markets_path(),
            gamma_query: HashMap::new(),
            gamma_end_date_max_hours: None,
            gamma_end_date_min_hours: None,
            clob_base_url: default_clob_base_url(),
            orderbook_path: default_orderbook_path(),
            orderbook_token_param: default_orderbook_token_param(),
            use_websocket: default_use_websocket(),
            ws_url: default_ws_url(),
            ws_message_type: default_ws_message_type(),
            ws_asset_ids_field: default_ws_asset_ids_field(),
            ws_operation_field: default_ws_operation_field(),
            ws_subscribe_operation: default_ws_subscribe_operation(),
            ws_unsubscribe_operation: default_ws_unsubscribe_operation(),
            ws_use_operation_on_connect: default_ws_use_operation_on_connect(),
            ws_subscribe_extra: HashMap::new(),
            ws_subscribe_batch_size: default_ws_subscribe_batch_size(),
            ws_subscribe_delay_ms: default_ws_subscribe_delay_ms(),
            ws_reconnect_delay_secs: default_ws_reconnect_delay_secs(),
            ws_reconnect_backoff_min_ms: default_ws_reconnect_backoff_min_ms(),
            ws_reconnect_backoff_max_ms: default_ws_reconnect_backoff_max_ms(),
            ws_reconnect_jitter_ms: default_ws_reconnect_jitter_ms(),
            ws_ping_interval_secs: default_ws_ping_interval_secs(),
            ws_update_queue_capacity: default_ws_update_queue_capacity(),
            max_quote_age_secs: default_max_quote_age_secs(),
            rest_snapshot_on_missing: default_rest_snapshot_on_missing(),
            rest_snapshot_min_interval_secs: default_rest_snapshot_min_interval_secs(),
            market_category: default_market_category(),
            market_category_keywords: Vec::new(),
            market_subcategory: None,
            market_limit: default_market_limit(),
            skip_restricted: default_skip_restricted(),
            request_timeout_secs: default_request_timeout_secs(),
            scan_interval_secs: default_scan_interval_secs(),
            max_concurrent_orderbook_requests: default_max_concurrency(),
            user_agent: default_user_agent(),
        }
    }
}

impl Default for ArbitrageConfig {
    fn default() -> Self {
        Self {
            min_profit: default_min_profit(),
            buffer_bps: default_buffer_bps(),
            fee_bps: default_fee_bps(),
            min_outcomes: default_min_outcomes(),
            max_outcomes: default_max_outcomes(),
            min_price: default_min_price(),
            max_price: default_max_price(),
            min_size: default_min_size(),
            max_depth_levels: default_max_depth_levels(),
            max_updates_per_sec: default_max_updates_per_sec(),
            max_best_ask_flips_per_sec: default_max_best_ask_flips_per_sec(),
            max_best_bid_flips_per_sec: default_max_best_bid_flips_per_sec(),
            target_notionals: default_target_notionals(),
            min_notional: default_min_notional(),
            max_notional: default_max_notional(),
            require_orderbook: default_require_orderbook(),
            probe_fraction: default_probe_fraction(),
            probe_min_size: default_probe_min_size(),
            probe_max_wait_ms: default_probe_max_wait_ms(),
        }
    }
}

impl Default for MarketSelectionConfig {
    fn default() -> Self {
        Self {
            min_score: default_selection_min_score(),
            min_volume: default_selection_min_volume(),
            min_volume_24h: default_selection_min_volume_24h(),
            min_liquidity: default_selection_min_liquidity(),
            min_traders: default_selection_min_traders(),
            consensus_threshold: default_selection_consensus_threshold(),
            bonus_volume: default_selection_bonus_volume(),
            bonus_volume_24h: default_selection_bonus_volume_24h(),
            bonus_liquidity: default_selection_bonus_liquidity(),
            bonus_retail: default_selection_bonus_retail(),
            bonus_boring: default_selection_bonus_boring(),
            bonus_consensus: default_selection_bonus_consensus(),
            penalty_breaking: default_selection_penalty_breaking(),
            penalty_legal: default_selection_penalty_legal(),
            penalty_election: default_selection_penalty_election(),
            penalty_niche: default_selection_penalty_niche(),
            boring_keywords: Vec::new(),
            breaking_keywords: Vec::new(),
            legal_keywords: Vec::new(),
            election_keywords: Vec::new(),
            niche_keywords: Vec::new(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            file_path: None,
            log_opportunity_skipped: default_log_opportunity_skipped(),
            log_health: default_log_health(),
            health_log_every_scans: default_health_log_every_scans(),
        }
    }
}

fn default_gamma_base_url() -> String {
    "https://gamma-api.polymarket.com".to_string()
}

fn default_gamma_markets_path() -> String {
    "/markets".to_string()
}

fn default_clob_base_url() -> String {
    "https://clob.polymarket.com".to_string()
}

fn default_orderbook_path() -> String {
    "/book".to_string()
}

fn default_orderbook_token_param() -> String {
    "token_id".to_string()
}

fn default_use_websocket() -> bool {
    true
}

fn default_ws_url() -> String {
    "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string()
}

fn default_ws_message_type() -> String {
    "MARKET".to_string()
}

fn default_ws_asset_ids_field() -> String {
    "asset_ids".to_string()
}

fn default_ws_operation_field() -> String {
    "operation".to_string()
}

fn default_ws_subscribe_operation() -> String {
    "subscribe".to_string()
}

fn default_ws_unsubscribe_operation() -> String {
    "unsubscribe".to_string()
}

fn default_ws_use_operation_on_connect() -> bool {
    false
}

fn default_ws_subscribe_batch_size() -> usize {
    25
}

fn default_ws_subscribe_delay_ms() -> u64 {
    100
}

fn default_ws_reconnect_delay_secs() -> u64 {
    5
}

fn default_ws_reconnect_backoff_min_ms() -> u64 {
    500
}

fn default_ws_reconnect_backoff_max_ms() -> u64 {
    30_000
}

fn default_ws_reconnect_jitter_ms() -> u64 {
    250
}

fn default_ws_ping_interval_secs() -> u64 {
    20
}

fn default_ws_update_queue_capacity() -> usize {
    2048
}

fn default_max_quote_age_secs() -> u64 {
    5
}

fn default_rest_snapshot_on_missing() -> bool {
    true
}

fn default_rest_snapshot_min_interval_secs() -> u64 {
    30
}

fn default_market_category() -> String {
    "Crypto".to_string()
}

fn default_market_limit() -> usize {
    50
}

fn default_skip_restricted() -> bool {
    false
}

fn default_request_timeout_secs() -> u64 {
    20
}

fn default_scan_interval_secs() -> u64 {
    2
}

fn default_max_concurrency() -> usize {
    8
}

fn default_user_agent() -> String {
    "polymarket-arb-bot/0.1".to_string()
}

fn default_min_profit() -> f64 {
    0.0
}

fn default_buffer_bps() -> f64 {
    8.0
}

fn default_fee_bps() -> f64 {
    0.0
}

fn default_min_outcomes() -> usize {
    2
}

fn default_max_outcomes() -> usize {
    2
}

fn default_min_price() -> f64 {
    0.01
}

fn default_max_price() -> f64 {
    0.99
}

fn default_min_size() -> f64 {
    0.0
}

fn default_max_depth_levels() -> usize {
    50
}

fn default_max_updates_per_sec() -> u64 {
    0
}

fn default_max_best_ask_flips_per_sec() -> u64 {
    0
}

fn default_max_best_bid_flips_per_sec() -> u64 {
    0
}

fn default_target_notionals() -> Vec<f64> {
    vec![5.0, 25.0, 100.0]
}

fn default_min_notional() -> f64 {
    5.0
}

fn default_max_notional() -> f64 {
    250.0
}

fn default_require_orderbook() -> bool {
    true
}

fn default_probe_fraction() -> f64 {
    0.1
}

fn default_probe_min_size() -> f64 {
    5.0
}

fn default_probe_max_wait_ms() -> u64 {
    1500
}

fn default_selection_min_score() -> f64 {
    0.0
}

fn default_selection_min_volume() -> f64 {
    50_000.0
}

fn default_selection_min_volume_24h() -> f64 {
    5_000.0
}

fn default_selection_min_liquidity() -> f64 {
    10_000.0
}

fn default_selection_min_traders() -> usize {
    100
}

fn default_selection_consensus_threshold() -> f64 {
    0.1
}

fn default_selection_bonus_volume() -> f64 {
    1.0
}

fn default_selection_bonus_volume_24h() -> f64 {
    0.6
}

fn default_selection_bonus_liquidity() -> f64 {
    0.8
}

fn default_selection_bonus_retail() -> f64 {
    0.7
}

fn default_selection_bonus_boring() -> f64 {
    0.6
}

fn default_selection_bonus_consensus() -> f64 {
    0.8
}

fn default_selection_penalty_breaking() -> f64 {
    1.6
}

fn default_selection_penalty_legal() -> f64 {
    1.2
}

fn default_selection_penalty_election() -> f64 {
    1.2
}

fn default_selection_penalty_niche() -> f64 {
    0.8
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_opportunity_skipped() -> bool {
    true
}

fn default_log_health() -> bool {
    true
}

fn default_health_log_every_scans() -> u64 {
    5
}

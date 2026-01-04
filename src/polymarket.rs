use crate::config::PolymarketConfig;
use anyhow::{Context, Result};
use reqwest::Url;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

#[derive(Debug, Clone)]
pub struct PolymarketClient {
    gamma_base_url: String,
    gamma_markets_path: String,
    gamma_query: HashMap<String, String>,
    gamma_end_date_max_hours: Option<u64>,
    gamma_end_date_min_hours: Option<u64>,
    clob_base_url: String,
    orderbook_path: String,
    orderbook_token_param: String,
    client: reqwest::Client,
}

#[derive(Debug, Clone)]
pub struct Market {
    pub id: String,
    pub question: String,
    pub slug: Option<String>,
    pub category: Option<String>,
    pub subcategory: Option<String>,
    pub outcomes: Vec<Outcome>,
    pub outcome_prices: Vec<f64>,
    pub volume: Option<f64>,
    pub volume_24h: Option<f64>,
    pub liquidity: Option<f64>,
    pub num_traders: Option<u64>,
    pub num_trades: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct Outcome {
    pub name: String,
    pub token_id: String,
}

#[derive(Debug, Clone)]
pub struct PriceLevel {
    pub price: f64,
    pub size: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub struct Orderbook {
    pub asks: Vec<PriceLevel>,
    pub bids: Vec<PriceLevel>,
}

#[derive(Debug, Clone)]
pub struct OrderbookUpdate {
    pub asks: Option<Vec<PriceLevel>>,
    pub bids: Option<Vec<PriceLevel>>,
}

impl PolymarketClient {
    pub fn new(config: &PolymarketConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.request_timeout_secs))
            .user_agent(config.user_agent.clone())
            .build()
            .context("build http client")?;

        Ok(Self {
            gamma_base_url: config.gamma_base_url.clone(),
            gamma_markets_path: config.gamma_markets_path.clone(),
            gamma_query: config.gamma_query.clone(),
            gamma_end_date_max_hours: config.gamma_end_date_max_hours,
            gamma_end_date_min_hours: config.gamma_end_date_min_hours,
            clob_base_url: config.clob_base_url.clone(),
            orderbook_path: config.orderbook_path.clone(),
            orderbook_token_param: config.orderbook_token_param.clone(),
            client,
        })
    }

    pub async fn fetch_markets(&self, limit: usize) -> Result<Vec<Market>> {
        let mut url = Url::parse(&self.gamma_base_url)
            .context("parse gamma base url")?
            .join(&self.gamma_markets_path)
            .context("build gamma markets url")?;

        {
            let mut pairs = url.query_pairs_mut();
            for (key, value) in &self.gamma_query {
                pairs.append_pair(key, value);
            }
            if !self.gamma_query.contains_key("limit") {
                pairs.append_pair("limit", &limit.to_string());
            }
            if !self.gamma_query.contains_key("end_date_max") {
                if let Some(hours) = self.gamma_end_date_max_hours {
                    if let Some(value) = format_end_date(hours as i64) {
                        pairs.append_pair("end_date_max", &value);
                    }
                }
            }
            if !self.gamma_query.contains_key("end_date_min") {
                if let Some(hours) = self.gamma_end_date_min_hours {
                    if let Some(value) = format_end_date(hours as i64) {
                        pairs.append_pair("end_date_min", &value);
                    }
                }
            }
        }

        let response = self
            .client
            .get(url)
            .send()
            .await
            .context("request gamma markets")?
            .error_for_status()
            .context("gamma markets returned error")?;

        let payload: Value = response.json().await.context("parse gamma json")?;
        let items = extract_market_items(&payload);

        let mut markets = Vec::with_capacity(items.len());
        for item in items {
            if let Some(market) = Market::from_value(&item) {
                markets.push(market);
            }
        }

        Ok(markets)
    }

    pub async fn fetch_orderbook(&self, token_id: &str) -> Result<Orderbook> {
        let mut url = Url::parse(&self.clob_base_url)
            .context("parse clob base url")?
            .join(&self.orderbook_path)
            .context("build orderbook url")?;

        url.query_pairs_mut()
            .append_pair(&self.orderbook_token_param, token_id);

        let response = self
            .client
            .get(url)
            .send()
            .await
            .with_context(|| format!("request orderbook for token {token_id}"))?
            .error_for_status()
            .with_context(|| format!("orderbook returned error for token {token_id}"))?;

        let payload: Value = response
            .json()
            .await
            .with_context(|| format!("parse orderbook json for token {token_id}"))?;

        extract_orderbook(&payload)
            .with_context(|| format!("orderbook missing bids/asks for token {token_id}"))
    }
}

impl Market {
    pub fn from_value(value: &Value) -> Option<Self> {
        let id = first_string(value, &["id", "market_id", "marketId", "conditionId"])?;
        let question = first_string(value, &["question", "title", "name"]).unwrap_or_else(|| id.clone());
        let slug = first_string(value, &["slug", "market_slug", "marketSlug"]);
        let category = first_string(value, &["category", "primaryCategory"]);
        let subcategory = first_string(value, &["subCategory", "subcategory"]);

        let outcomes = extract_outcomes(value);
        let outcome_prices = extract_outcome_prices(value);
        let volume = first_number(
            value,
            &[
                "volume",
                "volumeUsd",
                "volume_usd",
                "volumeUSD",
                "totalVolume",
                "total_volume",
            ],
        );
        let volume_24h = first_number(
            value,
            &[
                "volume24hr",
                "volume_24hr",
                "volume24h",
                "volume_24h",
                "volume24hrClob",
                "volume_24hr_clob",
                "volume24hrUsd",
                "volume_24hr_usd",
            ],
        );
        let liquidity = first_number(
            value,
            &[
                "liquidity",
                "liquidityUsd",
                "liquidity_usd",
                "liquidityNum",
                "liquidity_num",
            ],
        );
        let num_traders = first_u64(
            value,
            &[
                "numTraders",
                "num_traders",
                "uniqueTraders",
                "unique_traders",
                "traderCount",
                "trader_count",
            ],
        );
        let num_trades = first_u64(
            value,
            &[
                "numTrades",
                "num_trades",
                "trades",
                "tradeCount",
                "trade_count",
            ],
        );

        Some(Self {
            id,
            question,
            slug,
            category,
            subcategory,
            outcomes,
            outcome_prices,
            volume,
            volume_24h,
            liquidity,
            num_traders,
            num_trades,
        })
    }

    pub fn matches_category(&self, category: &str, subcategory: Option<&str>) -> bool {
        let category = category.trim();
        if category.is_empty() {
            return true;
        }
        let category_match = match self.category.as_deref() {
            Some(value) => contains_ignore_case(value, category),
            None => true,
        };

        let subcategory_match = match (subcategory, &self.subcategory) {
            (None, _) => true,
            (Some(expected), Some(actual)) => contains_ignore_case(actual, expected),
            (Some(_), None) => false,
        };

        category_match && subcategory_match
    }
}

fn extract_market_items(value: &Value) -> Vec<Value> {
    if let Some(items) = value.as_array() {
        return items.clone();
    }

    if let Some(obj) = value.as_object() {
        for key in ["data", "markets", "results", "items"] {
            if let Some(arr) = obj.get(key).and_then(Value::as_array) {
                return arr.clone();
            }
        }
    }

    Vec::new()
}

fn extract_outcomes(value: &Value) -> Vec<Outcome> {
    if let (Some(names), Some(tokens)) = (
        extract_string_array(value, &["outcomes", "outcomeNames", "outcome_names"]),
        extract_string_array(value, &["clobTokenIds", "clob_token_ids", "outcomeTokenIds"]),
    ) {
        let mut outcomes = Vec::new();
        for (idx, name) in names.into_iter().enumerate() {
            if let Some(token_id) = tokens.get(idx) {
                outcomes.push(Outcome {
                    name,
                    token_id: token_id.clone(),
                });
            }
        }
        if !outcomes.is_empty() {
            return outcomes;
        }
    }

    if let Some(items) = extract_array(value, &["tokens", "outcomeTokens", "outcome_tokens"]) {
        let mut outcomes = Vec::new();
        for item in items {
            if let Some(token_id) = extract_token_id(item) {
                let name = extract_outcome_name(item).unwrap_or_else(|| token_id.clone());
                outcomes.push(Outcome { name, token_id });
            }
        }
        if !outcomes.is_empty() {
            return outcomes;
        }
    }

    if let Some(items) = extract_array(value, &["outcomes"]) {
        let mut outcomes = Vec::new();
        for item in items {
            if let Some(token_id) = extract_token_id(item) {
                let name = extract_outcome_name(item).unwrap_or_else(|| token_id.clone());
                outcomes.push(Outcome { name, token_id });
            }
        }
        if !outcomes.is_empty() {
            return outcomes;
        }
    }

    Vec::new()
}

fn extract_outcome_prices(value: &Value) -> Vec<f64> {
    if let Some(prices) = extract_number_array(value, &["outcomePrices", "outcome_prices"])
    {
        return prices;
    }

    Vec::new()
}

pub(crate) fn extract_orderbook_update(value: &Value) -> Option<OrderbookUpdate> {
    let asks = extract_levels(
        value,
        &[
            "asks",
            "data.asks",
            "data.orderbook.asks",
            "data.book.asks",
            "orderbook.asks",
            "book.asks",
            "payload.asks",
        ],
    );
    let bids = extract_levels(
        value,
        &[
            "bids",
            "data.bids",
            "data.orderbook.bids",
            "data.book.bids",
            "orderbook.bids",
            "book.bids",
            "payload.bids",
        ],
    );

    if asks.is_none() && bids.is_none() {
        return None;
    }

    Some(OrderbookUpdate { asks, bids })
}

pub(crate) fn extract_ws_asset_id(value: &Value) -> Option<String> {
    first_string(
        value,
        &[
            "asset_id",
            "assetId",
            "clobTokenId",
            "clob_token_id",
            "data.asset_id",
            "data.assetId",
            "data.clobTokenId",
            "data.clob_token_id",
        ],
    )
}

pub(crate) fn extract_ws_message_type(value: &Value) -> Option<String> {
    first_string(
        value,
        &[
            "type",
            "data.type",
            "event_type",
            "data.event_type",
            "channel",
            "data.channel",
        ],
    )
}

pub(crate) fn extract_ws_seq(value: &Value) -> Option<u64> {
    first_u64(
        value,
        &[
            "seq",
            "sequence",
            "sequence_id",
            "sequenceId",
            "sequenceNumber",
            "seqNum",
            "data.seq",
            "data.sequence",
            "data.sequence_id",
            "data.sequenceId",
        ],
    )
}

pub(crate) fn extract_ws_prev_seq(value: &Value) -> Option<u64> {
    first_u64(
        value,
        &[
            "prev_seq",
            "prevSeq",
            "previous_sequence",
            "previousSequence",
            "data.prev_seq",
            "data.prevSeq",
            "data.previous_sequence",
            "data.previousSequence",
        ],
    )
}

pub(crate) fn extract_orderbook(value: &Value) -> Option<Orderbook> {
    let update = extract_orderbook_update(value)?;
    let asks = update.asks.unwrap_or_default();
    let bids = update.bids.unwrap_or_default();
    if asks.is_empty() && bids.is_empty() {
        return None;
    }
    Some(Orderbook { asks, bids })
}

fn extract_levels(value: &Value, keys: &[&str]) -> Option<Vec<PriceLevel>> {
    let items = find_array(value, keys)?;
    let mut levels = Vec::with_capacity(items.len());
    for item in items {
        if let Some(level) = parse_level(item) {
            levels.push(level);
        }
    }
    Some(levels)
}

fn parse_level(value: &Value) -> Option<PriceLevel> {
    match value {
        Value::Array(values) => {
            if values.len() < 2 {
                return None;
            }
            let price = parse_number(&values[0])?;
            let size = parse_number(&values[1]);
            Some(PriceLevel { price, size })
        }
        Value::Object(map) => {
            let price = map
                .get("price")
                .or_else(|| map.get("p"))
                .and_then(parse_number)?;
            let size = map
                .get("size")
                .or_else(|| map.get("s"))
                .or_else(|| map.get("quantity"))
                .and_then(parse_number);
            Some(PriceLevel { price, size })
        }
        _ => None,
    }
}

fn extract_array<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a Vec<Value>> {
    for key in keys {
        if let Some(found) = find_value(value, key).and_then(Value::as_array) {
            return Some(found);
        }
    }
    None
}

fn extract_string_array(value: &Value, keys: &[&str]) -> Option<Vec<String>> {
    let items = extract_array(value, keys)?;
    let mut results = Vec::with_capacity(items.len());
    for item in items {
        if let Some(text) = item.as_str() {
            results.push(text.to_string());
        } else if let Some(number) = item.as_i64() {
            results.push(number.to_string());
        } else if let Some(number) = item.as_u64() {
            results.push(number.to_string());
        }
    }
    if results.is_empty() {
        None
    } else {
        Some(results)
    }
}

fn extract_number_array(value: &Value, keys: &[&str]) -> Option<Vec<f64>> {
    let items = extract_array(value, keys)?;
    let mut results = Vec::with_capacity(items.len());
    for item in items {
        if let Some(number) = parse_number(item) {
            results.push(number);
        }
    }
    if results.is_empty() {
        None
    } else {
        Some(results)
    }
}

pub(crate) fn extract_token_id(value: &Value) -> Option<String> {
    if let Some(text) = first_string(
        value,
        &[
            "token_id",
            "tokenId",
            "clobTokenId",
            "clob_token_id",
            "data.token_id",
            "data.tokenId",
            "data.clobTokenId",
            "data.clob_token_id",
            "asset_id",
            "assetId",
            "data.asset_id",
            "data.assetId",
            "market",
            "marketId",
            "market_id",
            "data.market",
            "data.marketId",
            "data.market_id",
            "token",
            "id",
        ],
    ) {
        return Some(text);
    }

    None
}

fn extract_outcome_name(value: &Value) -> Option<String> {
    first_string(value, &["outcome", "name", "label", "title"])
}

fn first_string(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(text) = find_value(value, key).and_then(Value::as_str) {
            return Some(text.to_string());
        }
        if let Some(number) = find_value(value, key).and_then(Value::as_i64) {
            return Some(number.to_string());
        }
        if let Some(number) = find_value(value, key).and_then(Value::as_u64) {
            return Some(number.to_string());
        }
    }
    None
}

fn first_number(value: &Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        if let Some(number) = find_value(value, key).and_then(parse_number) {
            return Some(number);
        }
    }
    None
}

fn first_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    for key in keys {
        if let Some(found) = find_value(value, key) {
            if let Some(number) = found.as_u64() {
                return Some(number);
            }
            if let Some(number) = found.as_i64() {
                if number >= 0 {
                    return Some(number as u64);
                }
            }
            if let Some(text) = found.as_str() {
                if let Ok(number) = text.parse::<u64>() {
                    return Some(number);
                }
            }
        }
    }
    None
}

fn parse_number(value: &Value) -> Option<f64> {
    if let Some(number) = value.as_f64() {
        return Some(number);
    }
    if let Some(text) = value.as_str() {
        return text.parse::<f64>().ok();
    }
    None
}

fn find_array<'a>(value: &'a Value, paths: &[&str]) -> Option<&'a Vec<Value>> {
    for path in paths {
        if let Some(found) = find_value(value, path).and_then(Value::as_array) {
            return Some(found);
        }
    }
    None
}

fn find_value<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for key in path.split('.') {
        if let Value::Object(map) = current {
            current = map.get(key)?;
        } else {
            return None;
        }
    }
    Some(current)
}

fn contains_ignore_case(haystack: &str, needle: &str) -> bool {
    haystack.to_lowercase().contains(&needle.to_lowercase())
}

fn format_end_date(hours: i64) -> Option<String> {
    if hours == 0 {
        return None;
    }
    let now = OffsetDateTime::now_utc();
    let target = now + time::Duration::hours(hours);
    target.format(&Rfc3339).ok()
}

use crate::config::PolymarketConfig;
use crate::polymarket::{
    extract_orderbook_update, extract_ws_asset_id, extract_ws_message_type, extract_ws_prev_seq,
    extract_ws_seq, Orderbook, OrderbookUpdate, PolymarketClient, PriceLevel,
};
use anyhow::{Context, Result};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use rand::Rng;
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub struct BookState {
    pub book: Orderbook,
    pub updated_at: Instant,
    pub last_seq: Option<u64>,
    pub has_snapshot: bool,
}

pub type OrderbookCache = Arc<DashMap<String, BookState>>;

pub struct WsOrderbookHandle {
    cache: OrderbookCache,
    cmd_tx: mpsc::Sender<WsCommand>,
    subscribed: Arc<Mutex<HashSet<String>>>,
}

enum WsCommand {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
}

impl WsOrderbookHandle {
    pub fn cache(&self) -> OrderbookCache {
        self.cache.clone()
    }

    pub async fn subscribe_tokens(&self, tokens: Vec<String>) -> Result<()> {
        let mut new_tokens = Vec::new();
        {
            let mut guard = self.subscribed.lock().await;
            for token in tokens {
                if guard.insert(token.clone()) {
                    new_tokens.push(token);
                }
            }
        }

        if new_tokens.is_empty() {
            return Ok(());
        }

        self.cmd_tx
            .send(WsCommand::Subscribe(new_tokens))
            .await
            .context("send ws subscribe")?;
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn unsubscribe_tokens(&self, tokens: Vec<String>) -> Result<()> {
        let mut removed = Vec::new();
        {
            let mut guard = self.subscribed.lock().await;
            for token in tokens {
                if guard.remove(&token) {
                    removed.push(token);
                }
            }
        }

        if removed.is_empty() {
            return Ok(());
        }

        self.cmd_tx
            .send(WsCommand::Unsubscribe(removed))
            .await
            .context("send ws unsubscribe")?;
        Ok(())
    }

    pub async fn evict_tokens(&self, tokens: &[String]) {
        for token in tokens {
            self.cache.remove(token);
        }
    }
}

pub fn spawn_ws_orderbook(config: &PolymarketConfig) -> Result<WsOrderbookHandle> {
    let (cmd_tx, cmd_rx) = mpsc::channel(512);
    let cache = Arc::new(DashMap::new());
    let subscribed = Arc::new(Mutex::new(HashSet::new()));

    let task_config = config.clone();
    let task_cache = cache.clone();
    let task_subscribed = subscribed.clone();
    tokio::spawn(async move {
        ws_task(task_config, task_cache, task_subscribed, cmd_rx).await;
    });

    Ok(WsOrderbookHandle {
        cache,
        cmd_tx,
        subscribed,
    })
}

async fn ws_task(
    config: PolymarketConfig,
    cache: OrderbookCache,
    subscribed: Arc<Mutex<HashSet<String>>>,
    mut cmd_rx: mpsc::Receiver<WsCommand>,
) {
    let mut backoff = Backoff::new(
        config.ws_reconnect_backoff_min_ms,
        config.ws_reconnect_backoff_max_ms,
        config.ws_reconnect_jitter_ms,
        config.ws_reconnect_delay_secs,
    );
    loop {
        match tokio_tungstenite::connect_async(&config.ws_url).await {
            Ok((ws_stream, _)) => {
                backoff.reset();
                info!(event = "ws_connected", ws_url = %config.ws_url);
                let (mut write, mut read) = ws_stream.split();

                if let Err(err) = subscribe_existing(&config, &mut write, &subscribed).await {
                    warn!("ws subscribe existing failed: {:#}", err);
                }

                let (update_tx, update_rx) =
                    mpsc::channel::<String>(config.ws_update_queue_capacity.max(1));
                let update_config = config.clone();
                let update_cache = cache.clone();
                let update_handle =
                    tokio::spawn(async move { process_updates(update_config, update_cache, update_rx).await });

                let mut ping_interval = tokio::time::interval(Duration::from_secs(
                    config.ws_ping_interval_secs.max(1),
                ));
                let mut dropped_updates: u64 = 0;

                'connection: loop {
                    tokio::select! {
                        maybe_cmd = cmd_rx.recv() => {
                            match maybe_cmd {
                                Some(WsCommand::Subscribe(tokens)) => {
                                    if let Err(err) = send_asset_updates(&config, &mut write, tokens, Some(&config.ws_subscribe_operation)).await {
                                        warn!("ws subscribe failed: {:#}", err);
                                    }
                                }
                                Some(WsCommand::Unsubscribe(tokens)) => {
                                    if let Err(err) = send_asset_updates(&config, &mut write, tokens, Some(&config.ws_unsubscribe_operation)).await {
                                        warn!("ws unsubscribe failed: {:#}", err);
                                    }
                                }
                                None => {
                                    info!("ws command channel closed");
                                    return;
                                }
                            }
                        }
                        maybe_msg = read.next() => {
                            match maybe_msg {
                                Some(Ok(message)) => {
                                    match message {
                                        Message::Text(text) => {
                                            if update_tx.try_send(text).is_err() {
                                                dropped_updates += 1;
                                                if dropped_updates % 100 == 1 {
                                                    warn!(event = "ws_update_dropped", dropped_updates);
                                                }
                                            }
                                        }
                                        Message::Binary(bytes) => {
                                            if let Ok(text) = String::from_utf8(bytes) {
                                                if update_tx.try_send(text).is_err() {
                                                    dropped_updates += 1;
                                                    if dropped_updates % 100 == 1 {
                                                        warn!(event = "ws_update_dropped", dropped_updates);
                                                    }
                                                }
                                            }
                                        }
                                        Message::Ping(payload) => {
                                            if let Err(err) = write.send(Message::Pong(payload)).await {
                                                warn!("ws pong failed: {:#}", err);
                                                break 'connection;
                                            }
                                        }
                                        Message::Pong(_) => {}
                                        Message::Close(_) => {
                                            warn!(event = "ws_closed_by_server");
                                            break 'connection;
                                        }
                                        _ => {}
                                    }
                                }
                                Some(Err(err)) => {
                                    warn!(event = "ws_read_error", error = %err);
                                    break 'connection;
                                }
                                None => {
                                    warn!(event = "ws_closed_by_server");
                                    break 'connection;
                                }
                            }
                        }
                        _ = ping_interval.tick(), if config.ws_ping_interval_secs > 0 => {
                            if let Err(err) = write.send(Message::Ping(Vec::new())).await {
                                warn!(event = "ws_ping_failed", error = %err);
                                break 'connection;
                            }
                        }
                    }
                }

                drop(update_tx);
                let _ = update_handle.await;
            }
            Err(err) => {
                warn!(event = "ws_connect_failed", error = %err);
            }
        }
        let delay = backoff.next_delay();
        warn!(event = "ws_reconnect_wait", sleep_ms = delay.as_millis() as u64);
        tokio::time::sleep(delay).await;
    }
}

async fn subscribe_existing<S>(
    config: &PolymarketConfig,
    write: &mut S,
    subscribed: &Arc<Mutex<HashSet<String>>>,
) -> Result<()>
where
    S: futures::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let tokens: Vec<String> = {
        let guard = subscribed.lock().await;
        guard.iter().cloned().collect()
    };

    if tokens.is_empty() {
        return Ok(());
    }

    let operation = if config.ws_use_operation_on_connect {
        Some(config.ws_subscribe_operation.as_str())
    } else {
        None
    };
    send_asset_updates(config, write, tokens, operation).await
}

async fn send_asset_updates<S>(
    config: &PolymarketConfig,
    write: &mut S,
    tokens: Vec<String>,
    operation: Option<&str>,
) -> Result<()>
where
    S: futures::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let batch_size = config.ws_subscribe_batch_size.max(1);
    let delay = Duration::from_millis(config.ws_subscribe_delay_ms);

    for chunk in tokens.as_slice().chunks(batch_size) {
        let message = build_asset_message(config, chunk, operation)?;
        write
            .send(message)
            .await
            .map_err(|err| anyhow::anyhow!("send ws message: {err}"))?;
        if config.ws_subscribe_delay_ms > 0 {
            tokio::time::sleep(delay).await;
        }
    }

    Ok(())
}

fn build_asset_message(
    config: &PolymarketConfig,
    tokens: &[String],
    operation: Option<&str>,
) -> Result<Message> {
    let mut payload = serde_json::Map::new();
    payload.insert(
        "type".to_string(),
        Value::String(config.ws_message_type.clone()),
    );
    if let Some(operation) = operation {
        payload.insert(
            config.ws_operation_field.clone(),
            Value::String(operation.to_string()),
        );
    }
    payload.insert(
        config.ws_asset_ids_field.clone(),
        Value::Array(tokens.iter().cloned().map(Value::String).collect()),
    );

    for (key, value) in &config.ws_subscribe_extra {
        payload.insert(key.clone(), Value::String(value.clone()));
    }

    let text = Value::Object(payload).to_string();
    Ok(Message::Text(text))
}

async fn process_updates(
    config: PolymarketConfig,
    cache: OrderbookCache,
    mut update_rx: mpsc::Receiver<String>,
) {
    let client = match PolymarketClient::new(&config) {
        Ok(client) => client,
        Err(err) => {
            warn!(event = "ws_resync_init_failed", error = %err);
            return;
        }
    };
    let inflight = Arc::new(Mutex::new(HashSet::new()));
    let semaphore = Arc::new(Semaphore::new(
        config.max_concurrent_orderbook_requests.max(1),
    ));

    while let Some(text) = update_rx.recv().await {
        if let Err(err) =
            handle_ws_text(&config, &cache, &client, &inflight, &semaphore, &text).await
        {
            debug!(event = "ws_update_parse_error", error = %err);
        }
    }
}

async fn handle_ws_text(
    config: &PolymarketConfig,
    cache: &OrderbookCache,
    client: &PolymarketClient,
    inflight: &Arc<Mutex<HashSet<String>>>,
    semaphore: &Arc<Semaphore>,
    text: &str,
) -> Result<()> {
    let value: Value = serde_json::from_str(text).context("parse ws json")?;
    let root_type = extract_ws_message_type(&value);
    if let Value::Object(map) = &value {
        if let Some(items) = map.get("data").and_then(Value::as_array) {
            for item in items {
                handle_ws_value(
                    config,
                    cache,
                    client,
                    inflight,
                    semaphore,
                    item,
                    root_type.as_deref(),
                )
                .await;
            }
            return Ok(());
        }
        if let Some(items) = map.get("payload").and_then(Value::as_array) {
            for item in items {
                handle_ws_value(
                    config,
                    cache,
                    client,
                    inflight,
                    semaphore,
                    item,
                    root_type.as_deref(),
                )
                .await;
            }
            return Ok(());
        }
    }

    match &value {
        Value::Array(items) => {
            for item in items {
                handle_ws_value(
                    config,
                    cache,
                    client,
                    inflight,
                    semaphore,
                    item,
                    root_type.as_deref(),
                )
                .await;
            }
        }
        _ => {
            handle_ws_value(
                config,
                cache,
                client,
                inflight,
                semaphore,
                &value,
                root_type.as_deref(),
            )
            .await;
        }
    }
    Ok(())
}

async fn handle_ws_value(
    config: &PolymarketConfig,
    cache: &OrderbookCache,
    client: &PolymarketClient,
    inflight: &Arc<Mutex<HashSet<String>>>,
    semaphore: &Arc<Semaphore>,
    value: &Value,
    root_type: Option<&str>,
) {
    let event = match parse_ws_event(value, root_type, &config.ws_message_type) {
        Some(event) => event,
        None => return,
    };

    let mut resync_reason = None;
    let now = Instant::now();
    {
        let mut entry = cache.entry(event.token_id.clone()).or_insert_with(|| BookState {
            book: Orderbook::default(),
            updated_at: now,
            last_seq: None,
            has_snapshot: false,
        });

        let apply_snapshot = event.is_snapshot && event.update.asks.is_some() && event.update.bids.is_some();
        if apply_snapshot {
            entry.book.asks = normalize_levels(event.update.asks.unwrap_or_default(), true);
            entry.book.bids = normalize_levels(event.update.bids.unwrap_or_default(), false);
            entry.updated_at = now;
            entry.last_seq = event.seq;
            entry.has_snapshot = true;
        } else {
            if !entry.has_snapshot {
                resync_reason = Some("delta_without_snapshot");
            } else if event.seq.is_none() {
                resync_reason = Some("delta_without_seq");
            } else if let Some(seq) = event.seq {
                if let Some(last_seq) = entry.last_seq {
                    if seq <= last_seq {
                        return;
                    }
                    if seq != last_seq + 1 {
                        resync_reason = Some("seq_gap");
                    }
                }
            }

            if resync_reason.is_none() {
                if let Some(asks) = event.update.asks {
                    apply_delta_levels(&mut entry.book.asks, &asks, true);
                }
                if let Some(bids) = event.update.bids {
                    apply_delta_levels(&mut entry.book.bids, &bids, false);
                }
                entry.updated_at = now;
                entry.last_seq = event.seq.or(entry.last_seq);
            } else {
                entry.has_snapshot = false;
            }
        }
    }

    if let Some(reason) = resync_reason {
        schedule_resync(
            cache.clone(),
            client.clone(),
            inflight.clone(),
            semaphore.clone(),
            event.token_id,
            reason,
        )
        .await;
    }
}

async fn schedule_resync(
    cache: OrderbookCache,
    client: PolymarketClient,
    inflight: Arc<Mutex<HashSet<String>>>,
    semaphore: Arc<Semaphore>,
    token_id: String,
    reason: &'static str,
) {
    {
        let mut guard = inflight.lock().await;
        if !guard.insert(token_id.clone()) {
            return;
        }
    }
    info!(event = "ws_resync_start", token_id = %token_id, reason);

    let permit = match semaphore.acquire_owned().await {
        Ok(permit) => permit,
        Err(_) => {
            warn!(event = "ws_resync_aborted", token_id = %token_id);
            let mut guard = inflight.lock().await;
            guard.remove(&token_id);
            return;
        }
    };

    tokio::spawn(async move {
        let _permit = permit;
        let start = Instant::now();
        match client.fetch_orderbook(&token_id).await {
            Ok(mut book) => {
                book.asks = normalize_levels(book.asks, true);
                book.bids = normalize_levels(book.bids, false);
                cache.insert(
                    token_id.clone(),
                    BookState {
                        book,
                        updated_at: Instant::now(),
                        last_seq: None,
                        has_snapshot: true,
                    },
                );
                info!(
                    event = "ws_resync_done",
                    token_id = %token_id,
                    elapsed_ms = start.elapsed().as_millis() as u64
                );
            }
            Err(err) => {
                warn!(event = "ws_resync_failed", token_id = %token_id, error = %err);
            }
        }

        let mut guard = inflight.lock().await;
        guard.remove(&token_id);
    });
}

struct WsBookEvent {
    token_id: String,
    update: OrderbookUpdate,
    seq: Option<u64>,
    is_snapshot: bool,
}

fn parse_ws_event(
    value: &Value,
    root_type: Option<&str>,
    expected_type: &str,
) -> Option<WsBookEvent> {
    let msg_type = extract_ws_message_type(value)
        .or_else(|| root_type.map(|value| value.to_string()))?;
    if !msg_type.eq_ignore_ascii_case(expected_type) {
        return None;
    }

    let token_id = extract_ws_asset_id(value)?;
    let update = extract_orderbook_update(value)?;
    let seq = extract_ws_seq(value);
    let prev_seq = extract_ws_prev_seq(value);

    let mut is_snapshot = infer_snapshot(value, &update);
    let mut is_delta = infer_delta(value, &update, prev_seq.is_some());
    if !is_snapshot && !is_delta {
        if update.asks.is_some() && update.bids.is_some() {
            is_snapshot = true;
        } else {
            is_delta = true;
        }
    }

    if is_delta && !is_snapshot && update.asks.is_none() && update.bids.is_none() {
        return None;
    }

    Some(WsBookEvent {
        token_id,
        update,
        seq,
        is_snapshot,
    })
}

fn infer_snapshot(value: &Value, update: &OrderbookUpdate) -> bool {
    if let Some(flag) = find_bool(value, &["snapshot", "is_snapshot", "data.snapshot"]) {
        return flag;
    }
    if let Some(kind) = find_string(value, &["event", "event_type", "action", "message_type"]) {
        let kind = kind.to_lowercase();
        if kind.contains("snapshot") || kind.contains("book") {
            return true;
        }
    }
    update.asks.is_some() && update.bids.is_some()
}

fn infer_delta(value: &Value, update: &OrderbookUpdate, has_prev_seq: bool) -> bool {
    if has_prev_seq {
        return true;
    }
    if let Some(kind) = find_string(value, &["event", "event_type", "action", "message_type"]) {
        let kind = kind.to_lowercase();
        if kind.contains("update") || kind.contains("delta") || kind.contains("l2") {
            return true;
        }
    }
    update.asks.is_some() ^ update.bids.is_some()
}

fn normalize_levels(levels: Vec<PriceLevel>, is_ask: bool) -> Vec<PriceLevel> {
    let mut filtered: Vec<PriceLevel> = levels
        .into_iter()
        .filter_map(|level| {
            let size = level.size?;
            if !level.price.is_finite() || !size.is_finite() || size <= 0.0 {
                return None;
            }
            Some(PriceLevel {
                price: level.price,
                size: Some(size),
            })
        })
        .collect();

    filtered.sort_by(|a, b| {
        cmp_price(a.price, b.price, is_ask)
    });
    filtered
}

fn apply_delta_levels(levels: &mut Vec<PriceLevel>, updates: &[PriceLevel], is_ask: bool) {
    for update in updates {
        let price = update.price;
        let size = match update.size {
            Some(size) => size,
            None => continue,
        };
        if !price.is_finite() || !size.is_finite() {
            continue;
        }

        let remove = size <= 0.0;
        match levels.binary_search_by(|level| cmp_price(level.price, price, is_ask)) {
            Ok(idx) => {
                if remove {
                    levels.remove(idx);
                } else {
                    levels[idx].size = Some(size);
                }
            }
            Err(idx) => {
                if !remove {
                    levels.insert(
                        idx,
                        PriceLevel {
                            price,
                            size: Some(size),
                        },
                    );
                }
            }
        }
    }
}

fn cmp_price(a: f64, b: f64, is_ask: bool) -> std::cmp::Ordering {
    if is_ask {
        a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
    } else {
        b.partial_cmp(&a).unwrap_or(std::cmp::Ordering::Equal)
    }
}

fn find_bool(value: &Value, keys: &[&str]) -> Option<bool> {
    for key in keys {
        if let Some(found) = find_value(value, key).and_then(Value::as_bool) {
            return Some(found);
        }
    }
    None
}

fn find_string(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(found) = find_value(value, key).and_then(Value::as_str) {
            return Some(found.to_string());
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

struct Backoff {
    min_ms: u64,
    max_ms: u64,
    jitter_ms: u64,
    current_ms: u64,
    fallback_secs: u64,
}

impl Backoff {
    fn new(min_ms: u64, max_ms: u64, jitter_ms: u64, fallback_secs: u64) -> Self {
        Self {
            min_ms: min_ms.max(1),
            max_ms: max_ms.max(min_ms.max(1)),
            jitter_ms,
            current_ms: min_ms.max(1),
            fallback_secs,
        }
    }

    fn reset(&mut self) {
        self.current_ms = self.min_ms;
    }

    fn next_delay(&mut self) -> Duration {
        if self.min_ms == 0 || self.max_ms == 0 {
            return Duration::from_secs(self.fallback_secs.max(1));
        }
        let mut delay = self.current_ms;
        if delay > self.max_ms {
            delay = self.max_ms;
        }
        self.current_ms = (self.current_ms * 2).min(self.max_ms);
        let jitter = if self.jitter_ms > 0 {
            rand::thread_rng().gen_range(0..=self.jitter_ms)
        } else {
            0
        };
        Duration::from_millis(delay.saturating_add(jitter))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_delta_updates_levels() {
        let mut levels = normalize_levels(
            vec![
                PriceLevel {
                    price: 0.30,
                    size: Some(10.0),
                },
                PriceLevel {
                    price: 0.40,
                    size: Some(5.0),
                },
            ],
            true,
        );

        let updates = vec![
            PriceLevel {
                price: 0.35,
                size: Some(2.0),
            },
            PriceLevel {
                price: 0.30,
                size: Some(0.0),
            },
        ];
        apply_delta_levels(&mut levels, &updates, true);
        assert_eq!(levels.len(), 2);
        assert!((levels[0].price - 0.35).abs() < 1e-9);
        assert!((levels[1].price - 0.40).abs() < 1e-9);
    }
}

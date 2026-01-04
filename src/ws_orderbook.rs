use crate::config::PolymarketConfig;
use crate::polymarket::{extract_orderbook_update, extract_token_id, Orderbook};
use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub struct BookState {
    pub book: Orderbook,
    pub updated_at: Instant,
}

pub type OrderbookCache = Arc<RwLock<HashMap<String, BookState>>>;

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
}

pub fn spawn_ws_orderbook(config: &PolymarketConfig) -> Result<WsOrderbookHandle> {
    let (cmd_tx, cmd_rx) = mpsc::channel(512);
    let cache = Arc::new(RwLock::new(HashMap::new()));
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
    let reconnect_delay = Duration::from_secs(config.ws_reconnect_delay_secs.max(1));
    loop {
        match tokio_tungstenite::connect_async(&config.ws_url).await {
            Ok((ws_stream, _)) => {
                info!("ws connected: {}", config.ws_url);
                let (mut write, mut read) = ws_stream.split();

                if let Err(err) = subscribe_existing(&config, &mut write, &subscribed).await {
                    warn!("ws subscribe existing failed: {:#}", err);
                }

                let mut ping_interval = tokio::time::interval(Duration::from_secs(
                    config.ws_ping_interval_secs.max(1),
                ));

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
                                            if let Err(err) = update_cache_from_text(&cache, &text).await {
                                                debug!("ws json parse error: {:#}", err);
                                            }
                                        }
                                        Message::Binary(bytes) => {
                                            if let Ok(text) = String::from_utf8(bytes) {
                                                if let Err(err) = update_cache_from_text(&cache, &text).await {
                                                    debug!("ws json parse error: {:#}", err);
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
                                            warn!("ws closed by server");
                                            break 'connection;
                                        }
                                        _ => {}
                                    }
                                }
                                Some(Err(err)) => {
                                    warn!("ws read error: {:#}", err);
                                    break 'connection;
                                }
                                None => {
                                    warn!("ws closed by server");
                                    break 'connection;
                                }
                            }
                        }
                        _ = ping_interval.tick(), if config.ws_ping_interval_secs > 0 => {
                            if let Err(err) = write.send(Message::Ping(Vec::new())).await {
                                warn!("ws ping failed: {:#}", err);
                                break 'connection;
                            }
                        }
                    }
                }
            }
            Err(err) => {
                warn!("ws connect failed: {:#}", err);
            }
        }

        tokio::time::sleep(reconnect_delay).await;
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

async fn update_cache_from_text(cache: &OrderbookCache, text: &str) -> Result<()> {
    let value: Value = serde_json::from_str(text).context("parse ws json")?;
    match value {
        Value::Array(items) => {
            for item in items {
                update_cache_from_value(cache, &item).await;
            }
        }
        _ => update_cache_from_value(cache, &value).await,
    }

    Ok(())
}

async fn update_cache_from_value(cache: &OrderbookCache, value: &Value) {
    let token_id = extract_token_id(value);
    let update = extract_orderbook_update(value);
    if let (Some(token_id), Some(update)) = (token_id, update) {
        let mut guard = cache.write().await;
        let entry = guard.entry(token_id).or_insert_with(|| BookState {
            book: Orderbook::default(),
            updated_at: Instant::now(),
        });
        if let Some(asks) = update.asks {
            entry.book.asks = asks;
        }
        if let Some(bids) = update.bids {
            entry.book.bids = bids;
        }
        entry.updated_at = Instant::now();
    }
}

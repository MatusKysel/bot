# Polymarket Crypto Arbitrage Bot (Rust)

This bot scans Polymarket's Crypto markets for theoretical arbitrage opportunities by walking orderbook depth on both sides and optimizing bundle size. It only logs opportunities and theoretical executions; no trading or order submission is performed.

## What it does
- Pulls markets from the Polymarket Gamma API.
- Filters to the Crypto category (and optional subcategory).
- Subscribes to the Polymarket CLOB WebSocket for live orderbook updates.
- Walks bids/asks to compute the effective bundle price curve for buys and sells.
- Logs opportunities when the optimized bundle size maximizes total profit.
- Logs a two-phase execution plan (probe then scale) with marketable limits.
- Tracks overheated markets where the sum of best bids across outcomes exceeds 1.0.
- Applies market selection scoring before orderbook fetches.

## Quick start
1) Copy `config.example.toml` to `config.toml` and edit as needed.
2) Run:

```bash
cargo run -- --config config.toml
```

## Notes
- If the WS or API response format changes, adjust parsing in `src/polymarket.rs`.
- If the WS subscribe schema differs, tweak `polymarket.ws_*` fields in `config.toml`.
- WS subscriptions use `type=MARKET` with `asset_ids=[YES, NO]`.
- Default WS URL is `wss://ws-subscriptions-clob.polymarket.com/ws/market`.
- `polymarket.ws_use_operation_on_connect` controls whether the initial subscribe includes `operation=subscribe`.
- `arbitrage.min_profit` is absolute (0.005 = 0.5% edge per $1 payout) and is evaluated after fees.
- `arbitrage.fee_bps` is applied per leg (approximation).
- `arbitrage.min_size` is the minimum bundle size evaluated.
- Two-phase plan uses `arbitrage.probe_fraction`, `arbitrage.probe_min_size`, and `arbitrage.probe_max_wait_ms`.
- Market selection uses volume/liquidity/trader counts and keyword heuristics; tune `[selection]` in `config.toml`.
- Use `[polymarket.gamma_query]` to prefilter markets (closed/liquidity/volume/order).
- Keyword matching is case-insensitive substring.
- Set `selection.min_score > 0` to filter to the top-scoring markets.
- Set selection thresholds to 0 to disable that signal.
- Near-consensus scoring uses Gamma outcome prices when available (best with binary markets).
- The scanner maximizes total profit across size tiers, not just the best margin.
- Markets without depth (price + size) on all outcomes are skipped.
- `polymarket.max_quote_age_secs` controls staleness for cached WS books.
- `polymarket.gamma_end_date_max_hours` adds `end_date_max` to Gamma queries when set.
- BUY uses asks; SELL uses bids.
- Outcome price snapshots are ignored; depth is required for sizing.
- Overheat->Arb logs include the time since the last overheated signal.
- Overheated signals use top-of-book bids; adjust `arbitrage.max_price` if you expect bids near 1.0.
- Logs include per-leg marketable limit prices (worst price used) to reduce race risk.
- Logs include immediate unwind limits (sell at best bid for BUY, buy at best ask for SELL).
- Set `polymarket.scan_interval_secs = 0` to run a single scan.
- Set `polymarket.use_websocket = false` to fall back to REST orderbook fetches (still uses depth).
- This version logs only; trading is intentionally omitted.

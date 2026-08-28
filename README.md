# @solncebro/market-data-feeder

[![GitHub repo](https://img.shields.io/badge/github-solncebro%2Fmarket--data--feeder-blue?logo=github)](https://github.com/solncebro/market-data-feeder)

On-demand market-data feeder. One exchange connection per process fans klines, moving averages and 24h volume out to multiple strategy apps over a local WebSocket channel — so each strategy stops opening its own ~1200-symbol kline subscriptions.

The package is the **server only** (private, never published). The client (`MarketDataClient`),
the embedded source (`createEmbeddedMarketDataSource`), the wire protocol and the collection core
live in the published library `@solncebro/market-data-feeder-lib` (sibling repo
`../market-data-feeder-lib`), which this server consumes as a regular dependency — see its README
for how apps connect.

> **`isStale()` latency depends on the event set.** A subscription that includes `klineUpdated`/`klineUpdatedTick` receives data within seconds, so `isStale()` flips ~45s after data stops. A **closed-only** subscription legitimately gets one message per interval, so its threshold is `interval + 45s` (30m → ~31 min, 4h → ~4h1m) and the self-heal reconnect fires at twice that. If your app uses `isStale()` as a tight trade gate on a closed-only channel, add `klineUpdated` to the event set (or pass a lower `staleThresholdMs`).

## Run the feeder

```bash
cp .env.bybit.example .env   # or .env.binance.example — set the API key + Telegram bot token
yarn install
yarn dev                     # or: yarn build && yarn start
```

One feeder process per exchange (mirrors the existing per-exchange deploy model).

## Connect from an app

`MarketDataClient` (per-interval WebSocket client) and `createEmbeddedMarketDataSource`
(in-process core for a single-consumer machine) are imported from
`@solncebro/market-data-feeder-lib` — usage examples live in that package's README.

## Env

| Var | Required | Default | Purpose |
|---|---|---|---|
| `EXCHANGE_NAME` | yes | — | `binance` or `bybit` |
| `EXCHANGE_API_KEY` / `EXCHANGE_SECRET` | yes | — | read-only key (public data only) |
| `MARKET_DATA_FEEDER_PORT` | no | `7070` | local channel port |
| `MARKET_DATA_FEEDER_HOST` | no | `127.0.0.1` | local channel host |
| `TELEGRAM_BOT_TOKEN` | yes | — | control/monitoring bot token from @BotFather |
| `TELEGRAM_ALLOWED_CHAT_IDS` | yes | — | comma-separated chat-id whitelist (at least one) |
| `BETTERSTACK_TOKEN` / `BETTERSTACK_ENDPOINT` | no | — | BetterStack log transport (both must be set to enable) |

## Control & monitoring Telegram bot

Each feeder process runs a Telegram bot (in the same process) for live status and a remote restart. It is **mandatory** — the feeder refuses to start without `TELEGRAM_BOT_TOKEN` and `TELEGRAM_ALLOWED_CHAT_IDS`.

Setup:

1. Create a bot via [@BotFather](https://t.me/BotFather) and copy its token into `TELEGRAM_BOT_TOKEN`.
2. Find your numeric chat id (e.g. message [@userinfobot](https://t.me/userinfobot)) and put it into `TELEGRAM_ALLOWED_CHAT_IDS` (comma-separate several). Only whitelisted chats are served; everyone else is silently ignored.

Commands and menu:

- `/menu` — inline menu: **Status**, **Stale symbols**, **Restart**.
- `/status` — quick text snapshot.
- **Status** shows the exchange, address, uptime, connected-client count and one line per active interval (symbols / klines / stale / subscriptions) with a health dot: 🟢 none stale · 🟡 ≤10% stale · 🔴 >10% stale · ⚪ no symbols yet.
- **Stale symbols** lists the most-stale symbols (with age) for a chosen interval.
- **Restart** immediately (no confirmation) shuts the process down gracefully and exits; the supervisor brings it back, which posts a fresh "ready to accept clients" greeting.

> **The Restart button relies on a process supervisor.** Restart performs a clean shutdown and `process.exit(0)`; the process comes back only if a supervisor (pm2 / systemd / Docker `restart: always`) brings it up again. Without one, the feeder stays down.

## Active monitoring & self-healing

On startup, once the server is up and ready to accept clients, the feeder posts a greeting to every whitelisted chat ("✅ … feeder is up and serving … Ready to accept clients."). After that it never stays silent about a problem — many apps depend on its data, so any degradation is pushed to Telegram immediately (no need to open the menu):

- **Dead stream, fast.** While the exchange WebSocket is alive, ticks arrive across hundreds of symbols continuously. A total silence across a source for ~45s is flagged as a likely dead transport — within tens of seconds, instead of the per-symbol staleness threshold (which is `2 × interval` = 60 min on 30m, 8 h on 4h).
- **Transport & library signals.** Exchange transport notifications (incl. "max retries exceeded") and the trade-engine kline watchdog events ("stream stale / recovered / recovery failed") are surfaced as alerts.
- **Stuck symbol.** A symbol with no fresh candles past the threshold raises an alert.
- Alerts are de-duplicated (no flooding) and followed by a "recovered" message when the problem clears.

**Self-healing (alert first, then restart).** On prolonged degradation the feeder: (1) alerts immediately; (2) gives the library's own recovery (resubscribe + REST backfill) a grace window; (3) if still degraded, alerts "restarting" and exits for the supervisor to bring it back. A restart counter that **survives the restart** (a small on-disk log) blocks restart loops — after 3 restarts in 30 minutes it stops and asks for manual intervention instead.

> The restart fires only after ~3 minutes of total stream silence (≈45s detection + ≈2min recovery grace), which is practically impossible on a live market — so false restarts are very unlikely. Restart requires a supervisor (see above) to bring the process back.

## Scripts

`yarn dev` · `yarn build` (lint + test + compile) · `yarn start` · `yarn lint` (tsc) · `yarn test`.

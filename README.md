# @solncebro/market-data-feeder

On-demand market-data feeder. One exchange connection per process fans klines, moving averages and 24h volume out to multiple strategy apps over a local WebSocket channel — so each strategy stops opening its own ~1200-symbol kline subscriptions.

Two halves live in this package:

- **Server (feeder)** — `dist/feeder.js`. Holds a single `ExchangeConnector`, loads kline data **on demand** (an interval/symbol is fetched only when a client first asks for it, reference-counted, torn down when idle) and serves it to clients.
- **Client (`MarketDataClient`)** — imported by strategy apps. Mirrors the feed in memory and exposes the same read API + events as the in-process market-data manager, so strategy code does not change. Transport reuses `@solncebro/websocket-engine` (auto-reconnect + stale detection).

## Run the feeder

```bash
cp .env.example .env   # set EXCHANGE_NAME + a read-only API key
yarn install
yarn dev               # or: yarn build && yarn start
```

One feeder process per exchange (mirrors the existing per-exchange deploy model).

## Use from a strategy app

```ts
import { MarketDataClient } from '@solncebro/market-data-feeder';
import type { IMarketDataSource } from '@solncebro/market-data-feeder';

const client = new MarketDataClient({
  url: 'ws://127.0.0.1:7070',
  interval: '30m',
  scope: { kind: 'all' },
  events: ['klineClosed', 'klineUpdated', 'klineUpdatedTick'],
  wantMa: true,
  logger,
});

await client.waitUntilReady();
// client.getKlineList / getMaValues / getVolume24h ... + client.on('klineClosed', ...)
```

Create one `MarketDataClient` per interval the app needs (e.g. the chaser app: 30m + 5m + 4h; the breaker and rubber: 30m only).

## Env

| Var | Required | Default | Purpose |
|---|---|---|---|
| `EXCHANGE_NAME` | yes | — | `binance` or `bybit` |
| `EXCHANGE_API_KEY` / `EXCHANGE_SECRET` | yes | — | read-only key (public data only) |
| `MARKET_DATA_FEEDER_PORT` | no | `7070` | local channel port |
| `MARKET_DATA_FEEDER_HOST` | no | `127.0.0.1` | local channel host |

## Scripts

`yarn dev` · `yarn build` (lint + test + compile) · `yarn start` · `yarn lint` (tsc) · `yarn test`.

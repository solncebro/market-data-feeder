# CLAUDE.md — market-data-feeder

> Architecture and development guide. The user-facing instructions (how to run it, how to connect from an app) live in `README.md`; this file documents the internal design of the process.

## Purpose

`@solncebro/market-data-feeder` is a **shared market-data distributor**. One process per exchange holds the **single** set of exchange kline subscriptions (`ExchangeConnector` from `@solncebro/trade-engine`), computes moving averages and pulls the 24h volume, and distributes all of it to several strategy apps over a local WebSocket channel. The goal is to remove duplication: previously each app (ma-chaser, volume-breaker, rubber) opened ~1200 kline subscriptions per symbol on its own; now only the feeder opens the subscriptions and the apps mirror the stream in RAM.

Deployment mirrors the existing per-symbol model: **one feeder process per exchange** (`binance` / `bybit`), with `EXCHANGE_NAME` selecting the exchange.

Two halves of the package:
- **Server (feeder)** — entry `src/feeder.ts` (`dist/feeder.js`). Holds the `ExchangeConnector`, loads data **on demand** and serves clients.
- **Client (`MarketDataClient`)** — imported by apps from the barrel `src/index.ts`. Mirrors the stream in RAM and exposes the same read API + events as the in-process market-data manager, so strategy code does not change.

## On-demand loading + ref-counting + deferred teardown (`SubscriptionRegistry`)

`src/server/subscriptionRegistry.ts` is a generic registry (`SubscriptionRegistry<TSource extends ManagedSource>`), the core of "on demand". One `IntervalRegistration` per interval holds the canonical source and the reference counters:
- `allSubscriberCount` — how many clients are subscribed with `scope.kind === 'all'`.
- `symbolRefCountBySymbol` — ref-count for targeted `scope.kind === 'symbols'`.
- `isAllLoaded` / `allLoadPromise` — flag and in-flight promise of the full all-symbols load.
- `symbolLoadPromiseBySymbol` — in-flight promises of a single-symbol load (dedup of concurrent requests for the same symbol).

Logic:
- **`subscribe`** cancels a pending interval teardown, creates the `IntervalRegistration` on first use (`createSource` + `source.start()`). For `all` — increment `allSubscriberCount` + `ensureAllLoaded` (loads ALL symbols once; later subscribers await the same `allLoadPromise`). **On load failure** — decrements `allSubscriberCount` back to 0 and triggers `teardownIfIdle` before re-throwing, so the counter never leaks. For `symbols` — `acquireSymbol` per symbol: increment ref-count, and `ensureSymbolLoaded` only on the first reference (if the interval is not fully loaded). **On partial failure** — all already-acquired symbols are released via `releaseSymbolRef` and `teardownIfIdle` is triggered before re-throwing. If the interval is already `isAllLoaded`, the targeted load is skipped — the symbol is already present.
- **`unsubscribe`** decrements the matching counter (`all` → decrement; `symbols` → `releaseSymbolRef` per symbol; at ref-count → 0 with no `all` subscribers it calls `source.releaseSymbol`), then `teardownIfIdle`. A symbol whose targeted load was still in flight when it was unsubscribed is undone by `undoOrphanIfReleased` (re-checked after the load resolves), so a load that completes after its only ref was released does not leak an orphaned exchange subscription.
- **`teardownIfIdle`** — when `allSubscriberCount === 0` AND `symbolRefCountBySymbol.size === 0`: a deferred teardown via `setTimeout(DEFAULT_TEARDOWN_DELAY_MS = 30_000)` (debounce — so a client reconnect does not recreate the source). On fire, the timer re-checks that the interval is still idle and is the same registration, then calls `source.shutdown()`. `teardownDelayMs <= 0` → synchronous teardown without debounce.
- **`syncAllLoadedSources`** — for every fully loaded interval calls `source.syncAllSymbols()` (listings/delistings). Triggered by the server on an hourly timer.
- **`shutdown`** — clears all teardown timers and calls `source.shutdown()` for every registration.

`createSource` is injected from above (the `MarketDataManager` factory); the registry itself knows nothing about exchanges. The source contract is `ManagedSource` (`src/server/subscriptionRegistry.types.ts`): `start` / `loadAllSymbols` / `ensureSymbolLoaded` / `releaseSymbol` / `syncAllSymbols` / `shutdown` / `getInterval`.

## Canonical source (`MarketDataManager`)

`src/source/marketDataManager.ts` is the only component actually subscribed to the exchange. An `EventEmitter`, it implements `FeederSource` (= `ManagedSource` + read methods + typed `on`/`off`). One instance per interval, created by the registry.

- **Buffer** `KLINE_BUFFER_SIZE = 499` candles per symbol (`klineListBySymbol`), plus `maValuesBySymbol`, `currentKlineBySymbol`, `subscriptionBySymbol`, `handlerBySymbol`.
- **Loading**: `loadAllSymbols` takes `getFuturesSymbols()`, filters `endsWith('USDT')`, loads all symbols over REST through the shared pacer `DEFAULT_BACKFILL_QUEUE` (`RateLimitedRequestQueue` from `@solncebro/trade-engine`, `KLINE_BACKFILL_REQUESTS_PER_SECOND = 80` requests/sec, per-fetch timeout `LOAD_KLINES_TIMEOUT_MS = 60_000` via `withTimeout`, progress every `KLINE_LOAD_PROGRESS_LOG_EVERY = 100`), then `subscribeToKlines`. The pacer is a process-wide singleton shared across all interval sources: on restart, 5m/30m/4h no longer stack into a burst that triggers Bybit/Binance HTTP 403 "Access too frequent" (reads in `trade-engine` are not rate-limited — its rate-limit queue is for write operations only). The pacer is injected into the `MarketDataManager` constructor (3rd argument, default — the singleton; tests pass their own). `ensureSymbolLoaded(symbol)` does the same for a single symbol on demand through the same pacer (no-op if already buffered; returns `true` on a successful load).
- **MA**: `calculateAllMaValues` (`src/source/indicators.ts`) — SMA over `closePrice` for periods 25/50/100/200 (`MaValues`). Recomputed on every kline event, except the throttled branches.
- **`handleKline`** (wrapped in try/catch so an exception on one symbol does not kill the SDK callback loop) → `handleKlineUnsafe`: `isOlder` guard (a replayed candle older than the last one in the buffer → increment `skippedOlderKlineCountBySymbol` and return), a candle transition (`isNewCandle`) closes the previous candle and emits `klineClosed`, MA-recompute throttle `KLINE_UPDATE_THROTTLE_MS = 5000` (on a throttled tick the MA is taken from cache and `throttledMaRecomputeCount` is incremented), then `klineUpdated` + `klineUpdatedTick` emits.
- **Emits under a freshness guard** (`isKlineFresh`: age ≤ `STALENESS_THRESHOLD_MULTIPLIER × intervalMs`): `emitGuarded` (closed/updated) and `emitTickGuarded` (tick) drop stale/replayed candles — this protects downstream consumers in the apps from historical replays.
- **Staleness watchdog** (`startIntervalScheduler`, `STALENESS_CHECK_INTERVAL_MS = 60_000`, heartbeat every `STALENESS_HEARTBEAT_EVERY_N_TICKS = 15` ticks): counts `consecutiveStaleScanCountBySymbol`; at `PERSISTENT_STALE_THRESHOLD_TICK_COUNT = 10` it emits the `persistentStaleSymbol` event (and `persistentStaleRecovered` when a fresh candle later returns for that symbol). The heartbeat also flushes the aggregate of skipped older candles.
- **Silence watchdog** (`SILENCE_CHECK_INTERVAL_MS = 10_000`, threshold `SILENCE_THRESHOLD_MS = 45_000`): `lastInboundAtMs` (updated by `recordInboundMessage` on any inbound message, anchored at the first subscription); edge-triggered `streamSilent` when silence exceeds the threshold, `streamResumed` on return. The main fast transport-death detector — see "Active diagnostics".
- **`syncAllSymbols`**: `getFuturesSymbols()` → `computeSymbolListDelta` (`src/source/symbolListDelta.ts`, a pure added/removed delta) → for added: `ensureSymbolLoaded` + `emit('symbolAdded')`; for removed: `emit('symbolRemoved')` + `releaseSymbol`.
- **`releaseSymbol`**: `unsubscribeKlines` + full cleanup of every per-symbol Map for that symbol.
- **Post-shutdown guard**: `shutdown()` sets `isShutDown`; `handleKline` early-returns on a late SDK callback and `ensureSymbolLoaded` bails after its REST await — so a torn-down source can neither emit nor re-subscribe on the exchange after teardown.
- **24h volume** — `getVolume24h` reads `exchangeConnector.getTicker(symbol, Futures)?.quoteVolume` (cache miss → `+Infinity`). A background scheduler `startVolumeRefreshScheduler` (`VOLUME_REFRESH_INTERVAL_MS = 30_000`) polls `getVolume24h` for every buffered symbol and emits `volume24h` whenever the value changes (skips `+Infinity`). The server forwards the event to clients via `broadcastScoped`; the client applies it to the mirror store. There is no dedicated ticker WebSocket subscription.
- The source of interval lengths in ms is `resolveIntervalMs` (`src/domain/constants.ts`, the `INTERVAL_MS_BY_KEY` map).
- **Diagnostic getters** (for the Telegram bot): `getStreamLiveness()` → `{ lastInboundAtMs, silenceMs, isStreamSilent }` (the live state of the source stream), `getFreshSymbolCount()` (symbols minus stale), `getPersistentStaleCount()` (`consecutiveStaleScanCountBySymbol.size`). The heartbeat-reset counters `skippedOlderKlineCountBySymbol` / `throttledMaRecomputeCount` are intentionally NOT exposed (a menu read would catch a meaningless partial sample, since they reset on the ~15-min heartbeat).

## Channel (`FeederServer`) + protocol

`src/server/feederServer.ts` is a WebSocket server (`ws`) on top of `SubscriptionRegistry`. It listens on `host:port` and wraps every source in a forwarding shim (`createForwardingSource`): subscribes to the source events and relays them to clients.

**Protocol** (`src/protocol/messages.types.ts`, JSON via `src/protocol/codec.ts` — `encodeMessage`/`decodeMessage`; `decodeMessage` validates the `type` field against `KNOWN_TYPE_SET`, invalid → `null`):
- Client → server: `subscribe { interval, scope, events, wantMa }`, `unsubscribe { interval, scope }`.
- Server → client: `snapshot`, `klineClosed`, `klineUpdated`, `klineUpdatedTick`, `symbolAdded`, `symbolRemoved`, `volume24h`, `heartbeat`.

Server behavior:
- **Subscribe validation**: every `subscribe` is checked by `isValidSubscribe` (`src/protocol/clientMessageValidation.ts`) before any work — interval must be a known `KlineInterval`, `scope.kind` must be `all`/`symbols`, `symbolList` must be non-empty strings matching `^[A-Z0-9]{1,30}$` within `MAX_SUBSCRIBE_SYMBOL_COUNT = 5000`. Invalid messages are dropped with a warn (no phantom-interval source, no unbounded REST flood).
- **Snapshot on subscribe**: `handleSubscribe` records the client subscription **before** the `registry.subscribe` await (so scope-matched events during loading are queued to the client). If `registry.subscribe` throws, the subscription record is rolled back. A re-subscribe for an interval the client already holds **releases the superseded scope's registry refs** before acquiring the new one (so the old ref-count is not leaked). After `await`, if the client has already disconnected (`!clientSet.has(client)`), the snapshot is skipped. Then `buildSnapshotMessageList` slices the scoped symbols into chunks of `DEFAULT_SNAPSHOT_CHUNK_SIZE = 100`; each chunk is a separate `snapshot` message, the last marked `isFinal: true` (an empty set → one final empty message).
- **Scope filter + event filter**: `forwardFeedMessage` sends an event to a client only if it has a subscription for that interval, its `eventNameSet` contains the event name, and `scopeMatchesSymbol` is true (`all` → everyone; `symbols` → only the listed ones). `symbolAdded`/`symbolRemoved`/`volume24h` are filtered by scope but not by the event set (forwarded via `broadcastScoped`).
- **Heartbeat**: `HEARTBEAT_INTERVAL_MS = 15_000` — a `heartbeat` is sent to all clients (a liveness signal for the client-side stale detector).
- **Symbol-list sync**: `SYMBOL_LIST_SYNC_INTERVAL_MS = 3_600_000` (hourly) → `registry.syncAllLoadedSources()`.
- **Client disconnect** (`close`) removes all of its subscriptions via `registry.unsubscribe` — which drives ref-count/teardown.
- **Connection hardening**: the WS server is created with `maxPayload = DEFAULT_MAX_PAYLOAD_BYTES (256 KB)` (a subscribe is tiny; bounds a hostile-frame memory spike) and `handleConnection` rejects sockets beyond `DEFAULT_MAX_CONNECTIONS (64)`. Both are overridable via `FeederServerArgs.maxPayloadBytes` / `maxConnections` (tests pass small values).
- `start()` awaits `listening`, records the actually bound port (`getPort()` — important when `port: 0` in tests). The startup `listening`/`error` race uses named handlers (the error handler is removed once `listening` fires), then a **persistent** `error` handler is attached that logs server-level errors (the old race-only `once('error')` would swallow the first post-startup error and let a second crash the process). `shutdown()` stops the timers, closes clients, closes the WS server, and calls `registry.shutdown()`.

`src/feeder.ts` (entry): `loadEnvConfig` → `installCrashHandlers` → `createRestartGuard` → `createHealthMonitor` (with late-bound `sendAlert`/`shutdown`) → `new ExchangeConnector(name, ..., onNotify, undefined, klineWatchdogConfig)` → `initialize()` → `new FeederServer({ ..., onHealthEvent })` → `start()` → `createTelegramControlBot` → `controlBot.start()` → `healthMonitor.report('feederReady')`. SIGINT/SIGTERM → graceful `shutdown(reason?, exitCode = 0)` wrapped by `runWithHardExit(SHUTDOWN_HARD_EXIT_MS = 15s)`. **Exit code by intent**: the self-healing auto-restart (`onRestart`) calls `shutdown(reason, 1)` so a `Restart=on-failure` supervisor relaunches the self-healed process; operator SIGINT/SIGTERM and the manual bot Restart exit `0` (a deliberate stop should not loop-relaunch). The logger is `logger` from `@solncebro/trade-engine` (adapted into `FeederLogger`).

## Client (`MarketDataClient`)

`src/client/marketDataClient.ts` is an `EventEmitter` that implements `MarketDataSource` (`src/client/marketDataSource.types.ts`), an in-RAM mirror of the channel. Created **one per interval** that the app needs.

- Transport — `ReliableWebSocket` from `@solncebro/websocket-engine` (auto-reconnect + stale detection). On `onOpen` it sends a `subscribe` message (`interval` / `scope` / `events` / `wantMa` from `MarketDataClientArgs`).
- **`MirrorStore`** (`src/client/mirrorStore.ts`) holds candle buffers (cap `KLINE_BUFFER_SIZE`), MA, the current forming candle and the 24h volume. `applySnapshot`/`applySymbolAdded` seed symbols; `applyKlineClosed`/`applyKlineUpdated`/`applyKlineTick` update the buffer (via `applyKlineToBuffer`: older → drop, same `openTimestamp` → replace, new → push with trim); `retainSymbols` after the final snapshot drops symbols outside the snapshot.
- **Same events out**: for each server message the client applies it to the mirror and re-emits `klineClosed` / `klineUpdated` / `klineUpdatedTick` / `symbolAdded` / `symbolRemoved` — identical to the in-process manager, so strategy code switches to the feeder without edits.
- **Read API** `MarketDataSource`: `getInterval` / `getIntervalMs` / `getMaValues` / `getKlineList` / `getCurrentKline` / `getSymbolList` / `getLastKlineOpenTimestamp` / `getLastUpdateTimestamp` / `getVolume24h` / `isStale` / `getStaleSymbolList`.
- `waitUntilReady(timeoutMs = 30_000)` resolves on the first `snapshot` with `isFinal: true` (or rejects on timeout). `isStale` is true until the final snapshot or if the last **data** message (any message except `heartbeat`) is older than `CLIENT_STALE_THRESHOLD_MS = 45_000` — heartbeats are intentionally excluded so a live-but-stale connection is correctly detected (`dataStaleness.ts`). The staleness check runs every `CLIENT_STALE_CHECK_INTERVAL_MS = 15_000`.
- **`connectionLost` / `connectionRestored`** events are emitted on `onNotify` from `ReliableWebSocket` and on the next snapshot completion, respectively. Apps can listen to react to feeder disconnects.
- **`volume24h`** messages from the server are applied to the mirror store (`applyVolume24h`) so `getVolume24h` always returns the latest polled value.

## Telegram control and monitoring bot (`src/telegram/`)

The bot is **mandatory** (without `TELEGRAM_BOT_TOKEN`/`TELEGRAM_ALLOWED_CHAT_IDS` the process fails on startup). It lives **in the same process** as the feeder and reads status directly from `FeederServer`. It is built on the ready-made factories of `@solncebro/telegram-engine` (`createBotRegistry`, `registerBotCommands`, `createCallbackEncoder`, `createKeyboardBuilder`, `createMenuRouter`, `createInputStateManager`) — there is no custom transport/Telegraf wiring. **All bot-visible text is English.**

- **`telegramControlBot.ts`** — `createTelegramControlBot({ botToken, allowedChatIdList, statusProvider, exchangeName, logger, onReboot })` → `{ start, stop, sendAlert }`. `statusProvider` is the structural contract `FeederStatusProvider` (`getStatus` + `getStaleSymbolList` + `getSymbolDiagnostics`), satisfied by `FeederServer`. The whitelist is mandatory (via `registry.accessControl`); disallowed chat ids are silently ignored by the engine and by the manual reply-keyboard `guard`. `createTelegramControlBot` **throws on an empty `allowedChatIdList`** (fail-closed — a second, independent guard so the engine's fail-open-on-empty-set default can never be reached, even if env validation is bypassed).
- **Hybrid menu** — the library has no reply-keyboard builder, so the menu is a deliberate hybrid:
  - **Main menu = a persistent reply keyboard** (`Markup.keyboard(...).resize().persistent()`, docked at the bottom of the chat): `🛰️ Overview`, `🔌 Websockets`, `🕯️ Stale symbols`, `🔎 Symbol info`, `🔄 Restart`, `✖️ Close menu` (laid out as 2 rows × 3 columns). Button presses arrive as plain text and are routed via `bot.hears(BUTTON, guard(handler))`. Typed symbol input is sanitized (`normalizeSymbol` from trade-engine + uppercase + `[A-Z0-9]` whitelist + `USDT` suffix) before use, so it can never produce invalid MarkdownV2. The `bot.hears` and `bot.start` handlers are registered **before** `registerBotCommands` (which wires the generic `bot.on('message')` input handler) — otherwise button texts would fall through to it.
  - **Drill-down screens stay inline** (callback buttons via `createKeyboardBuilder`, edited in place through `editMessageText`; benign edit errors are swallowed by `isBenignTelegramEditError`). A reply-keyboard press opens the first inline screen as a NEW message; inline navigation then edits that message.
- **Screens** (all English, `Label: value` lines):
  - **Overview** (`MenuStep.Overview`) → server summary (exchange, host:port, uptime, clients connected) + one line per interval with a health indicator; inline `🔄 Refresh`.
  - **Websockets** (`MenuStep.Websockets`) → a button per interval with a liveness label (`🟢 30m · live · 450 sym`) → **Interval detail** (`MenuStep.IntervalDetail`): load mode (all/on-demand), stream state (`🟢 live` / `🔴 silent for …`), last inbound age, subscribers, subscriptions, symbols loaded, klines buffered, fresh/stale/persistent-stale counts; inline `🔄 Refresh` + `🕯️ Stale list` + `◀️ Back`.
  - **Stale symbols** (`MenuStep.StaleIntervals`) → pick an interval → top `STALE_SYMBOL_DISPLAY_LIMIT = 15` stale symbols with age (`MenuStep.Stale`).
  - **Symbol info** → the reply button sets a `symbolLookup` input state (`createInputStateManager`) and prompts for a symbol; the `messageHandler` reads the typed text and renders the symbol card (last price, last candle open age, MA25/50/100/200, 24h volume, last update age, fresh/stale state) via `statusProvider.getSymbolDiagnostics`. Not loaded anywhere → an explicit message.
  - **Restart** (`🔄 Restart` reply button) → no confirmation step: it immediately replies "Restarting…" and calls `onReboot`. The press is a plain reply-keyboard text routed to `triggerRestart`. A ready greeting is sent again on next startup (see the `feederReady` health event below).
- **Commands**: `/menu` and `/start` open the reply keyboard; `/status` replies with the Overview text (no menu).
- **`statusFormatter.ts`** — pure functions `formatOverviewMessage` / `formatIntervalDetailMessage` / `formatSymbolCard` / `formatStaleSymbolMessage` / `formatIntervalButtonLabel` / `resolveIntervalHealth`. Health by stale share: `⚪` no symbols / `🟢` 0 stale / `🟡` ≤ `STALE_CRITICAL_PERCENT = 10`% / `🔴` > 10%.
- **`menu.types.ts`** — `MenuStep` (`Overview` / `Websockets` / `IntervalDetail` / `StaleIntervals` / `Stale`) enum + `CallbackData` (`step` / `interval`, compactly encoded via `createCallbackEncoder`). The symbol lookup goes through the input-state manager, not callback data, to avoid the 64-byte callback limit. Restart has no callback step — it is a direct reply-keyboard action.
- **Restart** = process restart: `onReboot` is the feeder `shutdown()` (stops the bot → `server.shutdown()` → `exchangeConnector.disconnect()` → `process.exit(0)`); an external supervisor brings the process back. The same `shutdown()` is invoked by SIGINT/SIGTERM. **Requires an auto-restart supervisor on the server** — otherwise the process simply dies.

Status disclosure in the feeder code: `MarketDataManager.getKlineCount()` plus the existing `getStaleSymbolList` / `getSubscriptionCount` / `getIntervalMs` / `getLastKlineOpenTimestamp` / `getLastUpdateTimestamp` and the new `getStreamLiveness` / `getFreshSymbolCount` / `getPersistentStaleCount` are all part of the `FeederSource` contract; `SubscriptionRegistry.getRegistrationStatusList()` (per-interval `isAllLoaded`/`allSubscriberCount`/`refSymbolCount`); `FeederServer.getStatus()` (aggregator of `FeederStatus`/`IntervalStatus` — now including `liveness`/`freshCount`/`persistentStaleCount` — plus uptime from `startedAtMs`), `getStaleSymbolList(interval, limit)` and `getSymbolDiagnostics(symbol, interval?)` (assembles a per-symbol `SymbolDiagnostics` from the source getters, picking the first interval that has the symbol).

## Active diagnostics, alerts and self-healing (`src/health/`)

The bot above is **passive** (status on a button press). The active half: the feeder shouts into Telegram on any problem and, on prolonged degradation, restarts itself. The principle is **never stay silent on a problem**.

**Detectors (signal sources, all funnel into `HealthMonitor.report`):**
- **Source silence** (`MarketDataManager`, the main fast detector): `lastInboundAtMs` is updated on any inbound message (`recordInboundMessage` at the start of `handleKline`, before the guards); a fast scan `SILENCE_CHECK_INTERVAL_MS = 10_000`, threshold `SILENCE_THRESHOLD_MS = 45_000`. Edge-triggered `streamSilent` / `streamResumed` events (the silence scan flags once, the return is caught in `recordInboundMessage`). `lastInboundAtMs` is anchored at the first subscription — a stream that never delivered a single candle is flagged too. Catches transport death in tens of seconds instead of `2 × intervalMs` (60 min on 30m, 8h on 4h).
- **Mass staleness** (`MarketDataManager`): after every staleness scan `evaluateMassStale` checks if `staleCount / symbolCount ≥ MASS_STALE_RATIO_THRESHOLD (0.3)` and `symbolCount ≥ MASS_STALE_MIN_SYMBOLS (20)` (via `crossesMassStaleThreshold` in `src/source/massStale.ts`). Edge-triggered `sourceMassStale` (arms escalation) / `sourceMassStaleRecovered` (clears it).
- **Library transport notifications**: `onNotify` (3rd argument of `ExchangeConnector`) → `transportNotify`. The raw library text is sanitized of MarkdownV2 marker chars (`` ` `` `*` `_` `~` `|`) before interpolation so it can never break the rendered message. Critical markers are narrowed to `critical`/`fatal`/`max retries` — the broad `failed`/`exceeded` were removed so a per-symbol watchdog scan summary (which contains a `Failed: N` line even on partial recovery) no longer latches an irreversible whole-process restart. Everything else is an alert only; the whole-source silence watchdog remains the backstop for a genuinely dead transport.
- **trade-engine kline watchdog**: `klineWatchdogConfig` (5th argument) with `onStreamStale`/`onStreamRecovered`/`onStreamRecoveryFailed` → `klineStreamStale`/`klineStreamRecovered`/`klineStreamRecoveryFailed`. These are **per-symbol** signals — all batched, **none escalate to a restart** (a single dead/delisted symbol must not restart the whole process).
- **Stuck symbol**: `MarketDataManager` emits `persistentStaleSymbol` (threshold `PERSISTENT_STALE_THRESHOLD_TICK_COUNT = 10` ≈ 10 min) and the symmetric `persistentStaleRecovered` when a fresh candle returns for a previously-flagged symbol (tracked via `persistentStaleEmittedSet`).
- **On-demand load + readiness**: `MarketDataManager` emits `intervalLoadStarted`/`intervalLoadCompleted` (per-interval; `intervalLoadCompleted` now carries the ACTUAL loaded count, not the requested count) and `symbolLoadCompleted` (per-symbol on-demand / new listing); `feeder.ts` emits `feederReady` after startup.
- **Failed loads** (`symbolLoadFailed`): a REST load that returns empty or throws — in the bulk `loadAllSymbols` path (each requested-but-not-buffered symbol) and in on-demand `ensureSymbolLoaded` — emits `symbolLoadFailed` (per-symbol, batched, **non-escalating**), so a partial/failed load is surfaced in Telegram instead of silently serving fewer symbols (recovered at the next hourly sync).
- **Crash handlers** (`src/health/crashHandlers.ts`): `installCrashHandlers` wires `process.on('uncaughtException')` and `process.on('unhandledRejection')` — each writes to `stderr` synchronously, sends a best-effort Telegram alert with a grace period (`CRASH_ALERT_GRACE_MS = 3s`), then calls `process.exit(1)`. A re-entrancy guard prevents double-handling.

Source health events are relayed by `FeederServer` (`createForwardingSource` → `emitHealthEvent`) into the injected `FeederServerArgs.onHealthEvent`; transport and watchdog signals go into `HealthMonitor` directly from `feeder.ts`.

**`HealthMonitor`** (`src/health/healthMonitor.ts`) is the single point and the **only** path to Telegram (everything funnels into `sendAlert` → `Broadcaster.sendToAll(message, true)` as MarkdownV2). It classifies `HealthEvent` (a discriminated union) and routes each by cardinality:
- **Per-symbol events are batched** (`batchFlushMs = 3000`): each category — stalled / recovered / recovery-failed / stuck / unstuck / loaded — accumulates into its own pending map and flushes as **one grouped message** per category (grouped by interval, symbols wrapped in backticks via `formatClickableText`). A burst of N symbols never floods N messages. `activeStallKeySet` suppresses re-alerting an ongoing stall and is cleared on recovery or recovery-failed.
- **Per-interval / global events are immediate**: `streamSilent`/`streamResumed`, `intervalLoad*`, `feederReady`, and `transportNotify` (single dedup key). Every message is escaped once centrally in `emitAlert` (`escapeMarkdownV2WithFormatting`).
- **Escalation → restart** is decoupled from alerting (`setDegraded`/`clearDegraded` track degradation silently; messages go through the batchers/immediate path). Only whole-source signals arm it: `streamSilent` (whole-source silence), `sourceMassStale` (≥ `MASS_STALE_RATIO_THRESHOLD` of symbols stale), and critical `transportNotify`. Per-symbol watchdog failures do **not**. On `shutdown()` the pending batch is flushed so accumulated messages are not lost.

**`RestartGuard`** (`src/health/restartGuard.ts`) is the loop guard: a file-backed counter of auto-restarts (path `RESTART_STATE_FILE` in `feeder.ts`, default `./feeder-restart-log.json`), which **survives a process restart** (an in-memory counter would reset on startup). ≥ `MAX_AUTO_RESTARTS` (3) within `RESTART_WINDOW_MS` (30 min) → do not restart, send "manual intervention required" instead. Reads are fail-open (a corrupt/missing file = 0 restarts). On creation it runs a **writability probe** (persists the pruned state) and logs an `error` if the path is unwritable — so a silently-disabled loop guard (a persistent write failure would otherwise let the counter never persist) is surfaced in the logs/BetterStack.

**Self-healing is unconditional** (not behind a flag): if degradation did not clear within the soft-recovery window, a restart always happens (bounded only by `RestartGuard`). The effective window before a restart is ≈ `SILENCE_THRESHOLD_MS` (45s detect) + `HEALTH_RECOVERY_GRACE_MS` (120s grace) ≈ 3 minutes of continuous silence across the whole source — practically unreachable on a live market, so a false restart is unlikely. All thresholds are constants (`marketDataManager.ts` / `feeder.ts`).

Wiring (`feeder.ts`): because of the dependency ring (bot→server→monitor, connector→monitor) the `HealthMonitor` is created first with late-bound `sendAlert`/`shutdown`; the bot's `onReboot`, the monitor's `onRestart` and SIGINT/SIGTERM all call one `shutdown()` (stops the monitor → bot → server → connector → `process.exit`). Graceful `shutdown()` is wrapped by `runWithHardExit` (`src/utils/hardExit.ts`, `SHUTDOWN_HARD_EXIT_MS = 15_000`) — if the async shutdown sequence hangs, `process.exit(1)` fires unconditionally. `HealthMonitor.shutdown()` is **async** and awaited first in the shutdown sequence: it flushes the pending batch then **drains in-flight Telegram sends** (bounded by `ALERT_DRAIN_TIMEOUT_MS = 2500`) so the restart-reason alert is delivered before `process.exit`, instead of being lost to the exit race.

**Telegram channel connectivity monitor** (`src/telegram/channelConnectivityMonitor.ts`): `startChannelConnectivityMonitor` runs a background probe (`bot.telegram.getMe()`) every `CHANNEL_PROBE_RECHECK_MS = 60s` (retry on failure every `CHANNEL_PROBE_RETRY_MS = 15s`). On first success it logs «control channel reachable»; on failure it logs an error «control channel UNREACHABLE — alerts are NOT being delivered». Never blocks startup. Started after `controlBot.start()`, stopped in `controlBot.stop()`.

## Env (`src/config/env.ts`)

| Var | Required | Default | Purpose |
|---|---|---|---|
| `EXCHANGE_NAME` | yes | — | `binance` or `bybit` (validated against `ExchangeNameEnum`) |
| `EXCHANGE_API_KEY` / `EXCHANGE_SECRET` | yes | — | read-only key (public data only) |
| `MARKET_DATA_FEEDER_PORT` | no | `7070` | local channel port (positive integer) |
| `MARKET_DATA_FEEDER_HOST` | no | `127.0.0.1` | local channel host |
| `TELEGRAM_BOT_TOKEN` | yes | — | control/monitoring bot token from @BotFather |
| `TELEGRAM_ALLOWED_CHAT_IDS` | yes | — | comma-separated chat id whitelist (at least one; empty → throw) |
| `BETTERSTACK_TOKEN` / `BETTERSTACK_ENDPOINT` | no | — | BetterStack logging; read directly by `createLogger` from trade-engine (not via `loadEnvConfig`); both set → the transport is enabled |

`loadEnvConfig()` throws on a missing required var, an invalid `EXCHANGE_NAME`, an invalid port, or an empty chat-id list. Loaded via `dotenv/config`.

**Logging:** the feeder uses the shared singleton `logger` from `@solncebro/trade-engine` (console + file + optionally BetterStack by env — like ma-chaser/volume-breaker/rubber). pino transports run in a worker thread, so **fatal startup errors are written synchronously to `console.error`** (`feeder.ts`, `main().catch`) — otherwise `logger.error` right before `process.exit(1)` is lost and the process dies silently (e.g. when `TELEGRAM_BOT_TOKEN` is missing).

## How the three apps consume it

Each app creates a `MarketDataClient` per interval it needs and switches its data flow to the feeder instead of its own subscriptions:
- **ma-chaser** — 30m + 5m + 4h (source behind the `MARKET_DATA_SOURCE=feeder` / `MARKET_DATA_FEEDER_URL` flag; delisting handled via the `symbolRemoved` event).
- **volume-breaker** — 30m.
- **rubber** — 30m.

One feeder process per exchange serves all apps of that exchange; they incrementally load only the intervals/symbols they need (ref-count + debounced teardown guarantee that unused intervals are released).

## Stack and commands

| | |
|---|---|
| Runtime | Node.js (ESM, `type: module`, NodeNext) |
| TypeScript | ^5.7, strict |
| Transport | `ws` (server) + `@solncebro/websocket-engine` (client) |
| Exchange | `@solncebro/trade-engine` (`ExchangeConnector`, `logger`, `ExchangeNameEnum`, `MarketTypeEnum`) |
| Telegram | `@solncebro/telegram-engine` (factories over Telegraf: bot, menu, callback routing) |
| Tests | vitest (`src/__tests__/`) |

`yarn dev` (tsx `src/feeder.ts`) · `yarn build` (lint + test + `tsc`) · `yarn start` (`dist/feeder.js`) · `yarn lint` / `yarn typecheck` (`tsc --noEmit`) · `yarn test` / `yarn test:watch`.

## `src/` structure

```
src/
  feeder.ts                       # entry: ExchangeConnector + FeederServer + HealthMonitor + TelegramControlBot + graceful shutdown
  index.ts                        # barrel: MarketDataClient + public types
  config/env.ts + .types.ts       # EnvConfig + loadEnvConfig
  domain/
    constants.ts                  # KLINE_BUFFER_SIZE, STALENESS_THRESHOLD_MULTIPLIER, resolveIntervalMs
    marketData.types.ts           # Kline, KlineInterval, MaValues, StaleSymbolInfo
    events.types.ts               # source listener types
    snapshot.types.ts             # MarketDataSnapshotEntry
    subscription.types.ts         # SubscriptionScope, FeedEventName
  server/
    feederServer.ts + .types.ts   # WebSocket server: snapshot / scope+event filter / heartbeat / symbol sync / getStatus / getSymbolDiagnostics
    subscriptionRegistry.ts + .types.ts  # on-demand + ref-count + debounced teardown + getRegistrationStatusList
    feederSource.types.ts         # FeederSource (canonical-source contract) + StreamLiveness
  telegram/
    telegramControlBot.ts + .types.ts    # control/monitoring bot on @solncebro/telegram-engine (reply-keyboard hybrid + sendAlert)
    statusFormatter.ts            # formatOverviewMessage / formatIntervalDetailMessage / formatSymbolCard / formatStaleSymbolMessage / resolveIntervalHealth
    menu.types.ts                 # MenuStep / CallbackData
    channelConnectivityMonitor.ts # background probe that detects when Telegram channel is unreachable
  health/
    healthMonitor.ts + .types.ts  # HealthMonitor: per-symbol batching + immediate per-interval alerts + escalation (whole-source only + mass stale) → grace → restart
    restartGuard.ts               # file-backed auto-restart counter (loop guard)
    crashHandlers.ts              # installCrashHandlers: uncaughtException/unhandledRejection → stderr + alert + exit 1
  source/
    marketDataManager.ts          # canonical source: subscriptions/buffer/MA/staleness/sync/liveness getters + volume24h refresh scheduler + mass-stale detector
    indicators.ts                 # calculateAllMaValues (SMA 25/50/100/200)
    symbolListDelta.ts + .types.ts # computeSymbolListDelta (added/removed)
    massStale.ts                  # crossesMassStaleThreshold (ratio + min-symbols guard)
  client/
    marketDataClient.ts + .types.ts      # in-RAM channel mirror, event re-emit; connectionLost/connectionRestored events; isStale by data messages only
    marketDataSource.types.ts            # MarketDataSource (shared read+event contract)
    mirrorStore.ts + .types.ts           # client-side mirror buffers
    dataStaleness.ts              # isDataStale / isDataFreshnessMessage (heartbeat excluded from staleness clock)
  protocol/
    messages.types.ts             # ClientMessage / ServerMessage / FeederMessage
    codec.ts                      # encodeMessage / decodeMessage (+ type validation)
    clientMessageValidation.ts    # isValidSubscribe (interval/scope/symbol/cap validation at the boundary)
  utils/
    intervalScheduler.ts          # startIntervalScheduler (tick + heartbeat + error handling)
    timeout.ts                    # withTimeout
    hardExit.ts                   # runWithHardExit (wraps async shutdown with a hard-timeout fallback)
  __tests__/                      # vitest (codec, mirrorStore, subscriptionRegistry, symbolListDelta, statusFormatter, restartGuard, healthMonitor, streamSilence, feederChannel, massStale, dataStaleness, hardExit, crashHandlers, volumeRefresh, klineBackfillPacing, markBackfilledHistoryClosed, marketDataManagerMassStale, channelConnectivityMonitor)
```

## Conventions

- **Interfaces without the `I` prefix** (`EnvConfig`, `FeederSource`, `MarketDataSource`, `ManagedSource`, `Kline`, `MaValues`).
- Booleans — `is*` / `has*` / `should*`; never `bar`, only `kline`/`klineClosed`/`klineUpdated`.
- Collections — `*BySymbol` / `*ByInterval` / `*List` / `*Set`.
- Only `info` / `warn` / `error`; the symbol and key parameters go in the log text, not only in the JSON payload.
- All relative quantities are in percent, not bps (project rule).
- All bot-visible text (menus, status, alerts, command descriptions) is English; no Cyrillic anywhere in the codebase.

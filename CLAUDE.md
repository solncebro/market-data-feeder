# CLAUDE.md — market-data-feeder

> Architecture and development guide. The user-facing instructions (how to run it, how to connect from an app) live in `README.md`; this file documents the internal design of the process.

## Purpose

`@solncebro/market-data-feeder` is a **shared market-data distributor**. One process per exchange holds the **single** set of exchange kline subscriptions (`ExchangeConnector` from `@solncebro/trade-engine`), computes moving averages and pulls the 24h volume, and distributes all of it to several strategy apps over a local WebSocket channel. The goal is to remove duplication: previously each app (ma-chaser, volume-breaker, rubber) opened ~1200 kline subscriptions per symbol on its own; now only the feeder opens the subscriptions and the apps mirror the stream in RAM.

Deployment mirrors the existing per-symbol model: **one feeder process per exchange** (`binance` / `bybit`), with `EXCHANGE_NAME` selecting the exchange.

**Split into two repositories (2026-08-25).** This repository is the **server only** — a private,
never-published app. Everything an app embeds into its own process (the collection core
`MarketDataManager`, the client `MarketDataClient`, the embedded source, the wire protocol, the
domain types and the source contracts) lives in the sibling `../market-data-feeder-lib` (published
package `@solncebro/market-data-feeder-lib`), which this server consumes as a regular dependency.
The split exists because the two roles have opposite dependency needs: a library must borrow the
shared engines from its host (one copy per process), while a standalone app needs its own
installed dependencies.

Three consumption shapes, two repositories:
- **Server (feeder, THIS repo)** — entry `src/feeder.ts` (`dist/feeder.js`). Holds the `ExchangeConnector`, loads data **on demand** and serves clients.
- **Client (`MarketDataClient`, in the lib)** — imported by apps from `@solncebro/market-data-feeder-lib`. Mirrors the stream in RAM and exposes the same read API + events as the in-process market-data manager, so strategy code does not change.
- **Embedded source (`createEmbeddedMarketDataSource`, in the lib)** — the feeder core run in-process by a single-consumer app (no WS, no mirror, no second process). Same `MarketDataSource` contract as the client.

## On-demand loading + ref-counting + deferred teardown (`SubscriptionRegistry`)

`src/server/subscriptionRegistry.ts` is a generic registry (`SubscriptionRegistry<TSource extends ManagedSource>`), the core of "on demand". One `IntervalRegistration` per interval holds the canonical source and the reference counters:
- `allSubscriberCount` — how many clients are subscribed with `scope.kind === 'all'`.
- `symbolRefCountBySymbol` — ref-count for targeted `scope.kind === 'symbols'`.
- `isAllLoaded` / `allLoadPromise` — flag and in-flight promise of the full all-symbols load.
- `symbolLoadPromiseBySymbol` — in-flight promises of a single-symbol load (dedup of concurrent requests for the same symbol).

Logic:
- **`subscribe`** cancels a pending interval teardown, creates the `IntervalRegistration` on first use (`createSource` + `source.start()`). For `all` — increment `allSubscriberCount` + `ensureAllLoaded` (loads ALL symbols once; later subscribers await the same `allLoadPromise`). **On load failure** — decrements `allSubscriberCount` back to 0 and triggers `teardownIfIdle` before re-throwing, so the counter never leaks. For `symbols` — `acquireSymbol` per symbol: increment ref-count, and `ensureSymbolLoaded` only on the first reference (if the interval is not fully loaded). **On partial failure** — all already-acquired symbols are released via `releaseSymbolRef` and `teardownIfIdle` is triggered before re-throwing. If the interval is already `isAllLoaded`, the targeted load is skipped — the symbol is already present.
- **`unsubscribe`** decrements the matching counter (`all` → decrement; `symbols` → `releaseSymbolRef` per symbol; at ref-count → 0 with no `all` subscribers it calls `source.releaseSymbol`), then `teardownIfIdle`. A symbol whose targeted load was still in flight when it was unsubscribed is undone by `undoOrphanIfReleased` (re-checked after the load resolves), so a load that completes after its only ref was released does not leak an orphaned exchange subscription.
- **`teardownIfIdle`** — when `allSubscriberCount === 0` AND `symbolRefCountBySymbol.size === 0`: a deferred teardown via `setTimeout(DEFAULT_TEARDOWN_DELAY_MS = 30_000)` (debounce — so a client reconnect does not recreate the source). On fire, the timer re-checks that the interval is still idle and is the same registration, then calls `source.shutdown()`. `teardownDelayMs <= 0` → synchronous teardown without debounce. **An in-flight bulk load blocks teardown** (`allLoadPromise !== null` → skip, both in `teardownIfIdle` and in the timer callback): tearing the source down mid-load would let the load's tail subscribe orphaned exchange streams on a dead source (a Binance full backfill takes >30s, so the window was real). The re-check is event-driven, mirroring `undoOrphanIfReleased`: `subscribe()` calls `teardownIfIdle` again after `ensureAllLoaded` settles, so the synchronous (`teardownDelayMs <= 0`) test path needs no timer polling.
- **`syncAllLoadedSources`** — for every fully loaded interval calls `source.syncAllSymbols()` (listings/delistings). Triggered by the server on an hourly timer.
- **`shutdown`** — clears all teardown timers and calls `source.shutdown()` for every registration.

`createSource` is injected from above (the `MarketDataManager` factory); the registry itself knows nothing about exchanges. The source contract is `ManagedSource` (imported into `src/server/subscriptionRegistry.types.ts` from `@solncebro/market-data-feeder-lib`, defined there in `domain/managedSource.types.ts`): `start` / `loadAllSymbols` / `ensureSymbolLoaded` / `releaseSymbol` / `syncAllSymbols` / `shutdown` / `getInterval`. `ensureSymbolLoaded` returns a `SymbolLoadOutcome` (`'loaded' | 'alreadyLoaded' | 'notOnExchange' | 'noHistory' | 'aborted'`) and **throws on a transient failure** (REST error, timeout) — `notOnExchange` (not in the exchange list) and `noHistory` (listed but no candles yet, e.g. a contract published before trading starts; still fed to the `symbolLoadFailed` digest) are legit absences (empty snapshot is the correct answer — one such symbol must not blank a whole multi-symbol scope into a retry loop), while a throw rolls the refs back and propagates so the client can retry via reconnect (see "subscribe failure" below).

## Canonical source, client and embedded source — moved to `market-data-feeder-lib`

The collection core (`MarketDataManager`), the client (`MarketDataClient`), the embedded source
(`createEmbeddedMarketDataSource`), the wire protocol (codec/messages/validation), the domain
types/constants and the source contracts (`FeederSource`/`StreamLiveness`/`ManagedSource`/
`SymbolLoadOutcome`) live in the sibling repository `../market-data-feeder-lib` (package
`@solncebro/market-data-feeder-lib`) since the 2026-08-25 split — see its `CLAUDE.md` for their
design docs. This server imports all of it as a regular dependency; the barrel is pass-through,
so everything the server needs is importable from the package root.

## Channel (`FeederServer`) + protocol

`src/server/feederServer.ts` is a WebSocket server (`ws`) on top of `SubscriptionRegistry`. It listens on `host:port` and wraps every source in a forwarding shim (`createForwardingSource`): subscribes to the source events and relays them to clients.

**Protocol** (`protocol/messages.types.ts` in `@solncebro/market-data-feeder-lib`, JSON via `protocol/codec.ts` there — `encodeMessage`/`decodeMessage`; `decodeMessage` validates the `type` field against `KNOWN_TYPE_SET`, invalid → `null`):
- Client → server: `subscribe { interval, scope, events, wantMa }`, `unsubscribe { interval, scope }` (the `scope` in unsubscribe is IGNORED — see ownership below).
- Server → client: `snapshot`, `klineClosed`, `klineUpdated`, `klineUpdatedTick`, `symbolAdded`, `symbolRemoved`, `volume24h`, `heartbeat`. A gap repair is delivered as a **mid-stream single-entry `snapshot` chunk with `isFinal: false`** — no new message type; the client's `seedSymbol` replaces the mirror wholesale, and `retainSymbols`/`markReady` react only to `isFinal: true`, so a mid-stream chunk is safe in every client state.

Server behavior:
- **Subscribe validation**: every `subscribe` is checked by `isValidSubscribe` (`protocol/clientMessageValidation.ts` in `@solncebro/market-data-feeder-lib`) before any work — interval must be a known `KlineInterval`, `scope.kind` must be `all`/`symbols`, `symbolList` must be non-empty strings matching `^[A-Z0-9]{1,30}$` within `MAX_SUBSCRIBE_SYMBOL_COUNT = 5000`. Invalid messages are dropped with a warn (no phantom-interval source, no unbounded REST flood).
- **Serialized subscription ops + scope ownership**: all registry work for one client+interval runs strictly one operation at a time on a per-(client, interval) promise chain (`IntervalChainState`, rejection-proof by construction), and the chain **owns the scope it actually acquired** (`acquiredScope`) — releases always use it, never a scope named in a message. The routing record (`subscriptionByInterval`) is still updated synchronously on message receipt (so scope-matched events during loading are queued to the client). `runSubscribeOp`: skip if already superseded; acquire the NEW scope **before** releasing the previous one (overlapping scopes never drop a shared symbol to zero refs — no exchange-subscription churn on a scope extension); skip the snapshot if the client disconnected or was superseded while loading (the newer op sends its own). A disconnect mid-acquire is safe: the close op is queued behind the in-flight subscribe and releases `acquiredScope` once it settles. Then `buildSnapshotMessageList` slices the scoped symbols into chunks of `DEFAULT_SNAPSHOT_CHUNK_SIZE = 100`; each chunk is a separate `snapshot` message, the last marked `isFinal: true` (an empty set → one final empty message).
- **Subscribe failure → client self-healing**: a failed acquire (transient REST error during the load) logs, rolls back the routing record and schedules a socket close with the application code `4001` after `SUBSCRIBE_FAILURE_CLOSE_DELAY_MS = 5_000` — the client's `ReliableWebSocket` reconnects and re-sends the subscribe, forming a natural retry loop; the delay paces it (the transport resets its retry counters on every successful open, so it has no lasting backoff of its own). A symbol that is legitimately absent from the exchange is NOT a failure (`notOnExchange` → it is simply missing from the snapshot, like a delisted one).
- **Unsubscribe ownership**: `unsubscribe` is validated (`isKnownInterval`) and only accepted for an interval this client actually holds; the release always uses the chain-owned scope. A foreign local process can no longer release another client's refs (there is deliberately no channel auth — this closes the practical hole).
- **Scope filter + event filter**: `forwardFeedMessage` sends an event to a client only if it has a subscription for that interval, its `eventNameSet` contains the event name, and `scopeMatchesSymbol` is true (`all` → everyone; `symbols` → only the listed ones). `symbolAdded`/`symbolRemoved`/`volume24h` are filtered by scope but not by the event set (forwarded via `broadcastScoped`).
- **Heartbeat + half-dead-client protection**: every `HEARTBEAT_INTERVAL_MS = 15_000` tick sends a `heartbeat` message, pings each client (`socket.ping()`, `missedPongCount` reset on `pong`; ≥ `DEFAULT_MAX_MISSED_PONG_COUNT = 3` missed → warn + `terminate()` — the ws library answers pings automatically, so only a truly stuck client process misses them, and 3 ticks = 45s of grace for legit pauses), and checks `socket.bufferedAmount`: over `DEFAULT_SLOW_CLIENT_BUFFERED_LIMIT_BYTES = 64 МБ` on TWO consecutive ticks → warn + `terminate()` (two ticks so a legit one-off burst like a full snapshot is never punished; the check rides the tick, NOT `sendRaw`, for the same reason). `terminate()` fires `close` → the release chain frees the refs. `heartbeatIntervalMs` / `maxMissedPongCount` / `slowClientBufferedLimitBytes` / `subscribeFailureCloseDelayMs` are injectable via `FeederServerArgs` (test seams).
- **Symbol-list sync**: `SYMBOL_LIST_SYNC_INTERVAL_MS = 3_600_000` (hourly) → `registry.syncAllLoadedSources()`.
- **Client disconnect** (`close`) queues a release op per interval on the client's chains (freeing exactly the acquired scopes, even those still being acquired at disconnect time), then drops the chain entries so a dead client is not retained by its closures.
- **Connection hardening**: the WS server is created with `maxPayload = DEFAULT_MAX_PAYLOAD_BYTES (256 KB)` (a subscribe is tiny; bounds a hostile-frame memory spike) and `handleConnection` rejects sockets beyond `DEFAULT_MAX_CONNECTIONS (64)`. Both are overridable via `FeederServerArgs.maxPayloadBytes` / `maxConnections` (tests pass small values).
- `start()` awaits `listening`, records the actually bound port (`getPort()` — important when `port: 0` in tests). The startup `listening`/`error` race uses named handlers (the error handler is removed once `listening` fires), then a **persistent** `error` handler is attached that logs server-level errors (the old race-only `once('error')` would swallow the first post-startup error and let a second crash the process). `shutdown()` stops the timers, closes clients, closes the WS server, and calls `registry.shutdown()`.

`src/feeder.ts` (entry): `loadEnvConfig` → `installCrashHandlers` → `createRestartGuard` → `createHealthMonitor` (with late-bound `sendAlert`/`shutdown`) → `new ExchangeConnector(name, ..., onNotify, undefined, klineWatchdogConfig)` → `initialize()` → `new FeederServer({ ..., onHealthEvent })` → `start()` → `createTelegramControlBot` → `controlBot.start()` (**non-fatal**, wrapped in try/catch — see the bot section) → `healthMonitor.report('feederReady')`. SIGINT/SIGTERM → graceful `shutdown(reason?, exitCode = 0)`. **Shutdown always reaches `process.exit`**: an `isShuttingDown` flag makes it re-entry-safe (first caller wins, including its exit code — a concurrent escalation restart and an operator SIGTERM cannot run two overlapping teardowns), and the four steps (monitor → bot → server → connector) run through `runShutdownStepsBestEffort` (`src/utils/shutdownSteps.ts`) — each step individually try/caught, so a throwing step can no longer strand a zombie process with the health monitor already stopped; `runWithHardExit(SHUTDOWN_HARD_EXIT_MS = 15s)` stays as the belt. **Exit code by intent**: the self-healing auto-restart (`onRestart`) calls `shutdown(reason, 1)` so a `Restart=on-failure` supervisor relaunches the self-healed process; operator SIGINT/SIGTERM and the manual bot Restart exit `0` (a deliberate stop should not loop-relaunch). The logger is `logger` from `@solncebro/trade-engine` (adapted into `FeederLogger`).

## Telegram control and monitoring bot (`src/telegram/`)

The bot is **mandatory** (without `TELEGRAM_BOT_TOKEN`/`TELEGRAM_ALLOWED_CHAT_IDS` the process fails on startup). It lives **in the same process** as the feeder and reads status directly from `FeederServer`. It is built on the ready-made factories of `@solncebro/telegram-engine` (`createBotRegistry`, `registerBotCommands`, `createCallbackEncoder`, `createKeyboardBuilder`, `createMenuRouter`, `createInputStateManager`) — there is no custom transport/Telegraf wiring. **All bot-visible text is English.**

- **`telegramControlBot.ts`** — `createTelegramControlBot({ botToken, allowedChatIdList, statusProvider, exchangeName, logger, onReboot })` → `{ start, stop, sendAlert }`. `statusProvider` is the structural contract `FeederStatusProvider` (`getStatus` + `getStaleSymbolList` + `getSymbolDiagnostics`), satisfied by `FeederServer`. The whitelist is mandatory (via `registry.accessControl`); disallowed chat ids are silently ignored by the engine and by the manual reply-keyboard `guard`. `createTelegramControlBot` **throws on an empty `allowedChatIdList`** (fail-closed — a second, independent guard so the engine's fail-open-on-empty-set default can never be reached, even if env validation is bypassed).
- **Non-fatal, self-retrying start** — a Telegram outage must never take the data feeder down. `start()` never rejects: the local handler wiring runs once, the network phase (`registerBotCommands` → its first step is the network `setMyCommands`, so a rejected attempt wires no middleware and is safe to re-run; then the long-lived `launchAll`) goes through `createResilientStarter` (`src/utils/resilientStarter.ts`) with `BOT_START_RETRY_MS = 60_000` background retries. A later polling death (surfaced via `createBot`'s `onError`) also schedules a relaunch through the same starter. `stop()` never throws either (`stopAll` on a never-launched bot throws "Bot is not running!" — caught and logged), so an operator SIGTERM during a Telegram outage still exits `0` and shuts the server down cleanly. `feeder.ts` additionally wraps `controlBot.start()` in try/catch as a belt.
- The connectivity probe (`channelConnectivityMonitor`) is time-boxed (`probeTimeoutMs`, default 10s via `withTimeout`) — a probe that never settles (half-open connection) counts as a failure instead of silently killing the monitor (the next check is scheduled only from the current probe's settlement).
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
- **Mass staleness** (`MarketDataManager`): after every staleness scan `evaluateMassStale` checks if `staleCount / symbolCount ≥ MASS_STALE_RATIO_THRESHOLD (0.3)` and `symbolCount ≥ MASS_STALE_MIN_SYMBOLS (20)` (via `crossesMassStaleThreshold` in `source/massStale.ts` in `@solncebro/market-data-feeder-lib`). Edge-triggered `sourceMassStale` (arms escalation) / `sourceMassStaleRecovered` (clears it).
- **Library transport notifications**: `onNotify` (3rd argument of `ExchangeConnector`) → `transportNotify`. The raw library text is sanitized of MarkdownV2 marker chars (`` ` `` `*` `_` `~` `|`) before interpolation so it can never break the rendered message. Critical markers are narrowed to `critical`/`fatal`/`max retries` — the broad `failed`/`exceeded` were removed so a per-symbol watchdog scan summary (which contains a `Failed: N` line even on partial recovery) no longer latches an irreversible whole-process restart. Everything else is an alert only — except the kline-watchdog scan summaries (text with `kline` + `overdue`/`recover`), which are **dropped** here because the structured per-symbol events already feed the digest. The whole-source silence watchdog remains the backstop for a genuinely dead transport.
- **trade-engine kline watchdog**: `klineWatchdogConfig` (5th argument) with `graceMs = 180s` / `recoveryCooldownMs = 300s` (raised from the library defaults of 60s/120s so sparse/low-liquidity symbols stop churning overdue→recovery every cycle — that churn was the dominant log-volume source) and `onStreamStale`/`onStreamRecovered`/`onStreamRecoveryFailed` → `klineStreamStale`/`klineStreamRecovered`/`klineStreamRecoveryFailed`. These are **per-symbol** signals — they feed the 30-min digest and **none escalate to a restart**; only `klineStreamRecoveryFailed` alerts immediately. The watchdog's own per-recovery `INFO` log lines were downgraded to `debug` inside `trade-engine` (the per-scan summary remains) to stop the logger overflowing on persistently-quiet symbols.
- **Stuck symbol**: `MarketDataManager` emits `persistentStaleSymbol` (threshold `PERSISTENT_STALE_THRESHOLD_TICK_COUNT = 10` ≈ 10 min) and the symmetric `persistentStaleRecovered` when a fresh candle returns for a previously-flagged symbol (tracked via `persistentStaleEmittedSet`).
- **On-demand load + readiness**: `MarketDataManager` emits `intervalLoadStarted`/`intervalLoadCompleted` (per-interval; `intervalLoadCompleted` now carries the ACTUAL loaded count, not the requested count) and `symbolLoadCompleted` (per-symbol on-demand / new listing); `feeder.ts` emits `feederReady` after startup.
- **Failed loads** (`symbolLoadFailed`): a REST load that returns empty or throws — in the bulk `loadAllSymbols` path (each requested-but-not-buffered symbol) and in on-demand `ensureSymbolLoaded` — emits `symbolLoadFailed`, which feeds the 30-min digest (silent, non-escalating) rather than an immediate alert, while `intervalLoadCompleted` reports the ACTUAL loaded count so a partial load is not hidden.
- **Crash handlers** (`src/health/crashHandlers.ts`): `installCrashHandlers` wires `process.on('uncaughtException')` and `process.on('unhandledRejection')` — each writes to `stderr` synchronously, sends a best-effort Telegram alert with a grace period (`CRASH_ALERT_GRACE_MS = 3s`), then calls `process.exit(1)`. A re-entrancy guard prevents double-handling.

Source health events are relayed by `FeederServer` (`createForwardingSource` → `emitHealthEvent`) into the injected `FeederServerArgs.onHealthEvent`; transport and watchdog signals go into `HealthMonitor` directly from `feeder.ts`.

**`HealthMonitor`** (`src/health/healthMonitor.ts`) is the single point and the **only** path to Telegram (everything funnels into `sendAlert` → `Broadcaster.sendToAll(message, true)` as MarkdownV2). It classifies `HealthEvent` (a discriminated union) and routes each by cardinality:
- **Per-symbol signals are silent — they feed a 30-min digest, not immediate alerts.** `klineStreamStale`/`Recovered`, `persistentStaleSymbol`/`Recovered`, `symbolLoadFailed` and `symbolBackfillStuck`/`Recovered` (undergrown backfill latched as converged / caught up OR its short history was accepted as the exchange's truth — tracked by name in `stuckBackfillByKey`) accumulate into a rolling window (`windowDegradedKeySet` / `windowRecoveredKeySet`); `symbolLoadCompleted` is dropped. The ONLY per-symbol signal that alerts immediately is `klineStreamRecoveryFailed` (a stream the watchdog could not re-establish) — batched (`batchFlushMs = 3000`) into one `🛑 Stream recovery failed` message and tracked in `unrecoveredByKey` until that stream recovers. Rationale: a self-healing per-symbol blip showed the problem but never its resolution → constant false alarm; now the problem is quiet too, and only a genuine non-recovery is loud.
- **Digest** (`digestIntervalMs = 30 min`, `emitDigest`): one periodic report — `📋 Kline streams report: N degraded, M recovered` plus a `🛑 Still unrecovered (…)` list of streams that never came back and a `🕳️ Still under-filled (…)` list of symbols whose backfill converged below the expected history (both by name, until recovery); a clean window sends `✅ Kline streams healthy`. This replaced the previous 5-min per-symbol alert spam.
- **Per-interval / global events stay immediate**: `streamSilent`/`streamResumed`, `sourceMassStale`/`sourceMassStaleRecovered`, `intervalLoad*`, `feederReady`, and critical `transportNotify`. Every message is escaped once centrally in `emitAlert` (`escapeMarkdownV2WithFormatting`).
- **Escalation → restart** is decoupled from alerting (`setDegraded`/`clearDegraded` track degradation silently; messages go through the batchers/immediate path). Only whole-source signals arm it: `streamSilent` (whole-source silence), `sourceMassStale` (≥ `MASS_STALE_RATIO_THRESHOLD` of symbols stale), and critical `transportNotify`. Per-symbol watchdog failures do **not**. On `shutdown()` the pending batch is flushed so accumulated messages are not lost. **Grace is per-degradation** (`degradedSinceMsByKey`): escalation fires only when the OLDEST active degradation has outlived `recoveryGraceMs` — a later degradation gets its full grace even if an earlier one armed the timer and then cleared; the start time is first-report-wins (a repeated critical `transportNotify` cannot push the deadline forward). When `RestartGuard` blocks a restart, the "manual intervention" alert is deduped and the timer **re-arms** — once the guard window rolls over, a still-active degradation restarts without needing a fresh trigger.
- **Delisting scrub**: the server forwards the source `symbolRemoved` as a `symbolDelisted` health event; the monitor removes the `interval:symbol` key from the digest bookkeeping (`unrecoveredByKey`, `stuckBackfillByKey`, window sets, pending batch) — a delisted stream can never emit a recovery, so it must not sit in "Still unrecovered" forever. `symbolSyncAnomaly` (deferred mass removal — see the source section) alerts immediately with dedup, without escalation.

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

`loadEnvConfig()` throws on a missing required var, an invalid `EXCHANGE_NAME`, an invalid port, or an empty chat-id list. All values are **trimmed** and whitespace-only counts as missing (a trailing space smuggled in by a shell/systemd unit must not fail validation with a confusing message). Loaded via `dotenv/config`.

**Logging:** the feeder uses the shared singleton `logger` from `@solncebro/trade-engine` (console + file + optionally BetterStack by env — like ma-chaser/volume-breaker/rubber). pino transports run in a worker thread, so **fatal startup errors are written synchronously to `console.error`** (`feeder.ts`, `main().catch`) — otherwise `logger.error` right before `process.exit(1)` is lost and the process dies silently (e.g. when `TELEGRAM_BOT_TOKEN` is missing).

## How the three apps consume it

Each app creates a `MarketDataClient` per interval it needs (imported from `@solncebro/market-data-feeder-lib`) and switches its data flow to the feeder instead of its own subscriptions:
- **ma-chaser** — 30m + 5m + 4h (source behind the `MARKET_DATA_SOURCE=feeder` / `MARKET_DATA_FEEDER_URL` flag; delisting handled via the `symbolRemoved` event).
- **volume-breaker** — 30m.
- **rubber** — 30m; on a single-app machine it can instead run the core in-process via `createEmbeddedMarketDataSource` (`MARKET_DATA_SOURCE=embedded` on its side) — no feeder process needed there.

One feeder process per exchange serves all apps of that exchange; they incrementally load only the intervals/symbols they need (ref-count + debounced teardown guarantee that unused intervals are released).

**A fourth consumer speaks the wire protocol WITHOUT the package**: `kliner-autotrade-funding` has its own in-house client (`src/infrastructure/marketDataFeeder/` in that repo — `feederKlineClient.ts` on `ReliableWebSocket`, behind its `USE_MARKET_DATA_FEEDER` flag; 1m subscribes `klineClosed`+`klineUpdatedTick`, other intervals closed-only, `wantMa: false`). Any protocol change must stay compatible with it, not only with `MarketDataClient`. Its client mirrors the package client's staleness self-heal (interval-aware threshold, pre-ready floor, transport recreation).

## Stack and commands

| | |
|---|---|
| Runtime | Node.js (ESM, `type: module`, NodeNext) |
| TypeScript | ^5.7, strict |
| Library | `@solncebro/market-data-feeder-lib` (collection core, protocol, contracts — see its `CLAUDE.md`) |
| Transport | `ws` (server side of the channel) |
| Exchange | `@solncebro/trade-engine` (`ExchangeConnector`, `logger`, `ExchangeNameEnum`, `MarketTypeEnum`) |
| Telegram | `@solncebro/telegram-engine` (factories over Telegraf: bot, menu, callback routing) |
| Tests | vitest (`src/__tests__/` — 12 unit + 4 integration + `teardownDuringLoad`, which wires the registry to the lib's real `MarketDataManager`) |

`yarn dev` (tsx `src/feeder.ts`) · `yarn build` (lint + test + `tsc`) · `yarn start` (`dist/feeder.js`) · `yarn lint` / `yarn typecheck` (`tsc --noEmit`) · `yarn test` / `yarn test:watch`.

## `src/` structure

```
src/
  feeder.ts                       # entry: ExchangeConnector + FeederServer + HealthMonitor + TelegramControlBot + graceful shutdown
  config/env.ts + .types.ts       # EnvConfig + loadEnvConfig
  server/
    feederServer.ts + .types.ts   # WebSocket server: snapshot / scope+event filter / heartbeat / symbol sync / getStatus / getSymbolDiagnostics
    subscriptionRegistry.ts + .types.ts  # on-demand + ref-count + debounced teardown + getRegistrationStatusList
  telegram/
    telegramControlBot.ts + .types.ts    # control/monitoring bot on @solncebro/telegram-engine (reply-keyboard hybrid + sendAlert)
    statusFormatter.ts            # formatOverviewMessage / formatIntervalDetailMessage / formatSymbolCard / formatStaleSymbolMessage / resolveIntervalHealth
    menu.types.ts                 # MenuStep / CallbackData
    channelConnectivityMonitor.ts # background probe that detects when Telegram channel is unreachable
  health/
    healthMonitor.ts + .types.ts  # HealthMonitor: per-symbol batching + immediate per-interval alerts + escalation (whole-source only + mass stale) → grace → restart
    restartGuard.ts               # file-backed auto-restart counter (loop guard)
    crashHandlers.ts              # installCrashHandlers: uncaughtException/unhandledRejection → stderr + alert + exit 1
  utils/
    hardExit.ts                   # runWithHardExit (wraps async shutdown with a hard-timeout fallback)
    resilientStarter.ts           # createResilientStarter (never-fatal start with background retries — the Telegram bot)
    shutdownSteps.ts              # runShutdownStepsBestEffort (each teardown step try/caught individually)
  __tests__/                      # vitest: unit (channelConnectivityMonitor, crashHandlers, env, hardExit, healthMonitor, resilientStarter, restartGuard, shutdownSteps, statusFormatter, subscriptionRegistry, teardownDuringLoad, telegramControlBotGuard) + integration (feederChannel, slowClientProtection, subscribeSelfHeal, subscriptionOwnership)
```

The collection core, client, embedded source, protocol, domain types/constants and `withTimeout`/`startIntervalScheduler` live in `../market-data-feeder-lib` — see its `CLAUDE.md`.

## Conventions

- **Interfaces without the `I` prefix** (`EnvConfig`, `FeederSource`, `MarketDataSource`, `ManagedSource`, `Kline`, `MaValues`).
- Booleans — `is*` / `has*` / `should*`; never `bar`, only `kline`/`klineClosed`/`klineUpdated`.
- Collections — `*BySymbol` / `*ByInterval` / `*List` / `*Set`.
- Only `info` / `warn` / `error`; the symbol and key parameters go in the log text, not only in the JSON payload.
- All relative quantities are in percent, not bps (project rule).
- All bot-visible text (menus, status, alerts, command descriptions) is English; no Cyrillic anywhere in the codebase.

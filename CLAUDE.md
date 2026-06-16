# CLAUDE.md — market-data-feeder

> Гид по архитектуре и разработке. Пользовательская инструкция (как запустить, как подключиться из приложения) — в `README.md`; здесь — внутреннее устройство процесса.

## Назначение

`@solncebro/market-data-feeder` — **общий раздатчик рыночных данных**. Один процесс на биржу держит **единственный** набор биржевых kline-подписок (`ExchangeConnector` из `@solncebro/trade-engine`), считает скользящие средние и тянет 24h-объём, и раздаёт всё это нескольким стратегическим приложениям по локальному WebSocket-каналу. Цель — убрать дублирование: раньше каждое приложение (ma-chaser, volume-breaker, rubber) само открывало ~1200 kline-подписок на символ; теперь подписки открывает только фидер, а приложения зеркалят поток в ОЗУ.

Деплой зеркалит существующую посимвольную модель: **один процесс фидера на биржу** (`binance` / `bybit`), `EXCHANGE_NAME` задаёт биржу.

Две половины пакета:
- **Сервер (фидер)** — entry `src/feeder.ts` (`dist/feeder.js`). Держит `ExchangeConnector`, грузит данные **по требованию** и обслуживает клиентов.
- **Клиент (`MarketDataClient`)** — импортируется приложениями из barrel `src/index.ts`. Зеркалит поток в ОЗУ и отдаёт тот же read-API + события, что и in-process market-data-менеджер, так что код стратегий не меняется.

## Загрузка по требованию + ref-counting + отложенный teardown (`SubscriptionRegistry`)

`src/server/subscriptionRegistry.ts` — generic-реестр (`SubscriptionRegistry<TSource extends ManagedSource>`), ядро «по требованию». Один `IntervalRegistration` на интервал хранит canonical-source и счётчики ссылок:
- `allSubscriberCount` — сколько клиентов подписаны на `scope.kind === 'all'`.
- `symbolRefCountBySymbol` — ref-count для точечных `scope.kind === 'symbols'`.
- `isAllLoaded` / `allLoadPromise` — флаг и in-flight-промис полной загрузки всех символов.
- `symbolLoadPromiseBySymbol` — in-flight-промисы точечной загрузки символа (дедуп параллельных запросов на один символ).

Логика:
- **`subscribe`** отменяет отложенный teardown интервала, создаёт `IntervalRegistration` при первом обращении (`createSource` + `source.start()`). Для `all` — инкремент `allSubscriberCount` + `ensureAllLoaded` (грузит ВСЕ символы один раз, повторные подписчики ждут тот же `allLoadPromise`). Для `symbols` — `acquireSymbol` на каждый символ: инкремент ref-count, и `ensureSymbolLoaded` только на первой ссылке (если интервал не загружен целиком). Если интервал уже `isAllLoaded` — точечная загрузка пропускается, символ уже есть.
- **`unsubscribe`** уменьшает соответствующий счётчик (`all` → декремент; `symbols` → `releaseSymbolRef` на каждый символ, при ref-count → 0 и отсутствии `all`-подписчиков зовёт `source.releaseSymbol`), затем `teardownIfIdle`.
- **`teardownIfIdle`** — когда `allSubscriberCount === 0` И `symbolRefCountBySymbol.size === 0`: отложенный teardown через `setTimeout(DEFAULT_TEARDOWN_DELAY_MS = 30_000)` (дебаунс — чтобы переподключение клиента не пересоздавало source). По срабатыванию таймер перепроверяет, что интервал всё ещё idle и это та же регистрация, и зовёт `source.shutdown()`. `teardownDelayMs <= 0` → синхронный teardown без дебаунса.
- **`syncAllLoadedSources`** — для каждого полностью загруженного интервала зовёт `source.syncAllSymbols()` (листинги/делистинги). Дёргается сервером по часовому таймеру.
- **`shutdown`** — чистит все teardown-таймеры и зовёт `source.shutdown()` по каждой регистрации.

`createSource` инжектится сверху (фабрика `MarketDataManager`), реестр сам по себе не знает биржи. Контракт source — `ManagedSource` (`src/server/subscriptionRegistry.types.ts`): `start` / `loadAllSymbols` / `ensureSymbolLoaded` / `releaseSymbol` / `syncAllSymbols` / `shutdown` / `getInterval`.

## Canonical source (`MarketDataManager`)

`src/source/marketDataManager.ts` — единственный, кто реально подписан на биржу. `EventEmitter`, реализует `FeederSource` (= `ManagedSource` + read-методы + типизированные `on`/`off`). Один экземпляр на интервал, создаётся реестром.

- **Буфер** `KLINE_BUFFER_SIZE = 499` свечей на символ (`klineListBySymbol`), плюс `maValuesBySymbol`, `currentKlineBySymbol`, `subscriptionBySymbol`, `handlerBySymbol`.
- **Загрузка**: `loadAllSymbols` берёт `getFuturesSymbols()`, фильтрует `endsWith('USDT')`, грузит REST батчами (`FETCH_BATCH_SIZE = 200`, пауза `FETCH_BATCH_DELAY_MS = 300`, таймаут одного fetch `LOAD_KLINES_TIMEOUT_MS = 60_000` через `withTimeout`), затем `subscribeToKlines`. `ensureSymbolLoaded(symbol)` — то же для одного символа по требованию (no-op если уже в буфере; возвращает `true` при успешной загрузке).
- **MA**: `calculateAllMaValues` (`src/source/indicators.ts`) — SMA по `closePrice` для периодов 25/50/100/200 (`MaValues`). Пересчёт на каждом kline-событии, кроме throttled-веток.
- **`handleKline`** (обёрнут try/catch, чтобы исключение по одному символу не убивало SDK-callback-loop) → `handleKlineUnsafe`: guard `isOlder` (реплеенная свеча старее последней в буфере → инкремент `skippedOlderKlineCountBySymbol` и return), переход свечи (`isNewCandle`) закрывает предыдущую и эмитит `klineClosed`, throttle MA-пересчёта `KLINE_UPDATE_THROTTLE_MS = 5000` (на throttled-такте MA берётся из кэша, считается `throttledMaRecomputeCount`), эмиты `klineUpdated` + `klineUpdatedTick`.
- **Эмиты под guard свежести** (`isKlineFresh`: возраст ≤ `STALENESS_THRESHOLD_MULTIPLIER × intervalMs`): `emitGuarded` (closed/updated) и `emitTickGuarded` (tick) пропускают устаревшие/реплеенные свечи — это защищает downstream-потребителей в приложениях от исторических реплеев.
- **Staleness watchdog** (`startIntervalScheduler`, `STALENESS_CHECK_INTERVAL_MS = 60_000`, heartbeat каждые `STALENESS_HEARTBEAT_EVERY_N_TICKS = 15` тиков): считает `consecutiveStaleScanCountBySymbol`; по достижении `PERSISTENT_STALE_THRESHOLD_TICK_COUNT = 10` зовёт опциональный `onPersistentStaleSymbol`-callback. Heartbeat дополнительно флешит агрегат пропущенных older-свечей.
- **`syncAllSymbols`**: `getFuturesSymbols()` → `computeSymbolListDelta` (`src/source/symbolListDelta.ts`, чистая дельта added/removed) → для added `ensureSymbolLoaded` + `emit('symbolAdded')`, для removed `emit('symbolRemoved')` + `releaseSymbol`.
- **`releaseSymbol`**: `unsubscribeKlines` + полная чистка всех per-symbol-Map для этого символа.
- **24h-объём** — `getVolume24h` читает `exchangeConnector.getTicker(symbol, Futures)?.quoteVolume` (cache-miss → `+Infinity`); собственной подписки на тикеры нет.
- Источник интервалов в мс — `resolveIntervalMs` (`src/domain/constants.ts`, карта `INTERVAL_MS_BY_KEY`).

## Канал (`FeederServer`) + протокол

`src/server/feederServer.ts` — WebSocket-сервер (`ws`) поверх `SubscriptionRegistry`. Слушает `host:port`, оборачивает каждый source в forwarding-обвязку (`createForwardingSource`): подписывается на события source и ретранслирует их клиентам.

**Протокол** (`src/protocol/messages.types.ts`, JSON через `src/protocol/codec.ts` — `encodeMessage`/`decodeMessage`; `decodeMessage` валидирует поле `type` против `KNOWN_TYPE_SET`, невалидное → `null`):
- Клиент → сервер: `subscribe { interval, scope, events, wantMa }`, `unsubscribe { interval, scope }`.
- Сервер → клиент: `snapshot`, `klineClosed`, `klineUpdated`, `klineUpdatedTick`, `symbolAdded`, `symbolRemoved`, `volume24h`, `heartbeat`.

Поведение сервера:
- **Snapshot при подписке**: `handleSubscribe` → `registry.subscribe` → `buildSnapshotMessageList` режет символы по scope на чанки `DEFAULT_SNAPSHOT_CHUNK_SIZE = 100`; каждый чанк — отдельное `snapshot`-сообщение, последний помечен `isFinal: true` (пустой набор → одно финальное пустое сообщение). Запись подписки клиента (`scope` + `Set<FeedEventName>`) хранится per-interval.
- **Scope-фильтр + event-фильтр**: `forwardFeedMessage` отправляет событие клиенту только если у него есть подписка на этот интервал, `eventNameSet` содержит имя события и `scopeMatchesSymbol` истинно (`all` → всем; `symbols` → только перечисленным). `symbolAdded`/`symbolRemoved` фильтруются по scope, но не по event-set.
- **Heartbeat**: `HEARTBEAT_INTERVAL_MS = 15_000` — всем клиентам шлётся `heartbeat` (служит сигналом живости для клиентского stale-детектора).
- **Symbol-list sync**: `SYMBOL_LIST_SYNC_INTERVAL_MS = 3_600_000` (час) → `registry.syncAllLoadedSources()`.
- **Отключение клиента** (`close`) снимает все его подписки через `registry.unsubscribe` — что и запускает ref-count/teardown.
- `start()` ждёт `listening`, фиксирует реально занятый порт (`getPort()` — важно при `port: 0` в тестах). `shutdown()` гасит таймеры, закрывает клиентов, закрывает WS-сервер, зовёт `registry.shutdown()`.

`src/feeder.ts` (entry): `loadEnvConfig` → `new ExchangeConnector(name, { apiKey, secret })` → `initialize()` → `new FeederServer({ port, host, logger, createSource: (interval) => new MarketDataManager(exchangeConnector, interval) })` → `start()`. SIGINT/SIGTERM → graceful `server.shutdown()` + `exchangeConnector.disconnect()`. Логгер — `logger` из `@solncebro/trade-engine` (адаптирован в `FeederLogger`).

## Клиент (`MarketDataClient`)

`src/client/marketDataClient.ts` — `EventEmitter`, реализует `MarketDataSource` (`src/client/marketDataSource.types.ts`), in-RAM зеркало канала. Создаётся **по одному на нужный приложению интервал**.

- Транспорт — `ReliableWebSocket` из `@solncebro/websocket-engine` (авто-reconnect + stale-детекция). На `onOpen` шлёт `subscribe`-сообщение (`interval` / `scope` / `events` / `wantMa` из `MarketDataClientArgs`).
- **`MirrorStore`** (`src/client/mirrorStore.ts`) хранит буферы свечей (cap `KLINE_BUFFER_SIZE`), MA, текущую формирующуюся свечу и 24h-объём. `applySnapshot`/`applySymbolAdded` сидят символы; `applyKlineClosed`/`applyKlineUpdated`/`applyKlineTick` обновляют буфер (через `applyKlineToBuffer`: older → drop, тот же `openTimestamp` → replace, новый → push с обрезкой); `retainSymbols` после финального snapshot выкидывает символы вне снапшота.
- **Те же события наружу**: на каждое серверное сообщение клиент применяет его к зеркалу и ре-эмитит `klineClosed` / `klineUpdated` / `klineUpdatedTick` / `symbolAdded` / `symbolRemoved` — идентично in-process менеджеру, поэтому стратегический код переключается на фидер без правок.
- **Read-API** `MarketDataSource`: `getInterval` / `getIntervalMs` / `getMaValues` / `getKlineList` / `getCurrentKline` / `getSymbolList` / `getLastKlineOpenTimestamp` / `getLastUpdateTimestamp` / `getVolume24h` / `isStale` / `getStaleSymbolList`.
- `waitUntilReady(timeoutMs = 30_000)` резолвится по первому `snapshot` с `isFinal: true` (или reject по таймауту). `isStale` — true до финального snapshot либо если последнее сообщение старше `CLIENT_STALE_THRESHOLD_MS = 45_000` (проверка каждые `CLIENT_STALE_CHECK_INTERVAL_MS = 15_000`).

## Env (`src/config/env.ts`)

| Var | Обязателен | Default | Назначение |
|---|---|---|---|
| `EXCHANGE_NAME` | да | — | `binance` или `bybit` (валидируется против `ExchangeNameEnum`) |
| `EXCHANGE_API_KEY` / `EXCHANGE_SECRET` | да | — | read-only ключ (только публичные данные) |
| `MARKET_DATA_FEEDER_PORT` | нет | `7070` | порт локального канала (positive integer) |
| `MARKET_DATA_FEEDER_HOST` | нет | `127.0.0.1` | хост локального канала |

`loadEnvConfig()` бросает на отсутствующий обязательный var, невалидный `EXCHANGE_NAME` или невалидный порт. Загружается через `dotenv/config`.

## Как потребляют три приложения

Каждое приложение создаёт `MarketDataClient` на каждый нужный интервал и переключает поток данных на фидер вместо собственных подписок:
- **ma-chaser** — 30m + 5m + 4h (источник за флагом `MARKET_DATA_SOURCE=feeder` / `MARKET_DATA_FEEDER_URL`; делистинг отрабатывается по событию `symbolRemoved`).
- **volume-breaker** — 30m.
- **rubber** — 30m.

Один фидер-процесс на биржу обслуживает все приложения этой биржи; они инкрементально подгружают только нужные интервалы/символы (ref-count + дебаунс-teardown гарантируют, что неиспользуемые интервалы освобождаются).

## Стек и команды

| | |
|---|---|
| Runtime | Node.js (ESM, `type: module`, NodeNext) |
| TypeScript | ^5.7, strict |
| Транспорт | `ws` (сервер) + `@solncebro/websocket-engine` (клиент) |
| Биржа | `@solncebro/trade-engine` (`ExchangeConnector`, `logger`, `ExchangeNameEnum`, `MarketTypeEnum`) |
| Тесты | vitest (`src/__tests__/`) |

`yarn dev` (tsx `src/feeder.ts`) · `yarn build` (lint + test + `tsc`) · `yarn start` (`dist/feeder.js`) · `yarn lint` / `yarn typecheck` (`tsc --noEmit`) · `yarn test` / `yarn test:watch`.

## Структура `src/`

```
src/
  feeder.ts                       # entry: ExchangeConnector + FeederServer + graceful shutdown
  index.ts                        # barrel: MarketDataClient + публичные типы
  config/env.ts + .types.ts       # EnvConfig + loadEnvConfig
  domain/
    constants.ts                  # KLINE_BUFFER_SIZE, STALENESS_THRESHOLD_MULTIPLIER, resolveIntervalMs
    marketData.types.ts           # Kline, KlineInterval, MaValues, StaleSymbolInfo
    events.types.ts               # listener-типы source
    snapshot.types.ts             # MarketDataSnapshotEntry
    subscription.types.ts         # SubscriptionScope, FeedEventName
  server/
    feederServer.ts + .types.ts   # WebSocket-сервер: snapshot / scope+event фильтр / heartbeat / symbol sync
    subscriptionRegistry.ts + .types.ts  # on-demand + ref-count + дебаунс-teardown
    feederSource.types.ts         # FeederSource (контракт canonical-source)
  source/
    marketDataManager.ts          # canonical source: подписки/буфер/MA/staleness/sync
    indicators.ts                 # calculateAllMaValues (SMA 25/50/100/200)
    symbolListDelta.ts + .types.ts # computeSymbolListDelta (added/removed)
  client/
    marketDataClient.ts + .types.ts      # in-RAM зеркало канала, ре-эмит событий
    marketDataSource.types.ts            # MarketDataSource (общий read+event контракт)
    mirrorStore.ts + .types.ts           # буферы зеркала на стороне клиента
  protocol/
    messages.types.ts             # ClientMessage / ServerMessage / FeederMessage
    codec.ts                      # encodeMessage / decodeMessage (+ валидация type)
  utils/
    intervalScheduler.ts          # startIntervalScheduler (tick + heartbeat + error-handling)
    timeout.ts                    # withTimeout
  __tests__/                      # vitest (codec, mirrorStore, subscriptionRegistry, symbolListDelta, feederChannel)
```

## Соглашения

- **Интерфейсы без префикса `I`** (`EnvConfig`, `FeederSource`, `MarketDataSource`, `ManagedSource`, `Kline`, `MaValues`).
- Boolean — `is*` / `has*` / `should*`; не `bar`, только `kline`/`klineClosed`/`klineUpdated`.
- Коллекции — `*BySymbol` / `*ByInterval` / `*List` / `*Set`.
- Только `info` / `warn` / `error`; символ и ключевые параметры — в тексте лога, не только в JSON-payload.
- Все относительные величины — в процентах, не в bps (правило проекта).

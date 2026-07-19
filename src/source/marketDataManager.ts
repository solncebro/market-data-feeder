import { EventEmitter } from 'node:events';

import { MarketTypeEnum, RateLimitedRequestQueue, TradifiSymbolGate, logger } from '@solncebro/trade-engine';
import type { ExchangeConnector, Kline, SubscribeKlinesArgs } from '@solncebro/trade-engine';

import type { KlineInterval, MaValues, StaleSymbolInfo } from '../domain/marketData.types.js';
import { KLINE_BUFFER_SIZE, STALENESS_THRESHOLD_MULTIPLIER, resolveIntervalMs } from '../domain/constants.js';
import { calculateAllMaValues } from './indicators.js';
import { crossesMassStaleThreshold } from './massStale.js';
import { computeSymbolListDelta } from './symbolListDelta.js';
import { evaluateSymbolRemovalList } from './symbolRemovalGuard.js';
import { mergeRepairedKlines } from './gapRepair.js';
import { withTimeout } from '../utils/timeout.js';
import { startIntervalScheduler } from '../utils/intervalScheduler.js';
import type { IntervalSchedulerHandle } from '../utils/intervalScheduler.js';
import type { FeederSource, StreamLiveness } from '../server/feederSource.types.js';
import type { SymbolLoadOutcome } from '../server/subscriptionRegistry.types.js';

const KLINE_UPDATE_THROTTLE_MS = 5000;
const STALENESS_CHECK_INTERVAL_MS = 60_000;
const STALENESS_HEARTBEAT_EVERY_N_TICKS = 15;
const LOAD_KLINES_TIMEOUT_MS = 60_000;
const PERSISTENT_STALE_THRESHOLD_TICK_COUNT = 10;
const KLINE_LOAD_PROGRESS_LOG_EVERY = 100;

const SILENCE_CHECK_INTERVAL_MS = 10_000;
const SILENCE_THRESHOLD_MS = 45_000;

const VOLUME_REFRESH_INTERVAL_MS = 30_000;

const MASS_STALE_RATIO_THRESHOLD = 0.3;
const MASS_STALE_MIN_SYMBOLS = 20;

// Gap repair: a candle missed by the stream (or sealed locally without the exchange's closing
// frame) is healed by a REST refetch merged into the buffer, then pushed to clients as a reseed.
// The per-symbol cooldown bounds the REST cost of sparse symbols whose "gaps" are legit (the
// exchange never created the candle), and the concurrency cap keeps a mass-gap avalanche after a
// transport blip from monopolizing the shared backfill pacer.
const GAP_REPAIR_COOLDOWN_MS = 600_000;
const MAX_CONCURRENT_GAP_REPAIRS = 3;
const GAP_REPAIR_BASE_FETCH_COUNT = 2;

// Fallback pacer for the no-arg constructor (tests). Production injects an exchange-tuned queue from
// feeder.ts (resolveBackfillRequestsPerSecond) — Binance must run slower than this to stay under its
// per-IP REQUEST_WEIGHT budget.
const KLINE_BACKFILL_REQUESTS_PER_SECOND = 80;

const DEFAULT_BACKFILL_QUEUE = new RateLimitedRequestQueue({
  rateLimit: KLINE_BACKFILL_REQUESTS_PER_SECOND,
  intervalMs: 1000,
  loggerLabel: '[MarketData:backfill]',
});

const ZERO_MA: MaValues = { ma25: 0, ma50: 0, ma100: 0, ma200: 0 };

export function markBackfilledHistoryClosed(klineList: Kline[]): void {
  for (let index = 0; index < klineList.length - 1; index++) {
    if (klineList[index].isClosed !== true) {
      klineList[index].isClosed = true;
    }
  }
}

class MarketDataManager extends EventEmitter implements FeederSource {
  private readonly exchangeConnector: ExchangeConnector;
  private readonly tradifiSymbolGate: TradifiSymbolGate;
  private readonly interval: KlineInterval;
  private readonly klineListBySymbol: Map<string, Kline[]> = new Map();
  private readonly maValuesBySymbol: Map<string, MaValues> = new Map();
  private readonly subscriptionBySymbol: Map<string, SubscribeKlinesArgs> = new Map();
  private readonly handlerBySymbol: Map<string, (symbol: string, kline: Kline) => void> = new Map();
  private readonly currentKlineBySymbol: Map<string, Kline> = new Map();
  private readonly lastEmittedUpdateBySymbol: Map<string, { openTimestamp: number; emittedAtMs: number }> = new Map();
  private readonly consecutiveStaleScanCountBySymbol: Map<string, number> = new Map();
  private readonly persistentStaleEmittedSet: Set<string> = new Set();
  private readonly skippedOlderKlineCountBySymbol: Map<string, number> = new Map();
  private throttledMaRecomputeCount: number = 0;
  private skippedStaleEmitCount: number = 0;
  private lastInboundAtMs: number = 0;
  private isStreamSilent: boolean = false;
  private isMassStale: boolean = false;
  private isShutDown: boolean = false;
  private pendingRemovalSet: Set<string> = new Set();
  private isSymbolSyncAnomalyActive: boolean = false;
  private readonly lastGapRepairAtMsBySymbol: Map<string, number> = new Map();
  private readonly gapRepairInFlightSet: Set<string> = new Set();
  private readonly gapRepairPendingSymbolList: string[] = [];
  private readonly gapRepairFetchCountBySymbol: Map<string, number> = new Map();
  private locallyClosedCandleCount: number = 0;
  private stalenessSchedulerHandle: IntervalSchedulerHandle | null = null;
  private silenceSchedulerHandle: IntervalSchedulerHandle | null = null;
  private volumeSchedulerHandle: IntervalSchedulerHandle | null = null;
  private readonly lastEmittedVolumeBySymbol: Map<string, number> = new Map();
  private readonly backfillQueue: RateLimitedRequestQueue;

  constructor(exchangeConnector: ExchangeConnector, interval: KlineInterval, backfillQueue: RateLimitedRequestQueue = DEFAULT_BACKFILL_QUEUE) {
    super();
    this.exchangeConnector = exchangeConnector;
    this.tradifiSymbolGate = new TradifiSymbolGate({ connector: exchangeConnector });
    this.interval = interval;
    this.backfillQueue = backfillQueue;
  }

  start(): void {
    this.startStalenessWatchdog();
    this.startSilenceWatchdog();
    this.startVolumeRefreshScheduler();
  }

  async loadAllSymbols(): Promise<void> {
    const allSymbolList = await this.tradifiSymbolGate.loadUniverse();

    // Teardown can land at any await of this method (the registry defers it until the load settles,
    // but the source may also be shut down directly) — from here on every stage bails out on
    // isShutDown so a dead source never enqueues backfill work or subscribes exchange streams.
    if (this.isShutDown) {
      logger.warn({}, `[MarketData] loadAllSymbols aborted — source shut down during the symbol-list fetch [${this.interval}]`);

      return;
    }

    const usdtSymbolList = allSymbolList.filter((symbol) => symbol.endsWith('USDT'));

    logger.info({ symbolCount: usdtSymbolList.length, filtered: allSymbolList.length - usdtSymbolList.length }, `[MarketData] Starting kline loading: ${usdtSymbolList.length} USDT symbols [${this.interval}]`);

    this.emit('intervalLoadStarted', usdtSymbolList.length);

    await this.loadKlines(usdtSymbolList);

    if (this.isShutDown) {
      logger.warn({ requestedCount: usdtSymbolList.length }, `[MarketData] loadAllSymbols aborted — source shut down during backfill; skipping subscriptions and load accounting [${this.interval}]`);

      return;
    }

    this.subscribeToKlines(usdtSymbolList);

    const loadedCount = this.klineListBySymbol.size;
    const failedSymbolList = usdtSymbolList.filter((symbol) => !this.klineListBySymbol.has(symbol));

    for (const symbol of failedSymbolList) {
      this.emit('symbolLoadFailed', symbol);
    }

    logger.info({ loadedCount, requestedCount: usdtSymbolList.length, failedCount: failedSymbolList.length }, `[MarketData] All klines loaded (${loadedCount}/${usdtSymbolList.length} symbols), subscriptions active [${this.interval}]`);
    this.emit('intervalLoadCompleted', loadedCount);
  }

  getInterval(): KlineInterval {
    return this.interval;
  }

  getIntervalMs(): number {
    return resolveIntervalMs(this.interval);
  }

  getMaValues(symbol: string): MaValues {
    return this.maValuesBySymbol.get(symbol) ?? ZERO_MA;
  }

  getKlineList(symbol: string): Kline[] {
    return this.klineListBySymbol.get(symbol) ?? [];
  }

  getCurrentKline(symbol: string): Kline | undefined {
    return this.currentKlineBySymbol.get(symbol);
  }

  getSymbolList(): string[] {
    return Array.from(this.klineListBySymbol.keys());
  }

  getKlineCount(): number {
    let total = 0;

    for (const klineList of this.klineListBySymbol.values()) {
      total += klineList.length;
    }

    return total;
  }

  getLastUpdateTimestamp(symbol: string): number | undefined {
    return this.lastEmittedUpdateBySymbol.get(symbol)?.emittedAtMs;
  }

  getLastKlineOpenTimestamp(symbol: string): number | undefined {
    const klineList = this.klineListBySymbol.get(symbol);

    if (klineList === undefined || klineList.length === 0) {
      return undefined;
    }

    return klineList[klineList.length - 1].openTimestamp;
  }

  getVolume24h(symbol: string): number {
    const ticker = this.exchangeConnector.getTicker(symbol, MarketTypeEnum.Futures);

    return ticker?.quoteVolume ?? Number.POSITIVE_INFINITY;
  }

  getSubscriptionCount(): number {
    return this.subscriptionBySymbol.size;
  }

  getStreamLiveness(): StreamLiveness {
    const silenceMs = this.lastInboundAtMs > 0 ? Date.now() - this.lastInboundAtMs : 0;

    return { lastInboundAtMs: this.lastInboundAtMs, silenceMs, isStreamSilent: this.isStreamSilent };
  }

  getFreshSymbolCount(): number {
    return this.klineListBySymbol.size - this.getStaleSymbolList().length;
  }

  getPersistentStaleCount(): number {
    return this.consecutiveStaleScanCountBySymbol.size;
  }

  getStaleSymbolList(): StaleSymbolInfo[] {
    const intervalMs = resolveIntervalMs(this.interval);
    const thresholdMs = STALENESS_THRESHOLD_MULTIPLIER * intervalMs;
    const nowMs = Date.now();
    const staleSymbolList: StaleSymbolInfo[] = [];

    for (const [symbol, klineList] of this.klineListBySymbol) {
      if (klineList.length === 0) {
        continue;
      }

      const lastKline = klineList[klineList.length - 1];
      const referenceTimestamp = lastKline.closeTimestamp > 0 ? lastKline.closeTimestamp : lastKline.openTimestamp + intervalMs;
      const ageMs = nowMs - referenceTimestamp;

      if (ageMs > thresholdMs) {
        staleSymbolList.push({ symbol, ageMs });
      }
    }

    staleSymbolList.sort((first, second) => second.ageMs - first.ageMs);

    return staleSymbolList;
  }

  async ensureSymbolLoaded(symbol: string): Promise<SymbolLoadOutcome> {
    if (this.klineListBySymbol.has(symbol)) {
      return 'alreadyLoaded';
    }

    const exchangeClient = this.exchangeConnector.futures;
    const exchangeSymbolList = await this.tradifiSymbolGate.loadUniverse();

    // A non-empty exchange list that lacks the symbol = legit absence (delisted / never listed / a
    // typo that passed the boundary pattern) — an empty snapshot is the correct answer, not an
    // endless retry loop. An EMPTY list means the list read itself failed (getFuturesSymbols
    // swallows errors into []) — fall through to the REST fetch, which reports its own failure.
    if (exchangeSymbolList.length > 0 && !exchangeSymbolList.includes(symbol)) {
      logger.warn({ symbol }, `[MarketData] ${symbol} is not on the exchange — skipping load [${this.interval}]`);

      return 'notOnExchange';
    }

    logger.info({ symbol }, `[MarketData] ${symbol} ensureSymbolLoaded fetchKlines request limit=${KLINE_BUFFER_SIZE} [${this.interval}]`);

    try {
      const rawKlineList = await this.backfillQueue.execute(
        () => (this.isShutDown ? Promise.resolve([] as Kline[]) : this.fetchKlinesWithTimeout(exchangeClient.fetchKlines(symbol, this.interval, { limit: KLINE_BUFFER_SIZE }), LOAD_KLINES_TIMEOUT_MS, symbol)),
        `ensureSymbolLoaded ${symbol} [${this.interval}]`,
      );

      if (this.isShutDown) {
        return 'aborted';
      }

      // The bulk load may have populated (and subscribed) this symbol while our fetch was queued
      // behind the shared pacer — its live buffer wins over this stale copy.
      if (this.klineListBySymbol.has(symbol)) {
        return 'alreadyLoaded';
      }

      const klineList = rawKlineList.slice(-KLINE_BUFFER_SIZE);

      logger.info({ symbol, klineCount: klineList.length }, `[MarketData] ${symbol} ensureSymbolLoaded fetchKlines response klineCount=${klineList.length} [${this.interval}]`);

      // Listed but no candles yet (e.g. a contract published before trading starts): a legit
      // absence, not a transient failure — throwing here would blank a whole multi-symbol scope
      // into a 4001 retry loop because of one such symbol. The digest still hears about it.
      if (klineList.length === 0) {
        logger.warn({ symbol }, `[MarketData] ${symbol} ensureSymbolLoaded — empty kline history, skipping subscription [${this.interval}]`);
        this.emit('symbolLoadFailed', symbol);

        return 'noHistory';
      }

      markBackfilledHistoryClosed(klineList);
      this.klineListBySymbol.set(symbol, klineList);

      const lastKline = klineList[klineList.length - 1];

      if (lastKline.isClosed !== true) {
        this.currentKlineBySymbol.set(symbol, lastKline);
      }

      this.recalculateAndStoreMa(symbol, klineList);
      this.subscribeToKlines([symbol]);
      logger.info({ symbol }, `[MarketData] ${symbol} ensureSymbolLoaded — subscribed [${this.interval}]`);
      this.emit('symbolLoadCompleted', symbol);

      return 'loaded';
    } catch (error: unknown) {
      logger.error({ symbol, error }, `[MarketData] ${symbol} ensureSymbolLoaded failed [${this.interval}]`);
      this.emit('symbolLoadFailed', symbol);

      throw error;
    }
  }

  releaseSymbol(symbol: string): void {
    // Tell the health monitor the symbol's streams are gone for good (its digest keys can never
    // recover), and pull it from the gap-repair queue so a released symbol does not burn a paced
    // REST request on a buffer that no longer exists.
    this.emit('symbolReleased', symbol);

    const pendingRepairIndex = this.gapRepairPendingSymbolList.indexOf(symbol);

    if (pendingRepairIndex !== -1) {
      this.gapRepairPendingSymbolList.splice(pendingRepairIndex, 1);
    }

    const exchangeClient = this.exchangeConnector.futures;
    const subscription = this.subscriptionBySymbol.get(symbol);

    if (subscription !== undefined) {
      try {
        exchangeClient.unsubscribeKlines(subscription);
      } catch (error: unknown) {
        logger.warn({ symbol, error }, `[MarketData] ${symbol} releaseSymbol — unsubscribeKlines failed (continuing cleanup) [${this.interval}]`);
      }
    }

    this.subscriptionBySymbol.delete(symbol);
    this.handlerBySymbol.delete(symbol);
    this.klineListBySymbol.delete(symbol);
    this.maValuesBySymbol.delete(symbol);
    this.currentKlineBySymbol.delete(symbol);
    this.lastEmittedUpdateBySymbol.delete(symbol);
    this.consecutiveStaleScanCountBySymbol.delete(symbol);
    this.persistentStaleEmittedSet.delete(symbol);
    this.skippedOlderKlineCountBySymbol.delete(symbol);
    this.lastEmittedVolumeBySymbol.delete(symbol);
    this.lastGapRepairAtMsBySymbol.delete(symbol);
    this.gapRepairFetchCountBySymbol.delete(symbol);
  }

  async syncAllSymbols(): Promise<void> {
    try {
      // reloadUniverse refreshes the exchange symbol cache first — without it the hourly sync
      // compares the cached list against itself and can never see a listing or delisting.
      const allSymbolList = await this.tradifiSymbolGate.reloadUniverse();
      const usdtSymbolList = allSymbolList.filter((symbol) => symbol.endsWith('USDT'));

      if (usdtSymbolList.length === 0) {
        logger.warn({ loadedSymbolCount: this.klineListBySymbol.size }, `[MarketData] Symbol sync skipped — exchange returned an empty symbol list [${this.interval}]`);

        return;
      }

      const loadedSymbolCount = this.klineListBySymbol.size;
      const delta = computeSymbolListDelta({ loadedSymbolList: this.getSymbolList(), exchangeSymbolList: usdtSymbolList });

      if (delta.addedSymbolList.length === 0 && delta.removedSymbolList.length === 0) {
        this.pendingRemovalSet = new Set();
        this.resolveSymbolSyncAnomaly();

        return;
      }

      for (const symbol of delta.addedSymbolList) {
        try {
          const outcome = await this.ensureSymbolLoaded(symbol);

          if (outcome === 'loaded') {
            this.emit('symbolAdded', symbol);
          }
        } catch (error: unknown) {
          // symbolLoadFailed was already emitted inside ensureSymbolLoaded (feeds the digest); the
          // next hourly sync retries the listing, and one bad listing must not kill the whole sync.
          logger.warn({ symbol, error }, `[MarketData] ${symbol} new listing failed to load — will retry on the next sync [${this.interval}]`);
        }
      }

      const removalDecision = evaluateSymbolRemovalList({
        removedSymbolList: delta.removedSymbolList,
        loadedSymbolCount,
        pendingRemovalSet: this.pendingRemovalSet,
      });
      this.pendingRemovalSet = removalDecision.nextPendingRemovalSet;

      for (const symbol of removalDecision.approvedRemovalList) {
        this.emit('symbolRemoved', symbol);
        this.releaseSymbol(symbol);
      }

      if (removalDecision.deferredRemovalList.length > 0) {
        logger.error(
          { deferredCount: removalDecision.deferredRemovalList.length, loadedSymbolCount },
          `[MarketData] Symbol sync anomaly — exchange list dropped ${removalDecision.deferredRemovalList.length}/${loadedSymbolCount} symbols; removals deferred until the next sync confirms [${this.interval}]`,
        );

        if (!this.isSymbolSyncAnomalyActive) {
          this.isSymbolSyncAnomalyActive = true;
          this.emit('symbolSyncAnomaly', removalDecision.deferredRemovalList.length, loadedSymbolCount);
        }
      } else {
        this.resolveSymbolSyncAnomaly();
      }

      logger.info({ addedCount: delta.addedSymbolList.length, removedCount: removalDecision.approvedRemovalList.length, deferredCount: removalDecision.deferredRemovalList.length }, `[MarketData] Symbol list synced — +${delta.addedSymbolList.length} / -${removalDecision.approvedRemovalList.length} (deferred ${removalDecision.deferredRemovalList.length}) [${this.interval}]`);
    } catch (error: unknown) {
      logger.error({ error }, `[MarketData] syncAllSymbols failed [${this.interval}]`);
    }
  }

  private resolveSymbolSyncAnomaly(): void {
    if (this.isSymbolSyncAnomalyActive) {
      this.isSymbolSyncAnomalyActive = false;
      logger.info({}, `[MarketData] Symbol sync anomaly cleared [${this.interval}]`);
    }
  }

  async shutdown(): Promise<void> {
    this.emit('sourceShutdown');
    this.isShutDown = true;
    const exchangeClient = this.exchangeConnector.futures;

    // Snapshot the footprint before the cleanup so the teardown log reports what was actually freed.
    const releasedSymbolCount = this.klineListBySymbol.size;
    const releasedKlineCount = this.getKlineCount();

    if (this.stalenessSchedulerHandle !== null) {
      this.stalenessSchedulerHandle.stop();
      this.stalenessSchedulerHandle = null;
    }

    if (this.silenceSchedulerHandle !== null) {
      this.silenceSchedulerHandle.stop();
      this.silenceSchedulerHandle = null;
    }

    if (this.volumeSchedulerHandle !== null) {
      this.volumeSchedulerHandle.stop();
      this.volumeSchedulerHandle = null;
    }

    for (const subscription of this.subscriptionBySymbol.values()) {
      try {
        exchangeClient.unsubscribeKlines(subscription);
      } catch (error: unknown) {
        logger.warn({ symbol: subscription.symbol, error }, `[MarketData] ${subscription.symbol} shutdown — unsubscribeKlines failed (continuing cleanup) [${this.interval}]`);
      }
    }

    // Free every per-symbol buffer explicitly instead of trusting the whole instance to be
    // garbage-collected: a backfill or gap-repair task still queued in the shared pacer keeps this
    // source reachable through its closure, so without these clears the kline history (the heavy
    // part — up to KLINE_BUFFER_SIZE candles per symbol) stays resident until that task drains.
    this.subscriptionBySymbol.clear();
    this.handlerBySymbol.clear();
    this.klineListBySymbol.clear();
    this.maValuesBySymbol.clear();
    this.currentKlineBySymbol.clear();
    this.lastEmittedUpdateBySymbol.clear();
    this.consecutiveStaleScanCountBySymbol.clear();
    this.persistentStaleEmittedSet.clear();
    this.skippedOlderKlineCountBySymbol.clear();
    this.lastEmittedVolumeBySymbol.clear();
    this.lastGapRepairAtMsBySymbol.clear();
    this.gapRepairInFlightSet.clear();
    this.gapRepairPendingSymbolList.length = 0;
    this.gapRepairFetchCountBySymbol.clear();
    this.pendingRemovalSet = new Set();

    // Drop the forwarding listeners the server attached (emit('sourceShutdown') above already ran
    // synchronously) so nothing external pins this dead source in memory.
    this.removeAllListeners();

    logger.info(
      { releasedSymbolCount, releasedKlineCount },
      `[MarketData] Interval torn down — released ${releasedSymbolCount} symbols, ${releasedKlineCount} klines [${this.interval}]`,
    );
  }

  private async fetchKlinesWithTimeout(fetchPromise: Promise<Kline[]>, timeoutMs: number, symbol: string): Promise<Kline[]> {
    return withTimeout(fetchPromise, timeoutMs, `fetchKlines timeout after ${timeoutMs}ms for symbol ${symbol} [${this.interval}]`);
  }

  private async loadKlines(symbolList: string[]): Promise<void> {
    const exchangeClient = this.exchangeConnector.futures;
    let loadedCount = 0;

    const fetchResultList = await Promise.all(
      symbolList.map(async (symbol) => {
        try {
          // The shut-down check runs INSIDE the queue task: entries already queued behind the
          // process-wide pacer must not fire their REST request after teardown.
          const rawKlineList = await this.backfillQueue.execute(
            () => (this.isShutDown ? Promise.resolve([] as Kline[]) : this.fetchKlinesWithTimeout(exchangeClient.fetchKlines(symbol, this.interval, { limit: KLINE_BUFFER_SIZE }), LOAD_KLINES_TIMEOUT_MS, symbol)),
            `fetchKlines ${symbol} [${this.interval}]`,
          );
          const klineList = rawKlineList.slice(-KLINE_BUFFER_SIZE);

          return { symbol, klineList };
        } catch (error: unknown) {
          logger.warn({ symbol, error }, `[MarketData] ${symbol} Failed to load klines, skipping [${this.interval}]`);

          return { symbol, klineList: [] as Kline[] };
        } finally {
          loadedCount++;

          if (loadedCount % KLINE_LOAD_PROGRESS_LOG_EVERY === 0 || loadedCount === symbolList.length) {
            logger.info({ loaded: loadedCount, total: symbolList.length }, `[MarketData] Kline loading progress ${loadedCount}/${symbolList.length} [${this.interval}]`);
          }
        }
      }),
    );

    if (this.isShutDown) {
      return;
    }

    for (const { symbol, klineList } of fetchResultList) {
      if (klineList.length === 0) {
        continue;
      }

      // A symbol with a live handler already went live via the on-demand path while this bulk load
      // was fetching — its buffer is being fed by the stream and must not be rolled back to this
      // (older) REST copy. Handler presence is the discriminator on purpose: a stale buffer left by
      // a previously FAILED bulk attempt has NO handler and must be overwritten.
      if (this.handlerBySymbol.has(symbol)) {
        continue;
      }

      markBackfilledHistoryClosed(klineList);
      this.klineListBySymbol.set(symbol, klineList);

      const lastKline = klineList[klineList.length - 1];

      if (lastKline.isClosed !== true) {
        this.currentKlineBySymbol.set(symbol, lastKline);
      }

      this.recalculateAndStoreMa(symbol, klineList);
    }
  }

  private subscribeToKlines(symbolList: string[]): void {
    if (this.isShutDown) {
      return;
    }

    const exchangeClient = this.exchangeConnector.futures;

    for (const symbol of symbolList) {
      if (!this.klineListBySymbol.has(symbol)) {
        continue;
      }

      if (this.handlerBySymbol.has(symbol)) {
        continue;
      }

      const handler = (_symbol: string, kline: Kline): void => {
        this.handleKline(symbol, kline);
      };

      this.handlerBySymbol.set(symbol, handler);

      const subscription: SubscribeKlinesArgs = { symbol, interval: this.interval, handler };
      this.subscriptionBySymbol.set(symbol, subscription);
      exchangeClient.subscribeKlines(subscription);

      if (this.lastInboundAtMs === 0) {
        this.lastInboundAtMs = Date.now();
      }
    }
  }

  private handleKline(symbol: string, kline: Kline): void {
    if (this.isShutDown) {
      return;
    }

    this.recordInboundMessage();

    try {
      this.handleKlineUnsafe(symbol, kline);
    } catch (error: unknown) {
      logger.error({ error, symbol, kline }, `[MarketData] ${symbol} handleKline threw unexpected error — swallowing to keep SDK callback loop alive [${this.interval}]`);
    }
  }

  private recordInboundMessage(): void {
    const nowMs = Date.now();

    if (this.isStreamSilent) {
      const silenceMs = this.lastInboundAtMs > 0 ? nowMs - this.lastInboundAtMs : 0;
      this.isStreamSilent = false;
      logger.info({ silenceMs }, `[MarketData] Stream resumed after ${Math.round(silenceMs / 1000)}s of silence [${this.interval}]`);
      this.emit('streamResumed', silenceMs);
    }

    this.lastInboundAtMs = nowMs;
  }

  private handleKlineUnsafe(symbol: string, kline: Kline): void {
    const klineList = this.klineListBySymbol.get(symbol);

    if (klineList === undefined || klineList.length === 0) {
      return;
    }

    const lastKline = klineList[klineList.length - 1];
    const isOlder = kline.openTimestamp < lastKline.openTimestamp;

    if (isOlder) {
      const previousCount = this.skippedOlderKlineCountBySymbol.get(symbol) ?? 0;
      this.skippedOlderKlineCountBySymbol.set(symbol, previousCount + 1);

      return;
    }

    if (this.consecutiveStaleScanCountBySymbol.has(symbol) && this.isKlineFresh(kline)) {
      this.consecutiveStaleScanCountBySymbol.delete(symbol);

      if (this.persistentStaleEmittedSet.delete(symbol)) {
        this.emit('persistentStaleRecovered', symbol);
      }
    }

    const isNewCandle = kline.openTimestamp > lastKline.openTimestamp;
    const intervalMs = resolveIntervalMs(this.interval);
    const missedCandleCount = isNewCandle ? Math.floor((kline.openTimestamp - lastKline.openTimestamp) / intervalMs) - 1 : 0;
    let repairFetchCount = 0;

    if (isNewCandle && lastKline.isClosed !== true) {
      lastKline.isClosed = true;
      // Sealed locally = the exchange's closing frame was lost; the sealed values are the last seen,
      // not the true close — schedule a REST repair to correct them.
      this.locallyClosedCandleCount += 1;
      repairFetchCount = GAP_REPAIR_BASE_FETCH_COUNT;

      const maValues = this.recalculateAndStoreMa(symbol, klineList);
      this.emitGuarded('klineClosed', symbol, lastKline, maValues);
    }

    if (missedCandleCount > 0) {
      repairFetchCount = Math.max(repairFetchCount, missedCandleCount + GAP_REPAIR_BASE_FETCH_COUNT);
    }

    if (repairFetchCount > 0) {
      this.scheduleGapRepair(symbol, repairFetchCount);
    }

    if (kline.isClosed === true) {
      if (isNewCandle) {
        klineList.push(kline);

        if (klineList.length > KLINE_BUFFER_SIZE) {
          klineList.shift();
        }
      } else {
        klineList[klineList.length - 1] = kline;
      }

      this.currentKlineBySymbol.delete(symbol);
      this.lastEmittedUpdateBySymbol.delete(symbol);

      const maValues = this.recalculateAndStoreMa(symbol, klineList);
      this.emitGuarded('klineClosed', symbol, kline, maValues);

      return;
    }

    if (isNewCandle) {
      klineList.push(kline);

      if (klineList.length > KLINE_BUFFER_SIZE) {
        klineList.shift();
      }
    } else {
      klineList[klineList.length - 1] = kline;
    }

    this.currentKlineBySymbol.set(symbol, kline);

    const nowMs = Date.now();
    const lastRecompute = this.lastEmittedUpdateBySymbol.get(symbol);
    const isMaRecomputeThrottled = lastRecompute !== undefined
      && lastRecompute.openTimestamp === kline.openTimestamp
      && nowMs - lastRecompute.emittedAtMs < KLINE_UPDATE_THROTTLE_MS;

    let maValues: MaValues;

    if (isMaRecomputeThrottled) {
      maValues = this.maValuesBySymbol.get(symbol) ?? ZERO_MA;
      this.throttledMaRecomputeCount += 1;
    } else {
      maValues = this.recalculateAndStoreMa(symbol, klineList);
      this.lastEmittedUpdateBySymbol.set(symbol, { openTimestamp: kline.openTimestamp, emittedAtMs: nowMs });
    }

    this.emitGuarded('klineUpdated', symbol, kline, maValues);
    this.emitTickGuarded(symbol, kline);
  }

  private scheduleGapRepair(symbol: string, fetchCount: number): void {
    if (this.isShutDown) {
      return;
    }

    const previousFetchCount = this.gapRepairFetchCountBySymbol.get(symbol) ?? 0;
    this.gapRepairFetchCountBySymbol.set(symbol, Math.min(Math.max(fetchCount, previousFetchCount), KLINE_BUFFER_SIZE));

    if (this.gapRepairInFlightSet.has(symbol) || this.gapRepairPendingSymbolList.includes(symbol)) {
      return;
    }

    const lastRepairAtMs = this.lastGapRepairAtMsBySymbol.get(symbol);

    if (lastRepairAtMs !== undefined && Date.now() - lastRepairAtMs < GAP_REPAIR_COOLDOWN_MS) {
      return;
    }

    this.gapRepairPendingSymbolList.push(symbol);
    this.drainGapRepairQueue();
  }

  private drainGapRepairQueue(): void {
    while (!this.isShutDown && this.gapRepairInFlightSet.size < MAX_CONCURRENT_GAP_REPAIRS && this.gapRepairPendingSymbolList.length > 0) {
      const symbol = this.gapRepairPendingSymbolList.shift() as string;
      this.gapRepairInFlightSet.add(symbol);

      void this.runGapRepair(symbol).finally(() => {
        this.gapRepairInFlightSet.delete(symbol);
        this.drainGapRepairQueue();
      });
    }
  }

  private async runGapRepair(symbol: string): Promise<void> {
    // Released/delisted while waiting in the queue — no buffer to repair, no REST slot to burn.
    if (!this.klineListBySymbol.has(symbol)) {
      this.gapRepairFetchCountBySymbol.delete(symbol);

      return;
    }

    const fetchCount = this.gapRepairFetchCountBySymbol.get(symbol) ?? GAP_REPAIR_BASE_FETCH_COUNT;
    this.gapRepairFetchCountBySymbol.delete(symbol);
    this.lastGapRepairAtMsBySymbol.set(symbol, Date.now());
    const exchangeClient = this.exchangeConnector.futures;

    try {
      const rawKlineList = await this.backfillQueue.execute(
        () => (this.isShutDown ? Promise.resolve([] as Kline[]) : this.fetchKlinesWithTimeout(exchangeClient.fetchKlines(symbol, this.interval, { limit: fetchCount }), LOAD_KLINES_TIMEOUT_MS, symbol)),
        `gapRepair ${symbol} [${this.interval}]`,
      );

      if (this.isShutDown) {
        return;
      }

      const bufferKlineList = this.klineListBySymbol.get(symbol);

      // Released or delisted while the repair was in flight — do not resurrect the buffer.
      if (bufferKlineList === undefined) {
        return;
      }

      if (rawKlineList.length === 0) {
        logger.warn({ symbol, fetchCount }, `[MarketData] ${symbol} gap repair got an empty history — the hole stays until the next detection [${this.interval}]`);
        this.emit('symbolLoadFailed', symbol);

        return;
      }

      const fetchedKlineList = rawKlineList.slice(-KLINE_BUFFER_SIZE);
      markBackfilledHistoryClosed(fetchedKlineList);

      const formingOpenTimestamp = this.currentKlineBySymbol.get(symbol)?.openTimestamp ?? null;
      const mergeResult = mergeRepairedKlines({ bufferKlineList, fetchedKlineList, formingOpenTimestamp, maxBufferSize: KLINE_BUFFER_SIZE });

      if (mergeResult.changedCount === 0) {
        return;
      }

      this.klineListBySymbol.set(symbol, mergeResult.mergedKlineList);
      this.recalculateAndStoreMa(symbol, mergeResult.mergedKlineList);
      logger.info({ symbol, changedCount: mergeResult.changedCount, fetchCount }, `[MarketData] ${symbol} gap repair merged ${mergeResult.changedCount} candle(s) — reseeding clients [${this.interval}]`);
      this.emit('symbolReseeded', symbol);
    } catch (error: unknown) {
      logger.error({ symbol, error }, `[MarketData] ${symbol} gap repair failed — the hole stays until the next detection [${this.interval}]`);
      this.emit('symbolLoadFailed', symbol);
    }
  }

  private recalculateAndStoreMa(symbol: string, klineList: Kline[]): MaValues {
    const maValues = calculateAllMaValues(klineList);
    this.maValuesBySymbol.set(symbol, maValues);

    return maValues;
  }

  private isKlineFresh(kline: Kline): boolean {
    const intervalMs = resolveIntervalMs(this.interval);
    const klineEndMs = kline.openTimestamp + intervalMs;
    const klineAgeMs = Date.now() - klineEndMs;
    const thresholdMs = STALENESS_THRESHOLD_MULTIPLIER * intervalMs;

    return klineAgeMs <= thresholdMs;
  }

  private emitTickGuarded(symbol: string, kline: Kline): void {
    if (!this.isKlineFresh(kline)) {
      return;
    }

    this.emit('klineUpdatedTick', symbol, kline);
  }

  private emitGuarded(eventName: 'klineClosed' | 'klineUpdated', symbol: string, kline: Kline, maValues: MaValues): void {
    if (!this.isKlineFresh(kline)) {
      this.skippedStaleEmitCount += 1;

      return;
    }

    this.emit(eventName, symbol, kline, maValues);
  }

  private startStalenessWatchdog(): void {
    this.stalenessSchedulerHandle = startIntervalScheduler({
      tickHandler: () => this.stalenessScan(),
      intervalMs: STALENESS_CHECK_INTERVAL_MS,
      contextLabel: `[MarketData] Staleness scan failed [${this.interval}]`,
      heartbeatEveryNTicks: STALENESS_HEARTBEAT_EVERY_N_TICKS,
      heartbeatHandler: (tickCount) => {
        const symbolCount = this.klineListBySymbol.size;
        const persistentStaleCount = this.consecutiveStaleScanCountBySymbol.size;
        const throttledMaRecomputeCount = this.throttledMaRecomputeCount;
        this.throttledMaRecomputeCount = 0;
        const skippedStaleEmitCount = this.skippedStaleEmitCount;
        this.skippedStaleEmitCount = 0;
        const locallyClosedCandleCount = this.locallyClosedCandleCount;
        this.locallyClosedCandleCount = 0;
        logger.info({ tickCount, symbolCount, persistentStaleCount, throttledMaRecomputeCount, skippedStaleEmitCount, locallyClosedCandleCount }, `[MarketData] Staleness watchdog alive — tick #${tickCount}, ${symbolCount} symbols monitored, ${persistentStaleCount} persistent stale, ${throttledMaRecomputeCount} MA recomputes throttled, ${skippedStaleEmitCount} stale emits skipped, ${locallyClosedCandleCount} candles sealed locally in last period [${this.interval}]`);
        this.flushSkippedOlderKlineCounters();
      },
    });

    logger.info({ intervalMs: STALENESS_CHECK_INTERVAL_MS }, `[MarketData] Staleness watchdog started (check every ${STALENESS_CHECK_INTERVAL_MS / 1000}s) [${this.interval}]`);
  }

  private startSilenceWatchdog(): void {
    this.silenceSchedulerHandle = startIntervalScheduler({
      tickHandler: () => this.silenceScan(),
      intervalMs: SILENCE_CHECK_INTERVAL_MS,
      contextLabel: `[MarketData] Silence scan failed [${this.interval}]`,
    });

    logger.info({ intervalMs: SILENCE_CHECK_INTERVAL_MS, thresholdMs: SILENCE_THRESHOLD_MS }, `[MarketData] Silence watchdog started (check every ${SILENCE_CHECK_INTERVAL_MS / 1000}s, threshold ${SILENCE_THRESHOLD_MS / 1000}s) [${this.interval}]`);
  }

  private silenceScan(): void {
    if (this.subscriptionBySymbol.size === 0 || this.lastInboundAtMs === 0) {
      return;
    }

    const silenceMs = Date.now() - this.lastInboundAtMs;

    if (silenceMs <= SILENCE_THRESHOLD_MS || this.isStreamSilent) {
      return;
    }

    this.isStreamSilent = true;
    logger.warn({ silenceMs, thresholdMs: SILENCE_THRESHOLD_MS, subscriptionCount: this.subscriptionBySymbol.size }, `[MarketData] Stream SILENT — no inbound message for ${Math.round(silenceMs / 1000)}s across ${this.subscriptionBySymbol.size} subscriptions [${this.interval}]`);
    this.emit('streamSilent', silenceMs);
  }

  private startVolumeRefreshScheduler(): void {
    this.volumeSchedulerHandle = startIntervalScheduler({
      tickHandler: () => this.refreshVolumes(),
      intervalMs: VOLUME_REFRESH_INTERVAL_MS,
      contextLabel: `[MarketData] Volume refresh failed [${this.interval}]`,
    });
  }

  private refreshVolumes(): void {
    for (const symbol of this.klineListBySymbol.keys()) {
      const volume24hUsdt = this.getVolume24h(symbol);

      if (!Number.isFinite(volume24hUsdt)) {
        continue;
      }

      if (this.lastEmittedVolumeBySymbol.get(symbol) === volume24hUsdt) {
        continue;
      }

      this.lastEmittedVolumeBySymbol.set(symbol, volume24hUsdt);
      this.emit('volume24h', symbol, volume24hUsdt);
    }
  }

  private stalenessScan(): void {
    const intervalMs = resolveIntervalMs(this.interval);
    const thresholdMs = STALENESS_THRESHOLD_MULTIPLIER * intervalMs;
    const nowMs = Date.now();
    let staleCount = 0;

    for (const [symbol, klineList] of this.klineListBySymbol) {
      if (klineList.length === 0) {
        continue;
      }

      const lastKline = klineList[klineList.length - 1];
      const referenceTimestamp = lastKline.closeTimestamp > 0 ? lastKline.closeTimestamp : lastKline.openTimestamp + intervalMs;
      const ageMs = nowMs - referenceTimestamp;

      if (ageMs <= thresholdMs) {
        continue;
      }

      staleCount += 1;
      const previousCount = this.consecutiveStaleScanCountBySymbol.get(symbol) ?? 0;
      const nextCount = previousCount + 1;
      this.consecutiveStaleScanCountBySymbol.set(symbol, nextCount);

      if (nextCount === PERSISTENT_STALE_THRESHOLD_TICK_COUNT) {
        logger.warn({ symbol, count: nextCount }, `[MarketData] ${symbol} Persistent stale threshold reached (${nextCount} consecutive stale ticks) [${this.interval}]`);
        this.persistentStaleEmittedSet.add(symbol);
        this.emit('persistentStaleSymbol', symbol);
      }
    }

    if (staleCount > 0) {
      logger.warn({ staleCount, totalSymbols: this.klineListBySymbol.size }, `[MarketData] Staleness scan found ${staleCount} stale symbol(s) out of ${this.klineListBySymbol.size} [${this.interval}]`);
    }

    this.evaluateMassStale(staleCount);
  }

  private evaluateMassStale(staleCount: number): void {
    const symbolCount = this.klineListBySymbol.size;
    const isMassStaleNow = crossesMassStaleThreshold({ staleCount, symbolCount, ratioThreshold: MASS_STALE_RATIO_THRESHOLD, minSymbols: MASS_STALE_MIN_SYMBOLS });

    if (isMassStaleNow && !this.isMassStale) {
      this.isMassStale = true;
      logger.warn({ staleCount, symbolCount }, `[MarketData] MASS STALE — ${staleCount}/${symbolCount} symbols stale, escalating as a whole-source degradation [${this.interval}]`);
      this.emit('sourceMassStale', staleCount, symbolCount);

      return;
    }

    if (!isMassStaleNow && this.isMassStale) {
      this.isMassStale = false;
      logger.info({ staleCount, symbolCount }, `[MarketData] Mass stale recovered — ${staleCount}/${symbolCount} symbols stale [${this.interval}]`);
      this.emit('sourceMassStaleRecovered', staleCount, symbolCount);
    }
  }

  private flushSkippedOlderKlineCounters(): void {
    if (this.skippedOlderKlineCountBySymbol.size === 0) {
      return;
    }

    const countBySymbol: Record<string, number> = {};
    let totalCount = 0;

    for (const [symbol, count] of this.skippedOlderKlineCountBySymbol) {
      countBySymbol[symbol] = count;
      totalCount += count;
    }

    const symbolCount = this.skippedOlderKlineCountBySymbol.size;
    logger.info({ countBySymbol, totalCount, symbolCount }, `[MarketData] Skipped replayed older klines since last heartbeat: ${totalCount} total across ${symbolCount} symbol(s) [${this.interval}]`);
    this.skippedOlderKlineCountBySymbol.clear();
  }
}

export { MarketDataManager };

import { EventEmitter } from 'node:events';

import { MarketTypeEnum, logger } from '@solncebro/trade-engine';
import type { ExchangeConnector, Kline, SubscribeKlinesArgs } from '@solncebro/trade-engine';

import type { KlineInterval, MaValues, StaleSymbolInfo } from '../domain/marketData.types.js';
import { KLINE_BUFFER_SIZE, STALENESS_THRESHOLD_MULTIPLIER, resolveIntervalMs } from '../domain/constants.js';
import { calculateAllMaValues } from './indicators.js';
import { computeSymbolListDelta } from './symbolListDelta.js';
import { withTimeout } from '../utils/timeout.js';
import { startIntervalScheduler } from '../utils/intervalScheduler.js';
import type { IntervalSchedulerHandle } from '../utils/intervalScheduler.js';
import type { FeederSource } from '../server/feederSource.types.js';

const FETCH_BATCH_SIZE = 200;
const FETCH_BATCH_DELAY_MS = 300;
const KLINE_UPDATE_THROTTLE_MS = 5000;
const STALENESS_CHECK_INTERVAL_MS = 60_000;
const STALENESS_HEARTBEAT_EVERY_N_TICKS = 15;
const LOAD_KLINES_TIMEOUT_MS = 60_000;
const PERSISTENT_STALE_THRESHOLD_TICK_COUNT = 10;

const ZERO_MA: MaValues = { ma25: 0, ma50: 0, ma100: 0, ma200: 0 };

class MarketDataManager extends EventEmitter implements FeederSource {
  private readonly exchangeConnector: ExchangeConnector;
  private readonly interval: KlineInterval;
  private readonly klineListBySymbol: Map<string, Kline[]> = new Map();
  private readonly maValuesBySymbol: Map<string, MaValues> = new Map();
  private readonly subscriptionBySymbol: Map<string, SubscribeKlinesArgs> = new Map();
  private readonly handlerBySymbol: Map<string, (symbol: string, kline: Kline) => void> = new Map();
  private readonly currentKlineBySymbol: Map<string, Kline> = new Map();
  private readonly lastEmittedUpdateBySymbol: Map<string, { openTimestamp: number; emittedAtMs: number }> = new Map();
  private readonly consecutiveStaleScanCountBySymbol: Map<string, number> = new Map();
  private readonly skippedOlderKlineCountBySymbol: Map<string, number> = new Map();
  private throttledMaRecomputeCount: number = 0;
  private onPersistentStaleSymbol: ((symbol: string) => void) | null = null;
  private stalenessSchedulerHandle: IntervalSchedulerHandle | null = null;

  constructor(exchangeConnector: ExchangeConnector, interval: KlineInterval) {
    super();
    this.exchangeConnector = exchangeConnector;
    this.interval = interval;
  }

  setPersistentStaleSymbolCallback(callback: (symbol: string) => void): void {
    this.onPersistentStaleSymbol = callback;
  }

  start(): void {
    this.startStalenessWatchdog();
  }

  async loadAllSymbols(): Promise<void> {
    const allSymbolList = await this.exchangeConnector.getFuturesSymbols();
    const usdtSymbolList = allSymbolList.filter((symbol) => symbol.endsWith('USDT'));

    logger.info({ symbolCount: usdtSymbolList.length, filtered: allSymbolList.length - usdtSymbolList.length }, `[MarketData] Starting kline loading: ${usdtSymbolList.length} USDT symbols [${this.interval}]`);

    await this.loadKlinesInBatches(usdtSymbolList);
    this.subscribeToKlines(usdtSymbolList);

    logger.info({ totalSymbols: usdtSymbolList.length }, `[MarketData] All klines loaded (${usdtSymbolList.length} symbols), subscriptions active [${this.interval}]`);
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

  async ensureSymbolLoaded(symbol: string): Promise<boolean> {
    if (this.klineListBySymbol.has(symbol)) {
      return false;
    }

    const exchangeClient = this.exchangeConnector.futures;

    logger.info({ symbol }, `[MarketData] ${symbol} ensureSymbolLoaded fetchKlines request limit=${KLINE_BUFFER_SIZE} [${this.interval}]`);

    try {
      const rawKlineList = await this.fetchKlinesWithTimeout(exchangeClient.fetchKlines(symbol, this.interval, { limit: KLINE_BUFFER_SIZE }), LOAD_KLINES_TIMEOUT_MS, symbol);
      const klineList = rawKlineList.slice(-KLINE_BUFFER_SIZE);

      logger.info({ symbol, klineCount: klineList.length }, `[MarketData] ${symbol} ensureSymbolLoaded fetchKlines response klineCount=${klineList.length} [${this.interval}]`);

      if (klineList.length === 0) {
        logger.warn({ symbol }, `[MarketData] ${symbol} ensureSymbolLoaded — empty kline list, skipping subscription [${this.interval}]`);

        return false;
      }

      this.klineListBySymbol.set(symbol, klineList);

      const lastKline = klineList[klineList.length - 1];

      if (lastKline.isClosed !== true) {
        this.currentKlineBySymbol.set(symbol, lastKline);
      }

      this.recalculateAndStoreMa(symbol, klineList);
      this.subscribeToKlines([symbol]);
      logger.info({ symbol }, `[MarketData] ${symbol} ensureSymbolLoaded — subscribed [${this.interval}]`);

      return true;
    } catch (error: unknown) {
      logger.error({ symbol, error }, `[MarketData] ${symbol} ensureSymbolLoaded failed [${this.interval}]`);

      return false;
    }
  }

  releaseSymbol(symbol: string): void {
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
    this.skippedOlderKlineCountBySymbol.delete(symbol);
  }

  async syncAllSymbols(): Promise<void> {
    try {
      const allSymbolList = await this.exchangeConnector.getFuturesSymbols();
      const usdtSymbolList = allSymbolList.filter((symbol) => symbol.endsWith('USDT'));
      const delta = computeSymbolListDelta({ loadedSymbolList: this.getSymbolList(), exchangeSymbolList: usdtSymbolList });

      if (delta.addedSymbolList.length === 0 && delta.removedSymbolList.length === 0) {
        return;
      }

      for (const symbol of delta.addedSymbolList) {
        const isLoaded = await this.ensureSymbolLoaded(symbol);

        if (isLoaded) {
          this.emit('symbolAdded', symbol);
        }
      }

      for (const symbol of delta.removedSymbolList) {
        this.emit('symbolRemoved', symbol);
        this.releaseSymbol(symbol);
      }

      logger.info({ addedCount: delta.addedSymbolList.length, removedCount: delta.removedSymbolList.length }, `[MarketData] Symbol list synced — +${delta.addedSymbolList.length} / -${delta.removedSymbolList.length} [${this.interval}]`);
    } catch (error: unknown) {
      logger.error({ error }, `[MarketData] syncAllSymbols failed [${this.interval}]`);
    }
  }

  async shutdown(): Promise<void> {
    const exchangeClient = this.exchangeConnector.futures;

    if (this.stalenessSchedulerHandle !== null) {
      this.stalenessSchedulerHandle.stop();
      this.stalenessSchedulerHandle = null;
    }

    for (const subscription of this.subscriptionBySymbol.values()) {
      exchangeClient.unsubscribeKlines(subscription);
    }

    this.subscriptionBySymbol.clear();
    this.handlerBySymbol.clear();
  }

  private async fetchKlinesWithTimeout(fetchPromise: Promise<Kline[]>, timeoutMs: number, symbol: string): Promise<Kline[]> {
    return withTimeout(fetchPromise, timeoutMs, `fetchKlines timeout after ${timeoutMs}ms for symbol ${symbol} [${this.interval}]`);
  }

  private async loadKlinesInBatches(symbolList: string[]): Promise<void> {
    const exchangeClient = this.exchangeConnector.futures;
    let loadedCount = 0;

    for (let i = 0; i < symbolList.length; i += FETCH_BATCH_SIZE) {
      const symbolBatchList = symbolList.slice(i, i + FETCH_BATCH_SIZE);

      const fetchResultList = await Promise.all(
        symbolBatchList.map(async (symbol) => {
          try {
            const rawKlineList = await this.fetchKlinesWithTimeout(exchangeClient.fetchKlines(symbol, this.interval, { limit: KLINE_BUFFER_SIZE }), LOAD_KLINES_TIMEOUT_MS, symbol);
            const klineList = rawKlineList.slice(-KLINE_BUFFER_SIZE);

            return { symbol, klineList };
          } catch (error: unknown) {
            logger.warn({ symbol, error }, `[MarketData] ${symbol} Failed to load klines, skipping [${this.interval}]`);

            return { symbol, klineList: [] as Kline[] };
          }
        }),
      );

      for (const { symbol, klineList } of fetchResultList) {
        if (klineList.length === 0) {
          continue;
        }

        this.klineListBySymbol.set(symbol, klineList);

        const lastKline = klineList[klineList.length - 1];

        if (lastKline.isClosed !== true) {
          this.currentKlineBySymbol.set(symbol, lastKline);
        }

        this.recalculateAndStoreMa(symbol, klineList);
      }

      loadedCount += symbolBatchList.length;
      logger.info({ loaded: loadedCount, total: symbolList.length }, `[MarketData] Kline loading progress ${loadedCount}/${symbolList.length} [${this.interval}]`);

      if (i + FETCH_BATCH_SIZE < symbolList.length) {
        await new Promise((resolve) => setTimeout(resolve, FETCH_BATCH_DELAY_MS));
      }
    }
  }

  private subscribeToKlines(symbolList: string[]): void {
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
    }
  }

  private handleKline(symbol: string, kline: Kline): void {
    try {
      this.handleKlineUnsafe(symbol, kline);
    } catch (error: unknown) {
      logger.error({ error, symbol, kline }, `[MarketData] ${symbol} handleKline threw unexpected error — swallowing to keep SDK callback loop alive [${this.interval}]`);
    }
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
    }

    const isNewCandle = kline.openTimestamp > lastKline.openTimestamp;

    if (isNewCandle && lastKline.isClosed !== true) {
      lastKline.isClosed = true;

      const maValues = this.recalculateAndStoreMa(symbol, klineList);
      this.emitGuarded('klineClosed', symbol, lastKline, maValues);
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
      const intervalMs = resolveIntervalMs(this.interval);
      const klineEndMs = kline.openTimestamp + intervalMs;
      const klineAgeMs = Date.now() - klineEndMs;
      const thresholdMs = STALENESS_THRESHOLD_MULTIPLIER * intervalMs;
      logger.info({ symbol, eventName, klineAgeMs, thresholdMs, openTimestamp: kline.openTimestamp }, `[MarketData] ${symbol} Skipped emit ${eventName} for stale kline (age=${Math.round(klineAgeMs / 60_000)}m, threshold=${Math.round(thresholdMs / 60_000)}m) [${this.interval}]`);

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
        logger.info({ tickCount, symbolCount, persistentStaleCount, throttledMaRecomputeCount }, `[MarketData] Staleness watchdog alive — tick #${tickCount}, ${symbolCount} symbols monitored, ${persistentStaleCount} persistent stale, ${throttledMaRecomputeCount} MA recomputes throttled in last period [${this.interval}]`);
        this.flushSkippedOlderKlineCounters();
      },
    });

    logger.info({ intervalMs: STALENESS_CHECK_INTERVAL_MS }, `[MarketData] Staleness watchdog started (check every ${STALENESS_CHECK_INTERVAL_MS / 1000}s) [${this.interval}]`);
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

      if (nextCount === PERSISTENT_STALE_THRESHOLD_TICK_COUNT && this.onPersistentStaleSymbol !== null) {
        logger.warn({ symbol, count: nextCount }, `[MarketData] ${symbol} Persistent stale threshold reached (${nextCount} consecutive stale ticks) [${this.interval}]`);

        try {
          this.onPersistentStaleSymbol(symbol);
        } catch (error: unknown) {
          logger.error({ error, symbol }, `[MarketData] ${symbol} onPersistentStaleSymbol callback threw [${this.interval}]`);
        }
      }
    }

    if (staleCount > 0) {
      logger.warn({ staleCount, totalSymbols: this.klineListBySymbol.size }, `[MarketData] Staleness scan found ${staleCount} stale symbol(s) out of ${this.klineListBySymbol.size} [${this.interval}]`);
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

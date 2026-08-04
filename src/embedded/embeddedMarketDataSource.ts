import { EventEmitter } from 'node:events';

import { ExchangeConnector, RateLimitedRequestQueue, logger } from '@solncebro/trade-engine';

import type { Kline, KlineInterval, MaValues, StaleSymbolInfo } from '../domain/marketData.types.js';
import type { MarketDataSource } from '../client/marketDataSource.types.js';
import type { FeederSource } from '../server/feederSource.types.js';
import { KLINE_WATCHDOG_GRACE_MS, KLINE_WATCHDOG_RECOVERY_COOLDOWN_MS, SYMBOL_LIST_SYNC_INTERVAL_MS, resolveBackfillRequestsPerSecond } from '../domain/constants.js';
import { MarketDataManager } from '../source/marketDataManager.js';
import { startIntervalScheduler } from '../utils/intervalScheduler.js';
import type { IntervalSchedulerHandle } from '../utils/intervalScheduler.js';
import { withTimeout } from '../utils/timeout.js';
import type { CreateEmbeddedMarketDataSourceArgs, EmbeddedMarketDataSourceArgs } from './embeddedMarketDataSource.types.js';

// In-process replacement for MarketDataClient: the same MarketDataSource contract served straight
// from the feeder core, so a single-app host skips the second process, the WS/JSON hop and the
// client-side mirror copy of every kline buffer.
class EmbeddedMarketDataSource extends EventEmitter implements MarketDataSource {
  private readonly source: FeederSource;
  private readonly onNotify: (message: string) => void;
  private readonly teardownConnection: () => Promise<void> | void;
  private symbolSyncSchedulerHandle: IntervalSchedulerHandle | null = null;
  private isShutDown: boolean = false;

  constructor(args: EmbeddedMarketDataSourceArgs) {
    super();
    this.source = args.source;
    this.onNotify = args.onNotify ?? (() => undefined);
    this.teardownConnection = args.teardownConnection ?? (() => undefined);
    this.forwardSourceEvents();
  }

  async waitUntilReady(timeoutMs: number): Promise<void> {
    this.source.start();
    await withTimeout(this.source.loadAllSymbols(), timeoutMs, `Embedded market data source failed to load within ${timeoutMs}ms`);

    // The hourly listing/delisting sync lives in FeederServer for the standalone process; embedded,
    // the adapter drives it — otherwise the universe would freeze at its startup composition.
    this.symbolSyncSchedulerHandle = startIntervalScheduler({
      tickHandler: () => this.source.syncAllSymbols(),
      intervalMs: SYMBOL_LIST_SYNC_INTERVAL_MS,
      contextLabel: '[EmbeddedMarketData] symbol list sync failed',
    });
  }

  async shutdown(): Promise<void> {
    if (this.isShutDown) {
      return;
    }

    this.isShutDown = true;

    if (this.symbolSyncSchedulerHandle !== null) {
      this.symbolSyncSchedulerHandle.stop();
      this.symbolSyncSchedulerHandle = null;
    }

    await this.source.shutdown();
    await this.teardownConnection();
    this.removeAllListeners();
  }

  isStale(): boolean {
    return this.source.getStreamLiveness().isStreamSilent;
  }

  getInterval(): KlineInterval {
    return this.source.getInterval();
  }

  getIntervalMs(): number {
    return this.source.getIntervalMs();
  }

  getMaValues(symbol: string): MaValues {
    return this.source.getMaValues(symbol);
  }

  getKlineList(symbol: string): Kline[] {
    return this.source.getKlineList(symbol);
  }

  getCurrentKline(symbol: string): Kline | undefined {
    return this.source.getCurrentKline(symbol);
  }

  getSymbolList(): string[] {
    return this.source.getSymbolList();
  }

  getLastKlineOpenTimestamp(symbol: string): number | undefined {
    return this.source.getLastKlineOpenTimestamp(symbol);
  }

  getLastUpdateTimestamp(symbol: string): number | undefined {
    return this.source.getLastUpdateTimestamp(symbol);
  }

  getVolume24h(symbol: string): number {
    return this.source.getVolume24h(symbol);
  }

  getStaleSymbolList(): StaleSymbolInfo[] {
    return this.source.getStaleSymbolList();
  }

  private forwardSourceEvents(): void {
    this.source.on('klineClosed', (symbol, kline, maValues) => this.emit('klineClosed', symbol, kline, maValues));
    this.source.on('klineUpdated', (symbol, kline, maValues) => this.emit('klineUpdated', symbol, kline, maValues));
    this.source.on('klineUpdatedTick', (symbol, kline) => this.emit('klineUpdatedTick', symbol, kline));
    this.source.on('symbolAdded', (symbol) => this.emit('symbolAdded', symbol));
    this.source.on('symbolRemoved', (symbol) => this.emit('symbolRemoved', symbol));

    // Silence of the exchange kline stream is the embedded counterpart of losing the feeder socket:
    // both mean "no market data is flowing", and the shared connectionLost contract lets the host's
    // FeederConnectionGuard freeze trading without knowing which transport it sits on.
    this.source.on('streamSilent', (silenceMs) => this.emit('connectionLost', `exchange kline stream silent for ${Math.round(silenceMs / 1000)}s`));
    this.source.on('streamResumed', () => this.emit('connectionRestored'));

    // Whole-source degradations reach the operator directly (the standalone feeder routes them to
    // its health monitor + Telegram); per-symbol issues stay in the core's own logs — hundreds of
    // symbols would spam the notify channel without the monitor's dedup/batching.
    this.source.on('sourceMassStale', (staleCount, symbolCount) => this.onNotify(`Embedded market data: MASS STALE — ${staleCount}/${symbolCount} symbols without fresh klines`));
    this.source.on('sourceMassStaleRecovered', (staleCount, symbolCount) => this.onNotify(`Embedded market data: mass stale recovered (${staleCount}/${symbolCount} still stale)`));
    this.source.on('symbolSyncAnomaly', (deferredCount, symbolCount) => this.onNotify(`Embedded market data: symbol sync anomaly — exchange list dropped ${deferredCount}/${symbolCount} symbols, removals deferred`));
  }
}

async function createEmbeddedMarketDataSource(args: CreateEmbeddedMarketDataSourceArgs): Promise<EmbeddedMarketDataSource> {
  const exchangeConnector = new ExchangeConnector(
    args.exchangeName,
    { apiKey: args.exchangeApiKey, secret: args.exchangeSecret },
    (message: string) => logger.warn({ message }, `[EmbeddedMarketData] transport notify: ${message}`),
    undefined,
    {
      graceMs: KLINE_WATCHDOG_GRACE_MS,
      recoveryCooldownMs: KLINE_WATCHDOG_RECOVERY_COOLDOWN_MS,
      onStreamStale: (event) => logger.warn({ event }, `[EmbeddedMarketData] kline stream stale [${args.interval}]`),
      onStreamRecovered: (event) => logger.info({ event }, `[EmbeddedMarketData] kline stream recovered [${args.interval}]`),
      onStreamRecoveryFailed: (event) => logger.error({ event }, `[EmbeddedMarketData] kline stream recovery failed [${args.interval}]`),
    },
  );

  await exchangeConnector.initialize();

  const backfillQueue = new RateLimitedRequestQueue({
    rateLimit: resolveBackfillRequestsPerSecond(args.exchangeName),
    intervalMs: 1000,
    loggerLabel: '[EmbeddedMarketData:backfill]',
  });
  const source = new MarketDataManager(exchangeConnector, args.interval, backfillQueue);

  return new EmbeddedMarketDataSource({
    source,
    onNotify: args.onNotify,
    teardownConnection: () => exchangeConnector.disconnect(),
  });
}

export { EmbeddedMarketDataSource, createEmbeddedMarketDataSource };

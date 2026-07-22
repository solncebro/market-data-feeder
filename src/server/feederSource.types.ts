import type { Kline, MaValues, StaleSymbolInfo } from '../domain/marketData.types.js';
import type { IntervalLoadListener, KlineTickListener, KlineWithMaListener, SourceLifecycleListener, SourceMassStaleListener, StreamSilenceListener, SymbolNameListener, SymbolSyncAnomalyListener, Volume24hListener } from '../domain/events.types.js';
import type { ManagedSource } from './subscriptionRegistry.types.js';

interface StreamLiveness {
  lastInboundAtMs: number;
  silenceMs: number;
  isStreamSilent: boolean;
}

interface FeederSource extends ManagedSource {
  getSymbolList: () => string[];
  getKlineList: (symbol: string) => Kline[];
  getMaValues: (symbol: string) => MaValues;
  getCurrentKline: (symbol: string) => Kline | undefined;
  getVolume24h: (symbol: string) => number;
  getIntervalMs: () => number;
  getKlineCount: () => number;
  getSubscriptionCount: () => number;
  getStaleSymbolList: () => StaleSymbolInfo[];
  getLastKlineOpenTimestamp: (symbol: string) => number | undefined;
  getLastUpdateTimestamp: (symbol: string) => number | undefined;
  getStreamLiveness: () => StreamLiveness;
  getFreshSymbolCount: () => number;
  getPersistentStaleCount: () => number;
  on(eventName: 'klineClosed' | 'klineUpdated', listener: KlineWithMaListener): this;
  on(eventName: 'klineUpdatedTick', listener: KlineTickListener): this;
  on(eventName: 'symbolAdded' | 'symbolRemoved' | 'symbolReseeded' | 'symbolReleased' | 'persistentStaleSymbol' | 'persistentStaleRecovered' | 'symbolLoadCompleted' | 'symbolLoadFailed' | 'symbolBackfillStuck' | 'symbolBackfillRecovered', listener: SymbolNameListener): this;
  on(eventName: 'sourceShutdown', listener: SourceLifecycleListener): this;
  on(eventName: 'volume24h', listener: Volume24hListener): this;
  on(eventName: 'streamSilent' | 'streamResumed', listener: StreamSilenceListener): this;
  on(eventName: 'sourceMassStale' | 'sourceMassStaleRecovered', listener: SourceMassStaleListener): this;
  on(eventName: 'symbolSyncAnomaly', listener: SymbolSyncAnomalyListener): this;
  on(eventName: 'intervalLoadStarted' | 'intervalLoadCompleted', listener: IntervalLoadListener): this;
  off(eventName: 'klineClosed' | 'klineUpdated', listener: KlineWithMaListener): this;
  off(eventName: 'klineUpdatedTick', listener: KlineTickListener): this;
  off(eventName: 'symbolAdded' | 'symbolRemoved' | 'symbolReseeded' | 'symbolReleased' | 'persistentStaleSymbol' | 'persistentStaleRecovered' | 'symbolLoadCompleted' | 'symbolLoadFailed' | 'symbolBackfillStuck' | 'symbolBackfillRecovered', listener: SymbolNameListener): this;
  off(eventName: 'sourceShutdown', listener: SourceLifecycleListener): this;
  off(eventName: 'volume24h', listener: Volume24hListener): this;
  off(eventName: 'streamSilent' | 'streamResumed', listener: StreamSilenceListener): this;
  off(eventName: 'sourceMassStale' | 'sourceMassStaleRecovered', listener: SourceMassStaleListener): this;
  off(eventName: 'symbolSyncAnomaly', listener: SymbolSyncAnomalyListener): this;
  off(eventName: 'intervalLoadStarted' | 'intervalLoadCompleted', listener: IntervalLoadListener): this;
}

export type { FeederSource, StreamLiveness };

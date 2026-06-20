import type { Kline, KlineInterval, MaValues, StaleSymbolInfo } from '../domain/marketData.types.js';
import type { ConnectionLostListener, ConnectionRestoredListener, KlineTickListener, KlineWithMaListener, SymbolNameListener } from '../domain/events.types.js';

interface MarketDataSource {
  getInterval: () => KlineInterval;
  getIntervalMs: () => number;
  getMaValues: (symbol: string) => MaValues;
  getKlineList: (symbol: string) => Kline[];
  getCurrentKline: (symbol: string) => Kline | undefined;
  getSymbolList: () => string[];
  getLastKlineOpenTimestamp: (symbol: string) => number | undefined;
  getLastUpdateTimestamp: (symbol: string) => number | undefined;
  getVolume24h: (symbol: string) => number;
  isStale: () => boolean;
  getStaleSymbolList: () => StaleSymbolInfo[];
  on(eventName: 'klineClosed' | 'klineUpdated', listener: KlineWithMaListener): this;
  on(eventName: 'klineUpdatedTick', listener: KlineTickListener): this;
  on(eventName: 'symbolAdded' | 'symbolRemoved', listener: SymbolNameListener): this;
  on(eventName: 'connectionLost', listener: ConnectionLostListener): this;
  on(eventName: 'connectionRestored', listener: ConnectionRestoredListener): this;
  off(eventName: 'klineClosed' | 'klineUpdated', listener: KlineWithMaListener): this;
  off(eventName: 'klineUpdatedTick', listener: KlineTickListener): this;
  off(eventName: 'symbolAdded' | 'symbolRemoved', listener: SymbolNameListener): this;
  off(eventName: 'connectionLost', listener: ConnectionLostListener): this;
  off(eventName: 'connectionRestored', listener: ConnectionRestoredListener): this;
}

export type { MarketDataSource };

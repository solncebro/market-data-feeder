import type { Kline, MaValues } from '../domain/marketData.types.js';
import type { KlineTickListener, KlineWithMaListener, SymbolNameListener } from '../domain/events.types.js';
import type { ManagedSource } from './subscriptionRegistry.types.js';

interface FeederSource extends ManagedSource {
  getSymbolList: () => string[];
  getKlineList: (symbol: string) => Kline[];
  getMaValues: (symbol: string) => MaValues;
  getCurrentKline: (symbol: string) => Kline | undefined;
  getVolume24h: (symbol: string) => number;
  on(eventName: 'klineClosed' | 'klineUpdated', listener: KlineWithMaListener): this;
  on(eventName: 'klineUpdatedTick', listener: KlineTickListener): this;
  on(eventName: 'symbolAdded' | 'symbolRemoved', listener: SymbolNameListener): this;
  off(eventName: 'klineClosed' | 'klineUpdated', listener: KlineWithMaListener): this;
  off(eventName: 'klineUpdatedTick', listener: KlineTickListener): this;
  off(eventName: 'symbolAdded' | 'symbolRemoved', listener: SymbolNameListener): this;
}

export type { FeederSource };

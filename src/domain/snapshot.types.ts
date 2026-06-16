import type { Kline, MaValues } from './marketData.types.js';

interface MarketDataSnapshotEntry {
  symbol: string;
  klineList: Kline[];
  maValues: MaValues;
  currentKline: Kline | null;
  volume24hUsdt: number | null;
}

export type { MarketDataSnapshotEntry };

import type { Kline, MaValues } from '../domain/marketData.types.js';
import type { MarketDataSnapshotEntry } from '../domain/snapshot.types.js';

interface ApplySnapshotArgs {
  entryList: MarketDataSnapshotEntry[];
}

interface ApplyKlineWithMaArgs {
  symbol: string;
  kline: Kline;
  maValues: MaValues;
}

interface ApplyKlineArgs {
  symbol: string;
  kline: Kline;
}

interface ApplyVolume24hArgs {
  symbol: string;
  volume24hUsdt: number;
}

export type { ApplyKlineArgs, ApplyKlineWithMaArgs, ApplySnapshotArgs, ApplyVolume24hArgs };

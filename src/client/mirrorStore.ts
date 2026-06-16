import type { Kline, MaValues } from '../domain/marketData.types.js';
import type { MarketDataSnapshotEntry } from '../domain/snapshot.types.js';
import type {
  ApplyKlineArgs,
  ApplyKlineWithMaArgs,
  ApplySnapshotArgs,
  ApplyVolume24hArgs,
} from './mirrorStore.types.js';

const ZERO_MA: MaValues = { ma25: 0, ma50: 0, ma100: 0, ma200: 0 };

class MirrorStore {
  private readonly maxBufferSize: number;
  private readonly klineListBySymbol: Map<string, Kline[]> = new Map();
  private readonly maValuesBySymbol: Map<string, MaValues> = new Map();
  private readonly currentKlineBySymbol: Map<string, Kline> = new Map();
  private readonly volume24hBySymbol: Map<string, number> = new Map();

  constructor(maxBufferSize: number) {
    this.maxBufferSize = maxBufferSize;
  }

  applySnapshot(args: ApplySnapshotArgs): void {
    for (const entry of args.entryList) {
      this.seedSymbol(entry);
    }
  }

  applySymbolAdded(entry: MarketDataSnapshotEntry): void {
    this.seedSymbol(entry);
  }

  applySymbolRemoved(symbol: string): void {
    this.klineListBySymbol.delete(symbol);
    this.maValuesBySymbol.delete(symbol);
    this.currentKlineBySymbol.delete(symbol);
    this.volume24hBySymbol.delete(symbol);
  }

  retainSymbols(symbolSet: Set<string>): void {
    for (const symbol of this.klineListBySymbol.keys()) {
      if (!symbolSet.has(symbol)) {
        this.applySymbolRemoved(symbol);
      }
    }
  }

  applyKlineClosed(args: ApplyKlineWithMaArgs): void {
    const didApply = this.applyKlineToBuffer(args.symbol, args.kline);

    if (!didApply) {
      return;
    }

    this.maValuesBySymbol.set(args.symbol, args.maValues);
    this.currentKlineBySymbol.delete(args.symbol);
  }

  applyKlineUpdated(args: ApplyKlineWithMaArgs): void {
    const didApply = this.applyKlineToBuffer(args.symbol, args.kline);

    if (!didApply) {
      return;
    }

    this.maValuesBySymbol.set(args.symbol, args.maValues);
    this.currentKlineBySymbol.set(args.symbol, args.kline);
  }

  applyKlineTick(args: ApplyKlineArgs): void {
    const didApply = this.applyKlineToBuffer(args.symbol, args.kline);

    if (!didApply) {
      return;
    }

    this.currentKlineBySymbol.set(args.symbol, args.kline);
  }

  applyVolume24h(args: ApplyVolume24hArgs): void {
    this.volume24hBySymbol.set(args.symbol, args.volume24hUsdt);
  }

  getKlineList(symbol: string): Kline[] {
    return this.klineListBySymbol.get(symbol) ?? [];
  }

  getMaValues(symbol: string): MaValues {
    return this.maValuesBySymbol.get(symbol) ?? ZERO_MA;
  }

  getCurrentKline(symbol: string): Kline | undefined {
    return this.currentKlineBySymbol.get(symbol);
  }

  getSymbolList(): string[] {
    return Array.from(this.klineListBySymbol.keys());
  }

  getLastKlineOpenTimestamp(symbol: string): number | undefined {
    const klineList = this.klineListBySymbol.get(symbol);

    if (klineList === undefined || klineList.length === 0) {
      return undefined;
    }

    return klineList[klineList.length - 1].openTimestamp;
  }

  getVolume24h(symbol: string): number {
    return this.volume24hBySymbol.get(symbol) ?? Number.POSITIVE_INFINITY;
  }

  private seedSymbol(entry: MarketDataSnapshotEntry): void {
    this.klineListBySymbol.set(entry.symbol, entry.klineList.slice(-this.maxBufferSize));
    this.maValuesBySymbol.set(entry.symbol, entry.maValues);

    if (entry.currentKline !== null) {
      this.currentKlineBySymbol.set(entry.symbol, entry.currentKline);
    } else {
      this.currentKlineBySymbol.delete(entry.symbol);
    }

    if (entry.volume24hUsdt !== null) {
      this.volume24hBySymbol.set(entry.symbol, entry.volume24hUsdt);
    }
  }

  private applyKlineToBuffer(symbol: string, kline: Kline): boolean {
    const existingList = this.klineListBySymbol.get(symbol);
    const klineList = existingList ?? [];

    if (existingList === undefined) {
      this.klineListBySymbol.set(symbol, klineList);
    }

    if (klineList.length === 0) {
      klineList.push(kline);

      return true;
    }

    const lastKline = klineList[klineList.length - 1];

    if (kline.openTimestamp < lastKline.openTimestamp) {
      return false;
    }

    if (kline.openTimestamp === lastKline.openTimestamp) {
      klineList[klineList.length - 1] = kline;

      return true;
    }

    klineList.push(kline);

    if (klineList.length > this.maxBufferSize) {
      klineList.shift();
    }

    return true;
  }
}

export { MirrorStore };

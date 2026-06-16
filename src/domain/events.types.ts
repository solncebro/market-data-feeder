import type { Kline, MaValues } from './marketData.types.js';

type KlineWithMaListener = (symbol: string, kline: Kline, maValues: MaValues) => void;
type KlineTickListener = (symbol: string, kline: Kline) => void;
type SymbolNameListener = (symbol: string) => void;

export type { KlineTickListener, KlineWithMaListener, SymbolNameListener };

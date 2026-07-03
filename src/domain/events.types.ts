import type { Kline, MaValues } from './marketData.types.js';

type KlineWithMaListener = (symbol: string, kline: Kline, maValues: MaValues) => void;
type KlineTickListener = (symbol: string, kline: Kline) => void;
type SymbolNameListener = (symbol: string) => void;
type Volume24hListener = (symbol: string, volume24hUsdt: number) => void;
type StreamSilenceListener = (silenceMs: number) => void;
type SourceMassStaleListener = (staleCount: number, symbolCount: number) => void;
type SymbolSyncAnomalyListener = (deferredCount: number, symbolCount: number) => void;
type SourceLifecycleListener = () => void;
type IntervalLoadListener = (symbolCount: number) => void;
type ConnectionLostListener = (reason: string) => void;
type ConnectionRestoredListener = () => void;

export type { ConnectionLostListener, ConnectionRestoredListener, IntervalLoadListener, KlineTickListener, KlineWithMaListener, SourceLifecycleListener, SourceMassStaleListener, StreamSilenceListener, SymbolNameListener, SymbolSyncAnomalyListener, Volume24hListener };

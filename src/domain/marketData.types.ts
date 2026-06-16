type KlineInterval =
  | '1s'
  | '1m'
  | '3m'
  | '5m'
  | '15m'
  | '30m'
  | '1h'
  | '2h'
  | '4h'
  | '6h'
  | '12h'
  | '1d'
  | '3d'
  | '1w'
  | '1M';

interface Kline {
  openTimestamp: number;
  openPrice: number;
  highPrice: number;
  lowPrice: number;
  closePrice: number;
  volume: number;
  closeTimestamp: number;
  quoteAssetVolume: number;
  numberOfTrades: number;
  takerBuyBaseAssetVolume: number;
  takerBuyQuoteAssetVolume: number;
  isClosed?: boolean;
}

interface MaValues {
  ma25: number;
  ma50: number;
  ma100: number;
  ma200: number;
  ma99?: number | null;
  ma1000?: number | null;
}

interface StaleSymbolInfo {
  symbol: string;
  ageMs: number;
}

export type { Kline, KlineInterval, MaValues, StaleSymbolInfo };

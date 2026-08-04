import { ExchangeNameEnum } from '@solncebro/trade-engine';

import type { KlineInterval } from './marketData.types.js';

const KLINE_BUFFER_SIZE = 499;
const DEFAULT_INTERVAL_MS = 1_800_000;
const STALENESS_THRESHOLD_MULTIPLIER = 2;

// Shared by the standalone FeederServer and the embedded adapter: one listing/delisting discovery
// cadence and one exchange-stream watchdog tuning regardless of the hosting topology.
const SYMBOL_LIST_SYNC_INTERVAL_MS = 3_600_000;
const KLINE_WATCHDOG_GRACE_MS = 180_000;
const KLINE_WATCHDOG_RECOVERY_COOLDOWN_MS = 300_000;

// Backfill request rate is per-exchange because the two exchanges enforce REST limits differently.
// Binance USD-M futures uses a per-IP weight budget: REQUEST_WEIGHT 2400/min (live fapi/v1/exchangeInfo),
// and a klines call at limit=499 costs weight 2 -> the hard ceiling is 2400/60/2 = 20 req/s. We run 16
// (16*2*60 = 1920 weight/min) to leave headroom for the 30s ticker poll (~160/min), exchangeInfo and
// watchdog refetches that share the same per-IP budget. Bybit uses per-endpoint IP limits (not a weight
// budget), where the proven value is 80 req/s.
const KLINE_BACKFILL_REQUESTS_PER_SECOND_BINANCE = 16;
const KLINE_BACKFILL_REQUESTS_PER_SECOND_BYBIT = 80;

const INTERVAL_MS_BY_KEY: Record<KlineInterval, number> = {
  '1s': 1_000,
  '1m': 60_000,
  '3m': 180_000,
  '5m': 300_000,
  '15m': 900_000,
  '30m': 1_800_000,
  '1h': 3_600_000,
  '2h': 7_200_000,
  '4h': 14_400_000,
  '6h': 21_600_000,
  '12h': 43_200_000,
  '1d': 86_400_000,
  '3d': 259_200_000,
  '1w': 604_800_000,
  '1M': 2_592_000_000,
};

function resolveIntervalMs(interval: KlineInterval): number {
  return INTERVAL_MS_BY_KEY[interval] ?? DEFAULT_INTERVAL_MS;
}

function resolveBackfillRequestsPerSecond(exchangeName: ExchangeNameEnum): number {
  return exchangeName === ExchangeNameEnum.Binance
    ? KLINE_BACKFILL_REQUESTS_PER_SECOND_BINANCE
    : KLINE_BACKFILL_REQUESTS_PER_SECOND_BYBIT;
}

function isKnownInterval(value: string): value is KlineInterval {
  return Object.prototype.hasOwnProperty.call(INTERVAL_MS_BY_KEY, value);
}

export { KLINE_BUFFER_SIZE, KLINE_WATCHDOG_GRACE_MS, KLINE_WATCHDOG_RECOVERY_COOLDOWN_MS, STALENESS_THRESHOLD_MULTIPLIER, SYMBOL_LIST_SYNC_INTERVAL_MS, isKnownInterval, resolveIntervalMs, resolveBackfillRequestsPerSecond };

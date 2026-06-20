import type { KlineInterval } from './marketData.types.js';

const KLINE_BUFFER_SIZE = 499;
const DEFAULT_INTERVAL_MS = 1_800_000;
const STALENESS_THRESHOLD_MULTIPLIER = 2;

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

function isKnownInterval(value: string): value is KlineInterval {
  return Object.prototype.hasOwnProperty.call(INTERVAL_MS_BY_KEY, value);
}

export { KLINE_BUFFER_SIZE, STALENESS_THRESHOLD_MULTIPLIER, isKnownInterval, resolveIntervalMs };

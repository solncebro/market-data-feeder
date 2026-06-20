import type { KlineInterval } from '../domain/marketData.types.js';

enum MenuStep {
  Overview = 'overview',
  Websockets = 'websockets',
  IntervalDetail = 'intervalDetail',
  StaleIntervals = 'staleIntervals',
  Stale = 'stale',
}

interface CallbackData {
  step?: MenuStep;
  interval?: KlineInterval;
}

export { MenuStep };
export type { CallbackData };

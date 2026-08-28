import type { KlineInterval } from '@solncebro/market-data-feeder-lib';

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

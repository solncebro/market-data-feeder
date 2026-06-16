import type { Kline } from '@solncebro/trade-engine';

import type { MaValues } from '../domain/marketData.types.js';

function calculateSma(klineList: Kline[], period: number): number {
  if (klineList.length < period) {
    return 0;
  }

  const periodKlineList = klineList.slice(-period);
  let sum = 0;

  for (const kline of periodKlineList) {
    sum += kline.closePrice;
  }

  return sum / period;
}

function calculateAllMaValues(klineList: Kline[]): MaValues {
  return {
    ma25: calculateSma(klineList, 25),
    ma50: calculateSma(klineList, 50),
    ma100: calculateSma(klineList, 100),
    ma200: calculateSma(klineList, 200),
  };
}

export { calculateAllMaValues };

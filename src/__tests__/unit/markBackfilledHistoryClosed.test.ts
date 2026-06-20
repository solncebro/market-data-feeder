import { describe, expect, it } from 'vitest';

import type { Kline } from '@solncebro/trade-engine';
import { markBackfilledHistoryClosed } from '../../source/marketDataManager.js';

function makeKline(openTimestamp: number, isClosed: boolean | undefined): Kline {
  return {
    openTimestamp,
    openPrice: 100,
    highPrice: 110,
    lowPrice: 90,
    closePrice: 105,
    volume: 1000,
    closeTimestamp: openTimestamp + 1_799_999,
    quoteAssetVolume: 105_000,
    numberOfTrades: 50,
    takerBuyBaseAssetVolume: 500,
    takerBuyQuoteAssetVolume: 52_500,
    isClosed,
  };
}

describe('markBackfilledHistoryClosed', () => {
  it('stamps every REST-backfilled candle except the last as closed', () => {
    const klineList = [makeKline(1000, undefined), makeKline(2000, undefined), makeKline(3000, undefined)];

    markBackfilledHistoryClosed(klineList);

    expect(klineList.map((kline) => kline.isClosed)).toEqual([true, true, undefined]);
  });

  it('leaves the last candle untouched so it stays the current forming one', () => {
    const klineList = [makeKline(1000, undefined), makeKline(2000, false)];

    markBackfilledHistoryClosed(klineList);

    expect(klineList[0].isClosed).toBe(true);
    expect(klineList[1].isClosed).toBe(false);
  });

  it('is a no-op for a single candle (only the current one, nothing to close)', () => {
    const klineList = [makeKline(1000, undefined)];

    markBackfilledHistoryClosed(klineList);

    expect(klineList[0].isClosed).toBeUndefined();
  });

  it('handles an empty list without throwing', () => {
    expect(() => markBackfilledHistoryClosed([])).not.toThrow();
  });
});

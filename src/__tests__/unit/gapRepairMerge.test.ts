import { describe, expect, it } from 'vitest';

import type { Kline } from '@solncebro/trade-engine';

import { mergeRepairedKlines } from '../../source/gapRepair.js';

const INTERVAL_MS = 1_800_000;

function makeKline(openTimestamp: number, closePrice: number, isClosed: boolean): Kline {
  return {
    openTimestamp,
    openPrice: closePrice - 1,
    highPrice: closePrice + 1,
    lowPrice: closePrice - 2,
    closePrice,
    volume: 10,
    closeTimestamp: openTimestamp + INTERVAL_MS - 1,
    quoteAssetVolume: 10,
    numberOfTrades: 5,
    takerBuyBaseAssetVolume: 5,
    takerBuyQuoteAssetVolume: 5,
    isClosed,
  };
}

describe('mergeRepairedKlines', () => {
  it('inserts a missing candle into the hole', () => {
    const bufferKlineList = [makeKline(0, 100, true), makeKline(2 * INTERVAL_MS, 120, false)];
    const fetchedKlineList = [makeKline(0, 100, true), makeKline(INTERVAL_MS, 110, true), makeKline(2 * INTERVAL_MS, 119, false)];

    const result = mergeRepairedKlines({
      bufferKlineList,
      fetchedKlineList,
      formingOpenTimestamp: 2 * INTERVAL_MS,
      maxBufferSize: 499,
    });

    expect(result.mergedKlineList.map((kline) => kline.openTimestamp)).toEqual([0, INTERVAL_MS, 2 * INTERVAL_MS]);
    expect(result.changedCount).toBe(1);
    // The live forming candle keeps its live values — the fetched (older) snapshot must not roll it back.
    expect(result.mergedKlineList[2].closePrice).toBe(120);
  });

  it('replaces a locally sealed candle with the exchange truth (close price correction)', () => {
    const staleSealedKline = makeKline(INTERVAL_MS, 111, true);
    const bufferKlineList = [makeKline(0, 100, true), staleSealedKline, makeKline(2 * INTERVAL_MS, 130, false)];
    const correctedKline = makeKline(INTERVAL_MS, 115, true);
    const fetchedKlineList = [correctedKline, makeKline(2 * INTERVAL_MS, 129, false)];

    const result = mergeRepairedKlines({
      bufferKlineList,
      fetchedKlineList,
      formingOpenTimestamp: 2 * INTERVAL_MS,
      maxBufferSize: 499,
    });

    expect(result.mergedKlineList[1].closePrice).toBe(115);
    expect(result.changedCount).toBe(1);
    expect(result.mergedKlineList[2].closePrice).toBe(130);
  });

  it('reports zero changes when the fetched history matches the buffer', () => {
    const sharedKline = makeKline(0, 100, true);
    const bufferKlineList = [sharedKline, makeKline(INTERVAL_MS, 110, false)];
    const fetchedKlineList = [makeKline(0, 100, true)];

    const result = mergeRepairedKlines({
      bufferKlineList,
      fetchedKlineList,
      formingOpenTimestamp: INTERVAL_MS,
      maxBufferSize: 499,
    });

    expect(result.changedCount).toBe(0);
  });

  it('merges trailing closed candles when no forming candle exists (boundary open-ended)', () => {
    const bufferKlineList = [makeKline(0, 100, true)];
    const fetchedKlineList = [makeKline(INTERVAL_MS, 110, true), makeKline(2 * INTERVAL_MS, 120, true)];

    const result = mergeRepairedKlines({
      bufferKlineList,
      fetchedKlineList,
      formingOpenTimestamp: null,
      maxBufferSize: 499,
    });

    expect(result.mergedKlineList.map((kline) => kline.openTimestamp)).toEqual([0, INTERVAL_MS, 2 * INTERVAL_MS]);
    expect(result.changedCount).toBe(2);
  });

  it('ignores fetched candles that are not closed (forming snapshot from REST)', () => {
    const bufferKlineList = [makeKline(0, 100, true)];
    const fetchedKlineList = [makeKline(INTERVAL_MS, 110, false)];

    const result = mergeRepairedKlines({
      bufferKlineList,
      fetchedKlineList,
      formingOpenTimestamp: null,
      maxBufferSize: 499,
    });

    expect(result.changedCount).toBe(0);
    expect(result.mergedKlineList).toHaveLength(1);
  });

  it('trims the merged buffer to the cap, dropping the oldest candles', () => {
    const bufferKlineList = [makeKline(0, 100, true), makeKline(3 * INTERVAL_MS, 130, false)];
    const fetchedKlineList = [makeKline(INTERVAL_MS, 110, true), makeKline(2 * INTERVAL_MS, 120, true)];

    const result = mergeRepairedKlines({
      bufferKlineList,
      fetchedKlineList,
      formingOpenTimestamp: 3 * INTERVAL_MS,
      maxBufferSize: 3,
    });

    expect(result.mergedKlineList.map((kline) => kline.openTimestamp)).toEqual([INTERVAL_MS, 2 * INTERVAL_MS, 3 * INTERVAL_MS]);
  });
});

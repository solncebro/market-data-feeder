import type { Kline } from '@solncebro/trade-engine';

interface MergeRepairedKlinesArgs {
  bufferKlineList: Kline[];
  fetchedKlineList: Kline[];
  formingOpenTimestamp: number | null;
  maxBufferSize: number;
}

interface MergeRepairedKlinesResult {
  mergedKlineList: Kline[];
  changedCount: number;
}

function isSameKline(first: Kline, second: Kline): boolean {
  return first.openPrice === second.openPrice
    && first.highPrice === second.highPrice
    && first.lowPrice === second.lowPrice
    && first.closePrice === second.closePrice
    && first.volume === second.volume
    && first.isClosed === second.isClosed;
}

// Merges a REST-refetched history slice into a live buffer. Only exchange-confirmed CLOSED candles
// strictly below the live forming candle are merged — the live tail must never be rolled back to an
// older REST snapshot of itself. Equal-timestamp candles are replaced only when they differ (this
// is what corrects a locally sealed close price); missing candles are inserted in order.
function mergeRepairedKlines(args: MergeRepairedKlinesArgs): MergeRepairedKlinesResult {
  const { bufferKlineList, fetchedKlineList, formingOpenTimestamp, maxBufferSize } = args;
  const klineByOpenTimestamp = new Map<number, Kline>();

  for (const kline of bufferKlineList) {
    klineByOpenTimestamp.set(kline.openTimestamp, kline);
  }

  let changedCount = 0;

  for (const fetchedKline of fetchedKlineList) {
    if (fetchedKline.isClosed !== true) {
      continue;
    }

    if (formingOpenTimestamp !== null && fetchedKline.openTimestamp >= formingOpenTimestamp) {
      continue;
    }

    const existingKline = klineByOpenTimestamp.get(fetchedKline.openTimestamp);

    if (existingKline !== undefined && isSameKline(existingKline, fetchedKline)) {
      continue;
    }

    klineByOpenTimestamp.set(fetchedKline.openTimestamp, fetchedKline);
    changedCount += 1;
  }

  const mergedKlineList = Array.from(klineByOpenTimestamp.values())
    .sort((first, second) => first.openTimestamp - second.openTimestamp)
    .slice(-maxBufferSize);

  return { mergedKlineList, changedCount };
}

export { mergeRepairedKlines };
export type { MergeRepairedKlinesArgs, MergeRepairedKlinesResult };

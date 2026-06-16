import { describe, expect, it } from 'vitest';

import type { Kline, MaValues } from '../../domain/marketData.types.js';
import type { MarketDataSnapshotEntry } from '../../domain/snapshot.types.js';
import { MirrorStore } from '../../client/mirrorStore.js';

function makeKline(openTimestamp: number, overrides: Partial<Kline> = {}): Kline {
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
    isClosed: true,
    ...overrides,
  };
}

function makeMa(seed: number): MaValues {
  return { ma25: seed, ma50: seed + 1, ma100: seed + 2, ma200: seed + 3 };
}

function makeEntry(symbol: string, overrides: Partial<MarketDataSnapshotEntry> = {}): MarketDataSnapshotEntry {
  return {
    symbol,
    klineList: [makeKline(1000), makeKline(2000)],
    maValues: makeMa(10),
    currentKline: null,
    volume24hUsdt: 5_000_000,
    ...overrides,
  };
}

describe('MirrorStore', () => {
  it('applySnapshot seeds buffers and getters return them', () => {
    const store = new MirrorStore(499);

    store.applySnapshot({ entryList: [makeEntry('BTCUSDT')] });

    expect(store.getKlineList('BTCUSDT').map((kline) => kline.openTimestamp)).toEqual([1000, 2000]);
    expect(store.getMaValues('BTCUSDT')).toEqual(makeMa(10));
    expect(store.getVolume24h('BTCUSDT')).toBe(5_000_000);
    expect(store.getSymbolList()).toEqual(['BTCUSDT']);
    expect(store.getLastKlineOpenTimestamp('BTCUSDT')).toBe(2000);
  });

  it('returns empty defaults for unknown symbol', () => {
    const store = new MirrorStore(499);

    expect(store.getKlineList('NONE')).toEqual([]);
    expect(store.getMaValues('NONE')).toEqual({ ma25: 0, ma50: 0, ma100: 0, ma200: 0 });
    expect(store.getVolume24h('NONE')).toBe(Number.POSITIVE_INFINITY);
    expect(store.getCurrentKline('NONE')).toBeUndefined();
    expect(store.getLastKlineOpenTimestamp('NONE')).toBeUndefined();
  });

  it('applyKlineClosed on the same candle replaces the last kline, marks it closed, clears currentKline and updates maValues', () => {
    const store = new MirrorStore(499);
    store.applySnapshot({ entryList: [makeEntry('BTCUSDT', { currentKline: makeKline(2000, { isClosed: false }) })] });

    store.applyKlineClosed({ symbol: 'BTCUSDT', kline: makeKline(2000, { closePrice: 222, isClosed: true }), maValues: makeMa(20) });

    const klineList = store.getKlineList('BTCUSDT');
    expect(klineList).toHaveLength(2);
    expect(klineList[1].closePrice).toBe(222);
    expect(klineList[1].isClosed).toBe(true);
    expect(store.getCurrentKline('BTCUSDT')).toBeUndefined();
    expect(store.getMaValues('BTCUSDT')).toEqual(makeMa(20));
  });

  it('applyKlineClosed on a new candle appends and shifts the oldest beyond max buffer size', () => {
    const store = new MirrorStore(3);
    store.applySnapshot({ entryList: [makeEntry('BTCUSDT', { klineList: [makeKline(1000), makeKline(2000), makeKline(3000)] })] });

    store.applyKlineClosed({ symbol: 'BTCUSDT', kline: makeKline(4000), maValues: makeMa(40) });

    expect(store.getKlineList('BTCUSDT').map((kline) => kline.openTimestamp)).toEqual([2000, 3000, 4000]);
  });

  it('applyKlineUpdated on a new candle appends and sets currentKline', () => {
    const store = new MirrorStore(499);
    store.applySnapshot({ entryList: [makeEntry('BTCUSDT')] });

    store.applyKlineUpdated({ symbol: 'BTCUSDT', kline: makeKline(3000, { isClosed: false }), maValues: makeMa(30) });

    expect(store.getKlineList('BTCUSDT').map((kline) => kline.openTimestamp)).toEqual([1000, 2000, 3000]);
    expect(store.getCurrentKline('BTCUSDT')?.openTimestamp).toBe(3000);
    expect(store.getMaValues('BTCUSDT')).toEqual(makeMa(30));
  });

  it('applyKlineUpdated on the same candle replaces the last and sets currentKline', () => {
    const store = new MirrorStore(499);
    store.applySnapshot({ entryList: [makeEntry('BTCUSDT', { klineList: [makeKline(1000), makeKline(2000, { isClosed: false })] })] });

    store.applyKlineUpdated({ symbol: 'BTCUSDT', kline: makeKline(2000, { closePrice: 250, isClosed: false }), maValues: makeMa(25) });

    const klineList = store.getKlineList('BTCUSDT');
    expect(klineList).toHaveLength(2);
    expect(klineList[1].closePrice).toBe(250);
    expect(store.getCurrentKline('BTCUSDT')?.closePrice).toBe(250);
  });

  it('applyKlineTick updates the last kline and sets currentKline without touching maValues', () => {
    const store = new MirrorStore(499);
    store.applySnapshot({ entryList: [makeEntry('BTCUSDT', { klineList: [makeKline(1000), makeKline(2000, { isClosed: false })], maValues: makeMa(10) })] });

    store.applyKlineTick({ symbol: 'BTCUSDT', kline: makeKline(2000, { highPrice: 999, isClosed: false }) });

    expect(store.getKlineList('BTCUSDT')[1].highPrice).toBe(999);
    expect(store.getCurrentKline('BTCUSDT')?.highPrice).toBe(999);
    expect(store.getMaValues('BTCUSDT')).toEqual(makeMa(10));
  });

  it('ignores an older kline', () => {
    const store = new MirrorStore(499);
    store.applySnapshot({ entryList: [makeEntry('BTCUSDT', { klineList: [makeKline(1000), makeKline(2000)] })] });

    store.applyKlineUpdated({ symbol: 'BTCUSDT', kline: makeKline(1500, { closePrice: 1 }), maValues: makeMa(99) });

    expect(store.getKlineList('BTCUSDT').map((kline) => kline.openTimestamp)).toEqual([1000, 2000]);
  });

  it('applySymbolAdded makes the symbol available', () => {
    const store = new MirrorStore(499);

    store.applySymbolAdded(makeEntry('ETHUSDT'));

    expect(store.getSymbolList()).toEqual(['ETHUSDT']);
    expect(store.getKlineList('ETHUSDT')).toHaveLength(2);
  });

  it('applySymbolRemoved drops the symbol from getSymbolList and getKlineList', () => {
    const store = new MirrorStore(499);
    store.applySnapshot({ entryList: [makeEntry('BTCUSDT'), makeEntry('ETHUSDT')] });

    store.applySymbolRemoved('BTCUSDT');

    expect(store.getSymbolList()).toEqual(['ETHUSDT']);
    expect(store.getKlineList('BTCUSDT')).toEqual([]);
  });

  it('applyVolume24h updates the 24h volume', () => {
    const store = new MirrorStore(499);
    store.applySnapshot({ entryList: [makeEntry('BTCUSDT')] });

    store.applyVolume24h({ symbol: 'BTCUSDT', volume24hUsdt: 9_000_000 });

    expect(store.getVolume24h('BTCUSDT')).toBe(9_000_000);
  });

  it('retainSymbols drops symbols absent from the fresh set and keeps the rest', () => {
    const store = new MirrorStore(499);
    store.applySnapshot({ entryList: [makeEntry('BTCUSDT'), makeEntry('ETHUSDT'), makeEntry('SOLUSDT')] });

    store.retainSymbols(new Set(['BTCUSDT', 'SOLUSDT']));

    expect(store.getSymbolList().slice().sort()).toEqual(['BTCUSDT', 'SOLUSDT']);
    expect(store.getKlineList('ETHUSDT')).toEqual([]);
    expect(store.getKlineList('BTCUSDT')).toHaveLength(2);
  });
});

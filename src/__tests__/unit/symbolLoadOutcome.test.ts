import { afterEach, describe, expect, it, vi } from 'vitest';

import { RateLimitedRequestQueue } from '@solncebro/trade-engine';
import type { ExchangeConnector, Kline } from '@solncebro/trade-engine';

import { MarketDataManager } from '../../source/marketDataManager.js';

type FetchImpl = (symbol: string) => Promise<Kline[]>;

function makeKline(openTimestamp: number): Kline {
  return {
    openTimestamp,
    openPrice: 1,
    highPrice: 1,
    lowPrice: 1,
    closePrice: 1,
    volume: 1,
    closeTimestamp: openTimestamp + 1_800_000,
    quoteAssetVolume: 1,
    numberOfTrades: 1,
    takerBuyBaseAssetVolume: 1,
    takerBuyQuoteAssetVolume: 1,
    isClosed: true,
  };
}

function makeHarness(symbolList: string[], fetchImpl: FetchImpl) {
  const futuresClient = {
    fetchKlines: vi.fn((symbol: string) => fetchImpl(symbol)),
    subscribeKlines: vi.fn(),
    unsubscribeKlines: vi.fn(),
  };
  const connector = {
    getFuturesSymbols: vi.fn(async () => symbolList),
    refreshFuturesTradeSymbols: vi.fn(async () => undefined),
    getTicker: vi.fn(() => undefined),
    get futures() {
      return futuresClient;
    },
  } as unknown as ExchangeConnector;
  const manager = new MarketDataManager(connector, '30m', new RateLimitedRequestQueue({ rateLimit: 1000, intervalMs: 1000 }));

  return { manager, futuresClient };
}

describe('MarketDataManager.ensureSymbolLoaded outcome contract', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('returns alreadyLoaded for a buffered symbol without any REST call', async () => {
    const { manager, futuresClient } = makeHarness(['BTCUSDT'], async () => [makeKline(0)]);
    await manager.loadAllSymbols();
    futuresClient.fetchKlines.mockClear();

    await expect(manager.ensureSymbolLoaded('BTCUSDT')).resolves.toBe('alreadyLoaded');
    expect(futuresClient.fetchKlines).not.toHaveBeenCalled();

    await manager.shutdown();
  });

  it('returns notOnExchange for a symbol absent from a non-empty exchange list (no fetch, no failure event)', async () => {
    const { manager, futuresClient } = makeHarness(['BTCUSDT'], async () => [makeKline(0)]);
    const failedSpy = vi.fn();
    manager.on('symbolLoadFailed', failedSpy);

    await expect(manager.ensureSymbolLoaded('GHOSTUSDT')).resolves.toBe('notOnExchange');

    expect(futuresClient.fetchKlines).not.toHaveBeenCalled();
    expect(failedSpy).not.toHaveBeenCalled();

    await manager.shutdown();
  });

  it('falls through to the REST fetch when the exchange list read failed (empty list)', async () => {
    const { manager } = makeHarness([], async () => [makeKline(0)]);

    await expect(manager.ensureSymbolLoaded('BTCUSDT')).resolves.toBe('loaded');

    await manager.shutdown();
  });

  it('rejects on a transient fetch failure and emits symbolLoadFailed', async () => {
    const { manager } = makeHarness(['BTCUSDT'], async () => {
      throw new Error('boom');
    });
    const failedSpy = vi.fn();
    manager.on('symbolLoadFailed', failedSpy);

    await expect(manager.ensureSymbolLoaded('BTCUSDT')).rejects.toThrow();
    expect(failedSpy).toHaveBeenCalledWith('BTCUSDT');

    await manager.shutdown();
  });

  it('resolves noHistory on an empty kline history (a listing before trading starts must not blank the whole scope) and emits symbolLoadFailed once', async () => {
    const { manager } = makeHarness(['BTCUSDT'], async () => []);
    const failedSpy = vi.fn();
    manager.on('symbolLoadFailed', failedSpy);

    await expect(manager.ensureSymbolLoaded('BTCUSDT')).resolves.toBe('noHistory');
    expect(failedSpy).toHaveBeenCalledTimes(1);

    await manager.shutdown();
  });

  it('a transient failure of a new listing does not kill the hourly sync', async () => {
    let isFirstSync = true;
    const futuresClient = {
      fetchKlines: vi.fn(async (symbol: string) => {
        if (symbol === 'NEWUSDT' && isFirstSync) {
          throw new Error('temporarily unavailable');
        }

        return [makeKline(0)];
      }),
      subscribeKlines: vi.fn(),
      unsubscribeKlines: vi.fn(),
    };
    let symbolList = ['AAAUSDT'];
    const connector = {
      getFuturesSymbols: vi.fn(async () => symbolList),
      refreshFuturesTradeSymbols: vi.fn(async () => undefined),
      getTicker: vi.fn(() => undefined),
      get futures() {
        return futuresClient;
      },
    } as unknown as ExchangeConnector;
    const manager = new MarketDataManager(connector, '30m', new RateLimitedRequestQueue({ rateLimit: 1000, intervalMs: 1000 }));
    await manager.loadAllSymbols();

    symbolList = ['AAAUSDT', 'NEWUSDT'];
    await expect(manager.syncAllSymbols()).resolves.toBeUndefined();
    expect(manager.getSymbolList()).toEqual(['AAAUSDT']);

    isFirstSync = false;
    await manager.syncAllSymbols();
    expect(manager.getSymbolList()).toEqual(['AAAUSDT', 'NEWUSDT']);

    await manager.shutdown();
  });
});

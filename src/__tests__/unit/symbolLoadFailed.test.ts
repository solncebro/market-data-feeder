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

function makeConnector(symbolList: string[], fetchImpl: FetchImpl): ExchangeConnector {
  const futuresClient = {
    fetchKlines: vi.fn((symbol: string) => fetchImpl(symbol)),
    subscribeKlines: vi.fn(),
    unsubscribeKlines: vi.fn(),
  };

  return {
    getFuturesSymbols: vi.fn(async () => symbolList),
    getTicker: vi.fn(() => undefined),
    get futures() {
      return futuresClient;
    },
  } as unknown as ExchangeConnector;
}

function makeManager(symbolList: string[], fetchImpl: FetchImpl): MarketDataManager {
  return new MarketDataManager(makeConnector(symbolList, fetchImpl), '30m', new RateLimitedRequestQueue({ rateLimit: 1000, intervalMs: 1000 }));
}

describe('MarketDataManager symbolLoadFailed', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('emits symbolLoadFailed and resolves noHistory when an on-demand load returns no klines', async () => {
    vi.useFakeTimers();
    const manager = makeManager(['BTCUSDT'], async () => []);
    const failedSpy = vi.fn();
    manager.on('symbolLoadFailed', failedSpy);

    const loadPromise = manager.ensureSymbolLoaded('BTCUSDT');
    await vi.advanceTimersByTimeAsync(1100);
    const outcome = await loadPromise;

    expect(outcome).toBe('noHistory');
    expect(failedSpy).toHaveBeenCalledWith('BTCUSDT');

    await manager.shutdown();
  });

  it('emits symbolLoadFailed and rejects when an on-demand fetch throws', async () => {
    vi.useFakeTimers();
    const manager = makeManager(['BTCUSDT'], async () => {
      throw new Error('boom');
    });
    const failedSpy = vi.fn();
    manager.on('symbolLoadFailed', failedSpy);

    const loadPromise = manager.ensureSymbolLoaded('BTCUSDT');
    const rejection = expect(loadPromise).rejects.toThrow();
    await vi.advanceTimersByTimeAsync(1100);
    await rejection;

    expect(failedSpy).toHaveBeenCalledWith('BTCUSDT');

    await manager.shutdown();
  });

  it('emits symbolLoadFailed for bulk-load failures and reports the actual loaded count', async () => {
    vi.useFakeTimers();
    const manager = makeManager(['BTCUSDT', 'ETHUSDT'], async (symbol) => (symbol === 'ETHUSDT' ? [] : [makeKline(0)]));
    const failedSpy = vi.fn();
    const completedSpy = vi.fn();
    manager.on('symbolLoadFailed', failedSpy);
    manager.on('intervalLoadCompleted', completedSpy);

    const loadPromise = manager.loadAllSymbols();
    await vi.advanceTimersByTimeAsync(1100);
    await loadPromise;

    expect(failedSpy).toHaveBeenCalledWith('ETHUSDT');
    expect(failedSpy).toHaveBeenCalledTimes(1);
    expect(completedSpy).toHaveBeenCalledWith(1);

    await manager.shutdown();
  });
});

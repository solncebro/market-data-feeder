import { describe, expect, it, vi } from 'vitest';

import { RateLimitedRequestQueue } from '@solncebro/trade-engine';
import type { ExchangeConnector, Kline, SubscribeKlinesArgs } from '@solncebro/trade-engine';

import { MarketDataManager } from '../../source/marketDataManager.js';

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
  };
}

describe('MarketDataManager shutdown resilience', () => {
  it('keeps unsubscribing the remaining symbols when one unsubscribe throws', async () => {
    const unsubscribedSymbolList: string[] = [];
    const futuresClient = {
      fetchKlines: vi.fn(async () => [makeKline(0)]),
      subscribeKlines: vi.fn(),
      unsubscribeKlines: vi.fn((args: SubscribeKlinesArgs) => {
        if (args.symbol === 'AAAUSDT') {
          throw new Error('transport is down');
        }

        unsubscribedSymbolList.push(args.symbol);
      }),
    };
    const connector = {
      getFuturesSymbols: vi.fn(async () => ['AAAUSDT', 'BBBUSDT']),
      getTicker: vi.fn(() => undefined),
      get futures() {
        return futuresClient;
      },
    } as unknown as ExchangeConnector;
    const manager = new MarketDataManager(connector, '30m', new RateLimitedRequestQueue({ rateLimit: 1000, intervalMs: 1000 }));

    await manager.loadAllSymbols();
    expect(futuresClient.subscribeKlines).toHaveBeenCalledTimes(2);

    await expect(manager.shutdown()).resolves.toBeUndefined();
    expect(unsubscribedSymbolList).toEqual(['BBBUSDT']);
  });

  it('clears every per-symbol buffer on teardown so a torn-down interval frees its memory', async () => {
    const futuresClient = {
      fetchKlines: vi.fn(async () => [makeKline(0), makeKline(1_800_000)]),
      subscribeKlines: vi.fn(),
      unsubscribeKlines: vi.fn(),
    };
    const connector = {
      getFuturesSymbols: vi.fn(async () => ['AAAUSDT', 'BBBUSDT']),
      getTicker: vi.fn(() => undefined),
      get futures() {
        return futuresClient;
      },
    } as unknown as ExchangeConnector;
    const manager = new MarketDataManager(connector, '30m', new RateLimitedRequestQueue({ rateLimit: 1000, intervalMs: 1000 }));

    await manager.loadAllSymbols();
    expect(manager.getSymbolList()).toHaveLength(2);
    expect(manager.getKlineCount()).toBeGreaterThan(0);

    await manager.shutdown();

    expect(manager.getSymbolList()).toEqual([]);
    expect(manager.getKlineCount()).toBe(0);
    expect(manager.getMaValues('AAAUSDT')).toEqual({ ma25: 0, ma50: 0, ma100: 0, ma200: 0 });
    expect(futuresClient.unsubscribeKlines).toHaveBeenCalledTimes(2);
  });
});

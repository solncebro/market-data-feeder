import { describe, expect, it, vi } from 'vitest';

import { RateLimitedRequestQueue } from '@solncebro/trade-engine';
import type { ExchangeConnector, Kline } from '@solncebro/trade-engine';

import { MarketDataManager } from '../../source/marketDataManager.js';

const INTERVAL_MS = 1_800_000;

function makeKline(openTimestamp: number, closePrice: number): Kline {
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
    isClosed: false,
  };
}

describe('double-load race between the bulk load and the on-demand load', () => {
  it('the bulk apply-loop does not overwrite a symbol that already went live via the on-demand path', async () => {
    let resolveBulkFetch: ((klineList: Kline[]) => void) | null = null;
    let fetchCallCount = 0;
    const handlerBySymbol = new Map<string, (symbol: string, kline: Kline) => void>();
    const futuresClient = {
      fetchKlines: vi.fn(() => {
        fetchCallCount += 1;

        // First call = the bulk load (slow); second = the on-demand load (fast).
        if (fetchCallCount === 1) {
          return new Promise<Kline[]>((resolve) => {
            resolveBulkFetch = resolve;
          });
        }

        return Promise.resolve([makeKline(0, 200)]);
      }),
      subscribeKlines: vi.fn((args: { symbol: string; handler: (symbol: string, kline: Kline) => void }) => {
        handlerBySymbol.set(args.symbol, args.handler);
      }),
      unsubscribeKlines: vi.fn(),
    };
    const connector = {
      getFuturesSymbols: vi.fn(async () => ['BTCUSDT']),
      refreshFuturesTradeSymbols: vi.fn(async () => undefined),
      getTicker: vi.fn(() => undefined),
      get futures() {
        return futuresClient;
      },
    } as unknown as ExchangeConnector;
    const manager = new MarketDataManager(connector, '30m', new RateLimitedRequestQueue({ rateLimit: 1000, intervalMs: 1000 }));

    const bulkLoadPromise = manager.loadAllSymbols();
    await vi.waitFor(() => {
      expect(resolveBulkFetch).not.toBeNull();
    });

    // The on-demand path wins the race: symbol loaded, subscribed, and receiving live updates.
    await manager.ensureSymbolLoaded('BTCUSDT');
    handlerBySymbol.get('BTCUSDT')?.('BTCUSDT', makeKline(0, 250));
    expect(manager.getKlineList('BTCUSDT')[0].closePrice).toBe(250);

    // The stale bulk copy resolves afterwards — it must NOT roll the live buffer back.
    (resolveBulkFetch as unknown as (klineList: Kline[]) => void)([makeKline(0, 100)]);
    await bulkLoadPromise;

    expect(manager.getKlineList('BTCUSDT')[0].closePrice).toBe(250);

    await manager.shutdown();
  });

  it('an on-demand load resolving after the bulk load already populated the symbol yields alreadyLoaded', async () => {
    let resolveOnDemandFetch: ((klineList: Kline[]) => void) | null = null;
    let fetchCallCount = 0;
    const futuresClient = {
      fetchKlines: vi.fn(() => {
        fetchCallCount += 1;

        // First call = the on-demand load (slow); second = the bulk load (fast).
        if (fetchCallCount === 1) {
          return new Promise<Kline[]>((resolve) => {
            resolveOnDemandFetch = resolve;
          });
        }

        return Promise.resolve([makeKline(0, 300)]);
      }),
      subscribeKlines: vi.fn(),
      unsubscribeKlines: vi.fn(),
    };
    const connector = {
      getFuturesSymbols: vi.fn(async () => ['BTCUSDT']),
      refreshFuturesTradeSymbols: vi.fn(async () => undefined),
      getTicker: vi.fn(() => undefined),
      get futures() {
        return futuresClient;
      },
    } as unknown as ExchangeConnector;
    const manager = new MarketDataManager(connector, '30m', new RateLimitedRequestQueue({ rateLimit: 1000, intervalMs: 1000 }));

    const onDemandPromise = manager.ensureSymbolLoaded('BTCUSDT');
    await vi.waitFor(() => {
      expect(resolveOnDemandFetch).not.toBeNull();
    });

    await manager.loadAllSymbols();
    expect(manager.getKlineList('BTCUSDT')[0].closePrice).toBe(300);

    (resolveOnDemandFetch as unknown as (klineList: Kline[]) => void)([makeKline(0, 111)]);
    const outcome = await onDemandPromise;

    expect(outcome).toBe('alreadyLoaded');
    expect(manager.getKlineList('BTCUSDT')[0].closePrice).toBe(300);
    // The bulk path registered the only live subscription; the on-demand tail must not double it.
    expect(futuresClient.subscribeKlines).toHaveBeenCalledTimes(1);

    await manager.shutdown();
  });
});

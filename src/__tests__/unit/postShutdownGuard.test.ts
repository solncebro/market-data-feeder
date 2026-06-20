import { afterEach, describe, expect, it, vi } from 'vitest';

import { RateLimitedRequestQueue } from '@solncebro/trade-engine';
import type { ExchangeConnector, Kline } from '@solncebro/trade-engine';

import { MarketDataManager } from '../../source/marketDataManager.js';

interface CapturedHandler {
  handler: ((symbol: string, kline: Kline) => void) | null;
}

interface FetchHolder {
  resolve: ((klineList: Kline[]) => void) | null;
}

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

function makeConnector(captured: CapturedHandler, holder: FetchHolder) {
  const futuresClient = {
    fetchKlines: vi.fn(() => new Promise<Kline[]>((resolve) => {
      holder.resolve = resolve;
    })),
    subscribeKlines: vi.fn((args: { handler: (symbol: string, kline: Kline) => void }) => {
      captured.handler = args.handler;
    }),
    unsubscribeKlines: vi.fn(),
  };

  const connector = {
    getFuturesSymbols: vi.fn(async () => ['BTCUSDT']),
    getTicker: vi.fn(() => undefined),
    get futures() {
      return futuresClient;
    },
  } as unknown as ExchangeConnector;

  return { connector, futuresClient };
}

describe('MarketDataManager post-shutdown guard', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('ignores an inbound kline that arrives after shutdown', async () => {
    vi.useFakeTimers();
    const baseMs = 1_700_000_000_000;
    vi.setSystemTime(baseMs);
    const captured: CapturedHandler = { handler: null };
    const holder: FetchHolder = { resolve: null };
    const { connector } = makeConnector(captured, holder);
    const manager = new MarketDataManager(connector, '30m', new RateLimitedRequestQueue({ rateLimit: 1000, intervalMs: 1000 }));
    const closedSpy = vi.fn();
    manager.on('klineClosed', closedSpy);

    manager.start();
    const loadPromise = manager.ensureSymbolLoaded('BTCUSDT');
    await vi.advanceTimersByTimeAsync(1100);
    holder.resolve?.([makeKline(baseMs - 1_800_000)]);
    await loadPromise;

    await manager.shutdown();
    captured.handler?.('BTCUSDT', makeKline(baseMs));

    expect(closedSpy).not.toHaveBeenCalled();
  });

  it('does not subscribe or emit when an on-demand load resolves after shutdown', async () => {
    vi.useFakeTimers();
    const captured: CapturedHandler = { handler: null };
    const holder: FetchHolder = { resolve: null };
    const { connector, futuresClient } = makeConnector(captured, holder);
    const manager = new MarketDataManager(connector, '30m', new RateLimitedRequestQueue({ rateLimit: 1000, intervalMs: 1000 }));
    const loadedSpy = vi.fn();
    manager.on('symbolLoadCompleted', loadedSpy);

    manager.start();
    const loadPromise = manager.ensureSymbolLoaded('BTCUSDT');
    await vi.advanceTimersByTimeAsync(1100);

    await manager.shutdown();
    holder.resolve?.([makeKline(0)]);
    const result = await loadPromise;

    expect(result).toBe(false);
    expect(loadedSpy).not.toHaveBeenCalled();
    expect(futuresClient.subscribeKlines).not.toHaveBeenCalled();
  });
});

import { afterEach, describe, expect, it, vi } from 'vitest';

import { RateLimitedRequestQueue } from '@solncebro/trade-engine';
import type { ExchangeConnector, Kline } from '@solncebro/trade-engine';

import { MarketDataManager } from '../../source/marketDataManager.js';

const INTERVAL_MS = 60_000;
const BASE_OPEN_MS = 1_700_000_000_000;

interface HandlerHolder {
  handlerBySymbol: Map<string, (symbol: string, kline: Kline) => void>;
}

function makeKline(openTimestamp: number): Kline {
  return {
    openTimestamp,
    openPrice: 1,
    highPrice: 1,
    lowPrice: 1,
    closePrice: 1,
    volume: 1,
    closeTimestamp: openTimestamp + INTERVAL_MS,
    quoteAssetVolume: 1,
    numberOfTrades: 1,
    takerBuyBaseAssetVolume: 1,
    takerBuyQuoteAssetVolume: 1,
    isClosed: true,
  };
}

function makeSymbolList(count: number): string[] {
  return Array.from({ length: count }, (_value, index) => `SYM${index}USDT`);
}

function makeConnector(symbolList: string[], holder: HandlerHolder): ExchangeConnector {
  const futuresClient = {
    fetchKlines: vi.fn(async () => [makeKline(BASE_OPEN_MS)]),
    subscribeKlines: vi.fn((args: { symbol: string; handler: (symbol: string, kline: Kline) => void }) => {
      holder.handlerBySymbol.set(args.symbol, args.handler);
    }),
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

async function loadSymbols(manager: MarketDataManager): Promise<void> {
  const loadPromise = manager.loadAllSymbols();
  await vi.advanceTimersByTimeAsync(2000);
  await loadPromise;
}

describe('MarketDataManager mass-stale escalation', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('emits sourceMassStale once when at least 30% of symbols go stale, then recovers', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(BASE_OPEN_MS);
    const holder: HandlerHolder = { handlerBySymbol: new Map() };
    const manager = new MarketDataManager(makeConnector(makeSymbolList(25), holder), '1m', new RateLimitedRequestQueue({ rateLimit: 1000, intervalMs: 1000 }));
    const massStaleSpy = vi.fn();
    const recoveredSpy = vi.fn();
    manager.on('sourceMassStale', massStaleSpy);
    manager.on('sourceMassStaleRecovered', recoveredSpy);

    manager.start();
    await loadSymbols(manager);

    await vi.advanceTimersByTimeAsync(240_000);

    expect(massStaleSpy).toHaveBeenCalledTimes(1);
    expect(massStaleSpy.mock.calls[0][0]).toBe(25);
    expect(massStaleSpy.mock.calls[0][1]).toBe(25);
    expect(recoveredSpy).not.toHaveBeenCalled();

    const freshOpen = Date.now();

    for (const handler of holder.handlerBySymbol.values()) {
      handler('', makeKline(freshOpen));
    }

    await vi.advanceTimersByTimeAsync(60_000);

    expect(recoveredSpy).toHaveBeenCalledTimes(1);

    await manager.shutdown();
  });

  it('does not escalate when the symbol sample is below the minimum guard', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(BASE_OPEN_MS);
    const holder: HandlerHolder = { handlerBySymbol: new Map() };
    const manager = new MarketDataManager(makeConnector(makeSymbolList(5), holder), '1m', new RateLimitedRequestQueue({ rateLimit: 1000, intervalMs: 1000 }));
    const massStaleSpy = vi.fn();
    manager.on('sourceMassStale', massStaleSpy);

    manager.start();
    await loadSymbols(manager);

    await vi.advanceTimersByTimeAsync(240_000);

    expect(massStaleSpy).not.toHaveBeenCalled();

    await manager.shutdown();
  });
});

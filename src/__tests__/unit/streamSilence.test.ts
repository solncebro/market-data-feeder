import { afterEach, describe, expect, it, vi } from 'vitest';

import { RateLimitedRequestQueue } from '@solncebro/trade-engine';
import type { ExchangeConnector, Kline } from '@solncebro/trade-engine';

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

interface CapturedHandlerHolder {
  handler: ((symbol: string, kline: Kline) => void) | null;
}

function makeConnector(symbol: string, captured: CapturedHandlerHolder): ExchangeConnector {
  const futuresClient = {
    fetchKlines: vi.fn(async () => [makeKline(0)]),
    subscribeKlines: vi.fn((args: { handler: (symbol: string, kline: Kline) => void }) => {
      captured.handler = args.handler;
    }),
    unsubscribeKlines: vi.fn(),
  };

  return {
    getFuturesSymbols: vi.fn(async () => [symbol]),
    getTicker: vi.fn(() => undefined),
    get futures() {
      return futuresClient;
    },
  } as unknown as ExchangeConnector;
}

async function loadSingleSymbol(manager: MarketDataManager): Promise<void> {
  const loadPromise = manager.loadAllSymbols();
  await vi.advanceTimersByTimeAsync(1100);
  await loadPromise;
}

describe('MarketDataManager stream-silence detector', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('emits streamSilent once when no message arrives for longer than the threshold', async () => {
    vi.useFakeTimers();
    const captured: CapturedHandlerHolder = { handler: null };
    const manager = new MarketDataManager(makeConnector('BTCUSDT', captured), '30m', new RateLimitedRequestQueue({ rateLimit: 10, intervalMs: 1000 }));
    const silentSpy = vi.fn();
    manager.on('streamSilent', silentSpy);

    manager.start();
    await loadSingleSymbol(manager);

    await vi.advanceTimersByTimeAsync(60_000);
    expect(silentSpy).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(40_000);
    expect(silentSpy).toHaveBeenCalledTimes(1);

    await manager.shutdown();
  });

  it('emits streamResumed when a message arrives after a silence', async () => {
    vi.useFakeTimers();
    const captured: CapturedHandlerHolder = { handler: null };
    const manager = new MarketDataManager(makeConnector('BTCUSDT', captured), '30m', new RateLimitedRequestQueue({ rateLimit: 10, intervalMs: 1000 }));
    const resumedSpy = vi.fn();
    manager.on('streamResumed', resumedSpy);

    manager.start();
    await loadSingleSymbol(manager);

    await vi.advanceTimersByTimeAsync(60_000);
    captured.handler?.('BTCUSDT', makeKline(Date.now()));

    expect(resumedSpy).toHaveBeenCalledTimes(1);

    await manager.shutdown();
  });

  it('stays quiet while messages keep arriving', async () => {
    vi.useFakeTimers();
    const captured: CapturedHandlerHolder = { handler: null };
    const manager = new MarketDataManager(makeConnector('BTCUSDT', captured), '30m', new RateLimitedRequestQueue({ rateLimit: 10, intervalMs: 1000 }));
    const silentSpy = vi.fn();
    manager.on('streamSilent', silentSpy);

    manager.start();
    await loadSingleSymbol(manager);

    for (let index = 0; index < 6; index++) {
      await vi.advanceTimersByTimeAsync(10_000);
      captured.handler?.('BTCUSDT', makeKline(Date.now()));
    }

    expect(silentSpy).not.toHaveBeenCalled();

    await manager.shutdown();
  });

  it('does not flag silence before any subscription exists', async () => {
    vi.useFakeTimers();
    const captured: CapturedHandlerHolder = { handler: null };
    const manager = new MarketDataManager(makeConnector('BTCUSDT', captured), '30m', new RateLimitedRequestQueue({ rateLimit: 10, intervalMs: 1000 }));
    const silentSpy = vi.fn();
    manager.on('streamSilent', silentSpy);

    manager.start();
    await vi.advanceTimersByTimeAsync(120_000);

    expect(silentSpy).not.toHaveBeenCalled();

    await manager.shutdown();
  });
});

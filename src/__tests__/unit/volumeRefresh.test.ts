import { afterEach, describe, expect, it, vi } from 'vitest';

import { RateLimitedRequestQueue } from '@solncebro/trade-engine';
import type { ExchangeConnector, Kline } from '@solncebro/trade-engine';

import { MarketDataManager } from '../../source/marketDataManager.js';

const VOLUME_REFRESH_INTERVAL_MS = 30_000;

interface VolumeHolder {
  value: number | null;
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

function makeConnector(volumeHolder: VolumeHolder): ExchangeConnector {
  const futuresClient = {
    fetchKlines: vi.fn(async () => [makeKline(0)]),
    subscribeKlines: vi.fn(),
    unsubscribeKlines: vi.fn(),
  };

  return {
    getFuturesSymbols: vi.fn(async () => ['BTCUSDT']),
    getTicker: vi.fn(() => (volumeHolder.value === null ? undefined : { quoteVolume: volumeHolder.value })),
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

describe('MarketDataManager 24h volume refresh', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('emits volume24h only when the 24h volume changes', async () => {
    vi.useFakeTimers();
    const volumeHolder: VolumeHolder = { value: 1000 };
    const manager = new MarketDataManager(makeConnector(volumeHolder), '30m', new RateLimitedRequestQueue({ rateLimit: 10, intervalMs: 1000 }));
    const volumeSpy = vi.fn();
    manager.on('volume24h', volumeSpy);

    manager.start();
    await loadSingleSymbol(manager);

    await vi.advanceTimersByTimeAsync(VOLUME_REFRESH_INTERVAL_MS);
    expect(volumeSpy).toHaveBeenCalledWith('BTCUSDT', 1000);

    volumeSpy.mockClear();
    await vi.advanceTimersByTimeAsync(VOLUME_REFRESH_INTERVAL_MS);
    expect(volumeSpy).not.toHaveBeenCalled();

    volumeHolder.value = 2000;
    await vi.advanceTimersByTimeAsync(VOLUME_REFRESH_INTERVAL_MS);
    expect(volumeSpy).toHaveBeenCalledWith('BTCUSDT', 2000);

    await manager.shutdown();
  });

  it('does not emit volume24h while the ticker is missing (cache miss)', async () => {
    vi.useFakeTimers();
    const volumeHolder: VolumeHolder = { value: null };
    const manager = new MarketDataManager(makeConnector(volumeHolder), '30m', new RateLimitedRequestQueue({ rateLimit: 10, intervalMs: 1000 }));
    const volumeSpy = vi.fn();
    manager.on('volume24h', volumeSpy);

    manager.start();
    await loadSingleSymbol(manager);

    await vi.advanceTimersByTimeAsync(VOLUME_REFRESH_INTERVAL_MS * 3);

    expect(volumeSpy).not.toHaveBeenCalled();

    await manager.shutdown();
  });
});

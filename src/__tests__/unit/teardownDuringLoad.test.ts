import { afterEach, describe, expect, it, vi } from 'vitest';

import { RateLimitedRequestQueue } from '@solncebro/trade-engine';
import type { ExchangeConnector, Kline } from '@solncebro/trade-engine';

import type { KlineInterval } from '../../domain/marketData.types.js';
import type { ManagedSource } from '../../server/subscriptionRegistry.types.js';
import { SubscriptionRegistry } from '../../server/subscriptionRegistry.js';
import { MarketDataManager } from '../../source/marketDataManager.js';

interface FakeSource extends ManagedSource {
  shutdown: ReturnType<typeof vi.fn>;
}

interface LoadControl {
  resolveLoad: (() => void) | null;
}

function makeSlowLoadSource(interval: KlineInterval, control: LoadControl): FakeSource {
  return {
    start: vi.fn(),
    loadAllSymbols: vi.fn(() => new Promise<void>((resolve) => {
      control.resolveLoad = resolve;
    })),
    ensureSymbolLoaded: vi.fn(async () => 'loaded' as const),
    releaseSymbol: vi.fn(),
    syncAllSymbols: vi.fn(async () => undefined),
    shutdown: vi.fn(async () => undefined),
    getInterval: () => interval,
  };
}

describe('SubscriptionRegistry teardown vs in-flight bulk load', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('the deferred teardown timer does not destroy a registration while the all-load is in flight', async () => {
    vi.useFakeTimers();
    const control: LoadControl = { resolveLoad: null };
    let source: FakeSource | null = null;
    const registry = new SubscriptionRegistry<FakeSource>({
      createSource: (interval) => {
        source = makeSlowLoadSource(interval, control);

        return source;
      },
      teardownDelayMs: 30_000,
    });

    const subscribePromise = registry.subscribe({ interval: '30m', scope: { kind: 'all' } });
    await vi.advanceTimersByTimeAsync(0);

    // The only subscriber disconnects while the load is still running.
    await registry.unsubscribe({ interval: '30m', scope: { kind: 'all' } });
    await vi.advanceTimersByTimeAsync(60_000);

    expect(source?.shutdown).not.toHaveBeenCalled();

    // The load settles with nobody subscribed — the continuation re-checks idleness and the
    // deferred teardown finally runs.
    control.resolveLoad?.();
    await subscribePromise;
    await vi.advanceTimersByTimeAsync(30_000);

    expect(source?.shutdown).toHaveBeenCalledTimes(1);
  });

  it('synchronous teardown mode (delay <= 0) tears down right after the orphaned load settles', async () => {
    const control: LoadControl = { resolveLoad: null };
    let source: FakeSource | null = null;
    const registry = new SubscriptionRegistry<FakeSource>({
      createSource: (interval) => {
        source = makeSlowLoadSource(interval, control);

        return source;
      },
      teardownDelayMs: 0,
    });

    const subscribePromise = registry.subscribe({ interval: '30m', scope: { kind: 'all' } });
    await registry.unsubscribe({ interval: '30m', scope: { kind: 'all' } });
    expect(source?.shutdown).not.toHaveBeenCalled();

    control.resolveLoad?.();
    await subscribePromise;

    expect(source?.shutdown).toHaveBeenCalledTimes(1);
  });
});

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

describe('MarketDataManager loadAllSymbols aborted by shutdown', () => {
  it('does not subscribe on the exchange and does not emit load accounting after a mid-backfill shutdown', async () => {
    const fetchResolverList: Array<(klineList: Kline[]) => void> = [];
    const futuresClient = {
      fetchKlines: vi.fn(() => new Promise<Kline[]>((resolve) => {
        fetchResolverList.push(resolve);
      })),
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
    const loadFailedSpy = vi.fn();
    const loadCompletedSpy = vi.fn();
    manager.on('symbolLoadFailed', loadFailedSpy);
    manager.on('intervalLoadCompleted', loadCompletedSpy);

    const loadPromise = manager.loadAllSymbols();
    await vi.waitFor(() => {
      expect(fetchResolverList.length).toBeGreaterThan(0);
    });

    await manager.shutdown();

    for (const resolveFetch of fetchResolverList) {
      resolveFetch([makeKline(0)]);
    }

    await loadPromise;

    expect(futuresClient.subscribeKlines).not.toHaveBeenCalled();
    expect(loadFailedSpy).not.toHaveBeenCalled();
    expect(loadCompletedSpy).not.toHaveBeenCalled();
    expect(manager.getSymbolList()).toEqual([]);
  });

  it('does not enqueue any backfill fetches when shutdown lands during the symbol-list fetch', async () => {
    let resolveSymbolList: ((symbolList: string[]) => void) | null = null;
    const futuresClient = {
      fetchKlines: vi.fn(async () => [makeKline(0)]),
      subscribeKlines: vi.fn(),
      unsubscribeKlines: vi.fn(),
    };
    const connector = {
      getFuturesSymbols: vi.fn(() => new Promise<string[]>((resolve) => {
        resolveSymbolList = resolve;
      })),
      getTicker: vi.fn(() => undefined),
      get futures() {
        return futuresClient;
      },
    } as unknown as ExchangeConnector;
    const manager = new MarketDataManager(connector, '30m', new RateLimitedRequestQueue({ rateLimit: 1000, intervalMs: 1000 }));

    const loadPromise = manager.loadAllSymbols();
    await manager.shutdown();
    (resolveSymbolList as unknown as (symbolList: string[]) => void)(['AAAUSDT', 'BBBUSDT']);
    await loadPromise;

    expect(futuresClient.fetchKlines).not.toHaveBeenCalled();
  });
});

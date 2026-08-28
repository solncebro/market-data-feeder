import { describe, expect, it, vi } from 'vitest';

import type { KlineInterval } from '@solncebro/market-data-feeder-lib';
import type { ManagedSource } from '@solncebro/market-data-feeder-lib';
import { SubscriptionRegistry } from '../../server/subscriptionRegistry.js';

interface FakeSource extends ManagedSource {
  start: ReturnType<typeof vi.fn>;
  loadAllSymbols: ReturnType<typeof vi.fn>;
  ensureSymbolLoaded: ReturnType<typeof vi.fn>;
  releaseSymbol: ReturnType<typeof vi.fn>;
  syncAllSymbols: ReturnType<typeof vi.fn>;
  shutdown: ReturnType<typeof vi.fn>;
}

function makeFakeSource(interval: KlineInterval): FakeSource {
  return {
    start: vi.fn(),
    loadAllSymbols: vi.fn(async () => undefined),
    ensureSymbolLoaded: vi.fn(async () => 'loaded' as const),
    releaseSymbol: vi.fn(),
    syncAllSymbols: vi.fn(async () => undefined),
    shutdown: vi.fn(async () => undefined),
    getInterval: () => interval,
  };
}

function makeRegistry(teardownDelayMs: number = 0): { registry: SubscriptionRegistry<FakeSource>; createdByInterval: Map<KlineInterval, FakeSource>; createSource: ReturnType<typeof vi.fn> } {
  const createdByInterval = new Map<KlineInterval, FakeSource>();
  const createSource = vi.fn((interval: KlineInterval) => {
    const source = makeFakeSource(interval);
    createdByInterval.set(interval, source);

    return source;
  });
  const registry = new SubscriptionRegistry<FakeSource>({ createSource, teardownDelayMs });

  return { registry, createdByInterval, createSource };
}

describe('SubscriptionRegistry', () => {
  it('creates, starts and loads-all the source on the first all-scope subscribe', async () => {
    const { registry, createdByInterval, createSource } = makeRegistry();

    await registry.subscribe({ interval: '30m', scope: { kind: 'all' } });

    expect(createSource).toHaveBeenCalledTimes(1);
    const source = createdByInterval.get('30m');
    expect(source?.start).toHaveBeenCalledTimes(1);
    expect(source?.loadAllSymbols).toHaveBeenCalledTimes(1);
  });

  it('reuses the source and does not reload for a second all-scope subscriber', async () => {
    const { registry, createdByInterval, createSource } = makeRegistry();

    await registry.subscribe({ interval: '30m', scope: { kind: 'all' } });
    await registry.subscribe({ interval: '30m', scope: { kind: 'all' } });

    expect(createSource).toHaveBeenCalledTimes(1);
    expect(createdByInterval.get('30m')?.loadAllSymbols).toHaveBeenCalledTimes(1);
  });

  it('keeps the source alive while at least one all-scope subscriber remains', async () => {
    const { registry, createdByInterval } = makeRegistry();
    await registry.subscribe({ interval: '30m', scope: { kind: 'all' } });
    await registry.subscribe({ interval: '30m', scope: { kind: 'all' } });

    await registry.unsubscribe({ interval: '30m', scope: { kind: 'all' } });

    expect(createdByInterval.get('30m')?.shutdown).not.toHaveBeenCalled();
    expect(registry.getSource('30m')).toBeDefined();
  });

  it('tears the source down when the last all-scope subscriber leaves', async () => {
    const { registry, createdByInterval } = makeRegistry();
    await registry.subscribe({ interval: '30m', scope: { kind: 'all' } });

    await registry.unsubscribe({ interval: '30m', scope: { kind: 'all' } });

    expect(createdByInterval.get('30m')?.shutdown).toHaveBeenCalledTimes(1);
    expect(registry.getSource('30m')).toBeUndefined();
  });

  it('loads only the requested symbols for a symbol-scope subscribe', async () => {
    const { registry, createdByInterval } = makeRegistry();

    await registry.subscribe({ interval: '30m', scope: { kind: 'symbols', symbolList: ['BTCUSDT', 'ETHUSDT'] } });

    const source = createdByInterval.get('30m');
    expect(source?.start).toHaveBeenCalledTimes(1);
    expect(source?.loadAllSymbols).not.toHaveBeenCalled();
    expect(source?.ensureSymbolLoaded).toHaveBeenCalledTimes(2);
    expect(source?.ensureSymbolLoaded).toHaveBeenCalledWith('BTCUSDT');
    expect(source?.ensureSymbolLoaded).toHaveBeenCalledWith('ETHUSDT');
  });

  it('does not reload a symbol that a second subset subscriber also wants', async () => {
    const { registry, createdByInterval } = makeRegistry();

    await registry.subscribe({ interval: '30m', scope: { kind: 'symbols', symbolList: ['BTCUSDT'] } });
    await registry.subscribe({ interval: '30m', scope: { kind: 'symbols', symbolList: ['BTCUSDT'] } });

    expect(createdByInterval.get('30m')?.ensureSymbolLoaded).toHaveBeenCalledTimes(1);
  });

  it('releases a subset symbol and tears the idle source down when its last holder leaves', async () => {
    const { registry, createdByInterval } = makeRegistry();
    await registry.subscribe({ interval: '30m', scope: { kind: 'symbols', symbolList: ['BTCUSDT'] } });

    await registry.unsubscribe({ interval: '30m', scope: { kind: 'symbols', symbolList: ['BTCUSDT'] } });

    const source = createdByInterval.get('30m');
    expect(source?.releaseSymbol).toHaveBeenCalledWith('BTCUSDT');
    expect(source?.shutdown).toHaveBeenCalledTimes(1);
    expect(registry.getSource('30m')).toBeUndefined();
  });

  it('loads all symbols only once under concurrent all-scope subscribes', async () => {
    const { registry, createdByInterval, createSource } = makeRegistry();

    await Promise.all([
      registry.subscribe({ interval: '30m', scope: { kind: 'all' } }),
      registry.subscribe({ interval: '30m', scope: { kind: 'all' } }),
    ]);

    expect(createSource).toHaveBeenCalledTimes(1);
    expect(createdByInterval.get('30m')?.loadAllSymbols).toHaveBeenCalledTimes(1);
  });

  it('creates a separate source per interval', async () => {
    const { registry, createSource } = makeRegistry();

    await registry.subscribe({ interval: '30m', scope: { kind: 'all' } });
    await registry.subscribe({ interval: '5m', scope: { kind: 'all' } });

    expect(createSource).toHaveBeenCalledWith('30m');
    expect(createSource).toHaveBeenCalledWith('5m');
    expect(createSource).toHaveBeenCalledTimes(2);
  });

  it('syncAllLoadedSources runs only on all-loaded sources, not symbol-subset ones', async () => {
    const { registry, createdByInterval } = makeRegistry();
    await registry.subscribe({ interval: '30m', scope: { kind: 'all' } });
    await registry.subscribe({ interval: '5m', scope: { kind: 'symbols', symbolList: ['BTCUSDT'] } });

    await registry.syncAllLoadedSources();

    expect(createdByInterval.get('30m')?.syncAllSymbols).toHaveBeenCalledTimes(1);
    expect(createdByInterval.get('5m')?.syncAllSymbols).not.toHaveBeenCalled();
  });

  it('reports per-interval registration status (all-loaded vs symbol-subset)', async () => {
    const { registry } = makeRegistry();
    await registry.subscribe({ interval: '30m', scope: { kind: 'all' } });
    await registry.subscribe({ interval: '30m', scope: { kind: 'all' } });
    await registry.subscribe({ interval: '5m', scope: { kind: 'symbols', symbolList: ['BTCUSDT', 'ETHUSDT'] } });

    const statusList = registry.getRegistrationStatusList();
    const status30m = statusList.find((status) => status.interval === '30m');
    const status5m = statusList.find((status) => status.interval === '5m');

    expect(statusList).toHaveLength(2);
    expect(status30m).toEqual({ interval: '30m', isAllLoaded: true, allSubscriberCount: 2, refSymbolCount: 0 });
    expect(status5m).toEqual({ interval: '5m', isAllLoaded: false, allSubscriberCount: 0, refSymbolCount: 2 });
  });

  it('drops a torn-down interval from the registration status list', async () => {
    const { registry } = makeRegistry();
    await registry.subscribe({ interval: '30m', scope: { kind: 'all' } });
    await registry.unsubscribe({ interval: '30m', scope: { kind: 'all' } });

    expect(registry.getRegistrationStatusList()).toHaveLength(0);
  });

  it('shuts every source down on registry shutdown', async () => {
    const { registry, createdByInterval } = makeRegistry();
    await registry.subscribe({ interval: '30m', scope: { kind: 'all' } });
    await registry.subscribe({ interval: '5m', scope: { kind: 'all' } });

    await registry.shutdown();

    expect(createdByInterval.get('30m')?.shutdown).toHaveBeenCalledTimes(1);
    expect(createdByInterval.get('5m')?.shutdown).toHaveBeenCalledTimes(1);
    expect(registry.getSource('30m')).toBeUndefined();
    expect(registry.getSource('5m')).toBeUndefined();
  });

  it('defers teardown by the configured delay instead of tearing down immediately', async () => {
    vi.useFakeTimers();

    try {
      const { registry, createdByInterval } = makeRegistry(30_000);
      await registry.subscribe({ interval: '30m', scope: { kind: 'all' } });

      await registry.unsubscribe({ interval: '30m', scope: { kind: 'all' } });

      expect(createdByInterval.get('30m')?.shutdown).not.toHaveBeenCalled();
      expect(registry.getSource('30m')).toBeDefined();

      await vi.advanceTimersByTimeAsync(30_000);

      expect(createdByInterval.get('30m')?.shutdown).toHaveBeenCalledTimes(1);
      expect(registry.getSource('30m')).toBeUndefined();
    } finally {
      vi.useRealTimers();
    }
  });

  it('cancels a pending teardown when a new subscriber arrives within the delay window (no reload)', async () => {
    vi.useFakeTimers();

    try {
      const { registry, createdByInterval, createSource } = makeRegistry(30_000);
      await registry.subscribe({ interval: '30m', scope: { kind: 'all' } });
      await registry.unsubscribe({ interval: '30m', scope: { kind: 'all' } });

      await vi.advanceTimersByTimeAsync(10_000);
      await registry.subscribe({ interval: '30m', scope: { kind: 'all' } });
      await vi.advanceTimersByTimeAsync(30_000);

      expect(createSource).toHaveBeenCalledTimes(1);
      expect(createdByInterval.get('30m')?.loadAllSymbols).toHaveBeenCalledTimes(1);
      expect(createdByInterval.get('30m')?.shutdown).not.toHaveBeenCalled();
      expect(registry.getSource('30m')).toBeDefined();
    } finally {
      vi.useRealTimers();
    }
  });

  it('makes a concurrent second subset subscriber await the in-flight load before returning', async () => {
    const { registry, createdByInterval } = makeRegistry();
    const source = makeFakeSource('30m');
    let resolveLoad: ((value: boolean) => void) | null = null;
    source.ensureSymbolLoaded.mockImplementation(() => new Promise<boolean>((resolve) => {
      resolveLoad = resolve;
    }));
    createdByInterval.set('30m', source);
    const registryWithSource = new SubscriptionRegistry<FakeSource>({ createSource: () => source, teardownDelayMs: 0 });

    const firstPromise = registryWithSource.subscribe({ interval: '30m', scope: { kind: 'symbols', symbolList: ['BTCUSDT'] } });
    const secondPromise = registryWithSource.subscribe({ interval: '30m', scope: { kind: 'symbols', symbolList: ['BTCUSDT'] } });

    let isFirstResolved = false;
    let isSecondResolved = false;
    firstPromise.then(() => { isFirstResolved = true; });
    secondPromise.then(() => { isSecondResolved = true; });

    await Promise.resolve();
    await Promise.resolve();

    expect(source.ensureSymbolLoaded).toHaveBeenCalledTimes(1);
    expect(isFirstResolved).toBe(false);
    expect(isSecondResolved).toBe(false);

    resolveLoad!(true);
    await firstPromise;
    await secondPromise;

    expect(isFirstResolved).toBe(true);
    expect(isSecondResolved).toBe(true);
    expect(source.ensureSymbolLoaded).toHaveBeenCalledTimes(1);
  });

  it('releases a symbol that was unsubscribed while its on-demand load was still in flight', async () => {
    const source = makeFakeSource('30m');
    let resolveLoad: ((value: boolean) => void) | null = null;
    source.ensureSymbolLoaded.mockImplementation(() => new Promise<boolean>((resolve) => {
      resolveLoad = resolve;
    }));
    const registry = new SubscriptionRegistry<FakeSource>({ createSource: () => source, teardownDelayMs: 100_000 });

    const subscribePromise = registry.subscribe({ interval: '30m', scope: { kind: 'symbols', symbolList: ['BTCUSDT'] } });
    await registry.unsubscribe({ interval: '30m', scope: { kind: 'symbols', symbolList: ['BTCUSDT'] } });

    resolveLoad!(true);
    await subscribePromise;

    expect(source.releaseSymbol).toHaveBeenCalledTimes(2);
    expect(source.releaseSymbol).toHaveBeenLastCalledWith('BTCUSDT');
  });

  it('rolls back the all-subscriber count when the initial load fails', async () => {
    const source = makeFakeSource('30m');
    source.loadAllSymbols.mockRejectedValue(new Error('load failed'));
    const registry = new SubscriptionRegistry<FakeSource>({ createSource: () => source, teardownDelayMs: 0 });

    await expect(registry.subscribe({ interval: '30m', scope: { kind: 'all' } })).rejects.toThrow('load failed');

    const status = registry.getRegistrationStatusList().find((item) => item.interval === '30m');

    expect(status?.allSubscriberCount ?? 0).toBe(0);
  });

  it('retries the load for a fresh subscriber after a previous load failed', async () => {
    const source = makeFakeSource('30m');
    source.loadAllSymbols
      .mockRejectedValueOnce(new Error('load failed'))
      .mockResolvedValueOnce(undefined);
    const registry = new SubscriptionRegistry<FakeSource>({ createSource: () => source, teardownDelayMs: 0 });

    await expect(registry.subscribe({ interval: '30m', scope: { kind: 'all' } })).rejects.toThrow('load failed');
    await registry.subscribe({ interval: '30m', scope: { kind: 'all' } });

    const status = registry.getRegistrationStatusList().find((item) => item.interval === '30m');

    expect(status?.allSubscriberCount).toBe(1);
    expect(status?.isAllLoaded).toBe(true);
  });

  it('rolls back the symbol ref-count when an on-demand load fails', async () => {
    const source = makeFakeSource('30m');
    source.ensureSymbolLoaded.mockRejectedValue(new Error('symbol load failed'));
    const registry = new SubscriptionRegistry<FakeSource>({ createSource: () => source, teardownDelayMs: 0 });

    await expect(registry.subscribe({ interval: '30m', scope: { kind: 'symbols', symbolList: ['BTCUSDT'] } })).rejects.toThrow('symbol load failed');

    const status = registry.getRegistrationStatusList().find((item) => item.interval === '30m');

    expect(status?.refSymbolCount ?? 0).toBe(0);
  });
});

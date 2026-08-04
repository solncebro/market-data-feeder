import { EventEmitter } from 'node:events';

import { afterEach, describe, expect, it, vi } from 'vitest';

import type { Kline, KlineInterval, MaValues, StaleSymbolInfo } from '../../domain/marketData.types.js';
import type { FeederSource, StreamLiveness } from '../../server/feederSource.types.js';
import type { SymbolLoadOutcome } from '../../server/subscriptionRegistry.types.js';
import { EmbeddedMarketDataSource } from '../../embedded/embeddedMarketDataSource.js';

const SYMBOL_LIST_SYNC_INTERVAL_MS = 3_600_000;

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

class FakeFeederSource extends EventEmitter implements FeederSource {
  isStreamSilent = false;
  startCallCount = 0;
  syncCallCount = 0;
  shutdownCallCount = 0;
  loadPromise: Promise<void> = Promise.resolve();

  start(): void {
    this.startCallCount += 1;
  }

  loadAllSymbols(): Promise<void> {
    return this.loadPromise;
  }

  ensureSymbolLoaded(): Promise<SymbolLoadOutcome> {
    return Promise.resolve('loaded');
  }

  releaseSymbol(): void {
    return undefined;
  }

  syncAllSymbols(): Promise<void> {
    this.syncCallCount += 1;

    return Promise.resolve();
  }

  shutdown(): Promise<void> {
    this.shutdownCallCount += 1;

    return Promise.resolve();
  }

  getInterval(): KlineInterval {
    return '30m';
  }

  getIntervalMs(): number {
    return 1_800_000;
  }

  getSymbolList(): string[] {
    return ['BTCUSDT'];
  }

  getKlineList(): Kline[] {
    return [];
  }

  getMaValues(): MaValues {
    return { ma25: 0, ma50: 0, ma100: 0, ma200: 0 };
  }

  getCurrentKline(): Kline | undefined {
    return undefined;
  }

  getVolume24h(): number {
    return 123;
  }

  getKlineCount(): number {
    return 0;
  }

  getSubscriptionCount(): number {
    return 0;
  }

  getStaleSymbolList(): StaleSymbolInfo[] {
    return [];
  }

  getLastKlineOpenTimestamp(): number | undefined {
    return undefined;
  }

  getLastUpdateTimestamp(): number | undefined {
    return undefined;
  }

  getStreamLiveness(): StreamLiveness {
    return { lastInboundAtMs: 0, silenceMs: 0, isStreamSilent: this.isStreamSilent };
  }

  getFreshSymbolCount(): number {
    return 0;
  }

  getPersistentStaleCount(): number {
    return 0;
  }
}

describe('EmbeddedMarketDataSource', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('forwards kline and symbol events from the core', () => {
    const fake = new FakeFeederSource();
    const adapter = new EmbeddedMarketDataSource({ source: fake });
    const klineClosedSpy = vi.fn();
    const symbolAddedSpy = vi.fn();
    const symbolRemovedSpy = vi.fn();
    adapter.on('klineClosed', klineClosedSpy);
    adapter.on('symbolAdded', symbolAddedSpy);
    adapter.on('symbolRemoved', symbolRemovedSpy);
    const kline = makeKline(0);
    const maValues: MaValues = { ma25: 1, ma50: 2, ma100: 3, ma200: 4 };

    fake.emit('klineClosed', 'BTCUSDT', kline, maValues);
    fake.emit('symbolAdded', 'ETHUSDT');
    fake.emit('symbolRemoved', 'XRPUSDT');

    expect(klineClosedSpy).toHaveBeenCalledWith('BTCUSDT', kline, maValues);
    expect(symbolAddedSpy).toHaveBeenCalledWith('ETHUSDT');
    expect(symbolRemovedSpy).toHaveBeenCalledWith('XRPUSDT');
  });

  it('maps stream silence to connectionLost and connectionRestored', () => {
    const fake = new FakeFeederSource();
    const adapter = new EmbeddedMarketDataSource({ source: fake });
    const lostSpy = vi.fn();
    const restoredSpy = vi.fn();
    adapter.on('connectionLost', lostSpy);
    adapter.on('connectionRestored', restoredSpy);

    fake.emit('streamSilent', 61_000);
    fake.emit('streamResumed', 61_000);

    expect(lostSpy).toHaveBeenCalledWith('exchange kline stream silent for 61s');
    expect(restoredSpy).toHaveBeenCalledTimes(1);
  });

  it('reports staleness from the core stream liveness', () => {
    const fake = new FakeFeederSource();
    const adapter = new EmbeddedMarketDataSource({ source: fake });

    expect(adapter.isStale()).toBe(false);

    fake.isStreamSilent = true;

    expect(adapter.isStale()).toBe(true);
  });

  it('waitUntilReady starts the core, loads all symbols and arms the hourly symbol sync', async () => {
    vi.useFakeTimers();
    const fake = new FakeFeederSource();
    const adapter = new EmbeddedMarketDataSource({ source: fake });

    await adapter.waitUntilReady(1000);

    expect(fake.startCallCount).toBe(1);

    await vi.advanceTimersByTimeAsync(SYMBOL_LIST_SYNC_INTERVAL_MS);

    expect(fake.syncCallCount).toBe(1);

    await vi.advanceTimersByTimeAsync(SYMBOL_LIST_SYNC_INTERVAL_MS);

    expect(fake.syncCallCount).toBe(2);

    await adapter.shutdown();
  });

  it('waitUntilReady rejects when the load exceeds the timeout', async () => {
    vi.useFakeTimers();
    const fake = new FakeFeederSource();
    fake.loadPromise = new Promise(() => undefined);
    const adapter = new EmbeddedMarketDataSource({ source: fake });
    const readyPromise = adapter.waitUntilReady(10);
    const expectation = expect(readyPromise).rejects.toThrow('failed to load within 10ms');

    await vi.advanceTimersByTimeAsync(11);
    await expectation;
  });

  it('shutdown stops the symbol sync, shuts the core down and tears the exchange connection down', async () => {
    vi.useFakeTimers();
    const fake = new FakeFeederSource();
    const teardownSpy = vi.fn();
    const adapter = new EmbeddedMarketDataSource({ source: fake, teardownConnection: teardownSpy });

    await adapter.waitUntilReady(1000);
    await adapter.shutdown();

    expect(fake.shutdownCallCount).toBe(1);
    expect(teardownSpy).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(SYMBOL_LIST_SYNC_INTERVAL_MS * 2);

    expect(fake.syncCallCount).toBe(0);

    await adapter.shutdown();

    expect(fake.shutdownCallCount).toBe(1);
  });

  it('notifies the operator on whole-source degradations only', () => {
    const fake = new FakeFeederSource();
    const notifySpy = vi.fn();
    new EmbeddedMarketDataSource({ source: fake, onNotify: notifySpy });

    fake.emit('sourceMassStale', 30, 100);
    fake.emit('symbolSyncAnomaly', 5, 100);
    fake.emit('persistentStaleSymbol', 'BTCUSDT');

    expect(notifySpy).toHaveBeenCalledTimes(2);
    expect(notifySpy.mock.calls[0][0]).toContain('30/100');
  });

  it('delegates reads to the core', () => {
    const fake = new FakeFeederSource();
    const adapter = new EmbeddedMarketDataSource({ source: fake });

    expect(adapter.getInterval()).toBe('30m');
    expect(adapter.getIntervalMs()).toBe(1_800_000);
    expect(adapter.getSymbolList()).toEqual(['BTCUSDT']);
    expect(adapter.getVolume24h('BTCUSDT')).toBe(123);
    expect(adapter.getKlineList('BTCUSDT')).toEqual([]);
    expect(adapter.getStaleSymbolList()).toEqual([]);
    expect(adapter.getCurrentKline('BTCUSDT')).toBeUndefined();
  });
});

import { EventEmitter } from 'node:events';

import { WebSocket } from 'ws';
import { describe, expect, it } from 'vitest';
import type { WebSocketLogger } from '@solncebro/websocket-engine';

import type { Kline, KlineInterval, MaValues, StaleSymbolInfo } from '../../domain/marketData.types.js';
import type { FeederSource } from '../../server/feederSource.types.js';
import type { SymbolLoadOutcome } from '../../server/subscriptionRegistry.types.js';
import type { FeederLogger } from '../../server/feederServer.types.js';
import { FeederServer } from '../../server/feederServer.js';
import { MarketDataClient } from '../../client/marketDataClient.js';

const NOOP_FEEDER_LOGGER: FeederLogger = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

const NOOP_WS_LOGGER: WebSocketLogger = {
  debug: () => undefined,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
  fatal: () => undefined,
};

function makeKline(openTimestamp: number): Kline {
  return {
    openTimestamp,
    openPrice: 100,
    highPrice: 110,
    lowPrice: 90,
    closePrice: 105,
    volume: 1000,
    closeTimestamp: openTimestamp + 1_799_999,
    quoteAssetVolume: 105_000,
    numberOfTrades: 50,
    takerBuyBaseAssetVolume: 500,
    takerBuyQuoteAssetVolume: 52_500,
    isClosed: true,
  };
}

function makeMa(seed: number): MaValues {
  return { ma25: seed, ma50: seed + 1, ma100: seed + 2, ma200: seed + 3 };
}

class FakeFeederSource extends EventEmitter implements FeederSource {
  private readonly interval: KlineInterval;
  private readonly klineListBySymbol: Map<string, Kline[]>;

  constructor(interval: KlineInterval, klineListBySymbol: Map<string, Kline[]>) {
    super();
    this.interval = interval;
    this.klineListBySymbol = klineListBySymbol;
  }

  start(): void {
    return undefined;
  }

  async loadAllSymbols(): Promise<void> {
    return undefined;
  }

  async ensureSymbolLoaded(): Promise<SymbolLoadOutcome> {
    return 'loaded';
  }

  releaseSymbol(): void {
    return undefined;
  }

  async syncAllSymbols(): Promise<void> {
    return undefined;
  }

  async shutdown(): Promise<void> {
    return undefined;
  }

  setSymbolKlines(symbol: string, klineList: Kline[]): void {
    this.klineListBySymbol.set(symbol, klineList);
  }

  getInterval(): KlineInterval {
    return this.interval;
  }

  getIntervalMs(): number {
    return 1_800_000;
  }

  getKlineCount(): number {
    let total = 0;

    for (const klineList of this.klineListBySymbol.values()) {
      total += klineList.length;
    }

    return total;
  }

  getSubscriptionCount(): number {
    return this.klineListBySymbol.size;
  }

  getStaleSymbolList(): StaleSymbolInfo[] {
    return [];
  }

  getLastKlineOpenTimestamp(symbol: string): number | undefined {
    const klineList = this.klineListBySymbol.get(symbol);

    if (klineList === undefined || klineList.length === 0) {
      return undefined;
    }

    return klineList[klineList.length - 1].openTimestamp;
  }

  getLastUpdateTimestamp(): number | undefined {
    return undefined;
  }

  getSymbolList(): string[] {
    return Array.from(this.klineListBySymbol.keys());
  }

  getKlineList(symbol: string): Kline[] {
    return this.klineListBySymbol.get(symbol) ?? [];
  }

  getMaValues(): MaValues {
    return makeMa(10);
  }

  getCurrentKline(): Kline | undefined {
    return undefined;
  }

  getVolume24h(): number {
    return 5_000_000;
  }

  getStreamLiveness(): { lastInboundAtMs: number; silenceMs: number; isStreamSilent: boolean } {
    return { lastInboundAtMs: Date.now(), silenceMs: 0, isStreamSilent: false };
  }

  getFreshSymbolCount(): number {
    return this.klineListBySymbol.size;
  }

  getPersistentStaleCount(): number {
    return 0;
  }
}

async function waitFor(predicate: () => boolean, timeoutMs: number = 2000): Promise<void> {
  const startedAtMs = Date.now();

  while (!predicate()) {
    if (Date.now() - startedAtMs > timeoutMs) {
      throw new Error('waitFor timed out');
    }

    await new Promise<void>((resolve) => setTimeout(resolve, 10));
  }
}

function findAllSubscriberCount(server: FeederServer): number {
  return server.getStatus().intervalStatusList.find((item) => item.interval === '30m')?.allSubscriberCount ?? 0;
}

function buildClient(port: number, events: ('klineClosed' | 'klineUpdated' | 'klineUpdatedTick')[]): MarketDataClient {
  return new MarketDataClient({
    url: `ws://127.0.0.1:${port}`,
    interval: '30m',
    scope: { kind: 'all' },
    events,
    wantMa: true,
    logger: NOOP_WS_LOGGER,
  });
}

describe('feeder channel', () => {
  it('does not create a source until a client subscribes, then reuses one source for two clients', async () => {
    const source = new FakeFeederSource('30m', new Map([['BTCUSDT', [makeKline(1000)]]]));
    let createCount = 0;
    const server = new FeederServer({
      port: 0,
      logger: NOOP_FEEDER_LOGGER,
      createSource: () => {
        createCount += 1;

        return source;
      },
    });
    await server.start();
    expect(createCount).toBe(0);

    const clientA = buildClient(server.getPort(), ['klineClosed']);
    const clientB = buildClient(server.getPort(), ['klineClosed']);

    try {
      await clientA.waitUntilReady(3000);
      await clientB.waitUntilReady(3000);

      expect(createCount).toBe(1);
    } finally {
      clientA.close();
      clientB.close();
      await server.shutdown();
    }
  });

  it('delivers a snapshot and forwards a live klineClosed into the client mirror', async () => {
    const source = new FakeFeederSource('30m', new Map([['BTCUSDT', [makeKline(1000), makeKline(2000)]]]));
    const server = new FeederServer({ port: 0, logger: NOOP_FEEDER_LOGGER, createSource: () => source });
    await server.start();

    const client = buildClient(server.getPort(), ['klineClosed', 'klineUpdated', 'klineUpdatedTick']);

    try {
      await client.waitUntilReady(3000);

      expect(client.isStale()).toBe(false);
      expect(client.getSymbolList()).toEqual(['BTCUSDT']);
      expect(client.getKlineList('BTCUSDT').map((kline) => kline.openTimestamp)).toEqual([1000, 2000]);
      expect(client.getVolume24h('BTCUSDT')).toBe(5_000_000);
      expect(client.getMaValues('BTCUSDT')).toEqual(makeMa(10));

      const eventPromise = new Promise<{ symbol: string; openTimestamp: number }>((resolve) => {
        client.once('klineClosed', (symbol: string, kline: Kline) => {
          resolve({ symbol, openTimestamp: kline.openTimestamp });
        });
      });

      source.emit('klineClosed', 'BTCUSDT', makeKline(3000), makeMa(30));

      const event = await eventPromise;

      expect(event).toEqual({ symbol: 'BTCUSDT', openTimestamp: 3000 });
      expect(client.getKlineList('BTCUSDT').map((kline) => kline.openTimestamp)).toEqual([1000, 2000, 3000]);
      expect(client.getMaValues('BTCUSDT')).toEqual(makeMa(30));
    } finally {
      client.close();
      await server.shutdown();
    }
  });

  it('propagates symbolAdded and symbolRemoved into the client mirror and listeners', async () => {
    const source = new FakeFeederSource('30m', new Map([['BTCUSDT', [makeKline(1000)]]]));
    const server = new FeederServer({ port: 0, logger: NOOP_FEEDER_LOGGER, createSource: () => source });
    await server.start();

    const client = buildClient(server.getPort(), ['klineClosed']);

    try {
      await client.waitUntilReady(3000);
      expect(client.getSymbolList()).toEqual(['BTCUSDT']);

      source.setSymbolKlines('ETHUSDT', [makeKline(1000), makeKline(2000)]);
      const addedPromise = new Promise<string>((resolve) => {
        client.once('symbolAdded', (symbol: string) => {
          resolve(symbol);
        });
      });
      source.emit('symbolAdded', 'ETHUSDT');

      expect(await addedPromise).toBe('ETHUSDT');
      expect(client.getSymbolList().slice().sort()).toEqual(['BTCUSDT', 'ETHUSDT']);
      expect(client.getKlineList('ETHUSDT').map((kline) => kline.openTimestamp)).toEqual([1000, 2000]);

      const removedPromise = new Promise<string>((resolve) => {
        client.once('symbolRemoved', (symbol: string) => {
          resolve(symbol);
        });
      });
      source.emit('symbolRemoved', 'BTCUSDT');

      expect(await removedPromise).toBe('BTCUSDT');
      expect(client.getSymbolList()).toEqual(['ETHUSDT']);
      expect(client.getKlineList('BTCUSDT')).toEqual([]);
    } finally {
      client.close();
      await server.shutdown();
    }
  });

  it('releases the registry subscription when a client disconnects during the initial load', async () => {
    let isLoadStarted = false;
    let resolveLoad: (() => void) | null = null;
    const source = new FakeFeederSource('30m', new Map([['BTCUSDT', [makeKline(1000)]]]));
    source.loadAllSymbols = () => {
      isLoadStarted = true;

      return new Promise<void>((resolve) => {
        resolveLoad = resolve;
      });
    };
    const server = new FeederServer({ port: 0, logger: NOOP_FEEDER_LOGGER, createSource: () => source });
    await server.start();

    const socket = new WebSocket(`ws://127.0.0.1:${server.getPort()}`);

    try {
      await new Promise<void>((resolve) => socket.on('open', () => resolve()));
      socket.send(JSON.stringify({ type: 'subscribe', interval: '30m', scope: { kind: 'all' }, events: ['klineClosed'], wantMa: true }));

      await waitFor(() => isLoadStarted);

      const closePromise = new Promise<void>((resolve) => socket.on('close', () => resolve()));
      socket.close();
      await closePromise;

      await waitFor(() => server.getStatus().clientCount === 0);

      // The release is serialized behind the in-flight subscribe op: the counter is held while the
      // load runs and drops to zero right after it settles (no acquire/release race).
      resolveLoad?.();
      await waitFor(() => findAllSubscriberCount(server) === 0);
    } finally {
      resolveLoad?.();
      await server.shutdown();
    }
  });

  it('forwards a live volume24h update into the client mirror regardless of the event set', async () => {
    const source = new FakeFeederSource('30m', new Map([['BTCUSDT', [makeKline(1000)]]]));
    const server = new FeederServer({ port: 0, logger: NOOP_FEEDER_LOGGER, createSource: () => source });
    await server.start();

    const client = buildClient(server.getPort(), ['klineClosed']);

    try {
      await client.waitUntilReady(3000);
      expect(client.getVolume24h('BTCUSDT')).toBe(5_000_000);

      source.emit('volume24h', 'BTCUSDT', 7_777_777);

      await waitFor(() => client.getVolume24h('BTCUSDT') === 7_777_777);

      expect(client.getVolume24h('BTCUSDT')).toBe(7_777_777);
    } finally {
      client.close();
      await server.shutdown();
    }
  });

  it('releases the superseded scope when a client re-subscribes the same interval', async () => {
    const source = new FakeFeederSource('30m', new Map([['BTCUSDT', [makeKline(1000)]]]));
    const server = new FeederServer({ port: 0, logger: NOOP_FEEDER_LOGGER, createSource: () => source });
    await server.start();

    const socket = new WebSocket(`ws://127.0.0.1:${server.getPort()}`);

    try {
      await new Promise<void>((resolve) => socket.on('open', () => resolve()));

      socket.send(JSON.stringify({ type: 'subscribe', interval: '30m', scope: { kind: 'all' }, events: ['klineClosed'], wantMa: true }));
      await waitFor(() => findAllSubscriberCount(server) === 1);

      socket.send(JSON.stringify({ type: 'subscribe', interval: '30m', scope: { kind: 'symbols', symbolList: ['BTCUSDT'] }, events: ['klineClosed'], wantMa: true }));
      await waitFor(() => (server.getStatus().intervalStatusList.find((item) => item.interval === '30m')?.refSymbolCount ?? 0) === 1);

      expect(findAllSubscriberCount(server)).toBe(0);
    } finally {
      socket.close();
      await server.shutdown();
    }
  });

  it('rejects connections beyond the configured limit', async () => {
    const source = new FakeFeederSource('30m', new Map([['BTCUSDT', [makeKline(1000)]]]));
    const server = new FeederServer({ port: 0, logger: NOOP_FEEDER_LOGGER, createSource: () => source, maxConnections: 1 });
    await server.start();

    const firstSocket = new WebSocket(`ws://127.0.0.1:${server.getPort()}`);
    const secondSocket = new WebSocket(`ws://127.0.0.1:${server.getPort()}`);

    try {
      await new Promise<void>((resolve) => firstSocket.on('open', () => resolve()));
      await waitFor(() => server.getStatus().clientCount === 1);

      const secondClosed = new Promise<void>((resolve) => secondSocket.on('close', () => resolve()));
      await secondClosed;

      expect(server.getStatus().clientCount).toBe(1);
    } finally {
      firstSocket.close();
      secondSocket.close();
      await server.shutdown();
    }
  });

  it('rejects start() when the port is already bound', async () => {
    const first = new FeederServer({ port: 0, logger: NOOP_FEEDER_LOGGER, createSource: () => new FakeFeederSource('30m', new Map()) });
    await first.start();

    const second = new FeederServer({ port: first.getPort(), logger: NOOP_FEEDER_LOGGER, createSource: () => new FakeFeederSource('30m', new Map()) });

    try {
      await expect(second.start()).rejects.toThrow();
    } finally {
      await second.shutdown();
      await first.shutdown();
    }
  });

  it('does not forward events a client did not subscribe to', async () => {
    const source = new FakeFeederSource('30m', new Map([['BTCUSDT', [makeKline(1000)]]]));
    const server = new FeederServer({ port: 0, logger: NOOP_FEEDER_LOGGER, createSource: () => source });
    await server.start();

    const client = buildClient(server.getPort(), ['klineClosed']);

    try {
      await client.waitUntilReady(3000);

      let updatedCount = 0;
      client.on('klineUpdated', () => {
        updatedCount += 1;
      });

      const closedPromise = new Promise<void>((resolve) => {
        client.once('klineClosed', () => {
          resolve();
        });
      });

      source.emit('klineUpdated', 'BTCUSDT', makeKline(2000), makeMa(20));
      source.emit('klineClosed', 'BTCUSDT', makeKline(2000), makeMa(20));

      await closedPromise;

      expect(updatedCount).toBe(0);
    } finally {
      client.close();
      await server.shutdown();
    }
  });

  it('prunes per-symbol update timestamps for symbols dropped by a reconnect snapshot', async () => {
    const klineListBySymbol = new Map([['BTCUSDT', [makeKline(1000)]], ['ETHUSDT', [makeKline(1000)]]]);
    const source = new FakeFeederSource('30m', klineListBySymbol);
    const server = new FeederServer({ port: 0, logger: NOOP_FEEDER_LOGGER, createSource: () => source });
    await server.start();

    const client = new MarketDataClient({
      url: `ws://127.0.0.1:${server.getPort()}`,
      interval: '30m',
      scope: { kind: 'all' },
      events: ['klineClosed', 'klineUpdated', 'klineUpdatedTick'],
      wantMa: true,
      logger: NOOP_WS_LOGGER,
      staleThresholdMs: 200,
      staleCheckIntervalMs: 50,
    });

    try {
      await client.waitUntilReady(3000);
      source.emit('klineClosed', 'ETHUSDT', makeKline(2000), makeMa(20));
      await waitFor(() => client.getLastUpdateTimestamp('ETHUSDT') !== undefined);

      // ETHUSDT silently disappears server-side; the client reconnects on staleness and receives a
      // snapshot without it.
      klineListBySymbol.delete('ETHUSDT');
      const restoredPromise = new Promise<void>((resolve) => {
        client.once('connectionRestored', () => {
          resolve();
        });
      });
      await restoredPromise;

      expect(client.getSymbolList()).toEqual(['BTCUSDT']);
      expect(client.getLastUpdateTimestamp('ETHUSDT')).toBeUndefined();
    } finally {
      client.close();
      await server.shutdown();
    }
  }, 15_000);

  it('reseeds the client mirror mid-stream when the source repairs a symbol', async () => {
    const source = new FakeFeederSource('30m', new Map([['BTCUSDT', [makeKline(1000)]]]));
    const server = new FeederServer({ port: 0, logger: NOOP_FEEDER_LOGGER, createSource: () => source });
    await server.start();

    const client = buildClient(server.getPort(), ['klineClosed']);

    try {
      await client.waitUntilReady(3000);
      expect(client.getKlineList('BTCUSDT')).toHaveLength(1);

      // The source healed a hole: the buffer now holds the repaired history.
      source.setSymbolKlines('BTCUSDT', [makeKline(1000), makeKline(2000), makeKline(3000)]);
      source.emit('symbolReseeded', 'BTCUSDT');

      await waitFor(() => client.getKlineList('BTCUSDT').length === 3);
      expect(client.getKlineList('BTCUSDT').map((kline) => kline.openTimestamp)).toEqual([1000, 2000, 3000]);
      expect(client.isStale()).toBe(false);
      expect(client.getSymbolList()).toEqual(['BTCUSDT']);
    } finally {
      client.close();
      await server.shutdown();
    }
  });
});

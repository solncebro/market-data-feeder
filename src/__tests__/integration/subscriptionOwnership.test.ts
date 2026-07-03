import { EventEmitter } from 'node:events';

import { WebSocket } from 'ws';
import { describe, expect, it, vi } from 'vitest';

import type { Kline, KlineInterval, MaValues, StaleSymbolInfo } from '../../domain/marketData.types.js';
import type { FeederSource } from '../../server/feederSource.types.js';
import type { FeederLogger } from '../../server/feederServer.types.js';
import type { SymbolLoadOutcome } from '../../server/subscriptionRegistry.types.js';
import { FeederServer } from '../../server/feederServer.js';

const NOOP_FEEDER_LOGGER: FeederLogger = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
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

class ControllableSource extends EventEmitter implements FeederSource {
  private readonly interval: KlineInterval;
  private readonly klineListBySymbol: Map<string, Kline[]> = new Map();
  public readonly releaseSymbolMock = vi.fn((symbol: string) => {
    this.klineListBySymbol.delete(symbol);
  });
  public pendingLoadResolverBySymbol: Map<string, () => void> = new Map();
  public isSlowLoading = false;

  constructor(interval: KlineInterval) {
    super();
    this.interval = interval;
  }

  start(): void {
    return undefined;
  }

  async loadAllSymbols(): Promise<void> {
    this.klineListBySymbol.set('ALLUSDT', [makeKline(1000)]);
  }

  async ensureSymbolLoaded(symbol: string): Promise<SymbolLoadOutcome> {
    if (this.klineListBySymbol.has(symbol)) {
      return 'alreadyLoaded';
    }

    if (this.isSlowLoading) {
      await new Promise<void>((resolve) => {
        this.pendingLoadResolverBySymbol.set(symbol, resolve);
      });
    }

    this.klineListBySymbol.set(symbol, [makeKline(1000)]);

    return 'loaded';
  }

  releaseSymbol(symbol: string): void {
    this.releaseSymbolMock(symbol);
  }

  async syncAllSymbols(): Promise<void> {
    return undefined;
  }

  async shutdown(): Promise<void> {
    return undefined;
  }

  getInterval(): KlineInterval {
    return this.interval;
  }

  getIntervalMs(): number {
    return 1_800_000;
  }

  getKlineCount(): number {
    return 0;
  }

  getSubscriptionCount(): number {
    return this.klineListBySymbol.size;
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

  getSymbolList(): string[] {
    return Array.from(this.klineListBySymbol.keys());
  }

  getKlineList(symbol: string): Kline[] {
    return this.klineListBySymbol.get(symbol) ?? [];
  }

  getMaValues(): MaValues {
    return { ma25: 1, ma50: 2, ma100: 3, ma200: 4 };
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
    return 0;
  }

  getPersistentStaleCount(): number {
    return 0;
  }
}

async function waitFor(predicate: () => boolean, timeoutMs: number = 3000): Promise<void> {
  const startedAtMs = Date.now();

  while (!predicate()) {
    if (Date.now() - startedAtMs > timeoutMs) {
      throw new Error('waitFor timed out');
    }

    await new Promise<void>((resolve) => setTimeout(resolve, 10));
  }
}

function openRawSocket(port: number): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(`ws://127.0.0.1:${port}`);
    socket.on('open', () => resolve(socket));
    socket.on('error', reject);
  });
}

function statusFor(server: FeederServer, interval: KlineInterval) {
  return server.getStatus().intervalStatusList.find((item) => item.interval === interval);
}

describe('subscription ownership and serialization', () => {
  it('an unsubscribe from a client that never subscribed does not release another client\'s subscription', async () => {
    const source = new ControllableSource('30m');
    const server = new FeederServer({ port: 0, logger: NOOP_FEEDER_LOGGER, createSource: () => source });
    await server.start();

    const subscriber = await openRawSocket(server.getPort());
    const intruder = await openRawSocket(server.getPort());

    try {
      subscriber.send(JSON.stringify({ type: 'subscribe', interval: '30m', scope: { kind: 'all' }, events: ['klineClosed'], wantMa: true }));
      await waitFor(() => statusFor(server, '30m')?.allSubscriberCount === 1);

      intruder.send(JSON.stringify({ type: 'unsubscribe', interval: '30m', scope: { kind: 'all' } }));
      await new Promise((resolve) => setTimeout(resolve, 100));

      expect(statusFor(server, '30m')?.allSubscriberCount).toBe(1);
    } finally {
      subscriber.close();
      intruder.close();
      await server.shutdown();
    }
  });

  it('a disconnect while a symbols-scope acquire is in flight releases every ref (no leak)', async () => {
    const source = new ControllableSource('30m');
    source.isSlowLoading = true;
    const server = new FeederServer({ port: 0, logger: NOOP_FEEDER_LOGGER, createSource: () => source });
    await server.start();

    const client = await openRawSocket(server.getPort());

    try {
      client.send(JSON.stringify({ type: 'subscribe', interval: '30m', scope: { kind: 'symbols', symbolList: ['AAAUSDT', 'BBBUSDT'] }, events: ['klineClosed'], wantMa: true }));
      await waitFor(() => source.pendingLoadResolverBySymbol.has('AAAUSDT'));

      // The client dies while the first symbol is still loading.
      client.terminate();
      await new Promise((resolve) => setTimeout(resolve, 50));

      // The loads settle after the disconnect.
      source.pendingLoadResolverBySymbol.get('AAAUSDT')?.();
      await waitFor(() => source.pendingLoadResolverBySymbol.has('BBBUSDT'));
      source.pendingLoadResolverBySymbol.get('BBBUSDT')?.();

      await waitFor(() => (statusFor(server, '30m')?.refSymbolCount ?? -1) === 0);
    } finally {
      await server.shutdown();
    }
  });

  it('a rapid re-subscribe is serialized: refs do not leak and the LAST snapshot matches the new scope', async () => {
    const source = new ControllableSource('30m');
    source.isSlowLoading = true;
    const server = new FeederServer({ port: 0, logger: NOOP_FEEDER_LOGGER, createSource: () => source });
    await server.start();

    const client = await openRawSocket(server.getPort());
    const finalSnapshotSymbolLists: string[][] = [];
    let snapshotSymbolAccumulator: string[] = [];
    client.on('message', (raw: Buffer) => {
      const message = JSON.parse(raw.toString()) as { type: string; entryList?: Array<{ symbol: string }>; isFinal?: boolean };

      if (message.type !== 'snapshot') {
        return;
      }

      snapshotSymbolAccumulator = snapshotSymbolAccumulator.concat((message.entryList ?? []).map((entry) => entry.symbol));

      if (message.isFinal === true) {
        finalSnapshotSymbolLists.push(snapshotSymbolAccumulator);
        snapshotSymbolAccumulator = [];
      }
    });

    try {
      client.send(JSON.stringify({ type: 'subscribe', interval: '30m', scope: { kind: 'symbols', symbolList: ['AAAUSDT'] }, events: ['klineClosed'], wantMa: true }));
      // Immediately supersede with an all-scope subscribe — the first acquire may be skipped
      // entirely (serialized supersede) or may run; either way it must not leak or misorder.
      client.send(JSON.stringify({ type: 'subscribe', interval: '30m', scope: { kind: 'all' }, events: ['klineClosed'], wantMa: true }));

      const resolvePendingTimer = setInterval(() => {
        for (const resolveLoad of source.pendingLoadResolverBySymbol.values()) {
          resolveLoad();
        }

        source.pendingLoadResolverBySymbol.clear();
      }, 20);

      try {
        await waitFor(() => finalSnapshotSymbolLists.length >= 1);
      } finally {
        clearInterval(resolvePendingTimer);
      }

      expect(statusFor(server, '30m')?.allSubscriberCount).toBe(1);
      expect(statusFor(server, '30m')?.refSymbolCount).toBe(0);
      // Exactly one snapshot reaches the client (the superseded scope's snapshot is skipped), and it
      // is the NEW scope's content.
      expect(finalSnapshotSymbolLists).toHaveLength(1);
      expect(finalSnapshotSymbolLists[0]).toContain('ALLUSDT');
    } finally {
      client.close();
      await server.shutdown();
    }
  });

  it('re-subscribing with an overlapping symbols scope never drops shared symbols to zero refs', async () => {
    const source = new ControllableSource('30m');
    const server = new FeederServer({ port: 0, logger: NOOP_FEEDER_LOGGER, createSource: () => source });
    await server.start();

    const client = await openRawSocket(server.getPort());

    try {
      client.send(JSON.stringify({ type: 'subscribe', interval: '30m', scope: { kind: 'symbols', symbolList: ['AAAUSDT', 'BBBUSDT'] }, events: ['klineClosed'], wantMa: true }));
      await waitFor(() => (statusFor(server, '30m')?.refSymbolCount ?? 0) === 2);

      client.send(JSON.stringify({ type: 'subscribe', interval: '30m', scope: { kind: 'symbols', symbolList: ['AAAUSDT', 'BBBUSDT', 'CCCUSDT'] }, events: ['klineClosed'], wantMa: true }));
      await waitFor(() => (statusFor(server, '30m')?.refSymbolCount ?? 0) === 3);

      // Shared symbols kept their refs throughout — the exchange subscription never churned.
      expect(source.releaseSymbolMock).not.toHaveBeenCalledWith('AAAUSDT');
      expect(source.releaseSymbolMock).not.toHaveBeenCalledWith('BBBUSDT');
    } finally {
      client.close();
      await server.shutdown();
    }
  });
});

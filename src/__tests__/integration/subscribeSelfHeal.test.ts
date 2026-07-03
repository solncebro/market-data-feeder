import { EventEmitter } from 'node:events';

import { describe, expect, it } from 'vitest';
import type { WebSocketLogger } from '@solncebro/websocket-engine';

import type { Kline, KlineInterval, MaValues, StaleSymbolInfo } from '../../domain/marketData.types.js';
import type { FeederSource } from '../../server/feederSource.types.js';
import type { FeederLogger } from '../../server/feederServer.types.js';
import type { SymbolLoadOutcome } from '../../server/subscriptionRegistry.types.js';
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

class FlakyLoadSource extends EventEmitter implements FeederSource {
  private readonly interval: KlineInterval;
  private readonly klineListBySymbol: Map<string, Kline[]> = new Map();
  public failuresRemaining: number;
  public loadAttemptCount = 0;

  constructor(interval: KlineInterval, failuresRemaining: number) {
    super();
    this.interval = interval;
    this.failuresRemaining = failuresRemaining;
  }

  start(): void {
    return undefined;
  }

  async loadAllSymbols(): Promise<void> {
    this.loadAttemptCount += 1;

    if (this.failuresRemaining > 0) {
      this.failuresRemaining -= 1;
      throw new Error('exchange REST is down');
    }

    this.klineListBySymbol.set('BTCUSDT', [makeKline(1000)]);
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

class SlowLoadSource extends FlakyLoadSource {
  public resolveLoad: (() => void) | null = null;

  override async loadAllSymbols(): Promise<void> {
    await new Promise<void>((resolve) => {
      this.resolveLoad = resolve;
    });

    await super.loadAllSymbols();
  }
}

describe('subscribe self-healing', () => {
  it('does NOT force a reconnect while the FIRST snapshot is still being built (cold-feeder bulk load)', async () => {
    const source = new SlowLoadSource('30m', 0);
    const server = new FeederServer({
      port: 0,
      logger: NOOP_FEEDER_LOGGER,
      createSource: () => source,
    });
    await server.start();

    const client = new MarketDataClient({
      url: `ws://127.0.0.1:${server.getPort()}`,
      interval: '30m',
      scope: { kind: 'all' },
      events: ['klineClosed', 'klineUpdated', 'klineUpdatedTick'],
      wantMa: true,
      logger: NOOP_WS_LOGGER,
      staleThresholdMs: 100,
      staleCheckIntervalMs: 25,
    });
    const lostSpy: string[] = [];
    client.on('connectionLost', (reason: string) => {
      lostSpy.push(reason);
    });

    try {
      // Far past 2× the stale threshold — a pre-ready client must NOT self-heal-reconnect while the
      // server is legitimately still backfilling (a Binance cold start takes minutes).
      await new Promise((resolve) => setTimeout(resolve, 600));
      expect(lostSpy).toEqual([]);

      source.resolveLoad?.();
      await client.waitUntilReady(5_000);
      expect(client.getSymbolList()).toEqual(['BTCUSDT']);
    } finally {
      client.close();
      await server.shutdown();
    }
  }, 15_000);

  it('a failed subscribe closes the client socket; the client reconnects, retries and recovers', async () => {
    const source = new FlakyLoadSource('30m', 1);
    const server = new FeederServer({
      port: 0,
      logger: NOOP_FEEDER_LOGGER,
      createSource: () => source,
      subscribeFailureCloseDelayMs: 20,
    });
    await server.start();

    const client = new MarketDataClient({
      url: `ws://127.0.0.1:${server.getPort()}`,
      interval: '30m',
      scope: { kind: 'all' },
      events: ['klineClosed'],
      wantMa: true,
      logger: NOOP_WS_LOGGER,
    });

    try {
      await client.waitUntilReady(10_000);

      expect(source.loadAttemptCount).toBeGreaterThanOrEqual(2);
      expect(client.getSymbolList()).toEqual(['BTCUSDT']);
    } finally {
      client.close();
      await server.shutdown();
    }
  }, 15_000);

  it('the client forces a reconnect when data staleness persists (silent lost subscription)', async () => {
    const source = new FlakyLoadSource('30m', 0);
    const server = new FeederServer({
      port: 0,
      logger: NOOP_FEEDER_LOGGER,
      createSource: () => source,
    });
    await server.start();

    const client = new MarketDataClient({
      url: `ws://127.0.0.1:${server.getPort()}`,
      interval: '30m',
      scope: { kind: 'all' },
      events: ['klineClosed', 'klineUpdated', 'klineUpdatedTick'],
      wantMa: true,
      logger: NOOP_WS_LOGGER,
      staleThresholdMs: 300,
      staleCheckIntervalMs: 50,
    });

    const restoredPromise = new Promise<void>((resolve) => {
      client.once('connectionRestored', () => {
        resolve();
      });
    });

    try {
      await client.waitUntilReady(5_000);

      // The server sends no data messages after the snapshot (heartbeats only), so the client goes
      // stale, forces a reconnect and completes a fresh snapshot — observable as connectionRestored.
      await restoredPromise;

      expect(client.getSymbolList()).toEqual(['BTCUSDT']);
    } finally {
      client.close();
      await server.shutdown();
    }
  }, 15_000);
});

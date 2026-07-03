import { EventEmitter } from 'node:events';

import { WebSocket } from 'ws';
import { describe, expect, it } from 'vitest';

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

class StaticSource extends EventEmitter implements FeederSource {
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

async function waitFor(predicate: () => boolean, timeoutMs: number = 5000): Promise<void> {
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

// Reaches into the underlying TCP socket to simulate a hung client process: the kernel keeps
// ACKing, but the application never reads — no pongs, no message processing.
function pauseTcp(socket: WebSocket): void {
  (socket as unknown as { _socket: { pause: () => void } })._socket.pause();
}

describe('slow / half-dead client protection', () => {
  it('terminates a client that stops answering pings and releases its subscription refs', async () => {
    const source = new StaticSource('30m', new Map([['BTCUSDT', [makeKline(1000)]]]));
    const server = new FeederServer({
      port: 0,
      logger: NOOP_FEEDER_LOGGER,
      createSource: () => source,
      heartbeatIntervalMs: 30,
      maxMissedPongCount: 2,
    });
    await server.start();

    const client = await openRawSocket(server.getPort());

    try {
      client.send(JSON.stringify({ type: 'subscribe', interval: '30m', scope: { kind: 'all' }, events: ['klineClosed'], wantMa: true }));
      await waitFor(() => server.getStatus().clientCount === 1);

      pauseTcp(client);

      await waitFor(() => server.getStatus().clientCount === 0);
      await waitFor(() => (server.getStatus().intervalStatusList.find((item) => item.interval === '30m')?.allSubscriberCount ?? -1) === 0);
    } finally {
      client.terminate();
      await server.shutdown();
    }
  });

  it('a healthy responsive client is NOT terminated by the ping watchdog', async () => {
    const source = new StaticSource('30m', new Map([['BTCUSDT', [makeKline(1000)]]]));
    const server = new FeederServer({
      port: 0,
      logger: NOOP_FEEDER_LOGGER,
      createSource: () => source,
      heartbeatIntervalMs: 20,
      maxMissedPongCount: 2,
    });
    await server.start();

    const client = await openRawSocket(server.getPort());

    try {
      client.send(JSON.stringify({ type: 'subscribe', interval: '30m', scope: { kind: 'all' }, events: ['klineClosed'], wantMa: true }));
      await waitFor(() => server.getStatus().clientCount === 1);

      // Live through many heartbeat ticks — the ws library answers pings automatically.
      await new Promise((resolve) => setTimeout(resolve, 300));

      expect(server.getStatus().clientCount).toBe(1);
    } finally {
      client.close();
      await server.shutdown();
    }
  });

  it('terminates a client whose send buffer stays over the limit for two consecutive ticks', async () => {
    const source = new StaticSource('30m', new Map([['BTCUSDT', [makeKline(1000)]]]));
    const server = new FeederServer({
      port: 0,
      logger: NOOP_FEEDER_LOGGER,
      createSource: () => source,
      heartbeatIntervalMs: 30,
      maxMissedPongCount: 1000,
      slowClientBufferedLimitBytes: 1,
    });
    await server.start();

    const client = await openRawSocket(server.getPort());

    try {
      client.send(JSON.stringify({ type: 'subscribe', interval: '30m', scope: { kind: 'all' }, events: ['klineClosed'], wantMa: true }));
      await waitFor(() => server.getStatus().clientCount === 1);

      pauseTcp(client);

      // Flood the paused client until the kernel send buffer fills and bytes start piling up in
      // the ws user-space buffer (bufferedAmount > 0).
      const floodTimer = setInterval(() => {
        for (let i = 0; i < 2000; i += 1) {
          source.emit('klineClosed', 'BTCUSDT', makeKline(1000 + i), { ma25: 1, ma50: 2, ma100: 3, ma200: 4 });
        }
      }, 20);

      try {
        await waitFor(() => server.getStatus().clientCount === 0, 10_000);
      } finally {
        clearInterval(floodTimer);
      }
    } finally {
      client.terminate();
      await server.shutdown();
    }
  }, 20_000);
});

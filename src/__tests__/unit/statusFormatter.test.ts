import { describe, expect, it } from 'vitest';

import type { FeederStatus, IntervalStatus, SymbolDiagnostics } from '../../server/feederServer.types.js';
import { formatIntervalDetailMessage, formatOverviewMessage, formatStaleSymbolMessage, formatSymbolCard, resolveIntervalHealth } from '../../telegram/statusFormatter.js';

function makeIntervalStatus(overrides: Partial<IntervalStatus> = {}): IntervalStatus {
  return {
    interval: '30m',
    intervalMs: 1_800_000,
    isAllLoaded: true,
    allSubscriberCount: 1,
    refSymbolCount: 0,
    symbolCount: 100,
    klineCount: 49_900,
    staleCount: 0,
    subscriptionCount: 100,
    liveness: { lastInboundAtMs: Date.now() - 4000, silenceMs: 4000, isStreamSilent: false },
    freshCount: 100,
    persistentStaleCount: 0,
    ...overrides,
  };
}

function makeSymbolDiagnostics(overrides: Partial<SymbolDiagnostics> = {}): SymbolDiagnostics {
  return {
    symbol: 'BTCUSDT',
    interval: '30m',
    lastPrice: 64_210.5,
    lastKlineOpenMs: Date.now() - 240_000,
    lastUpdateMs: Date.now() - 4000,
    maValues: { ma25: 64_000, ma50: 63_500, ma100: 62_000, ma200: 60_000 },
    volume24hUsdt: 1_204_553_000,
    isStale: false,
    staleAgeMs: undefined,
    ...overrides,
  };
}

describe('resolveIntervalHealth', () => {
  it('returns the idle indicator when there are no symbols loaded yet', () => {
    expect(resolveIntervalHealth(makeIntervalStatus({ symbolCount: 0, klineCount: 0, subscriptionCount: 0 }))).toBe('⚪');
  });

  it('returns the healthy indicator when nothing is stale', () => {
    expect(resolveIntervalHealth(makeIntervalStatus({ symbolCount: 100, staleCount: 0 }))).toBe('🟢');
  });

  it('returns the warning indicator when a small fraction is stale', () => {
    expect(resolveIntervalHealth(makeIntervalStatus({ symbolCount: 100, staleCount: 3 }))).toBe('🟡');
  });

  it('returns the critical indicator when a large fraction is stale', () => {
    expect(resolveIntervalHealth(makeIntervalStatus({ symbolCount: 100, staleCount: 40 }))).toBe('🔴');
  });
});

describe('formatOverviewMessage', () => {
  it('shows the exchange, address, uptime, client count and a line per interval', () => {
    const status: FeederStatus = {
      host: '127.0.0.1',
      port: 7071,
      clientCount: 3,
      uptimeMs: 7_500_000,
      intervalStatusList: [
        makeIntervalStatus({ interval: '30m', symbolCount: 450, klineCount: 224_550, staleCount: 0, subscriptionCount: 450, allSubscriberCount: 2, freshCount: 450 }),
        makeIntervalStatus({ interval: '5m', isAllLoaded: false, allSubscriberCount: 0, refSymbolCount: 50, symbolCount: 50, klineCount: 998, staleCount: 1, subscriptionCount: 50, freshCount: 49 }),
      ],
    };

    const message = formatOverviewMessage(status, 'bybit');

    expect(message).toContain('BYBIT');
    expect(message).toContain('127.0.0.1:7071');
    expect(message).toContain('Clients connected: 3');
    expect(message).toContain('30m');
    expect(message).toContain('5m');
    expect(message).toContain('450 symbols');
    expect(message).toContain('🟢');
    expect(message).toContain('🟡');
  });

  it('states explicitly when no interval is active', () => {
    const status: FeederStatus = { host: '127.0.0.1', port: 7070, clientCount: 0, uptimeMs: 1000, intervalStatusList: [] };

    const message = formatOverviewMessage(status, 'binance');

    expect(message).toContain('BINANCE');
    expect(message.toLowerCase()).toContain('no active');
  });
});

describe('formatIntervalDetailMessage', () => {
  it('shows the load mode, a live stream and the diagnostic counters', () => {
    const message = formatIntervalDetailMessage(makeIntervalStatus({ interval: '30m', symbolCount: 450, klineCount: 224_550, staleCount: 1, freshCount: 449, persistentStaleCount: 0 }), 'bybit');

    expect(message).toContain('30m interval');
    expect(message).toContain('Load mode: all loaded');
    expect(message).toContain('Stream: 🟢 live');
    expect(message).toContain('Klines buffered: 224,550');
    expect(message).toContain('Fresh symbols: 449');
    expect(message).toContain('Stale symbols: 1');
    expect(message).toContain('Persistent-stale symbols: 0');
  });

  it('shows a silent stream with its silence duration', () => {
    const message = formatIntervalDetailMessage(makeIntervalStatus({ liveness: { lastInboundAtMs: Date.now() - 73_000, silenceMs: 73_000, isStreamSilent: true } }), 'bybit');

    expect(message).toContain('🔴 silent for');
  });
});

describe('formatSymbolCard', () => {
  it('renders a fresh symbol with price, moving averages and volume', () => {
    const message = formatSymbolCard(makeSymbolDiagnostics());

    expect(message).toContain('`BTCUSDT`');
    expect(message).toContain('· 30m');
    expect(message).toContain('Last price:');
    expect(message).toContain('MA25 / MA50 / MA100 / MA200');
    expect(message).toContain('24h volume (USDT):');
    expect(message).toContain('🟢 fresh');
  });

  it('renders a stale symbol with the stale state', () => {
    const message = formatSymbolCard(makeSymbolDiagnostics({ isStale: true, staleAgeMs: 4_320_000 }));

    expect(message).toContain('🔴 stale');
  });
});

describe('formatStaleSymbolMessage', () => {
  it('lists the stale symbols with their age', () => {
    const message = formatStaleSymbolMessage('30m', [
      { symbol: 'FOOUSDT', ageMs: 7_200_000 },
      { symbol: 'BARUSDT', ageMs: 3_900_000 },
    ]);

    expect(message).toContain('30m');
    expect(message).toContain('FOOUSDT');
    expect(message).toContain('BARUSDT');
  });

  it('states explicitly when nothing is stale', () => {
    const message = formatStaleSymbolMessage('30m', []);

    expect(message).toContain('30m');
    expect(message.toLowerCase()).toContain('no stale');
  });

  it('renders tiny prices with enough precision instead of a misleading "0"', () => {
    const message = formatSymbolCard(makeSymbolDiagnostics({ lastPrice: 0.00001234 }));

    expect(message).toContain('0.00001234');
  });
});

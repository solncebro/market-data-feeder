import { afterEach, describe, expect, it, vi } from 'vitest';

import { RateLimitedRequestQueue } from '@solncebro/trade-engine';
import type { ExchangeConnector } from '@solncebro/trade-engine';

import { MarketDataManager } from '../../source/marketDataManager.js';

const BACKFILL_RATE_LIMIT = 5;

function makeSymbolList(count: number): string[] {
  return Array.from({ length: count }, (_, index) => `SYM${index}USDT`);
}

function makeNeverResolvingConnector(symbolList: string[]): { connector: ExchangeConnector; fetchKlines: ReturnType<typeof vi.fn> } {
  const fetchKlines = vi.fn(() => new Promise<never>(() => undefined));
  const futuresClient = {
    fetchKlines,
    subscribeKlines: vi.fn(),
    unsubscribeKlines: vi.fn(),
  };

  const connector = {
    getFuturesSymbols: vi.fn(async () => symbolList),
    get futures() {
      return futuresClient;
    },
  } as unknown as ExchangeConnector;

  return { connector, fetchKlines };
}

async function flushMicrotasks(times: number = 500): Promise<void> {
  for (let index = 0; index < times; index++) {
    await Promise.resolve();
  }
}

describe('MarketDataManager backfill pacing', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('does not fire the whole symbol list at once — caps in-flight backfill requests at the queue rate limit', async () => {
    vi.useFakeTimers();
    const symbolList = makeSymbolList(50);
    const { connector, fetchKlines } = makeNeverResolvingConnector(symbolList);
    const backfillQueue = new RateLimitedRequestQueue({ rateLimit: BACKFILL_RATE_LIMIT, intervalMs: 1000 });
    const manager = new MarketDataManager(connector, '30m', backfillQueue);

    void manager.loadAllSymbols();
    await flushMicrotasks();

    expect(fetchKlines.mock.calls.length).toBe(BACKFILL_RATE_LIMIT);
    expect(fetchKlines.mock.calls.length).toBeLessThan(symbolList.length);
  });

  it('eventually fetches every symbol once the rate-limit window advances — nothing is dropped', async () => {
    vi.useFakeTimers();
    const symbolList = makeSymbolList(50);
    const { connector, fetchKlines } = makeNeverResolvingConnector(symbolList);
    const backfillQueue = new RateLimitedRequestQueue({ rateLimit: BACKFILL_RATE_LIMIT, intervalMs: 1000 });
    const manager = new MarketDataManager(connector, '30m', backfillQueue);

    void manager.loadAllSymbols();
    await flushMicrotasks();
    await vi.advanceTimersByTimeAsync(11_000);
    await flushMicrotasks();

    expect(fetchKlines.mock.calls.length).toBe(symbolList.length);
  });
});

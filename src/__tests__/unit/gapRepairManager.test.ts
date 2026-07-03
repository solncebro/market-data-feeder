import { describe, expect, it, vi } from 'vitest';

import { RateLimitedRequestQueue } from '@solncebro/trade-engine';
import type { ExchangeConnector, Kline } from '@solncebro/trade-engine';

import { MarketDataManager } from '../../source/marketDataManager.js';

const INTERVAL_MS = 1_800_000;

function makeKline(openTimestamp: number, closePrice: number, isClosed: boolean): Kline {
  return {
    openTimestamp,
    openPrice: closePrice - 1,
    highPrice: closePrice + 1,
    lowPrice: closePrice - 2,
    closePrice,
    volume: 10,
    closeTimestamp: openTimestamp + INTERVAL_MS - 1,
    quoteAssetVolume: 10,
    numberOfTrades: 5,
    takerBuyBaseAssetVolume: 5,
    takerBuyQuoteAssetVolume: 5,
    isClosed,
  };
}

interface Harness {
  manager: MarketDataManager;
  futuresClient: {
    fetchKlines: ReturnType<typeof vi.fn>;
    subscribeKlines: ReturnType<typeof vi.fn>;
    unsubscribeKlines: ReturnType<typeof vi.fn>;
  };
  pushKline: (symbol: string, kline: Kline) => void;
  baseOpenMs: number;
}

// The initial backfill returns one closed candle at baseOpenMs and a forming candle right after it;
// later fetches are controlled per-test via fetchImplHolder.
function makeHarness(fetchImplHolder: { impl: ((symbol: string) => Promise<Kline[]>) | null }): Harness {
  // Anchor near "now" so the freshness guard sees live candles as fresh.
  const nowMs = Date.now();
  const baseOpenMs = nowMs - (nowMs % INTERVAL_MS) - 4 * INTERVAL_MS;
  const handlerBySymbol = new Map<string, (symbol: string, kline: Kline) => void>();
  const futuresClient = {
    fetchKlines: vi.fn(async (symbol: string) => {
      if (fetchImplHolder.impl !== null) {
        return fetchImplHolder.impl(symbol);
      }

      return [makeKline(baseOpenMs, 100, true), makeKline(baseOpenMs + INTERVAL_MS, 110, false)];
    }),
    subscribeKlines: vi.fn((args: { symbol: string; handler: (symbol: string, kline: Kline) => void }) => {
      handlerBySymbol.set(args.symbol, args.handler);
    }),
    unsubscribeKlines: vi.fn(),
  };
  const connector = {
    getFuturesSymbols: vi.fn(async () => ['BTCUSDT']),
    refreshFuturesTradeSymbols: vi.fn(async () => undefined),
    getTicker: vi.fn(() => undefined),
    get futures() {
      return futuresClient;
    },
  } as unknown as ExchangeConnector;
  const manager = new MarketDataManager(connector, '30m', new RateLimitedRequestQueue({ rateLimit: 1000, intervalMs: 1000 }));

  return {
    manager,
    futuresClient,
    pushKline: (symbol, kline) => {
      handlerBySymbol.get(symbol)?.(symbol, kline);
    },
    baseOpenMs,
  };
}

describe('MarketDataManager gap repair', () => {
  it('a gapped candle triggers a REST repair that heals the hole and reseeds clients', async () => {
    const fetchImplHolder: { impl: ((symbol: string) => Promise<Kline[]>) | null } = { impl: null };
    const { manager, futuresClient, pushKline, baseOpenMs } = makeHarness(fetchImplHolder);
    await manager.loadAllSymbols();
    const reseedSpy = vi.fn();
    manager.on('symbolReseeded', reseedSpy);

    // The repair fetch returns the true history including the candle the stream skipped.
    fetchImplHolder.impl = async () => [
      makeKline(baseOpenMs, 100, true),
      makeKline(baseOpenMs + INTERVAL_MS, 111, true),
      makeKline(baseOpenMs + 2 * INTERVAL_MS, 120, true),
      makeKline(baseOpenMs + 3 * INTERVAL_MS, 130, false),
    ];

    // The live stream resumes at baseOpen+3 intervals — candle at baseOpen+2 was never delivered.
    pushKline('BTCUSDT', makeKline(baseOpenMs + 3 * INTERVAL_MS, 130, false));

    await vi.waitFor(() => {
      expect(reseedSpy).toHaveBeenCalledWith('BTCUSDT');
    });

    const openList = manager.getKlineList('BTCUSDT').map((kline) => kline.openTimestamp);
    expect(openList).toEqual([baseOpenMs, baseOpenMs + INTERVAL_MS, baseOpenMs + 2 * INTERVAL_MS, baseOpenMs + 3 * INTERVAL_MS]);

    await manager.shutdown();
  });

  it('a locally sealed close is corrected from REST (the exchange close frame was lost)', async () => {
    const fetchImplHolder: { impl: ((symbol: string) => Promise<Kline[]>) | null } = { impl: null };
    const { manager, pushKline, baseOpenMs } = makeHarness(fetchImplHolder);
    await manager.loadAllSymbols();
    const reseedSpy = vi.fn();
    manager.on('symbolReseeded', reseedSpy);

    // The exchange says the candle at baseOpen+1 really closed at 115, not the last-seen 110.
    fetchImplHolder.impl = async () => [
      makeKline(baseOpenMs + INTERVAL_MS, 115, true),
      makeKline(baseOpenMs + 2 * INTERVAL_MS, 121, false),
    ];

    // The close frame of the forming candle was lost — the next candle arrives directly.
    pushKline('BTCUSDT', makeKline(baseOpenMs + 2 * INTERVAL_MS, 121, false));

    await vi.waitFor(() => {
      expect(reseedSpy).toHaveBeenCalledWith('BTCUSDT');
    });

    const klineList = manager.getKlineList('BTCUSDT');
    const sealedKline = klineList.find((kline) => kline.openTimestamp === baseOpenMs + INTERVAL_MS);
    expect(sealedKline?.closePrice).toBe(115);

    await manager.shutdown();
  });

  it('repairs are rate-limited per symbol by a cooldown', async () => {
    const fetchImplHolder: { impl: ((symbol: string) => Promise<Kline[]>) | null } = { impl: null };
    const { manager, futuresClient, pushKline, baseOpenMs } = makeHarness(fetchImplHolder);
    await manager.loadAllSymbols();
    const reseedSpy = vi.fn();
    manager.on('symbolReseeded', reseedSpy);
    fetchImplHolder.impl = async () => [makeKline(baseOpenMs + INTERVAL_MS, 115, true)];

    pushKline('BTCUSDT', makeKline(baseOpenMs + 2 * INTERVAL_MS, 121, false));
    await vi.waitFor(() => {
      expect(reseedSpy).toHaveBeenCalledTimes(1);
    });
    const fetchCallCountAfterFirstRepair = futuresClient.fetchKlines.mock.calls.length;

    // A second local close right after — inside the cooldown, no second REST fetch.
    pushKline('BTCUSDT', makeKline(baseOpenMs + 3 * INTERVAL_MS, 125, false));
    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(futuresClient.fetchKlines.mock.calls.length).toBe(fetchCallCountAfterFirstRepair);

    await manager.shutdown();
  });

  it('a repair resolving after the symbol was released does not resurrect the buffer', async () => {
    const fetchImplHolder: { impl: ((symbol: string) => Promise<Kline[]>) | null } = { impl: null };
    const { manager, pushKline, baseOpenMs } = makeHarness(fetchImplHolder);
    await manager.loadAllSymbols();
    const reseedSpy = vi.fn();
    manager.on('symbolReseeded', reseedSpy);

    let resolveFetch: ((klineList: Kline[]) => void) | null = null;
    fetchImplHolder.impl = () => new Promise<Kline[]>((resolve) => {
      resolveFetch = resolve;
    });

    pushKline('BTCUSDT', makeKline(baseOpenMs + 3 * INTERVAL_MS, 130, false));
    await vi.waitFor(() => {
      expect(resolveFetch).not.toBeNull();
    });

    manager.releaseSymbol('BTCUSDT');
    (resolveFetch as unknown as (klineList: Kline[]) => void)([makeKline(baseOpenMs + 2 * INTERVAL_MS, 120, true)]);
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(manager.getSymbolList()).toEqual([]);
    expect(reseedSpy).not.toHaveBeenCalled();

    await manager.shutdown();
  });
});

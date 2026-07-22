import { afterEach, describe, expect, it, vi } from 'vitest';

import { RateLimitedRequestQueue } from '@solncebro/trade-engine';
import type { ExchangeConnector, Kline, TradeSymbol } from '@solncebro/trade-engine';

import { MarketDataManager } from '../../source/marketDataManager.js';

const INTERVAL_MS = 1_800_000;
const STALENESS_CHECK_INTERVAL_MS = 60_000;
const GAP_REPAIR_COOLDOWN_MS = 600_000;

function makeKlineAt(openTimestamp: number, closePrice: number, isClosed: boolean, intervalMs: number): Kline {
  return {
    openTimestamp,
    openPrice: closePrice - 1,
    highPrice: closePrice + 1,
    lowPrice: closePrice - 2,
    closePrice,
    volume: 10,
    closeTimestamp: openTimestamp + intervalMs - 1,
    quoteAssetVolume: 10,
    numberOfTrades: 5,
    takerBuyBaseAssetVolume: 5,
    takerBuyQuoteAssetVolume: 5,
    isClosed,
  };
}

function makeKline(openTimestamp: number, closePrice: number, isClosed: boolean): Kline {
  return makeKlineAt(openTimestamp, closePrice, isClosed, INTERVAL_MS);
}

// A contiguous run of closed candles [baseOpenMs, baseOpenMs + (count-1)*intervalMs].
function makeClosedRunAt(baseOpenMs: number, count: number, intervalMs: number): Kline[] {
  return Array.from({ length: count }, (_value, index) => makeKlineAt(baseOpenMs + index * intervalMs, 100 + index, true, intervalMs));
}

function makeClosedRun(baseOpenMs: number, count: number): Kline[] {
  return makeClosedRunAt(baseOpenMs, count, INTERVAL_MS);
}

async function flushMicrotasks(): Promise<void> {
  for (let index = 0; index < 8; index++) {
    await Promise.resolve();
  }
}

interface HarnessArgs {
  // Candles returned by the INITIAL bulk load.
  initialKlineList: Kline[];
  // launchTimestamp exposed via tradeSymbols; undefined omits the trade-symbol entry entirely.
  launchTimestamp: number | undefined;
  // Kline interval of the manager (default '30m'); tests about multi-hour probe cadences use '1d'
  // so the launch-based expectation does not drift while fake time advances.
  interval?: '30m' | '1d';
}

interface Harness {
  manager: MarketDataManager;
  futuresClient: {
    fetchKlines: ReturnType<typeof vi.fn>;
    subscribeKlines: ReturnType<typeof vi.fn>;
    unsubscribeKlines: ReturnType<typeof vi.fn>;
    tradeSymbols: Map<string, TradeSymbol>;
  };
  // Controls what every fetch AFTER the initial load returns. The requested limit is passed through
  // so a test exchange can honor it like the real one (a 2-candle repair must not "magically"
  // deliver the full history — that would hide the exact bug class this suite guards against).
  fetchImplHolder: { impl: ((symbol: string, limit: number) => Promise<Kline[]>) | null };
  pushKline: (symbol: string, kline: Kline) => void;
}

// An exchange that honors the requested limit: serves the LAST `limit` candles of the full history.
function makeLimitHonoringExchange(fullHistory: Kline[]): (symbol: string, limit: number) => Promise<Kline[]> {
  return async (_symbol: string, limit: number) => fullHistory.slice(-limit);
}

function makeHarness(args: HarnessArgs): Harness {
  const fetchImplHolder: { impl: ((symbol: string, limit: number) => Promise<Kline[]>) | null } = { impl: null };
  let isInitialLoadDone = false;
  const tradeSymbols = new Map<string, TradeSymbol>();
  const handlerBySymbol = new Map<string, (symbol: string, kline: Kline) => void>();

  if (args.launchTimestamp !== undefined) {
    tradeSymbols.set('BTCUSDT', { launchTimestamp: args.launchTimestamp } as unknown as TradeSymbol);
  }

  const futuresClient = {
    fetchKlines: vi.fn(async (symbol: string, _interval: string, options?: { limit?: number }) => {
      if (!isInitialLoadDone) {
        isInitialLoadDone = true;

        return args.initialKlineList;
      }

      if (fetchImplHolder.impl !== null) {
        return fetchImplHolder.impl(symbol, options?.limit ?? Number.MAX_SAFE_INTEGER);
      }

      return args.initialKlineList;
    }),
    subscribeKlines: vi.fn((subscribeArgs: { symbol: string; handler: (symbol: string, kline: Kline) => void }) => {
      handlerBySymbol.set(subscribeArgs.symbol, subscribeArgs.handler);
    }),
    unsubscribeKlines: vi.fn(),
    tradeSymbols,
  };

  const connector = {
    getFuturesSymbols: vi.fn(async () => ['BTCUSDT']),
    refreshFuturesTradeSymbols: vi.fn(async () => undefined),
    getTicker: vi.fn(() => undefined),
    get futures() {
      return futuresClient;
    },
  } as unknown as ExchangeConnector;

  const manager = new MarketDataManager(connector, args.interval ?? '30m', new RateLimitedRequestQueue({ rateLimit: 1000, intervalMs: 1000 }));

  return {
    manager,
    futuresClient,
    fetchImplHolder,
    pushKline: (symbol, kline) => {
      handlerBySymbol.get(symbol)?.(symbol, kline);
    },
  };
}

describe('MarketDataManager undergrown backfill', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('re-backfills a symbol whose buffer loaded far below its expected history', async () => {
    vi.useFakeTimers();
    const nowMs = 1_900_000_000_000;
    vi.setSystemTime(nowMs);
    const baseOpenMs = nowMs - (nowMs % INTERVAL_MS) - 250 * INTERVAL_MS;

    // The symbol was listed ~250 intervals ago, but the initial load returned only one candle.
    const { manager, fetchImplHolder } = makeHarness({
      initialKlineList: [makeKline(baseOpenMs, 100, true)],
      launchTimestamp: baseOpenMs,
    });
    manager.start();

    const loadPromise = manager.loadAllSymbols();
    await vi.advanceTimersByTimeAsync(2000);
    await loadPromise;

    expect(manager.getKlineList('BTCUSDT').length).toBe(1);

    const reseedSpy = vi.fn();
    manager.on('symbolReseeded', reseedSpy);

    // The exchange now serves the full history.
    fetchImplHolder.impl = async () => makeClosedRun(baseOpenMs, 250);

    // The 60s staleness scan should notice the undergrown buffer and schedule a backfill.
    await vi.advanceTimersByTimeAsync(STALENESS_CHECK_INTERVAL_MS);
    await flushMicrotasks();

    expect(reseedSpy).toHaveBeenCalledWith('BTCUSDT');
    expect(manager.getKlineList('BTCUSDT').length).toBe(250);

    await manager.shutdown();
  });

  it('does not flag a symbol whose short buffer matches its recent listing', async () => {
    vi.useFakeTimers();
    const nowMs = 1_900_000_000_000;
    vi.setSystemTime(nowMs);
    // Listed ~5 intervals ago and loaded 5 candles — genuinely young, not undergrown.
    const baseOpenMs = nowMs - (nowMs % INTERVAL_MS) - 5 * INTERVAL_MS;

    const { manager, futuresClient, fetchImplHolder } = makeHarness({
      initialKlineList: makeClosedRun(baseOpenMs, 5),
      launchTimestamp: baseOpenMs,
    });
    manager.start();

    const loadPromise = manager.loadAllSymbols();
    await vi.advanceTimersByTimeAsync(2000);
    await loadPromise;

    const fetchCallsAfterLoad = futuresClient.fetchKlines.mock.calls.length;
    fetchImplHolder.impl = async () => makeClosedRun(baseOpenMs, 5);

    await vi.advanceTimersByTimeAsync(STALENESS_CHECK_INTERVAL_MS);
    await flushMicrotasks();

    expect(futuresClient.fetchKlines.mock.calls.length).toBe(fetchCallsAfterLoad);

    await manager.shutdown();
  });

  it('stops re-scheduling once a backfill converges below the expected size', async () => {
    vi.useFakeTimers();
    const nowMs = 1_900_000_000_000;
    vi.setSystemTime(nowMs);
    // Listing time implies ~250 candles, but the exchange only ever serves 100.
    const baseOpenMs = nowMs - (nowMs % INTERVAL_MS) - 250 * INTERVAL_MS;

    const { manager, futuresClient, fetchImplHolder } = makeHarness({
      initialKlineList: [makeKline(baseOpenMs, 100, true)],
      launchTimestamp: baseOpenMs,
    });
    manager.start();

    const loadPromise = manager.loadAllSymbols();
    await vi.advanceTimersByTimeAsync(2000);
    await loadPromise;

    fetchImplHolder.impl = async () => makeClosedRun(baseOpenMs, 100);

    // First scan: backfill grows the buffer to 100 (still < 250 → undergrown, so it stays a candidate).
    await vi.advanceTimersByTimeAsync(STALENESS_CHECK_INTERVAL_MS);
    await flushMicrotasks();
    expect(manager.getKlineList('BTCUSDT').length).toBe(100);

    // Two refetches past the cooldown return the same 100 (changedCount 0) — the first is the
    // pending vote, the second latches the convergence.
    await vi.advanceTimersByTimeAsync(GAP_REPAIR_COOLDOWN_MS);
    await flushMicrotasks();
    await vi.advanceTimersByTimeAsync(GAP_REPAIR_COOLDOWN_MS);
    await flushMicrotasks();
    const fetchCallsAfterConverge = futuresClient.fetchKlines.mock.calls.length;

    // Two more cooldown windows: a converged symbol must not be refetched again.
    await vi.advanceTimersByTimeAsync(GAP_REPAIR_COOLDOWN_MS * 2);
    await flushMicrotasks();

    expect(futuresClient.fetchKlines.mock.calls.length).toBe(fetchCallsAfterConverge);

    await manager.shutdown();
  });

  it('a partial merge on a converged symbol resumes retries WITHOUT a premature recovered signal', async () => {
    vi.useFakeTimers();
    const nowMs = 1_900_000_000_000;
    vi.setSystemTime(nowMs);
    const baseOpenMs = nowMs - (nowMs % INTERVAL_MS) - 250 * INTERVAL_MS;

    // Honest short load: 100 candles while the listing time implies ~250.
    const { manager, futuresClient, fetchImplHolder, pushKline } = makeHarness({
      initialKlineList: makeClosedRun(baseOpenMs, 100),
      launchTimestamp: baseOpenMs,
    });
    const stuckSpy = vi.fn();
    const recoveredSpy = vi.fn();
    manager.on('symbolBackfillStuck', stuckSpy);
    manager.on('symbolBackfillRecovered', recoveredSpy);
    manager.start();

    const loadPromise = manager.loadAllSymbols();
    await vi.advanceTimersByTimeAsync(2000);
    await loadPromise;

    // Two full-window refetches add nothing → latch.
    fetchImplHolder.impl = async () => makeClosedRun(baseOpenMs, 100);
    await vi.advanceTimersByTimeAsync(STALENESS_CHECK_INTERVAL_MS);
    await flushMicrotasks();
    await vi.advanceTimersByTimeAsync(GAP_REPAIR_COOLDOWN_MS);
    await flushMicrotasks();
    expect(stuckSpy).toHaveBeenCalledTimes(1);

    // A live candle arrives with a one-candle hole → a SMALL hole repair merges one candle.
    // The buffer is still far below the expected history: retries must resume, but "recovered"
    // must NOT fire — the symbol has not caught up.
    const fullHistory = [...makeClosedRun(baseOpenMs, 101), makeKline(baseOpenMs + 101 * INTERVAL_MS, 202, false)];
    fetchImplHolder.impl = makeLimitHonoringExchange(fullHistory);
    await vi.advanceTimersByTimeAsync(1_240_000);
    pushKline('BTCUSDT', makeKline(baseOpenMs + 101 * INTERVAL_MS, 202, false));
    await vi.advanceTimersByTimeAsync(1000);
    await flushMicrotasks();

    expect(manager.getKlineList('BTCUSDT').length).toBe(102);
    expect(recoveredSpy).not.toHaveBeenCalled();

    // The converged flag is gone, so the scan resumes full backfills once the cooldown allows.
    const fullFetchCountBefore = futuresClient.fetchKlines.mock.calls.filter((call) => (call[2] as { limit?: number } | undefined)?.limit === 499).length;
    await vi.advanceTimersByTimeAsync(GAP_REPAIR_COOLDOWN_MS + STALENESS_CHECK_INTERVAL_MS);
    await flushMicrotasks();
    const fullFetchCountAfter = futuresClient.fetchKlines.mock.calls.filter((call) => (call[2] as { limit?: number } | undefined)?.limit === 499).length;

    expect(fullFetchCountAfter).toBeGreaterThan(fullFetchCountBefore);
    expect(recoveredSpy).not.toHaveBeenCalled();

    await manager.shutdown();
  });

  it('a converged symbol is re-probed hourly so a persistent truncation heals within the hour', async () => {
    vi.useFakeTimers();
    const nowMs = 1_900_000_000_000;
    vi.setSystemTime(nowMs);
    const baseOpenMs = nowMs - (nowMs % INTERVAL_MS) - 250 * INTERVAL_MS;
    const truncatedResponse = [makeKline(baseOpenMs + 249 * INTERVAL_MS, 349, true)];

    const { manager, futuresClient, fetchImplHolder } = makeHarness({
      initialKlineList: truncatedResponse,
      launchTimestamp: baseOpenMs,
    });
    const stuckSpy = vi.fn();
    const recoveredSpy = vi.fn();
    manager.on('symbolBackfillStuck', stuckSpy);
    manager.on('symbolBackfillRecovered', recoveredSpy);
    manager.start();

    const loadPromise = manager.loadAllSymbols();
    await vi.advanceTimersByTimeAsync(2000);
    await loadPromise;

    // The truncation persists across BOTH full-window retries → the symbol latches as converged.
    fetchImplHolder.impl = async () => truncatedResponse;
    await vi.advanceTimersByTimeAsync(STALENESS_CHECK_INTERVAL_MS);
    await flushMicrotasks();
    await vi.advanceTimersByTimeAsync(GAP_REPAIR_COOLDOWN_MS);
    await flushMicrotasks();
    expect(stuckSpy).toHaveBeenCalledTimes(1);
    const fetchCallsAfterLatch = futuresClient.fetchKlines.mock.calls.length;

    // The exchange recovers. Until the hourly re-probe is due, no fetches happen…
    fetchImplHolder.impl = async (_symbol: string, limit: number) => makeClosedRun(baseOpenMs, 250).slice(-limit);
    await vi.advanceTimersByTimeAsync(3_480_000);
    await flushMicrotasks();
    expect(futuresClient.fetchKlines.mock.calls.length).toBe(fetchCallsAfterLatch);

    // …and once it is due, the probe fetches the real history and the symbol recovers.
    await vi.advanceTimersByTimeAsync(180_000);
    await flushMicrotasks();

    expect(futuresClient.fetchKlines.mock.calls.length).toBe(fetchCallsAfterLatch + 1);
    expect(manager.getKlineList('BTCUSDT').length).toBe(250);
    expect(recoveredSpy).toHaveBeenCalledTimes(1);

    await manager.shutdown();
  });

  it('accepts a twice-reconfirmed short history as the exchange truth and stops flagging the symbol', async () => {
    vi.useFakeTimers();
    const nowMs = 1_900_000_000_000;
    vi.setSystemTime(nowMs);
    const dayMs = 86_400_000;
    const baseOpenMs = nowMs - (nowMs % dayMs) - 250 * dayMs;
    // Listed 250 intervals ago, but trading genuinely started 150 intervals later — the exchange
    // serves exactly 100 candles, forever.
    const honestShortHistory = makeClosedRunAt(baseOpenMs + 150 * dayMs, 100, dayMs);

    const { manager, futuresClient, fetchImplHolder } = makeHarness({
      initialKlineList: honestShortHistory,
      launchTimestamp: baseOpenMs,
      interval: '1d',
    });
    const stuckSpy = vi.fn();
    const recoveredSpy = vi.fn();
    manager.on('symbolBackfillStuck', stuckSpy);
    manager.on('symbolBackfillRecovered', recoveredSpy);
    manager.start();

    const loadPromise = manager.loadAllSymbols();
    await vi.advanceTimersByTimeAsync(2000);
    await loadPromise;
    fetchImplHolder.impl = async (_symbol: string, limit: number) => honestShortHistory.slice(-limit);

    // Two full-window refetches (vote + latch) …
    await vi.advanceTimersByTimeAsync(STALENESS_CHECK_INTERVAL_MS);
    await flushMicrotasks();
    await vi.advanceTimersByTimeAsync(GAP_REPAIR_COOLDOWN_MS);
    await flushMicrotasks();
    expect(stuckSpy).toHaveBeenCalledTimes(1);

    // …then the first hourly re-probe reconfirms (still no recovery)…
    await vi.advanceTimersByTimeAsync(3_660_000);
    await flushMicrotasks();
    expect(recoveredSpy).not.toHaveBeenCalled();

    // …and the second re-probe accepts the short history as the exchange's truth: the symbol
    // recovers (leaves the digest) and is measured from its own first candle from now on.
    await vi.advanceTimersByTimeAsync(3_600_000);
    await flushMicrotasks();
    expect(recoveredSpy).toHaveBeenCalledTimes(1);
    const fetchCallsAfterAcceptance = futuresClient.fetchKlines.mock.calls.length;

    // No more probes, no re-flag: the accepted symbol reads healthy.
    await vi.advanceTimersByTimeAsync(3_700_000);
    await flushMicrotasks();

    expect(futuresClient.fetchKlines.mock.calls.length).toBe(fetchCallsAfterAcceptance);
    expect(stuckSpy).toHaveBeenCalledTimes(1);
    expect(manager.getKlineList('BTCUSDT').length).toBe(100);

    await manager.shutdown();
  });

  it('leaves a symbol untouched when its listing time is unknown', async () => {
    vi.useFakeTimers();
    const nowMs = 1_900_000_000_000;
    vi.setSystemTime(nowMs);
    const baseOpenMs = nowMs - (nowMs % INTERVAL_MS) - 250 * INTERVAL_MS;

    // No trade-symbol entry → unknown listing time → the scan must not flag it.
    const { manager, futuresClient, fetchImplHolder } = makeHarness({
      initialKlineList: [makeKline(baseOpenMs, 100, true)],
      launchTimestamp: undefined,
    });
    manager.start();

    const loadPromise = manager.loadAllSymbols();
    await vi.advanceTimersByTimeAsync(2000);
    await loadPromise;

    const fetchCallsAfterLoad = futuresClient.fetchKlines.mock.calls.length;
    fetchImplHolder.impl = async () => makeClosedRun(baseOpenMs, 250);

    await vi.advanceTimersByTimeAsync(STALENESS_CHECK_INTERVAL_MS);
    await flushMicrotasks();

    expect(futuresClient.fetchKlines.mock.calls.length).toBe(fetchCallsAfterLoad);
    expect(manager.getKlineList('BTCUSDT').length).toBe(1);

    await manager.shutdown();
  });

  it('a converged symbol vanishing from the trade-symbol cache must not read as recovered', async () => {
    vi.useFakeTimers();
    const nowMs = 1_900_000_000_000;
    vi.setSystemTime(nowMs);
    const baseOpenMs = nowMs - (nowMs % INTERVAL_MS) - 250 * INTERVAL_MS;
    const truncatedResponse = [makeKline(baseOpenMs + 249 * INTERVAL_MS, 349, true)];

    const { manager, futuresClient, fetchImplHolder } = makeHarness({
      initialKlineList: truncatedResponse,
      launchTimestamp: baseOpenMs,
    });
    const recoveredSpy = vi.fn();
    manager.on('symbolBackfillRecovered', recoveredSpy);
    manager.start();

    const loadPromise = manager.loadAllSymbols();
    await vi.advanceTimersByTimeAsync(2000);
    await loadPromise;

    // Latch the convergence (two full-window refetches add nothing).
    fetchImplHolder.impl = async () => truncatedResponse;
    await vi.advanceTimersByTimeAsync(STALENESS_CHECK_INTERVAL_MS);
    await flushMicrotasks();
    await vi.advanceTimersByTimeAsync(GAP_REPAIR_COOLDOWN_MS);
    await flushMicrotasks();

    // The hourly trade-symbol refresh momentarily drops the symbol (a flaky exchange list read):
    // "unknown listing time" must be treated as unknown — not as "healthy".
    futuresClient.tradeSymbols.delete('BTCUSDT');
    await vi.advanceTimersByTimeAsync(STALENESS_CHECK_INTERVAL_MS * 3);
    await flushMicrotasks();

    expect(recoveredSpy).not.toHaveBeenCalled();

    await manager.shutdown();
  });

  it('clears the converged flag when a symbol is released so a re-listing backfills again', async () => {
    vi.useFakeTimers();
    const nowMs = 1_900_000_000_000;
    vi.setSystemTime(nowMs);
    const baseOpenMs = nowMs - (nowMs % INTERVAL_MS) - 250 * INTERVAL_MS;

    const { manager, fetchImplHolder } = makeHarness({
      initialKlineList: [makeKline(baseOpenMs, 100, true)],
      launchTimestamp: baseOpenMs,
    });
    manager.start();

    const loadPromise = manager.loadAllSymbols();
    await vi.advanceTimersByTimeAsync(2000);
    await loadPromise;

    // Exchange serves only 100 → the symbol converges (two refetches in a row add nothing).
    fetchImplHolder.impl = async () => makeClosedRun(baseOpenMs, 100);
    await vi.advanceTimersByTimeAsync(STALENESS_CHECK_INTERVAL_MS);
    await flushMicrotasks();
    await vi.advanceTimersByTimeAsync(GAP_REPAIR_COOLDOWN_MS);
    await flushMicrotasks();
    await vi.advanceTimersByTimeAsync(GAP_REPAIR_COOLDOWN_MS);
    await flushMicrotasks();

    manager.releaseSymbol('BTCUSDT');
    expect(manager.getSymbolList()).toEqual([]);

    // Re-listing: load one candle again, then the exchange serves full history.
    fetchImplHolder.impl = async () => [makeKline(baseOpenMs, 100, true)];
    const outcome = await manager.ensureSymbolLoaded('BTCUSDT');
    expect(outcome).toBe('loaded');

    fetchImplHolder.impl = async () => makeClosedRun(baseOpenMs, 250);
    const reseedSpy = vi.fn();
    manager.on('symbolReseeded', reseedSpy);

    await vi.advanceTimersByTimeAsync(STALENESS_CHECK_INTERVAL_MS);
    await flushMicrotasks();

    // A converged flag surviving the release would make the scan skip the re-listed symbol.
    expect(reseedSpy).toHaveBeenCalledWith('BTCUSDT');
    expect(manager.getKlineList('BTCUSDT').length).toBe(250);

    await manager.shutdown();
  });

  it('a small seal repair returning known candles must not disable the full backfill', async () => {
    vi.useFakeTimers();
    const nowMs = 1_900_000_000_000;
    vi.setSystemTime(nowMs);
    const baseOpenMs = nowMs - (nowMs % INTERVAL_MS) - 250 * INTERVAL_MS;
    // The exchange holds the full history; the initial load was truncated to the newest two candles.
    const fullHistory = [...makeClosedRun(baseOpenMs, 251), makeKline(baseOpenMs + 251 * INTERVAL_MS, 352, false)];

    const { manager, futuresClient, fetchImplHolder, pushKline } = makeHarness({
      initialKlineList: [makeKline(baseOpenMs + 249 * INTERVAL_MS, 349, true), makeKline(baseOpenMs + 250 * INTERVAL_MS, 350, false)],
      launchTimestamp: baseOpenMs,
    });
    const stuckSpy = vi.fn();
    const reseedSpy = vi.fn();
    manager.on('symbolBackfillStuck', stuckSpy);
    manager.on('symbolReseeded', reseedSpy);
    manager.start();

    const loadPromise = manager.loadAllSymbols();
    await vi.advanceTimersByTimeAsync(2000);
    await loadPromise;
    fetchImplHolder.impl = makeLimitHonoringExchange(fullHistory);

    // The exchange's closing frame for the forming candle is lost — the next candle seals it
    // locally and triggers a SMALL (2-candle) repair whose response is already fully known.
    pushKline('BTCUSDT', makeKline(baseOpenMs + 251 * INTERVAL_MS, 352, false));
    await flushMicrotasks();

    // A 2-candle probe proves nothing about the missing depth — the symbol must NOT be marked
    // converged, and the scan must still run the full backfill once the cooldown allows.
    await vi.advanceTimersByTimeAsync(660_000);
    await flushMicrotasks();

    expect(stuckSpy).not.toHaveBeenCalled();
    const fullFetchCallList = futuresClient.fetchKlines.mock.calls.filter((call) => (call[2] as { limit?: number } | undefined)?.limit === 499);
    expect(fullFetchCallList.length).toBeGreaterThan(0);
    expect(manager.getKlineList('BTCUSDT').length).toBeGreaterThanOrEqual(250);

    await manager.shutdown();
  });

  it('a single truncated full-window response does not latch convergence — the next retry heals', async () => {
    vi.useFakeTimers();
    const nowMs = 1_900_000_000_000;
    vi.setSystemTime(nowMs);
    const baseOpenMs = nowMs - (nowMs % INTERVAL_MS) - 250 * INTERVAL_MS;
    const truncatedResponse = [makeKline(baseOpenMs + 249 * INTERVAL_MS, 349, true)];

    // The initial load AND the first full backfill both hit the same transient truncation — the
    // exact incident this machinery exists for. One converged measurement must not latch.
    const { manager, fetchImplHolder } = makeHarness({
      initialKlineList: truncatedResponse,
      launchTimestamp: baseOpenMs,
    });
    const stuckSpy = vi.fn();
    manager.on('symbolBackfillStuck', stuckSpy);
    manager.start();

    const loadPromise = manager.loadAllSymbols();
    await vi.advanceTimersByTimeAsync(2000);
    await loadPromise;

    let isTruncationActive = true;
    fetchImplHolder.impl = async (_symbol: string, limit: number) => {
      if (isTruncationActive) {
        isTruncationActive = false;

        return truncatedResponse;
      }

      return makeClosedRun(baseOpenMs, 250).slice(-limit);
    };

    // First scan: full backfill hits the truncation again (nothing new). Second retry past the
    // cooldown gets the real history and heals — no stuck signal anywhere on the way.
    await vi.advanceTimersByTimeAsync(60_000);
    await flushMicrotasks();
    await vi.advanceTimersByTimeAsync(GAP_REPAIR_COOLDOWN_MS);
    await flushMicrotasks();

    expect(stuckSpy).not.toHaveBeenCalled();
    expect(manager.getKlineList('BTCUSDT').length).toBe(250);

    await manager.shutdown();
  });

  it('emits a stuck signal once on convergence and a recovered signal when the buffer catches up', async () => {
    vi.useFakeTimers();
    const nowMs = 1_900_000_000_000;
    vi.setSystemTime(nowMs);
    // Listing time implies 6 candles; the exchange only serves 2.
    const launchTimestamp = nowMs - 6 * INTERVAL_MS;
    const baseOpenMs = nowMs - (nowMs % INTERVAL_MS) - 2 * INTERVAL_MS;

    const { manager, fetchImplHolder, pushKline } = makeHarness({
      initialKlineList: [makeKline(baseOpenMs, 100, true)],
      launchTimestamp,
    });
    const stuckSpy = vi.fn();
    const recoveredSpy = vi.fn();
    manager.on('symbolBackfillStuck', stuckSpy);
    manager.on('symbolBackfillRecovered', recoveredSpy);
    manager.start();

    const loadPromise = manager.loadAllSymbols();
    await vi.advanceTimersByTimeAsync(2000);
    await loadPromise;

    fetchImplHolder.impl = async () => makeClosedRun(baseOpenMs, 2);

    // First scan grows the buffer to 2; two refetches past the cooldown add nothing → the first
    // votes, the second latches the convergence.
    await vi.advanceTimersByTimeAsync(STALENESS_CHECK_INTERVAL_MS);
    await flushMicrotasks();
    await vi.advanceTimersByTimeAsync(GAP_REPAIR_COOLDOWN_MS);
    await flushMicrotasks();
    await vi.advanceTimersByTimeAsync(GAP_REPAIR_COOLDOWN_MS);
    await flushMicrotasks();

    expect(stuckSpy).toHaveBeenCalledTimes(1);
    expect(stuckSpy).toHaveBeenCalledWith('BTCUSDT');
    expect(recoveredSpy).not.toHaveBeenCalled();

    // A live candle grows the buffer to 3 (3 + tolerance ≥ expected 6) → no longer undergrown.
    pushKline('BTCUSDT', makeKline(baseOpenMs + 2 * INTERVAL_MS, 130, true));

    await vi.advanceTimersByTimeAsync(STALENESS_CHECK_INTERVAL_MS);
    await flushMicrotasks();

    expect(recoveredSpy).toHaveBeenCalledTimes(1);
    expect(recoveredSpy).toHaveBeenCalledWith('BTCUSDT');
    expect(stuckSpy).toHaveBeenCalledTimes(1);

    await manager.shutdown();
  });
});

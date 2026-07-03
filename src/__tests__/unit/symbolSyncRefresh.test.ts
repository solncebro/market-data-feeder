import { describe, expect, it, vi } from 'vitest';

import { RateLimitedRequestQueue } from '@solncebro/trade-engine';
import type { ExchangeConnector, Kline } from '@solncebro/trade-engine';

import { MarketDataManager } from '../../source/marketDataManager.js';

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
  };
}

interface ConnectorHarness {
  connector: ExchangeConnector;
  callLog: string[];
  setSymbolList: (symbolList: string[]) => void;
}

function makeConnector(initialSymbolList: string[]): ConnectorHarness {
  const callLog: string[] = [];
  let symbolList = initialSymbolList;
  const futuresClient = {
    fetchKlines: vi.fn(async () => [makeKline(0)]),
    subscribeKlines: vi.fn(),
    unsubscribeKlines: vi.fn(),
  };

  const connector = {
    refreshFuturesTradeSymbols: vi.fn(async () => {
      callLog.push('refresh');
    }),
    getFuturesSymbols: vi.fn(async () => {
      callLog.push('getSymbols');

      return symbolList;
    }),
    getTicker: vi.fn(() => undefined),
    get futures() {
      return futuresClient;
    },
  } as unknown as ExchangeConnector;

  return { connector, callLog, setSymbolList: (nextList) => { symbolList = nextList; } };
}

function makeManager(harness: ConnectorHarness): MarketDataManager {
  return new MarketDataManager(harness.connector, '30m', new RateLimitedRequestQueue({ rateLimit: 1000, intervalMs: 1000 }));
}

describe('MarketDataManager.syncAllSymbols', () => {
  it('refreshes the trade-symbol cache BEFORE reading the symbol list (otherwise the sync is dead)', async () => {
    const harness = makeConnector(['AAAUSDT']);
    const manager = makeManager(harness);
    await manager.loadAllSymbols();
    harness.callLog.length = 0;

    await manager.syncAllSymbols();

    expect(harness.callLog[0]).toBe('refresh');
    expect(harness.callLog).toContain('getSymbols');
  });

  it('skips the sync entirely when the exchange returns an empty list (no mass purge)', async () => {
    const harness = makeConnector(['AAAUSDT', 'BBBUSDT']);
    const manager = makeManager(harness);
    await manager.loadAllSymbols();
    const removedSpy = vi.fn();
    manager.on('symbolRemoved', removedSpy);

    harness.setSymbolList([]);
    await manager.syncAllSymbols();

    expect(removedSpy).not.toHaveBeenCalled();
    expect(manager.getSymbolList()).toEqual(['AAAUSDT', 'BBBUSDT']);
  });

  it('applies a small delisting immediately', async () => {
    const harness = makeConnector(['AAAUSDT', 'BBBUSDT', 'CCCUSDT']);
    const manager = makeManager(harness);
    await manager.loadAllSymbols();
    const removedSpy = vi.fn();
    manager.on('symbolRemoved', removedSpy);

    harness.setSymbolList(['AAAUSDT', 'BBBUSDT']);
    await manager.syncAllSymbols();

    expect(removedSpy).toHaveBeenCalledWith('CCCUSDT');
    expect(manager.getSymbolList()).toEqual(['AAAUSDT', 'BBBUSDT']);
  });

  it('defers a mass removal until the next sync confirms it, then applies; anomaly event fires once', async () => {
    const fullSymbolList = Array.from({ length: 50 }, (_item, index) => `SYM${index}USDT`);
    const harness = makeConnector(fullSymbolList);
    const manager = makeManager(harness);
    await manager.loadAllSymbols();
    const removedSpy = vi.fn();
    const anomalySpy = vi.fn();
    manager.on('symbolRemoved', removedSpy);
    manager.on('symbolSyncAnomaly', anomalySpy);

    // 30 of 50 symbols vanish — way past the 20% share and the absolute floor.
    const truncatedList = fullSymbolList.slice(0, 20);
    harness.setSymbolList(truncatedList);

    await manager.syncAllSymbols();
    expect(removedSpy).not.toHaveBeenCalled();
    expect(manager.getSymbolList()).toHaveLength(50);
    expect(anomalySpy).toHaveBeenCalledTimes(1);

    await manager.syncAllSymbols();
    expect(removedSpy).toHaveBeenCalledTimes(30);
    expect(manager.getSymbolList()).toEqual(truncatedList);
    expect(anomalySpy).toHaveBeenCalledTimes(1);
  });

  it('a transient glitch never removes anything and clears the pending state', async () => {
    const fullSymbolList = Array.from({ length: 50 }, (_item, index) => `SYM${index}USDT`);
    const harness = makeConnector(fullSymbolList);
    const manager = makeManager(harness);
    await manager.loadAllSymbols();
    const removedSpy = vi.fn();
    manager.on('symbolRemoved', removedSpy);

    harness.setSymbolList(fullSymbolList.slice(0, 20));
    await manager.syncAllSymbols();

    harness.setSymbolList(fullSymbolList);
    await manager.syncAllSymbols();

    expect(removedSpy).not.toHaveBeenCalled();
    expect(manager.getSymbolList()).toHaveLength(50);
  });
});

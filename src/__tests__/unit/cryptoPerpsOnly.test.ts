import { describe, expect, it, vi } from 'vitest';

import type { ExchangeConnector } from '@solncebro/trade-engine';

import { MarketDataManager } from '../../source/marketDataManager.js';

function makeConnector(): { connector: ExchangeConnector; getFuturesSymbols: ReturnType<typeof vi.fn> } {
  const getFuturesSymbols = vi.fn(async () => [] as string[]);
  const futuresClient = {
    fetchKlines: vi.fn(async () => []),
    subscribeKlines: vi.fn(),
    unsubscribeKlines: vi.fn(),
  };

  const connector = {
    getFuturesSymbols,
    refreshFuturesTradeSymbols: vi.fn(async () => undefined),
    get futures() {
      return futuresClient;
    },
  } as unknown as ExchangeConnector;

  return { connector, getFuturesSymbols };
}

describe('MarketDataManager requests crypto perpetuals only', () => {
  it('loadAllSymbols excludes TRADIFI tokenized-equity perps', async () => {
    const { connector, getFuturesSymbols } = makeConnector();
    const manager = new MarketDataManager(connector, '30m');

    await manager.loadAllSymbols();

    expect(getFuturesSymbols).toHaveBeenCalledWith({ excludeTradifi: true });
  });

  it('syncAllSymbols excludes TRADIFI tokenized-equity perps', async () => {
    const { connector, getFuturesSymbols } = makeConnector();
    const manager = new MarketDataManager(connector, '30m');

    await manager.syncAllSymbols();

    expect(getFuturesSymbols).toHaveBeenCalledWith({ excludeTradifi: true });
  });
});

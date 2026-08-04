import type { ExchangeNameEnum } from '@solncebro/trade-engine';

import type { KlineInterval } from '../domain/marketData.types.js';
import type { FeederSource } from '../server/feederSource.types.js';

interface EmbeddedMarketDataSourceArgs {
  source: FeederSource;
  onNotify?: (message: string) => void;
  teardownConnection?: () => Promise<void> | void;
}

interface CreateEmbeddedMarketDataSourceArgs {
  exchangeName: ExchangeNameEnum;
  exchangeApiKey: string;
  exchangeSecret: string;
  interval: KlineInterval;
  onNotify?: (message: string) => void;
}

export type { CreateEmbeddedMarketDataSourceArgs, EmbeddedMarketDataSourceArgs };

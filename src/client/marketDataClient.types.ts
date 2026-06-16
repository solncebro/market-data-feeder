import type { WebSocketLogger } from '@solncebro/websocket-engine';

import type { KlineInterval } from '../domain/marketData.types.js';
import type { FeedEventName, SubscriptionScope } from '../domain/subscription.types.js';

interface MarketDataClientArgs {
  url: string;
  interval: KlineInterval;
  scope: SubscriptionScope;
  events: FeedEventName[];
  wantMa: boolean;
  logger: WebSocketLogger;
}

export type { MarketDataClientArgs };

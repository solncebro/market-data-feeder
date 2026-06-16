interface SubscriptionScopeAll {
  kind: 'all';
}

interface SubscriptionScopeSymbols {
  kind: 'symbols';
  symbolList: string[];
}

type SubscriptionScope = SubscriptionScopeAll | SubscriptionScopeSymbols;

type FeedEventName = 'klineClosed' | 'klineUpdated' | 'klineUpdatedTick';

export type { FeedEventName, SubscriptionScope, SubscriptionScopeAll, SubscriptionScopeSymbols };

import type { KlineInterval, ManagedSource, SubscriptionScope } from '@solncebro/market-data-feeder-lib';

interface SubscribeArgs {
  interval: KlineInterval;
  scope: SubscriptionScope;
}

interface SubscriptionRegistryArgs<TSource extends ManagedSource> {
  createSource: (interval: KlineInterval) => TSource;
  teardownDelayMs?: number;
}

interface RegistrationStatus {
  interval: KlineInterval;
  isAllLoaded: boolean;
  allSubscriberCount: number;
  refSymbolCount: number;
}

export type { RegistrationStatus, SubscribeArgs, SubscriptionRegistryArgs };

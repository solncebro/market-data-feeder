import type { KlineInterval } from '../domain/marketData.types.js';
import type { SubscriptionScope } from '../domain/subscription.types.js';

interface ManagedSource {
  start: () => void;
  loadAllSymbols: () => Promise<void>;
  ensureSymbolLoaded: (symbol: string) => Promise<boolean>;
  releaseSymbol: (symbol: string) => void;
  syncAllSymbols: () => Promise<void>;
  shutdown: () => Promise<void>;
  getInterval: () => KlineInterval;
}

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

export type { ManagedSource, RegistrationStatus, SubscribeArgs, SubscriptionRegistryArgs };

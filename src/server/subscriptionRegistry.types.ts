import type { KlineInterval } from '../domain/marketData.types.js';
import type { SubscriptionScope } from '../domain/subscription.types.js';

// The outcome distinguishes a legit absence from a transient failure: 'notOnExchange' means the
// exchange list simply lacks the symbol and 'noHistory' means it is listed but has no candles yet
// (e.g. a contract listed before trading starts) — both are legit absences (an empty snapshot is
// the correct answer; one such symbol must not blank a whole multi-symbol scope into a retry
// loop). A transient failure (REST error, timeout) REJECTS so the caller can roll back and let the
// client retry via reconnect.
type SymbolLoadOutcome = 'loaded' | 'alreadyLoaded' | 'notOnExchange' | 'noHistory' | 'aborted';

interface ManagedSource {
  start: () => void;
  loadAllSymbols: () => Promise<void>;
  ensureSymbolLoaded: (symbol: string) => Promise<SymbolLoadOutcome>;
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

export type { ManagedSource, RegistrationStatus, SubscribeArgs, SubscriptionRegistryArgs, SymbolLoadOutcome };

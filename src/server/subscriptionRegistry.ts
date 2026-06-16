import type { KlineInterval } from '../domain/marketData.types.js';
import type { ManagedSource, SubscribeArgs, SubscriptionRegistryArgs } from './subscriptionRegistry.types.js';

const DEFAULT_TEARDOWN_DELAY_MS = 30_000;

interface IntervalRegistration<TSource extends ManagedSource> {
  source: TSource;
  isAllLoaded: boolean;
  allLoadPromise: Promise<void> | null;
  allSubscriberCount: number;
  symbolRefCountBySymbol: Map<string, number>;
  symbolLoadPromiseBySymbol: Map<string, Promise<boolean>>;
}

class SubscriptionRegistry<TSource extends ManagedSource> {
  private readonly createSource: (interval: KlineInterval) => TSource;
  private readonly teardownDelayMs: number;
  private readonly registrationByInterval: Map<KlineInterval, IntervalRegistration<TSource>> = new Map();
  private readonly teardownTimerByInterval: Map<KlineInterval, ReturnType<typeof setTimeout>> = new Map();

  constructor(args: SubscriptionRegistryArgs<TSource>) {
    this.createSource = args.createSource;
    this.teardownDelayMs = args.teardownDelayMs ?? DEFAULT_TEARDOWN_DELAY_MS;
  }

  async subscribe(args: SubscribeArgs): Promise<TSource> {
    this.cancelTeardown(args.interval);

    const registration = this.ensureRegistration(args.interval);

    if (args.scope.kind === 'all') {
      registration.allSubscriberCount += 1;

      await this.ensureAllLoaded(registration);

      return registration.source;
    }

    for (const symbol of args.scope.symbolList) {
      await this.acquireSymbol(registration, symbol);
    }

    return registration.source;
  }

  async unsubscribe(args: SubscribeArgs): Promise<void> {
    const registration = this.registrationByInterval.get(args.interval);

    if (registration === undefined) {
      return;
    }

    if (args.scope.kind === 'all') {
      registration.allSubscriberCount = Math.max(0, registration.allSubscriberCount - 1);
    } else {
      for (const symbol of args.scope.symbolList) {
        this.releaseSymbolRef(registration, symbol);
      }
    }

    await this.teardownIfIdle(args.interval, registration);
  }

  getSource(interval: KlineInterval): TSource | undefined {
    return this.registrationByInterval.get(interval)?.source;
  }

  async syncAllLoadedSources(): Promise<void> {
    for (const registration of this.registrationByInterval.values()) {
      if (!registration.isAllLoaded) {
        continue;
      }

      await registration.source.syncAllSymbols();
    }
  }

  getLoadedIntervalList(): KlineInterval[] {
    return Array.from(this.registrationByInterval.keys());
  }

  async shutdown(): Promise<void> {
    for (const timer of this.teardownTimerByInterval.values()) {
      clearTimeout(timer);
    }

    this.teardownTimerByInterval.clear();

    const registrationList = Array.from(this.registrationByInterval.values());
    this.registrationByInterval.clear();

    for (const registration of registrationList) {
      await registration.source.shutdown();
    }
  }

  private ensureRegistration(interval: KlineInterval): IntervalRegistration<TSource> {
    const existing = this.registrationByInterval.get(interval);

    if (existing !== undefined) {
      return existing;
    }

    const source = this.createSource(interval);
    source.start();

    const registration: IntervalRegistration<TSource> = {
      source,
      isAllLoaded: false,
      allLoadPromise: null,
      allSubscriberCount: 0,
      symbolRefCountBySymbol: new Map(),
      symbolLoadPromiseBySymbol: new Map(),
    };

    this.registrationByInterval.set(interval, registration);

    return registration;
  }

  private async ensureAllLoaded(registration: IntervalRegistration<TSource>): Promise<void> {
    if (registration.isAllLoaded) {
      return;
    }

    if (registration.allLoadPromise === null) {
      registration.allLoadPromise = registration.source.loadAllSymbols();
    }

    try {
      await registration.allLoadPromise;
      registration.isAllLoaded = true;
    } finally {
      registration.allLoadPromise = null;
    }
  }

  private async acquireSymbol(registration: IntervalRegistration<TSource>, symbol: string): Promise<void> {
    const previousCount = registration.symbolRefCountBySymbol.get(symbol) ?? 0;
    registration.symbolRefCountBySymbol.set(symbol, previousCount + 1);

    if (registration.isAllLoaded) {
      return;
    }

    const existingLoadPromise = registration.symbolLoadPromiseBySymbol.get(symbol);

    if (existingLoadPromise !== undefined) {
      await existingLoadPromise;

      return;
    }

    if (previousCount > 0) {
      return;
    }

    const loadPromise = registration.source.ensureSymbolLoaded(symbol);
    registration.symbolLoadPromiseBySymbol.set(symbol, loadPromise);

    try {
      await loadPromise;
    } finally {
      registration.symbolLoadPromiseBySymbol.delete(symbol);
    }
  }

  private releaseSymbolRef(registration: IntervalRegistration<TSource>, symbol: string): void {
    const previousCount = registration.symbolRefCountBySymbol.get(symbol) ?? 0;

    if (previousCount <= 0) {
      return;
    }

    const nextCount = previousCount - 1;

    if (nextCount > 0) {
      registration.symbolRefCountBySymbol.set(symbol, nextCount);

      return;
    }

    registration.symbolRefCountBySymbol.delete(symbol);

    if (registration.allSubscriberCount === 0 && !registration.isAllLoaded) {
      registration.source.releaseSymbol(symbol);
    }
  }

  private async teardownIfIdle(interval: KlineInterval, registration: IntervalRegistration<TSource>): Promise<void> {
    if (registration.allSubscriberCount > 0 || registration.symbolRefCountBySymbol.size > 0) {
      return;
    }

    if (this.teardownDelayMs <= 0) {
      await this.performTeardown(interval, registration);

      return;
    }

    this.cancelTeardown(interval);

    const timer = setTimeout(() => {
      this.teardownTimerByInterval.delete(interval);

      const current = this.registrationByInterval.get(interval);

      if (current !== registration || current.allSubscriberCount > 0 || current.symbolRefCountBySymbol.size > 0) {
        return;
      }

      this.performTeardown(interval, registration).catch(() => undefined);
    }, this.teardownDelayMs);

    this.teardownTimerByInterval.set(interval, timer);
  }

  private cancelTeardown(interval: KlineInterval): void {
    const timer = this.teardownTimerByInterval.get(interval);

    if (timer === undefined) {
      return;
    }

    clearTimeout(timer);
    this.teardownTimerByInterval.delete(interval);
  }

  private async performTeardown(interval: KlineInterval, registration: IntervalRegistration<TSource>): Promise<void> {
    this.registrationByInterval.delete(interval);

    await registration.source.shutdown();
  }
}

export { SubscriptionRegistry };

import { EventEmitter } from 'node:events';

import { ReliableWebSocket } from '@solncebro/websocket-engine';

import type { Kline, KlineInterval, MaValues, StaleSymbolInfo } from '../domain/marketData.types.js';
import { KLINE_BUFFER_SIZE, STALENESS_THRESHOLD_MULTIPLIER, resolveIntervalMs } from '../domain/constants.js';
import type { FeederMessage, SubscribeMessage } from '../protocol/messages.types.js';
import { decodeMessage, encodeMessage } from '../protocol/codec.js';
import { MirrorStore } from './mirrorStore.js';
import { isDataFreshnessMessage, isDataStale, resolveDataStaleThresholdMs } from './dataStaleness.js';
import type { MarketDataSource } from './marketDataSource.types.js';
import type { MarketDataClientArgs } from './marketDataClient.types.js';

const DEFAULT_SUBSCRIBE_TIMEOUT_MS = 30_000;
const CLIENT_STALE_THRESHOLD_MS = 45_000;
const CLIENT_STALE_CHECK_INTERVAL_MS = 15_000;
// Self-healing: data staleness persisting past this multiple of the stale threshold means the
// subscription is silently gone (the socket may still be OPEN receiving heartbeats) — recreate the
// transport and re-subscribe. Recreation also resurrects a transport that exhausted its retry
// budget and parked itself in a terminal FAILED state during a long feeder outage.
const CLIENT_FORCE_RECONNECT_STALE_MULTIPLIER = 2;
// Before the FIRST final snapshot the mirror is legitimately stale for as long as the feeder's
// bulk backfill takes (a Binance cold start pushes ~1600 requests through a 16 req/s pacer —
// minutes, not seconds), so the pre-ready self-heal uses this much larger floor. A genuinely
// failed subscribe is covered separately: the server closes the socket with code 4001.
const CLIENT_PRE_READY_FORCE_RECONNECT_MS = 300_000;

class MarketDataClient extends EventEmitter implements MarketDataSource {
  private readonly url: string;
  private readonly logger: MarketDataClientArgs['logger'];
  private readonly interval: KlineInterval;
  private readonly intervalMs: number;
  private readonly dataStaleThresholdMs: number;
  private readonly forceReconnectStaleMs: number;
  private readonly subscribeMessage: SubscribeMessage;
  private readonly mirror: MirrorStore;
  private readonly lastUpdateMsBySymbol: Map<string, number> = new Map();
  private socket: ReliableWebSocket<FeederMessage | null>;
  private staleCheckTimer: ReturnType<typeof setInterval> | null = null;
  private staleSinceMs: number | null = null;
  private snapshotSymbolSet: Set<string> = new Set();
  private isSnapshotComplete: boolean = false;
  private isConnectionLost: boolean = false;
  private isClosed: boolean = false;
  private lastDataMessageMs: number = 0;
  private resolveReady: (() => void) | null = null;

  constructor(args: MarketDataClientArgs) {
    super();
    this.url = args.url;
    this.logger = args.logger;
    this.interval = args.interval;
    this.intervalMs = resolveIntervalMs(args.interval);
    this.dataStaleThresholdMs = resolveDataStaleThresholdMs({ intervalMs: this.intervalMs, events: args.events, baseThresholdMs: args.staleThresholdMs ?? CLIENT_STALE_THRESHOLD_MS });
    this.forceReconnectStaleMs = this.dataStaleThresholdMs * CLIENT_FORCE_RECONNECT_STALE_MULTIPLIER;
    this.subscribeMessage = { type: 'subscribe', interval: args.interval, scope: args.scope, events: args.events, wantMa: args.wantMa };
    this.mirror = new MirrorStore(KLINE_BUFFER_SIZE);
    this.socket = this.createSocket();
    this.startStaleSelfHealCheck(args.staleCheckIntervalMs ?? CLIENT_STALE_CHECK_INTERVAL_MS);
  }

  private createSocket(): ReliableWebSocket<FeederMessage | null> {
    return new ReliableWebSocket<FeederMessage | null>({
      url: this.url,
      label: `market-data-${this.interval}`,
      logger: this.logger,
      parseMessage: (rawData) => decodeMessage(rawData.toString()),
      onMessage: (message) => {
        this.handleMessage(message);
      },
      onOpen: async (context) => {
        this.isSnapshotComplete = false;
        this.snapshotSymbolSet = new Set();
        this.staleSinceMs = null;
        context.send(encodeMessage(this.subscribeMessage));
      },
      onNotify: (reason: string) => {
        this.markConnectionLost(reason);
      },
    });
  }

  private markConnectionLost(reason: string): void {
    if (this.isConnectionLost) {
      return;
    }

    this.isConnectionLost = true;
    this.emit('connectionLost', reason);
  }

  private startStaleSelfHealCheck(checkIntervalMs: number): void {
    this.staleCheckTimer = setInterval(() => {
      this.evaluateStaleSelfHeal();
    }, checkIntervalMs);
    this.staleCheckTimer.unref();
  }

  private evaluateStaleSelfHeal(): void {
    if (this.isClosed) {
      return;
    }

    if (!this.isStale()) {
      this.staleSinceMs = null;

      return;
    }

    const nowMs = Date.now();

    if (this.staleSinceMs === null) {
      this.staleSinceMs = nowMs;

      return;
    }

    const forceReconnectMs = this.isSnapshotComplete
      ? this.forceReconnectStaleMs
      : Math.max(this.forceReconnectStaleMs, CLIENT_PRE_READY_FORCE_RECONNECT_MS);

    if (nowMs - this.staleSinceMs < forceReconnectMs) {
      return;
    }

    this.staleSinceMs = null;
    this.logger.warn(`[market-data-${this.interval}] data stale for over ${Math.round(this.forceReconnectStaleMs / 1000)}s — forcing a reconnect to re-subscribe`);
    this.markConnectionLost('stale data — forcing reconnect');
    this.socket.close();
    this.socket = this.createSocket();
  }

  async waitUntilReady(timeoutMs: number = DEFAULT_SUBSCRIBE_TIMEOUT_MS): Promise<void> {
    if (this.isSnapshotComplete) {
      return;
    }

    await new Promise<void>((resolve, reject) => {
      const timeoutHandle = setTimeout(() => {
        this.resolveReady = null;
        reject(new Error(`Snapshot not received within ${timeoutMs}ms for ${this.interval}`));
      }, timeoutMs);

      this.resolveReady = () => {
        clearTimeout(timeoutHandle);
        resolve();
      };
    });
  }

  close(): void {
    this.isClosed = true;

    if (this.staleCheckTimer !== null) {
      clearInterval(this.staleCheckTimer);
      this.staleCheckTimer = null;
    }

    this.socket.close();
  }

  async shutdown(): Promise<void> {
    this.close();
  }

  getInterval(): KlineInterval {
    return this.interval;
  }

  getIntervalMs(): number {
    return this.intervalMs;
  }

  getMaValues(symbol: string): MaValues {
    return this.mirror.getMaValues(symbol);
  }

  getKlineList(symbol: string): Kline[] {
    return this.mirror.getKlineList(symbol);
  }

  getCurrentKline(symbol: string): Kline | undefined {
    return this.mirror.getCurrentKline(symbol);
  }

  getSymbolList(): string[] {
    return this.mirror.getSymbolList();
  }

  getLastKlineOpenTimestamp(symbol: string): number | undefined {
    return this.mirror.getLastKlineOpenTimestamp(symbol);
  }

  getLastUpdateTimestamp(symbol: string): number | undefined {
    return this.lastUpdateMsBySymbol.get(symbol);
  }

  getVolume24h(symbol: string): number {
    return this.mirror.getVolume24h(symbol);
  }

  isStale(): boolean {
    return isDataStale({
      isSnapshotComplete: this.isSnapshotComplete,
      lastDataMessageMs: this.lastDataMessageMs,
      nowMs: Date.now(),
      thresholdMs: this.dataStaleThresholdMs,
    });
  }

  getStaleSymbolList(): StaleSymbolInfo[] {
    const thresholdMs = STALENESS_THRESHOLD_MULTIPLIER * this.intervalMs;
    const nowMs = Date.now();
    const staleSymbolList: StaleSymbolInfo[] = [];

    for (const symbol of this.mirror.getSymbolList()) {
      const klineList = this.mirror.getKlineList(symbol);

      if (klineList.length === 0) {
        continue;
      }

      const lastKline = klineList[klineList.length - 1];
      const referenceTimestamp = lastKline.closeTimestamp > 0 ? lastKline.closeTimestamp : lastKline.openTimestamp + this.intervalMs;
      const ageMs = nowMs - referenceTimestamp;

      if (ageMs > thresholdMs) {
        staleSymbolList.push({ symbol, ageMs });
      }
    }

    staleSymbolList.sort((first, second) => second.ageMs - first.ageMs);

    return staleSymbolList;
  }

  private handleMessage(message: FeederMessage | null): void {
    if (message === null) {
      return;
    }

    if (isDataFreshnessMessage(message.type)) {
      this.lastDataMessageMs = Date.now();
    }

    if (message.type === 'snapshot') {
      this.mirror.applySnapshot({ entryList: message.entryList });

      for (const entry of message.entryList) {
        this.snapshotSymbolSet.add(entry.symbol);
      }

      if (message.isFinal) {
        this.mirror.retainSymbols(this.snapshotSymbolSet);

        for (const symbol of this.lastUpdateMsBySymbol.keys()) {
          if (!this.snapshotSymbolSet.has(symbol)) {
            this.lastUpdateMsBySymbol.delete(symbol);
          }
        }

        this.snapshotSymbolSet = new Set();
        this.markReady();
      }

      return;
    }

    if (message.type === 'klineClosed') {
      this.mirror.applyKlineClosed({ symbol: message.symbol, kline: message.kline, maValues: message.maValues });
      this.lastUpdateMsBySymbol.set(message.symbol, Date.now());
      this.emit('klineClosed', message.symbol, message.kline, message.maValues);

      return;
    }

    if (message.type === 'klineUpdated') {
      this.mirror.applyKlineUpdated({ symbol: message.symbol, kline: message.kline, maValues: message.maValues });
      this.lastUpdateMsBySymbol.set(message.symbol, Date.now());
      this.emit('klineUpdated', message.symbol, message.kline, message.maValues);

      return;
    }

    if (message.type === 'klineUpdatedTick') {
      this.mirror.applyKlineTick({ symbol: message.symbol, kline: message.kline });
      this.lastUpdateMsBySymbol.set(message.symbol, Date.now());
      this.emit('klineUpdatedTick', message.symbol, message.kline);

      return;
    }

    if (message.type === 'symbolAdded') {
      this.mirror.applySymbolAdded(message.entry);
      this.lastUpdateMsBySymbol.set(message.entry.symbol, Date.now());
      this.emit('symbolAdded', message.entry.symbol);

      return;
    }

    if (message.type === 'symbolRemoved') {
      this.mirror.applySymbolRemoved(message.symbol);
      this.lastUpdateMsBySymbol.delete(message.symbol);
      this.emit('symbolRemoved', message.symbol);

      return;
    }

    if (message.type === 'volume24h') {
      this.mirror.applyVolume24h({ symbol: message.symbol, volume24hUsdt: message.volume24hUsdt });
    }
  }

  private markReady(): void {
    this.isSnapshotComplete = true;

    if (this.isConnectionLost) {
      this.isConnectionLost = false;
      this.emit('connectionRestored');
    }

    if (this.resolveReady !== null) {
      const resolve = this.resolveReady;
      this.resolveReady = null;
      resolve();
    }
  }
}

export { MarketDataClient };

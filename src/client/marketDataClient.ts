import { EventEmitter } from 'node:events';

import { ReliableWebSocket } from '@solncebro/websocket-engine';

import type { Kline, KlineInterval, MaValues, StaleSymbolInfo } from '../domain/marketData.types.js';
import { KLINE_BUFFER_SIZE, STALENESS_THRESHOLD_MULTIPLIER, resolveIntervalMs } from '../domain/constants.js';
import type { FeederMessage, SubscribeMessage } from '../protocol/messages.types.js';
import { decodeMessage, encodeMessage } from '../protocol/codec.js';
import { MirrorStore } from './mirrorStore.js';
import { isDataFreshnessMessage, isDataStale } from './dataStaleness.js';
import type { MarketDataSource } from './marketDataSource.types.js';
import type { MarketDataClientArgs } from './marketDataClient.types.js';

const DEFAULT_SUBSCRIBE_TIMEOUT_MS = 30_000;
const CLIENT_STALE_THRESHOLD_MS = 45_000;
const CLIENT_STALE_CHECK_INTERVAL_MS = 15_000;

class MarketDataClient extends EventEmitter implements MarketDataSource {
  private readonly interval: KlineInterval;
  private readonly intervalMs: number;
  private readonly subscribeMessage: SubscribeMessage;
  private readonly mirror: MirrorStore;
  private readonly lastUpdateMsBySymbol: Map<string, number> = new Map();
  private readonly socket: ReliableWebSocket<FeederMessage | null>;
  private snapshotSymbolSet: Set<string> = new Set();
  private isSnapshotComplete: boolean = false;
  private isConnectionLost: boolean = false;
  private lastDataMessageMs: number = 0;
  private resolveReady: (() => void) | null = null;

  constructor(args: MarketDataClientArgs) {
    super();
    this.interval = args.interval;
    this.intervalMs = resolveIntervalMs(args.interval);
    this.subscribeMessage = { type: 'subscribe', interval: args.interval, scope: args.scope, events: args.events, wantMa: args.wantMa };
    this.mirror = new MirrorStore(KLINE_BUFFER_SIZE);
    this.socket = new ReliableWebSocket<FeederMessage | null>({
      url: args.url,
      label: `market-data-${args.interval}`,
      logger: args.logger,
      parseMessage: (rawData) => decodeMessage(rawData.toString()),
      onMessage: (message) => {
        this.handleMessage(message);
      },
      onOpen: async (context) => {
        this.isSnapshotComplete = false;
        this.snapshotSymbolSet = new Set();
        context.send(encodeMessage(this.subscribeMessage));
      },
      onNotify: (reason: string) => {
        if (this.isConnectionLost) {
          return;
        }

        this.isConnectionLost = true;
        this.emit('connectionLost', reason);
      },
      configuration: { staleThreshold: CLIENT_STALE_THRESHOLD_MS, staleCheckInterval: CLIENT_STALE_CHECK_INTERVAL_MS },
    });
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
    this.socket.close();
  }

  async shutdown(): Promise<void> {
    this.socket.close();
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
      thresholdMs: CLIENT_STALE_THRESHOLD_MS,
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

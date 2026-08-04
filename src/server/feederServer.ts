import { WebSocket, WebSocketServer } from 'ws';

import type { KlineInterval, StaleSymbolInfo } from '../domain/marketData.types.js';
import type { FeedEventName, SubscriptionScope } from '../domain/subscription.types.js';
import type { MarketDataSnapshotEntry } from '../domain/snapshot.types.js';
import type { ServerMessage, SnapshotMessage, SubscribeMessage, SymbolAddedMessage, SymbolRemovedMessage, UnsubscribeMessage, Volume24hMessage } from '../protocol/messages.types.js';
import { decodeMessage, encodeMessage } from '../protocol/codec.js';
import { isValidSubscribe } from '../protocol/clientMessageValidation.js';
import { SYMBOL_LIST_SYNC_INTERVAL_MS, isKnownInterval } from '../domain/constants.js';
import { SubscriptionRegistry } from './subscriptionRegistry.js';
import type { HealthEvent } from '../health/healthMonitor.types.js';
import type { FeederSource } from './feederSource.types.js';
import type { FeederLogger, FeederServerArgs, FeederStatus, ForwardFeedMessageArgs, IntervalStatus, SymbolDiagnostics } from './feederServer.types.js';

const DEFAULT_SNAPSHOT_CHUNK_SIZE = 100;
const HEARTBEAT_INTERVAL_MS = 15_000;
const DEFAULT_MAX_CONNECTIONS = 64;
const DEFAULT_MAX_PAYLOAD_BYTES = 262_144;
// A failed subscribe closes the client socket so its ReliableWebSocket reconnects and re-sends the
// subscribe — a natural retry loop. The delay paces that loop (the transport has no lasting backoff
// of its own — its counters reset on every successful open), so a persistent failure cannot hammer
// the exchange REST API. 4001 is an application close code (out of the transport's fast-reconnect
// list), signalling "subscribe failed, retry later".
const DEFAULT_SUBSCRIBE_FAILURE_CLOSE_DELAY_MS = 5_000;
const SUBSCRIBE_FAILURE_CLOSE_CODE = 4001;
// Half-dead-client protection, both checks ride the heartbeat tick. Missed pongs catch a hung
// client process (the ws library answers pings automatically, so only a truly stuck event loop
// misses them; 3 ticks = 45s of grace for legit pauses like parsing a huge snapshot). The buffered
// limit catches a client that ACKs at the TCP level but drains too slowly — it must stay over the
// limit on TWO consecutive ticks so a legit one-off burst (a full snapshot is tens of MB and drains
// in well under a tick on localhost) is never punished.
const DEFAULT_MAX_MISSED_PONG_COUNT = 3;
const DEFAULT_SLOW_CLIENT_BUFFERED_LIMIT_BYTES = 64 * 1024 * 1024;
const SLOW_CLIENT_OVER_LIMIT_TICK_COUNT = 2;

interface ClientSubscription {
  scope: SubscriptionScope;
  eventNameSet: Set<FeedEventName>;
}

// Registry work for one client+interval runs strictly one operation at a time on this chain, and
// the chain owns the scope it actually acquired — releases always use acquiredScope, never a scope
// named in a message. This closes two whole classes of refcount bugs: a disconnect landing while a
// multi-symbol acquire is mid-loop, and a foreign unsubscribe releasing someone else's refs.
interface IntervalChainState {
  tail: Promise<void>;
  acquiredScope: SubscriptionScope | null;
}

interface ClientConnection {
  socket: WebSocket;
  subscriptionByInterval: Map<KlineInterval, ClientSubscription>;
  chainStateByInterval: Map<KlineInterval, IntervalChainState>;
  missedPongCount: number;
  overBufferLimitTickCount: number;
}

function scopeMatchesSymbol(scope: SubscriptionScope, symbol: string): boolean {
  if (scope.kind === 'all') {
    return true;
  }

  return scope.symbolList.includes(symbol);
}

class FeederServer {
  private readonly port: number;
  private readonly host: string;
  private readonly logger: FeederLogger;
  private readonly snapshotChunkSize: number;
  private readonly registry: SubscriptionRegistry<FeederSource>;
  private readonly clientSet: Set<ClientConnection> = new Set();
  private webSocketServer: WebSocketServer | null = null;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private symbolSyncTimer: ReturnType<typeof setInterval> | null = null;
  private boundPort: number;
  private startedAtMs: number = 0;
  private readonly onHealthEvent: ((event: HealthEvent) => void) | null;
  private readonly maxConnections: number;
  private readonly maxPayloadBytes: number;
  private readonly subscribeFailureCloseDelayMs: number;
  private readonly heartbeatIntervalMs: number;
  private readonly maxMissedPongCount: number;
  private readonly slowClientBufferedLimitBytes: number;
  private readonly pendingCloseTimerSet: Set<ReturnType<typeof setTimeout>> = new Set();

  constructor(args: FeederServerArgs) {
    this.port = args.port;
    this.host = args.host ?? '127.0.0.1';
    this.logger = args.logger;
    this.snapshotChunkSize = args.snapshotChunkSize ?? DEFAULT_SNAPSHOT_CHUNK_SIZE;
    this.boundPort = args.port;
    this.onHealthEvent = args.onHealthEvent ?? null;
    this.maxConnections = args.maxConnections ?? DEFAULT_MAX_CONNECTIONS;
    this.maxPayloadBytes = args.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES;
    this.subscribeFailureCloseDelayMs = args.subscribeFailureCloseDelayMs ?? DEFAULT_SUBSCRIBE_FAILURE_CLOSE_DELAY_MS;
    this.heartbeatIntervalMs = args.heartbeatIntervalMs ?? HEARTBEAT_INTERVAL_MS;
    this.maxMissedPongCount = args.maxMissedPongCount ?? DEFAULT_MAX_MISSED_PONG_COUNT;
    this.slowClientBufferedLimitBytes = args.slowClientBufferedLimitBytes ?? DEFAULT_SLOW_CLIENT_BUFFERED_LIMIT_BYTES;
    this.registry = new SubscriptionRegistry<FeederSource>({
      createSource: (interval) => this.createForwardingSource(interval, args.createSource),
    });
  }

  async start(): Promise<void> {
    const webSocketServer = new WebSocketServer({ port: this.port, host: this.host, maxPayload: this.maxPayloadBytes });
    this.webSocketServer = webSocketServer;

    webSocketServer.on('connection', (socket) => {
      this.handleConnection(socket);
    });

    await new Promise<void>((resolve, reject) => {
      const onListening = (): void => {
        webSocketServer.off('error', onStartupError);
        resolve();
      };

      const onStartupError = (error: Error): void => {
        webSocketServer.off('listening', onListening);
        reject(error);
      };

      webSocketServer.once('listening', onListening);
      webSocketServer.once('error', onStartupError);
    });

    webSocketServer.on('error', (error: unknown) => {
      this.logger.error({ error }, '[Feeder] WebSocket server error');
    });

    const address = webSocketServer.address();

    if (address !== null && typeof address === 'object') {
      this.boundPort = address.port;
    }

    this.startedAtMs = Date.now();
    this.startHeartbeat();
    this.startSymbolListSync();
    this.logger.info({ port: this.boundPort, host: this.host }, `[Feeder] WebSocket server listening on ${this.host}:${this.boundPort}`);
  }

  getPort(): number {
    return this.boundPort;
  }

  getStatus(): FeederStatus {
    const intervalStatusList: IntervalStatus[] = [];

    for (const registrationStatus of this.registry.getRegistrationStatusList()) {
      const source = this.registry.getSource(registrationStatus.interval);

      if (source === undefined) {
        continue;
      }

      intervalStatusList.push({
        interval: registrationStatus.interval,
        intervalMs: source.getIntervalMs(),
        isAllLoaded: registrationStatus.isAllLoaded,
        allSubscriberCount: registrationStatus.allSubscriberCount,
        refSymbolCount: registrationStatus.refSymbolCount,
        symbolCount: source.getSymbolList().length,
        klineCount: source.getKlineCount(),
        staleCount: source.getStaleSymbolList().length,
        subscriptionCount: source.getSubscriptionCount(),
        liveness: source.getStreamLiveness(),
        freshCount: source.getFreshSymbolCount(),
        persistentStaleCount: source.getPersistentStaleCount(),
      });
    }

    return {
      host: this.host,
      port: this.boundPort,
      clientCount: this.clientSet.size,
      uptimeMs: this.startedAtMs > 0 ? Date.now() - this.startedAtMs : 0,
      intervalStatusList,
    };
  }

  getStaleSymbolList(interval: KlineInterval, limit: number): StaleSymbolInfo[] {
    const source = this.registry.getSource(interval);

    if (source === undefined) {
      return [];
    }

    return source.getStaleSymbolList().slice(0, limit);
  }

  getSymbolDiagnostics(symbol: string, interval?: KlineInterval): SymbolDiagnostics | null {
    const intervalList = interval !== undefined ? [interval] : this.registry.getLoadedIntervalList();

    for (const candidateInterval of intervalList) {
      const source = this.registry.getSource(candidateInterval);

      if (source === undefined || !source.getSymbolList().includes(symbol)) {
        continue;
      }

      return this.buildSymbolDiagnostics(source, candidateInterval, symbol);
    }

    return null;
  }

  private buildSymbolDiagnostics(source: FeederSource, interval: KlineInterval, symbol: string): SymbolDiagnostics {
    const klineList = source.getKlineList(symbol);
    const lastKline = klineList.length > 0 ? klineList[klineList.length - 1] : undefined;
    const lastPrice = source.getCurrentKline(symbol)?.closePrice ?? lastKline?.closePrice ?? null;
    const volume24hUsdt = source.getVolume24h(symbol);
    const staleInfo = source.getStaleSymbolList().find((info) => info.symbol === symbol);

    return {
      symbol,
      interval,
      lastPrice,
      lastKlineOpenMs: source.getLastKlineOpenTimestamp(symbol),
      lastUpdateMs: source.getLastUpdateTimestamp(symbol),
      maValues: source.getMaValues(symbol),
      volume24hUsdt: Number.isFinite(volume24hUsdt) ? volume24hUsdt : null,
      isStale: staleInfo !== undefined,
      staleAgeMs: staleInfo?.ageMs,
    };
  }

  async shutdown(): Promise<void> {
    if (this.heartbeatTimer !== null) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }

    if (this.symbolSyncTimer !== null) {
      clearInterval(this.symbolSyncTimer);
      this.symbolSyncTimer = null;
    }

    for (const timer of this.pendingCloseTimerSet) {
      clearTimeout(timer);
    }

    this.pendingCloseTimerSet.clear();

    // terminate(), not close(): a graceful close handshake with a half-dead client can hang for the
    // ws library's internal 30s timeout — longer than the 15s hard-exit belt, turning a deliberate
    // operator stop into exit code 1. The process is going down anyway; client transports treat the
    // abort like any disconnect and reconnect to the next process.
    for (const client of this.clientSet) {
      client.socket.terminate();
    }

    this.clientSet.clear();

    const webSocketServer = this.webSocketServer;

    if (webSocketServer !== null) {
      await new Promise<void>((resolve) => {
        webSocketServer.close(() => {
          resolve();
        });
      });

      this.webSocketServer = null;
    }

    await this.registry.shutdown();
  }

  private createForwardingSource(interval: KlineInterval, factory: (interval: KlineInterval) => FeederSource): FeederSource {
    const source = factory(interval);

    source.on('klineClosed', (symbol, kline, maValues) => {
      this.forwardFeedMessage({ eventName: 'klineClosed', interval, symbol, message: { type: 'klineClosed', interval, symbol, kline, maValues } });
    });

    source.on('klineUpdated', (symbol, kline, maValues) => {
      this.forwardFeedMessage({ eventName: 'klineUpdated', interval, symbol, message: { type: 'klineUpdated', interval, symbol, kline, maValues } });
    });

    source.on('klineUpdatedTick', (symbol, kline) => {
      this.forwardFeedMessage({ eventName: 'klineUpdatedTick', interval, symbol, message: { type: 'klineUpdatedTick', interval, symbol, kline } });
    });

    source.on('symbolAdded', (symbol) => {
      this.forwardSymbolAdded(source, interval, symbol);
    });

    source.on('symbolRemoved', (symbol) => {
      this.forwardSymbolRemoved(interval, symbol);
      this.emitHealthEvent({ kind: 'symbolDelisted', interval, symbol });
    });

    source.on('symbolReseeded', (symbol) => {
      this.forwardSymbolReseeded(source, interval, symbol);
    });

    // Health-only lifecycle signals (never forwarded to clients): a released symbol / torn-down
    // interval can never emit a recovery, so the monitor must scrub its digest keys and drop the
    // interval's whole-source degradations.
    source.on('symbolReleased', (symbol) => {
      this.emitHealthEvent({ kind: 'symbolDelisted', interval, symbol });
    });

    source.on('sourceShutdown', () => {
      this.emitHealthEvent({ kind: 'intervalReleased', interval });
    });

    source.on('volume24h', (symbol, volume24hUsdt) => {
      this.forwardVolume24h(interval, symbol, volume24hUsdt);
    });

    source.on('streamSilent', (silenceMs) => {
      this.emitHealthEvent({ kind: 'streamSilent', interval, silenceMs });
    });

    source.on('streamResumed', (silenceMs) => {
      this.emitHealthEvent({ kind: 'streamResumed', interval, silenceMs });
    });

    source.on('sourceMassStale', (staleCount, symbolCount) => {
      this.emitHealthEvent({ kind: 'sourceMassStale', interval, staleCount, symbolCount });
    });

    source.on('sourceMassStaleRecovered', () => {
      this.emitHealthEvent({ kind: 'sourceMassStaleRecovered', interval });
    });

    source.on('symbolSyncAnomaly', (deferredCount, symbolCount) => {
      this.emitHealthEvent({ kind: 'symbolSyncAnomaly', interval, deferredCount, symbolCount });
    });

    source.on('persistentStaleSymbol', (symbol) => {
      this.emitHealthEvent({ kind: 'persistentStaleSymbol', interval, symbol });
    });

    source.on('persistentStaleRecovered', (symbol) => {
      this.emitHealthEvent({ kind: 'persistentStaleRecovered', interval, symbol });
    });

    source.on('intervalLoadStarted', (symbolCount) => {
      this.emitHealthEvent({ kind: 'intervalLoadStarted', interval, symbolCount });
    });

    source.on('intervalLoadCompleted', (symbolCount) => {
      this.emitHealthEvent({ kind: 'intervalLoadCompleted', interval, symbolCount });
    });

    source.on('symbolLoadCompleted', (symbol) => {
      this.emitHealthEvent({ kind: 'symbolLoadCompleted', interval, symbol });
    });

    source.on('symbolLoadFailed', (symbol) => {
      this.emitHealthEvent({ kind: 'symbolLoadFailed', interval, symbol });
    });

    source.on('symbolBackfillStuck', (symbol) => {
      this.emitHealthEvent({ kind: 'symbolBackfillStuck', interval, symbol });
    });

    source.on('symbolBackfillRecovered', (symbol) => {
      this.emitHealthEvent({ kind: 'symbolBackfillRecovered', interval, symbol });
    });

    return source;
  }

  private emitHealthEvent(event: HealthEvent): void {
    if (this.onHealthEvent === null) {
      return;
    }

    try {
      this.onHealthEvent(event);
    } catch (error: unknown) {
      this.logger.error({ error }, '[Feeder] onHealthEvent handler threw');
    }
  }

  private handleConnection(socket: WebSocket): void {
    if (this.clientSet.size >= this.maxConnections) {
      this.logger.warn({ limit: this.maxConnections }, '[Feeder] connection rejected — client connection limit reached');
      socket.close();

      return;
    }

    const client: ClientConnection = { socket, subscriptionByInterval: new Map(), chainStateByInterval: new Map(), missedPongCount: 0, overBufferLimitTickCount: 0 };
    this.clientSet.add(client);

    socket.on('message', (data) => {
      this.handleClientMessage(client, data.toString());
    });

    socket.on('pong', () => {
      client.missedPongCount = 0;
    });

    socket.on('close', () => {
      this.handleClientClose(client);
    });

    socket.on('error', (error: unknown) => {
      this.logger.error({ error }, '[Feeder] client socket error');
    });
  }

  private handleClientMessage(client: ClientConnection, raw: string): void {
    const message = decodeMessage(raw);

    if (message === null) {
      this.logger.warn({ raw }, '[Feeder] dropped invalid client message');

      return;
    }

    if (message.type === 'subscribe') {
      if (!isValidSubscribe(message)) {
        this.logger.warn({ interval: message.interval }, '[Feeder] dropped invalid subscribe message');

        return;
      }

      const subscription: ClientSubscription = { scope: message.scope, eventNameSet: new Set(message.events) };
      // Routing updates synchronously so scope-matched events emitted DURING the load already reach
      // this client (documented contract); the registry work runs serialized on the interval chain.
      client.subscriptionByInterval.set(message.interval, subscription);
      this.enqueueIntervalOp(client, message.interval, () => this.runSubscribeOp(client, message.interval, subscription));

      return;
    }

    if (message.type === 'unsubscribe') {
      this.handleUnsubscribeMessage(client, message);
    }
  }

  private handleUnsubscribeMessage(client: ClientConnection, message: UnsubscribeMessage): void {
    if (!isKnownInterval(message.interval)) {
      this.logger.warn({ interval: message.interval }, '[Feeder] dropped invalid unsubscribe message');

      return;
    }

    // Ownership rule: only a subscription this client actually holds can be released, and the scope
    // released is the chain-owned one — never the scope named in the message. A client that never
    // subscribed cannot decrement anyone else's refcounts.
    if (!client.subscriptionByInterval.has(message.interval)) {
      this.logger.warn({ interval: message.interval }, '[Feeder] ignored unsubscribe for an interval the client does not hold');

      return;
    }

    client.subscriptionByInterval.delete(message.interval);
    this.enqueueIntervalOp(client, message.interval, () => this.runReleaseOp(client, message.interval));
  }

  private getChainState(client: ClientConnection, interval: KlineInterval): IntervalChainState {
    let state = client.chainStateByInterval.get(interval);

    if (state === undefined) {
      state = { tail: Promise.resolve(), acquiredScope: null };
      client.chainStateByInterval.set(interval, state);
    }

    return state;
  }

  private enqueueIntervalOp(client: ClientConnection, interval: KlineInterval, op: () => Promise<void>): void {
    const state = this.getChainState(client, interval);
    // Ops are non-throwing by construction; the rejection handler is a belt so no bug can ever
    // leave the chain permanently rejected (which would silently drop every later operation).
    state.tail = state.tail.then(op, op);
  }

  private async runSubscribeOp(client: ClientConnection, interval: KlineInterval, subscription: ClientSubscription): Promise<void> {
    // Superseded (or the client disconnected) before this op started — skip the acquire entirely.
    if (client.subscriptionByInterval.get(interval) !== subscription) {
      return;
    }

    let source: FeederSource;

    try {
      source = await this.registry.subscribe({ interval, scope: subscription.scope });
    } catch (error: unknown) {
      this.logger.error({ error, interval }, '[Feeder] subscribe failed — closing the client so it retries via reconnect');

      if (client.subscriptionByInterval.get(interval) === subscription) {
        client.subscriptionByInterval.delete(interval);
      }

      this.scheduleSubscribeFailureClose(client);

      return;
    }

    const state = this.getChainState(client, interval);
    const previousScope = state.acquiredScope;
    state.acquiredScope = subscription.scope;

    // Acquire-new-then-release-old: overlapping scopes never drop a shared symbol to zero refs, so
    // a scope extension does not churn the exchange subscription of every shared symbol.
    if (previousScope !== null) {
      try {
        await this.registry.unsubscribe({ interval, scope: previousScope });
      } catch (error: unknown) {
        this.logger.error({ error, interval }, '[Feeder] failed to release superseded subscription');
      }
    }

    // Disconnected mid-acquire: the close op queued behind this one releases acquiredScope.
    if (!this.clientSet.has(client)) {
      return;
    }

    // Superseded while loading: the newer op sends its own snapshot.
    if (client.subscriptionByInterval.get(interval) !== subscription) {
      return;
    }

    const snapshotMessageList = this.buildSnapshotMessageList(source, interval, subscription.scope);

    for (const snapshotMessage of snapshotMessageList) {
      this.sendMessage(client, snapshotMessage);
    }
  }

  private async runReleaseOp(client: ClientConnection, interval: KlineInterval): Promise<void> {
    const state = client.chainStateByInterval.get(interval);

    if (state === undefined || state.acquiredScope === null) {
      return;
    }

    const scope = state.acquiredScope;
    state.acquiredScope = null;

    try {
      await this.registry.unsubscribe({ interval, scope });
    } catch (error: unknown) {
      this.logger.error({ error, interval }, '[Feeder] unsubscribe release failed');
    }
  }

  private scheduleSubscribeFailureClose(client: ClientConnection): void {
    if (!this.clientSet.has(client)) {
      return;
    }

    const timer = setTimeout(() => {
      this.pendingCloseTimerSet.delete(timer);

      if (!this.clientSet.has(client)) {
        return;
      }

      client.socket.close(SUBSCRIBE_FAILURE_CLOSE_CODE, 'subscribe failed');
    }, this.subscribeFailureCloseDelayMs);

    this.pendingCloseTimerSet.add(timer);
  }

  private handleClientClose(client: ClientConnection): void {
    this.clientSet.delete(client);
    client.subscriptionByInterval.clear();

    for (const interval of client.chainStateByInterval.keys()) {
      this.enqueueIntervalOp(client, interval, () => this.runReleaseOp(client, interval));

      // Drop the chain entry after the final release so a disconnected client cannot be retained
      // forever through its chain closures.
      const state = this.getChainState(client, interval);
      state.tail = state.tail.then(() => {
        client.chainStateByInterval.delete(interval);
      });
    }
  }

  private buildSnapshotMessageList(source: FeederSource, interval: KlineInterval, scope: SubscriptionScope): SnapshotMessage[] {
    const symbolList = source.getSymbolList().filter((symbol) => scopeMatchesSymbol(scope, symbol));
    const entryList = symbolList.map((symbol) => this.buildSnapshotEntry(source, symbol));
    const messageList: SnapshotMessage[] = [];

    for (let i = 0; i < entryList.length; i += this.snapshotChunkSize) {
      const chunkEntryList = entryList.slice(i, i + this.snapshotChunkSize);
      const isFinal = i + this.snapshotChunkSize >= entryList.length;
      messageList.push({ type: 'snapshot', interval, entryList: chunkEntryList, isFinal });
    }

    if (messageList.length === 0) {
      messageList.push({ type: 'snapshot', interval, entryList: [], isFinal: true });
    }

    return messageList;
  }

  private buildSnapshotEntry(source: FeederSource, symbol: string): MarketDataSnapshotEntry {
    const volume24hUsdt = source.getVolume24h(symbol);

    return {
      symbol,
      klineList: source.getKlineList(symbol),
      maValues: source.getMaValues(symbol),
      currentKline: source.getCurrentKline(symbol) ?? null,
      volume24hUsdt: Number.isFinite(volume24hUsdt) ? volume24hUsdt : null,
    };
  }

  private forwardFeedMessage(args: ForwardFeedMessageArgs): void {
    const encoded = encodeMessage(args.message);

    for (const client of this.clientSet) {
      const subscription = client.subscriptionByInterval.get(args.interval);

      if (subscription === undefined || !subscription.eventNameSet.has(args.eventName) || !scopeMatchesSymbol(subscription.scope, args.symbol)) {
        continue;
      }

      this.sendRaw(client, encoded);
    }
  }

  private forwardSymbolAdded(source: FeederSource, interval: KlineInterval, symbol: string): void {
    const message: SymbolAddedMessage = { type: 'symbolAdded', interval, entry: this.buildSnapshotEntry(source, symbol) };

    this.broadcastScoped(interval, symbol, message);
  }

  private forwardSymbolRemoved(interval: KlineInterval, symbol: string): void {
    const message: SymbolRemovedMessage = { type: 'symbolRemoved', interval, symbol };

    this.broadcastScoped(interval, symbol, message);
  }

  // A gap repair rewrote the symbol's buffer server-side; a mid-stream single-entry snapshot chunk
  // (isFinal: false) makes the client replace its mirror wholesale — no protocol change needed, and
  // it is safe in every client state (retainSymbols/markReady react only to isFinal chunks).
  private forwardSymbolReseeded(source: FeederSource, interval: KlineInterval, symbol: string): void {
    const message: SnapshotMessage = { type: 'snapshot', interval, entryList: [this.buildSnapshotEntry(source, symbol)], isFinal: false };

    this.broadcastScoped(interval, symbol, message);
  }

  private forwardVolume24h(interval: KlineInterval, symbol: string, volume24hUsdt: number): void {
    const message: Volume24hMessage = { type: 'volume24h', interval, symbol, volume24hUsdt };

    this.broadcastScoped(interval, symbol, message);
  }

  private broadcastScoped(interval: KlineInterval, symbol: string, message: ServerMessage): void {
    const encoded = encodeMessage(message);

    for (const client of this.clientSet) {
      const subscription = client.subscriptionByInterval.get(interval);

      if (subscription === undefined || !scopeMatchesSymbol(subscription.scope, symbol)) {
        continue;
      }

      this.sendRaw(client, encoded);
    }
  }

  private startHeartbeat(): void {
    this.heartbeatTimer = setInterval(() => {
      this.runHeartbeatTick();
    }, this.heartbeatIntervalMs);
  }

  private runHeartbeatTick(): void {
    const encoded = encodeMessage({ type: 'heartbeat' });

    for (const client of this.clientSet) {
      if (client.socket.readyState !== WebSocket.OPEN) {
        continue;
      }

      if (client.socket.bufferedAmount > this.slowClientBufferedLimitBytes) {
        client.overBufferLimitTickCount += 1;

        if (client.overBufferLimitTickCount >= SLOW_CLIENT_OVER_LIMIT_TICK_COUNT) {
          this.logger.warn({ bufferedAmount: client.socket.bufferedAmount, limitBytes: this.slowClientBufferedLimitBytes, intervalList: Array.from(client.subscriptionByInterval.keys()) }, '[Feeder] terminating slow client — send buffer over the limit for consecutive heartbeat ticks');
          client.socket.terminate();

          continue;
        }
      } else {
        client.overBufferLimitTickCount = 0;
      }

      if (client.missedPongCount >= this.maxMissedPongCount) {
        this.logger.warn({ missedPongCount: client.missedPongCount, intervalList: Array.from(client.subscriptionByInterval.keys()) }, '[Feeder] terminating unresponsive client — no pong across consecutive heartbeat ticks');
        client.socket.terminate();

        continue;
      }

      client.missedPongCount += 1;
      client.socket.ping();
      this.sendRaw(client, encoded);
    }
  }

  private startSymbolListSync(): void {
    this.symbolSyncTimer = setInterval(() => {
      this.registry.syncAllLoadedSources().catch((error: unknown) => {
        this.logger.error({ error }, '[Feeder] symbol list sync failed');
      });
    }, SYMBOL_LIST_SYNC_INTERVAL_MS);
  }

  private sendMessage(client: ClientConnection, message: ServerMessage): void {
    this.sendRaw(client, encodeMessage(message));
  }

  private sendRaw(client: ClientConnection, encoded: string): void {
    if (client.socket.readyState !== WebSocket.OPEN) {
      return;
    }

    client.socket.send(encoded);
  }
}

export { FeederServer };

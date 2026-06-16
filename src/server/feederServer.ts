import { WebSocket, WebSocketServer } from 'ws';

import type { KlineInterval } from '../domain/marketData.types.js';
import type { FeedEventName, SubscriptionScope } from '../domain/subscription.types.js';
import type { MarketDataSnapshotEntry } from '../domain/snapshot.types.js';
import type { ServerMessage, SnapshotMessage, SubscribeMessage, SymbolAddedMessage, SymbolRemovedMessage, UnsubscribeMessage } from '../protocol/messages.types.js';
import { decodeMessage, encodeMessage } from '../protocol/codec.js';
import { SubscriptionRegistry } from './subscriptionRegistry.js';
import type { FeederSource } from './feederSource.types.js';
import type { FeederLogger, FeederServerArgs, ForwardFeedMessageArgs } from './feederServer.types.js';

const DEFAULT_SNAPSHOT_CHUNK_SIZE = 100;
const HEARTBEAT_INTERVAL_MS = 15_000;
const SYMBOL_LIST_SYNC_INTERVAL_MS = 3_600_000;

interface ClientSubscription {
  scope: SubscriptionScope;
  eventNameSet: Set<FeedEventName>;
}

interface ClientConnection {
  socket: WebSocket;
  subscriptionByInterval: Map<KlineInterval, ClientSubscription>;
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

  constructor(args: FeederServerArgs) {
    this.port = args.port;
    this.host = args.host ?? '127.0.0.1';
    this.logger = args.logger;
    this.snapshotChunkSize = args.snapshotChunkSize ?? DEFAULT_SNAPSHOT_CHUNK_SIZE;
    this.boundPort = args.port;
    this.registry = new SubscriptionRegistry<FeederSource>({
      createSource: (interval) => this.createForwardingSource(interval, args.createSource),
    });
  }

  async start(): Promise<void> {
    const webSocketServer = new WebSocketServer({ port: this.port, host: this.host });
    this.webSocketServer = webSocketServer;

    webSocketServer.on('connection', (socket) => {
      this.handleConnection(socket);
    });

    await new Promise<void>((resolve, reject) => {
      webSocketServer.once('listening', () => {
        resolve();
      });

      webSocketServer.once('error', (error) => {
        reject(error);
      });
    });

    const address = webSocketServer.address();

    if (address !== null && typeof address === 'object') {
      this.boundPort = address.port;
    }

    this.startHeartbeat();
    this.startSymbolListSync();
    this.logger.info({ port: this.boundPort, host: this.host }, `[Feeder] WebSocket server listening on ${this.host}:${this.boundPort}`);
  }

  getPort(): number {
    return this.boundPort;
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

    for (const client of this.clientSet) {
      client.socket.close();
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
    });

    return source;
  }

  private handleConnection(socket: WebSocket): void {
    const client: ClientConnection = { socket, subscriptionByInterval: new Map() };
    this.clientSet.add(client);

    socket.on('message', (data) => {
      this.handleClientMessage(client, data.toString());
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
      this.handleSubscribe(client, message).catch((error: unknown) => {
        this.logger.error({ error, interval: message.interval }, '[Feeder] handleSubscribe failed');
      });

      return;
    }

    if (message.type === 'unsubscribe') {
      this.handleUnsubscribe(client, message).catch((error: unknown) => {
        this.logger.error({ error, interval: message.interval }, '[Feeder] handleUnsubscribe failed');
      });
    }
  }

  private async handleSubscribe(client: ClientConnection, message: SubscribeMessage): Promise<void> {
    const source = await this.registry.subscribe({ interval: message.interval, scope: message.scope });
    const snapshotMessageList = this.buildSnapshotMessageList(source, message.interval, message.scope);

    client.subscriptionByInterval.set(message.interval, { scope: message.scope, eventNameSet: new Set(message.events) });

    for (const snapshotMessage of snapshotMessageList) {
      this.sendMessage(client, snapshotMessage);
    }
  }

  private async handleUnsubscribe(client: ClientConnection, message: UnsubscribeMessage): Promise<void> {
    client.subscriptionByInterval.delete(message.interval);

    await this.registry.unsubscribe({ interval: message.interval, scope: message.scope });
  }

  private handleClientClose(client: ClientConnection): void {
    this.clientSet.delete(client);

    for (const [interval, subscription] of client.subscriptionByInterval) {
      this.registry.unsubscribe({ interval, scope: subscription.scope }).catch((error: unknown) => {
        this.logger.error({ error, interval }, '[Feeder] unsubscribe on close failed');
      });
    }

    client.subscriptionByInterval.clear();
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
    const encoded = encodeMessage(message);

    for (const client of this.clientSet) {
      const subscription = client.subscriptionByInterval.get(interval);

      if (subscription === undefined || !scopeMatchesSymbol(subscription.scope, symbol)) {
        continue;
      }

      this.sendRaw(client, encoded);
    }
  }

  private forwardSymbolRemoved(interval: KlineInterval, symbol: string): void {
    const message: SymbolRemovedMessage = { type: 'symbolRemoved', interval, symbol };
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
      const encoded = encodeMessage({ type: 'heartbeat' });

      for (const client of this.clientSet) {
        this.sendRaw(client, encoded);
      }
    }, HEARTBEAT_INTERVAL_MS);
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

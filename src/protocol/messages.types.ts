import type { Kline, KlineInterval, MaValues } from '../domain/marketData.types.js';
import type { FeedEventName, SubscriptionScope } from '../domain/subscription.types.js';
import type { MarketDataSnapshotEntry } from '../domain/snapshot.types.js';

interface SubscribeMessage {
  type: 'subscribe';
  interval: KlineInterval;
  scope: SubscriptionScope;
  events: FeedEventName[];
  wantMa: boolean;
}

interface UnsubscribeMessage {
  type: 'unsubscribe';
  interval: KlineInterval;
  scope: SubscriptionScope;
}

interface SnapshotMessage {
  type: 'snapshot';
  interval: KlineInterval;
  entryList: MarketDataSnapshotEntry[];
  isFinal: boolean;
}

interface KlineClosedMessage {
  type: 'klineClosed';
  interval: KlineInterval;
  symbol: string;
  kline: Kline;
  maValues: MaValues;
}

interface KlineUpdatedMessage {
  type: 'klineUpdated';
  interval: KlineInterval;
  symbol: string;
  kline: Kline;
  maValues: MaValues;
}

interface KlineUpdatedTickMessage {
  type: 'klineUpdatedTick';
  interval: KlineInterval;
  symbol: string;
  kline: Kline;
}

interface SymbolAddedMessage {
  type: 'symbolAdded';
  interval: KlineInterval;
  entry: MarketDataSnapshotEntry;
}

interface SymbolRemovedMessage {
  type: 'symbolRemoved';
  interval: KlineInterval;
  symbol: string;
}

interface Volume24hMessage {
  type: 'volume24h';
  interval: KlineInterval;
  symbol: string;
  volume24hUsdt: number;
}

interface HeartbeatMessage {
  type: 'heartbeat';
}

type ClientMessage = SubscribeMessage | UnsubscribeMessage;

type ServerMessage =
  | SnapshotMessage
  | KlineClosedMessage
  | KlineUpdatedMessage
  | KlineUpdatedTickMessage
  | SymbolAddedMessage
  | SymbolRemovedMessage
  | Volume24hMessage
  | HeartbeatMessage;

type FeederMessage = ClientMessage | ServerMessage;

export type {
  ClientMessage,
  FeederMessage,
  HeartbeatMessage,
  KlineClosedMessage,
  KlineUpdatedMessage,
  KlineUpdatedTickMessage,
  ServerMessage,
  SnapshotMessage,
  SubscribeMessage,
  SymbolAddedMessage,
  SymbolRemovedMessage,
  UnsubscribeMessage,
  Volume24hMessage,
};

import type { KlineInterval } from '../domain/marketData.types.js';
import type { FeedEventName } from '../domain/subscription.types.js';
import type { ServerMessage } from '../protocol/messages.types.js';
import type { FeederSource } from './feederSource.types.js';

interface FeederLogger {
  info: (payload: Record<string, unknown>, message: string) => void;
  warn: (payload: Record<string, unknown>, message: string) => void;
  error: (payload: Record<string, unknown>, message: string) => void;
}

interface FeederServerArgs {
  port: number;
  host?: string;
  createSource: (interval: KlineInterval) => FeederSource;
  logger: FeederLogger;
  snapshotChunkSize?: number;
}

interface ForwardFeedMessageArgs {
  eventName: FeedEventName;
  interval: KlineInterval;
  symbol: string;
  message: ServerMessage;
}

export type { FeederLogger, FeederServerArgs, ForwardFeedMessageArgs };

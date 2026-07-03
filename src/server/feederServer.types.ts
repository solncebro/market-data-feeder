import type { KlineInterval, MaValues } from '../domain/marketData.types.js';
import type { FeedEventName } from '../domain/subscription.types.js';
import type { ServerMessage } from '../protocol/messages.types.js';
import type { HealthEvent } from '../health/healthMonitor.types.js';
import type { FeederSource, StreamLiveness } from './feederSource.types.js';

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
  onHealthEvent?: (event: HealthEvent) => void;
  maxConnections?: number;
  maxPayloadBytes?: number;
  subscribeFailureCloseDelayMs?: number;
  heartbeatIntervalMs?: number;
  maxMissedPongCount?: number;
  slowClientBufferedLimitBytes?: number;
}

interface ForwardFeedMessageArgs {
  eventName: FeedEventName;
  interval: KlineInterval;
  symbol: string;
  message: ServerMessage;
}

interface IntervalStatus {
  interval: KlineInterval;
  intervalMs: number;
  isAllLoaded: boolean;
  allSubscriberCount: number;
  refSymbolCount: number;
  symbolCount: number;
  klineCount: number;
  staleCount: number;
  subscriptionCount: number;
  liveness: StreamLiveness;
  freshCount: number;
  persistentStaleCount: number;
}

interface FeederStatus {
  host: string;
  port: number;
  clientCount: number;
  uptimeMs: number;
  intervalStatusList: IntervalStatus[];
}

interface SymbolDiagnostics {
  symbol: string;
  interval: KlineInterval;
  lastPrice: number | null;
  lastKlineOpenMs: number | undefined;
  lastUpdateMs: number | undefined;
  maValues: MaValues;
  volume24hUsdt: number | null;
  isStale: boolean;
  staleAgeMs: number | undefined;
}

export type { FeederLogger, FeederServerArgs, FeederStatus, ForwardFeedMessageArgs, IntervalStatus, SymbolDiagnostics };

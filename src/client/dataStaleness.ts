import type { FeedEventName } from '../domain/subscription.types.js';

interface DataStalenessArgs {
  isSnapshotComplete: boolean;
  lastDataMessageMs: number;
  nowMs: number;
  thresholdMs: number;
}

interface ResolveDataStaleThresholdArgs {
  intervalMs: number;
  events: FeedEventName[];
  baseThresholdMs: number;
}

function isDataStale(args: DataStalenessArgs): boolean {
  if (!args.isSnapshotComplete) {
    return true;
  }

  return args.nowMs - args.lastDataMessageMs > args.thresholdMs;
}

function isDataFreshnessMessage(messageType: string): boolean {
  return messageType !== 'heartbeat';
}

// The staleness window must match how often this subscription actually receives data. A subscription
// with a high-frequency event (klineUpdated/klineUpdatedTick) gets messages within seconds, so the base
// threshold is right. A closed-only subscription gets a message just once per interval — one full
// interval can legitimately pass between klines — so the threshold must exceed an interval (plus base
// grace); otherwise the channel reads STALE for most of every interval even while the feeder is healthy.
function resolveDataStaleThresholdMs(args: ResolveDataStaleThresholdArgs): number {
  const hasHighFrequencyEvent = args.events.some((event) => event === 'klineUpdated' || event === 'klineUpdatedTick');

  if (hasHighFrequencyEvent) {
    return args.baseThresholdMs;
  }

  return args.intervalMs + args.baseThresholdMs;
}

export { isDataFreshnessMessage, isDataStale, resolveDataStaleThresholdMs };
export type { DataStalenessArgs, ResolveDataStaleThresholdArgs };

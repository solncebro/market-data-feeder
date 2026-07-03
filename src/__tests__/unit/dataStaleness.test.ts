import { describe, expect, it } from 'vitest';

import { isDataFreshnessMessage, isDataStale, resolveDataStaleThresholdMs } from '../../client/dataStaleness.js';

describe('isDataStale', () => {
  it('is stale while the snapshot is not yet complete, even with a recent message', () => {
    expect(isDataStale({ isSnapshotComplete: false, lastDataMessageMs: 1000, nowMs: 1000, thresholdMs: 45_000 })).toBe(true);
  });

  it('is fresh when a data message arrived within the threshold', () => {
    expect(isDataStale({ isSnapshotComplete: true, lastDataMessageMs: 10_000, nowMs: 40_000, thresholdMs: 45_000 })).toBe(false);
  });

  it('is stale when no data message arrived within the threshold', () => {
    expect(isDataStale({ isSnapshotComplete: true, lastDataMessageMs: 10_000, nowMs: 60_000, thresholdMs: 45_000 })).toBe(true);
  });
});

describe('resolveDataStaleThresholdMs', () => {
  const baseThresholdMs = 45_000;
  const thirtyMinutesMs = 1_800_000;

  it('keeps the base threshold when a high-frequency tick event is subscribed', () => {
    expect(resolveDataStaleThresholdMs({ intervalMs: thirtyMinutesMs, events: ['klineUpdatedTick'], baseThresholdMs })).toBe(baseThresholdMs);
  });

  it('keeps the base threshold when a high-frequency updated event is subscribed', () => {
    expect(resolveDataStaleThresholdMs({ intervalMs: thirtyMinutesMs, events: ['klineUpdated'], baseThresholdMs })).toBe(baseThresholdMs);
  });

  it('extends the threshold past one interval for a closed-only subscription', () => {
    expect(resolveDataStaleThresholdMs({ intervalMs: thirtyMinutesMs, events: ['klineClosed'], baseThresholdMs })).toBe(thirtyMinutesMs + baseThresholdMs);
  });

  it('extends the threshold for an empty event subscription', () => {
    expect(resolveDataStaleThresholdMs({ intervalMs: thirtyMinutesMs, events: [], baseThresholdMs })).toBe(thirtyMinutesMs + baseThresholdMs);
  });

  it('a closed-only 30m channel is not stale after one minute of silence (regression: false STALE/DOWN)', () => {
    const thresholdMs = resolveDataStaleThresholdMs({ intervalMs: thirtyMinutesMs, events: ['klineClosed'], baseThresholdMs });

    expect(isDataStale({ isSnapshotComplete: true, lastDataMessageMs: 0, nowMs: 60_000, thresholdMs })).toBe(false);
  });

  it('a closed-only 30m channel is stale once a full interval plus grace has elapsed with no klines', () => {
    const thresholdMs = resolveDataStaleThresholdMs({ intervalMs: thirtyMinutesMs, events: ['klineClosed'], baseThresholdMs });

    expect(isDataStale({ isSnapshotComplete: true, lastDataMessageMs: 0, nowMs: thirtyMinutesMs + baseThresholdMs + 1, thresholdMs })).toBe(true);
  });
});

describe('isDataFreshnessMessage', () => {
  it('does not count a heartbeat as a data-freshness signal', () => {
    expect(isDataFreshnessMessage('heartbeat')).toBe(false);
  });

  it('counts kline and snapshot messages as data-freshness signals', () => {
    expect(isDataFreshnessMessage('snapshot')).toBe(true);
    expect(isDataFreshnessMessage('klineClosed')).toBe(true);
    expect(isDataFreshnessMessage('klineUpdatedTick')).toBe(true);
    expect(isDataFreshnessMessage('volume24h')).toBe(true);
  });
});

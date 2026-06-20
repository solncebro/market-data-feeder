import { describe, expect, it } from 'vitest';

import { isDataFreshnessMessage, isDataStale } from '../../client/dataStaleness.js';

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

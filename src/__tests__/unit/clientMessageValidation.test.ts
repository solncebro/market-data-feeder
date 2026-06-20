import { describe, expect, it } from 'vitest';

import { isValidSubscribe } from '../../protocol/clientMessageValidation.js';

describe('isValidSubscribe', () => {
  it('accepts a valid all-scope subscribe', () => {
    expect(isValidSubscribe({ type: 'subscribe', interval: '30m', scope: { kind: 'all' }, events: ['klineClosed'], wantMa: true })).toBe(true);
  });

  it('accepts a valid symbols-scope subscribe', () => {
    expect(isValidSubscribe({ type: 'subscribe', interval: '5m', scope: { kind: 'symbols', symbolList: ['BTCUSDT', 'ETHUSDT'] }, events: [], wantMa: false })).toBe(true);
  });

  it('rejects an unknown interval', () => {
    expect(isValidSubscribe({ type: 'subscribe', interval: 'bogus', scope: { kind: 'all' }, events: [], wantMa: false })).toBe(false);
  });

  it('rejects an invalid scope kind', () => {
    expect(isValidSubscribe({ type: 'subscribe', interval: '30m', scope: { kind: 'everything' }, events: [], wantMa: false })).toBe(false);
  });

  it('rejects a missing scope', () => {
    expect(isValidSubscribe({ type: 'subscribe', interval: '30m', events: [], wantMa: false })).toBe(false);
  });

  it('rejects a symbol that is not plain uppercase alphanumeric', () => {
    expect(isValidSubscribe({ type: 'subscribe', interval: '30m', scope: { kind: 'symbols', symbolList: ['BTC*USDT'] }, events: [], wantMa: false })).toBe(false);
  });

  it('rejects an empty symbol list', () => {
    expect(isValidSubscribe({ type: 'subscribe', interval: '30m', scope: { kind: 'symbols', symbolList: [] }, events: [], wantMa: false })).toBe(false);
  });

  it('rejects a symbol list above the cap', () => {
    const hugeSymbolList = Array.from({ length: 5001 }, () => 'BTCUSDT');

    expect(isValidSubscribe({ type: 'subscribe', interval: '30m', scope: { kind: 'symbols', symbolList: hugeSymbolList }, events: [], wantMa: false })).toBe(false);
  });

  it('rejects non-array events', () => {
    expect(isValidSubscribe({ type: 'subscribe', interval: '30m', scope: { kind: 'all' }, events: 'klineClosed', wantMa: false })).toBe(false);
  });
});

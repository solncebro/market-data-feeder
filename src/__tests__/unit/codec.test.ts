import { describe, expect, it } from 'vitest';

import type { KlineClosedMessage, SubscribeMessage } from '../../protocol/messages.types.js';
import { decodeMessage, encodeMessage } from '../../protocol/codec.js';

const SUBSCRIBE_MESSAGE: SubscribeMessage = {
  type: 'subscribe',
  interval: '30m',
  scope: { kind: 'symbols', symbolList: ['BTCUSDT', 'ETHUSDT'] },
  events: ['klineClosed', 'klineUpdated'],
  wantMa: true,
};

const KLINE_CLOSED_MESSAGE: KlineClosedMessage = {
  type: 'klineClosed',
  interval: '30m',
  symbol: 'BTCUSDT',
  kline: {
    openTimestamp: 1000,
    openPrice: 100,
    highPrice: 110,
    lowPrice: 90,
    closePrice: 105,
    volume: 1000,
    closeTimestamp: 2000,
    quoteAssetVolume: 105_000,
    numberOfTrades: 50,
    takerBuyBaseAssetVolume: 500,
    takerBuyQuoteAssetVolume: 52_500,
    isClosed: true,
  },
  maValues: { ma25: 1, ma50: 2, ma100: 3, ma200: 4 },
};

describe('codec', () => {
  it('round-trips a subscribe message', () => {
    expect(decodeMessage(encodeMessage(SUBSCRIBE_MESSAGE))).toEqual(SUBSCRIBE_MESSAGE);
  });

  it('round-trips a klineClosed message', () => {
    expect(decodeMessage(encodeMessage(KLINE_CLOSED_MESSAGE))).toEqual(KLINE_CLOSED_MESSAGE);
  });

  it('returns null for non-JSON input', () => {
    expect(decodeMessage('not json {')).toBeNull();
  });

  it('returns null for an object without a known type', () => {
    expect(decodeMessage(JSON.stringify({ type: 'somethingElse', interval: '30m' }))).toBeNull();
  });

  it('returns null for a missing type field', () => {
    expect(decodeMessage(JSON.stringify({ interval: '30m' }))).toBeNull();
  });

  it('returns null for a JSON primitive', () => {
    expect(decodeMessage(JSON.stringify(42))).toBeNull();
  });
});

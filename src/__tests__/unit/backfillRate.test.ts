import { describe, expect, it } from 'vitest';

import { ExchangeNameEnum } from '@solncebro/trade-engine';

import { resolveBackfillRequestsPerSecond } from '../../domain/constants.js';

const BINANCE_KLINE_WEIGHT = 2; // GET /fapi/v1/klines at limit=499 costs weight 2
const BINANCE_REQUEST_WEIGHT_LIMIT_PER_MIN = 2400; // live fapi/v1/exchangeInfo REQUEST_WEIGHT cap per IP

describe('resolveBackfillRequestsPerSecond', () => {
  it('keeps Binance backfill weight under the 2400/min REQUEST_WEIGHT cap with headroom', () => {
    const rate = resolveBackfillRequestsPerSecond(ExchangeNameEnum.Binance);
    const weightPerMinute = rate * BINANCE_KLINE_WEIGHT * 60;

    expect(weightPerMinute).toBeLessThanOrEqual(BINANCE_REQUEST_WEIGHT_LIMIT_PER_MIN);
    expect(rate).toBe(16);
  });

  it('keeps the proven Bybit backfill rate unchanged', () => {
    expect(resolveBackfillRequestsPerSecond(ExchangeNameEnum.Bybit)).toBe(80);
  });
});

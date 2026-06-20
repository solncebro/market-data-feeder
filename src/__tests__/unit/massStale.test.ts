import { describe, expect, it } from 'vitest';

import { crossesMassStaleThreshold } from '../../source/massStale.js';

describe('crossesMassStaleThreshold', () => {
  it('never crosses below the minimum-symbol guard', () => {
    expect(crossesMassStaleThreshold({ staleCount: 5, symbolCount: 5, ratioThreshold: 0.3, minSymbols: 20 })).toBe(false);
  });

  it('does not divide by zero when there are no symbols', () => {
    expect(crossesMassStaleThreshold({ staleCount: 0, symbolCount: 0, ratioThreshold: 0.3, minSymbols: 20 })).toBe(false);
  });

  it('crosses at the ratio threshold once the sample is large enough', () => {
    expect(crossesMassStaleThreshold({ staleCount: 9, symbolCount: 30, ratioThreshold: 0.3, minSymbols: 20 })).toBe(true);
  });

  it('does not cross below the ratio threshold', () => {
    expect(crossesMassStaleThreshold({ staleCount: 8, symbolCount: 30, ratioThreshold: 0.3, minSymbols: 20 })).toBe(false);
  });
});

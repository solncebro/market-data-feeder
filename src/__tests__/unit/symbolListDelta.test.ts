import { describe, expect, it } from 'vitest';

import { computeSymbolListDelta } from '../../source/symbolListDelta.js';

describe('computeSymbolListDelta', () => {
  it('returns symbols on the exchange but not yet loaded as added', () => {
    const delta = computeSymbolListDelta({ loadedSymbolList: ['BTCUSDT'], exchangeSymbolList: ['BTCUSDT', 'ETHUSDT'] });

    expect(delta.addedSymbolList).toEqual(['ETHUSDT']);
    expect(delta.removedSymbolList).toEqual([]);
  });

  it('returns loaded symbols no longer on the exchange as removed', () => {
    const delta = computeSymbolListDelta({ loadedSymbolList: ['BTCUSDT', 'OLDUSDT'], exchangeSymbolList: ['BTCUSDT'] });

    expect(delta.addedSymbolList).toEqual([]);
    expect(delta.removedSymbolList).toEqual(['OLDUSDT']);
  });

  it('returns an empty delta when the lists match', () => {
    const delta = computeSymbolListDelta({ loadedSymbolList: ['BTCUSDT', 'ETHUSDT'], exchangeSymbolList: ['ETHUSDT', 'BTCUSDT'] });

    expect(delta.addedSymbolList).toEqual([]);
    expect(delta.removedSymbolList).toEqual([]);
  });

  it('reports added and removed simultaneously', () => {
    const delta = computeSymbolListDelta({ loadedSymbolList: ['BTCUSDT', 'OLDUSDT'], exchangeSymbolList: ['BTCUSDT', 'NEWUSDT'] });

    expect(delta.addedSymbolList).toEqual(['NEWUSDT']);
    expect(delta.removedSymbolList).toEqual(['OLDUSDT']);
  });
});

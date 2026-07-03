import { describe, expect, it } from 'vitest';

import { evaluateSymbolRemovalList } from '../../source/symbolRemovalGuard.js';

describe('evaluateSymbolRemovalList', () => {
  it('approves a small removal immediately (a normal delisting)', () => {
    const decision = evaluateSymbolRemovalList({
      removedSymbolList: ['AAAUSDT', 'BBBUSDT'],
      loadedSymbolCount: 500,
      pendingRemovalSet: new Set(),
    });

    expect(decision.approvedRemovalList).toEqual(['AAAUSDT', 'BBBUSDT']);
    expect(decision.deferredRemovalList).toEqual([]);
    expect(decision.nextPendingRemovalSet.size).toBe(0);
  });

  it('defers a mass removal on the first sync (no confirmation yet)', () => {
    const removedSymbolList = Array.from({ length: 150 }, (_item, index) => `SYM${index}USDT`);

    const decision = evaluateSymbolRemovalList({
      removedSymbolList,
      loadedSymbolCount: 500,
      pendingRemovalSet: new Set(),
    });

    expect(decision.approvedRemovalList).toEqual([]);
    expect(decision.deferredRemovalList).toEqual(removedSymbolList);
    expect(decision.nextPendingRemovalSet).toEqual(new Set(removedSymbolList));
  });

  it('approves a mass removal on the second consecutive sync (confirmed)', () => {
    const removedSymbolList = Array.from({ length: 150 }, (_item, index) => `SYM${index}USDT`);

    const decision = evaluateSymbolRemovalList({
      removedSymbolList,
      loadedSymbolCount: 500,
      pendingRemovalSet: new Set(removedSymbolList),
    });

    expect(decision.approvedRemovalList).toEqual(removedSymbolList);
    expect(decision.deferredRemovalList).toEqual([]);
    expect(decision.nextPendingRemovalSet.size).toBe(0);
  });

  it('a transient glitch clears without ever approving (list back to normal on the next sync)', () => {
    const decision = evaluateSymbolRemovalList({
      removedSymbolList: [],
      loadedSymbolCount: 500,
      pendingRemovalSet: new Set(['SYM1USDT', 'SYM2USDT']),
    });

    expect(decision.approvedRemovalList).toEqual([]);
    expect(decision.deferredRemovalList).toEqual([]);
    expect(decision.nextPendingRemovalSet.size).toBe(0);
  });

  it('does not treat a large share of a tiny loaded set as mass (absolute floor)', () => {
    const decision = evaluateSymbolRemovalList({
      removedSymbolList: ['AAAUSDT', 'BBBUSDT', 'CCCUSDT'],
      loadedSymbolCount: 5,
      pendingRemovalSet: new Set(),
    });

    expect(decision.approvedRemovalList).toEqual(['AAAUSDT', 'BBBUSDT', 'CCCUSDT']);
    expect(decision.deferredRemovalList).toEqual([]);
  });

  it('mixed second sync: previously flagged symbols approved, newly missing ones deferred', () => {
    const previouslyFlaggedList = Array.from({ length: 120 }, (_item, index) => `OLD${index}USDT`);
    const newlyMissingList = Array.from({ length: 30 }, (_item, index) => `NEW${index}USDT`);

    const decision = evaluateSymbolRemovalList({
      removedSymbolList: [...previouslyFlaggedList, ...newlyMissingList],
      loadedSymbolCount: 500,
      pendingRemovalSet: new Set(previouslyFlaggedList),
    });

    expect(decision.approvedRemovalList).toEqual(previouslyFlaggedList);
    expect(decision.deferredRemovalList).toEqual(newlyMissingList);
    expect(decision.nextPendingRemovalSet).toEqual(new Set(newlyMissingList));
  });
});

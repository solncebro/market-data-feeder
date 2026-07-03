// Guard against a truncated exchange symbol list masquerading as a mass delisting: a real mass
// removal must survive two consecutive hourly syncs before it is applied. A transient API glitch
// differs between syncs and never confirms; a genuine mass delisting persists and applies on the
// second sync, so the guard always converges.
const MIN_REMOVED_SYMBOL_COUNT_FOR_GUARD = 10;
const MAX_REMOVED_SYMBOL_SHARE = 0.2;

interface EvaluateSymbolRemovalArgs {
  removedSymbolList: string[];
  loadedSymbolCount: number;
  pendingRemovalSet: ReadonlySet<string>;
}

interface SymbolRemovalDecision {
  approvedRemovalList: string[];
  deferredRemovalList: string[];
  nextPendingRemovalSet: Set<string>;
}

function evaluateSymbolRemovalList(args: EvaluateSymbolRemovalArgs): SymbolRemovalDecision {
  const { loadedSymbolCount, pendingRemovalSet, removedSymbolList } = args;
  const removedShare = loadedSymbolCount > 0 ? removedSymbolList.length / loadedSymbolCount : 0;
  const isMassRemoval = removedSymbolList.length >= MIN_REMOVED_SYMBOL_COUNT_FOR_GUARD && removedShare > MAX_REMOVED_SYMBOL_SHARE;

  if (!isMassRemoval) {
    return { approvedRemovalList: removedSymbolList, deferredRemovalList: [], nextPendingRemovalSet: new Set() };
  }

  const approvedRemovalList = removedSymbolList.filter((symbol) => pendingRemovalSet.has(symbol));
  const deferredRemovalList = removedSymbolList.filter((symbol) => !pendingRemovalSet.has(symbol));

  return { approvedRemovalList, deferredRemovalList, nextPendingRemovalSet: new Set(deferredRemovalList) };
}

export { evaluateSymbolRemovalList };
export type { EvaluateSymbolRemovalArgs, SymbolRemovalDecision };

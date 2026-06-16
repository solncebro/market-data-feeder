interface ComputeSymbolListDeltaArgs {
  loadedSymbolList: string[];
  exchangeSymbolList: string[];
}

interface SymbolListDelta {
  addedSymbolList: string[];
  removedSymbolList: string[];
}

export type { ComputeSymbolListDeltaArgs, SymbolListDelta };

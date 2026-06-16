import type { ComputeSymbolListDeltaArgs, SymbolListDelta } from './symbolListDelta.types.js';

function computeSymbolListDelta(args: ComputeSymbolListDeltaArgs): SymbolListDelta {
  const loadedSymbolSet = new Set(args.loadedSymbolList);
  const exchangeSymbolSet = new Set(args.exchangeSymbolList);

  const addedSymbolList = args.exchangeSymbolList.filter((symbol) => !loadedSymbolSet.has(symbol));
  const removedSymbolList = args.loadedSymbolList.filter((symbol) => !exchangeSymbolSet.has(symbol));

  return { addedSymbolList, removedSymbolList };
}

export { computeSymbolListDelta };

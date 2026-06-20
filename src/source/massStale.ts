interface MassStaleThresholdArgs {
  staleCount: number;
  symbolCount: number;
  ratioThreshold: number;
  minSymbols: number;
}

function crossesMassStaleThreshold(args: MassStaleThresholdArgs): boolean {
  if (args.symbolCount < args.minSymbols) {
    return false;
  }

  return args.staleCount / args.symbolCount >= args.ratioThreshold;
}

export { crossesMassStaleThreshold };
export type { MassStaleThresholdArgs };

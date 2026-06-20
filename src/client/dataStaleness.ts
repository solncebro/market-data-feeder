interface DataStalenessArgs {
  isSnapshotComplete: boolean;
  lastDataMessageMs: number;
  nowMs: number;
  thresholdMs: number;
}

function isDataStale(args: DataStalenessArgs): boolean {
  if (!args.isSnapshotComplete) {
    return true;
  }

  return args.nowMs - args.lastDataMessageMs > args.thresholdMs;
}

function isDataFreshnessMessage(messageType: string): boolean {
  return messageType !== 'heartbeat';
}

export { isDataFreshnessMessage, isDataStale };
export type { DataStalenessArgs };

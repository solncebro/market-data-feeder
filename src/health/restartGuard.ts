import { readFileSync, writeFileSync } from 'node:fs';

interface RestartGuardLogger {
  warn: (payload: Record<string, unknown>, message: string) => void;
  error?: (payload: Record<string, unknown>, message: string) => void;
}

interface RestartGuardArgs {
  filePath: string;
  windowMs: number;
  maxRestarts: number;
  now?: () => number;
  logger?: RestartGuardLogger;
}

interface RestartGuard {
  canRestart: () => boolean;
  recordRestart: () => void;
  getRecentRestartCount: () => number;
}

interface RestartGuardState {
  restartList: number[];
}

/**
 * File-backed counter of automatic restarts that survives the process restart it guards against.
 * An in-memory counter would reset on every reboot, so a degraded feeder could restart forever.
 * Reads fail-open (a missing/corrupt file means "0 restarts → allowed") because keeping a dead
 * feeder down on an unrelated read error is worse than one extra restart; writes swallow errors
 * (best-effort loop protection, not a critical path).
 */
function createRestartGuard(args: RestartGuardArgs): RestartGuard {
  const now = args.now ?? Date.now;

  const readTimestampList = (): number[] => {
    try {
      const raw = readFileSync(args.filePath, 'utf8');
      const parsed = JSON.parse(raw) as Partial<RestartGuardState>;

      if (!Array.isArray(parsed.restartList)) {
        return [];
      }

      return parsed.restartList.filter((timestamp) => Number.isFinite(timestamp));
    } catch {
      return [];
    }
  };

  const recentTimestampList = (): number[] => {
    const thresholdMs = now() - args.windowMs;

    return readTimestampList().filter((timestamp) => timestamp >= thresholdMs);
  };

  const getRecentRestartCount = (): number => recentTimestampList().length;

  const canRestart = (): boolean => getRecentRestartCount() < args.maxRestarts;

  const persist = (restartList: number[]): void => {
    try {
      writeFileSync(args.filePath, JSON.stringify({ restartList } satisfies RestartGuardState), 'utf8');
    } catch (error: unknown) {
      args.logger?.error?.({ error, filePath: args.filePath }, '[Health] restart-log path is not writable — auto-restart loop protection cannot persist its counter');
    }
  };

  const recordRestart = (): void => {
    const restartList = recentTimestampList();
    restartList.push(now());
    persist(restartList);
  };

  persist(recentTimestampList());

  return { canRestart, recordRestart, getRecentRestartCount };
}

export { createRestartGuard };
export type { RestartGuard, RestartGuardArgs };

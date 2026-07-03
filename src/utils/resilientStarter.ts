import type { FeederLogger } from '../server/feederServer.types.js';

interface ResilientStarterArgs {
  attempt: () => Promise<void>;
  retryDelayMs: number;
  label: string;
  logger: FeederLogger;
}

interface ResilientStarter {
  start: () => Promise<void>;
  requestRetry: () => void;
  stop: () => void;
}

// Keeps a non-critical dependency (e.g. the Telegram bot) from ever being fatal: a failed start
// attempt is logged and retried in the background instead of rejecting up to the caller.
function createResilientStarter(args: ResilientStarterArgs): ResilientStarter {
  const { attempt, label, logger, retryDelayMs } = args;
  let retryTimer: ReturnType<typeof setTimeout> | null = null;
  let isStopped = false;

  const scheduleRetry = (): void => {
    if (isStopped || retryTimer !== null) {
      return;
    }

    retryTimer = setTimeout(() => {
      retryTimer = null;
      void runAttempt();
    }, retryDelayMs);
    retryTimer.unref();
  };

  const runAttempt = async (): Promise<void> => {
    try {
      await attempt();
    } catch (error: unknown) {
      logger.error({ error }, `[ResilientStarter] ${label} start attempt failed — retrying in ${Math.round(retryDelayMs / 1000)}s`);
      scheduleRetry();
    }
  };

  const stop = (): void => {
    isStopped = true;

    if (retryTimer !== null) {
      clearTimeout(retryTimer);
      retryTimer = null;
    }
  };

  return { start: runAttempt, requestRetry: scheduleRetry, stop };
}

export { createResilientStarter };
export type { ResilientStarter, ResilientStarterArgs };

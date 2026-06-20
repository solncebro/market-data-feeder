import type { FeederLogger } from '../server/feederServer.types.js';

interface ChannelConnectivityMonitorArgs {
  probe: () => Promise<void>;
  logger: FeederLogger;
  retryDelayMs: number;
  recheckIntervalMs: number;
}

interface ChannelConnectivityMonitor {
  stop: () => void;
}

function startChannelConnectivityMonitor(args: ChannelConnectivityMonitorArgs): ChannelConnectivityMonitor {
  let state: 'unknown' | 'up' | 'down' = 'unknown';
  let isStopped = false;
  let timer: ReturnType<typeof setTimeout> | null = null;

  const scheduleNext = (delayMs: number): void => {
    if (isStopped) {
      return;
    }

    timer = setTimeout(runCheck, delayMs);
    timer.unref();
  };

  const handleSuccess = (): void => {
    if (state !== 'up') {
      state = 'up';
      args.logger.info({}, '[TelegramBot] control channel reachable — alerts will be delivered');
    }

    scheduleNext(args.recheckIntervalMs);
  };

  const handleFailure = (error: unknown): void => {
    state = 'down';
    args.logger.error({ error }, '[TelegramBot] control channel UNREACHABLE — alerts are NOT being delivered, retrying');
    scheduleNext(args.retryDelayMs);
  };

  const runCheck = (): void => {
    timer = null;

    if (isStopped) {
      return;
    }

    args.probe().then(handleSuccess).catch(handleFailure);
  };

  runCheck();

  return {
    stop: () => {
      isStopped = true;

      if (timer !== null) {
        clearTimeout(timer);
        timer = null;
      }
    },
  };
}

export { startChannelConnectivityMonitor };
export type { ChannelConnectivityMonitor, ChannelConnectivityMonitorArgs };

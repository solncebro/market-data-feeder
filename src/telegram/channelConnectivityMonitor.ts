import type { FeederLogger } from '../server/feederServer.types.js';
import { withTimeout } from '../utils/timeout.js';

const DEFAULT_PROBE_TIMEOUT_MS = 10_000;

interface ChannelConnectivityMonitorArgs {
  probe: () => Promise<void>;
  logger: FeederLogger;
  retryDelayMs: number;
  recheckIntervalMs: number;
  probeTimeoutMs?: number;
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

  const probeTimeoutMs = args.probeTimeoutMs ?? DEFAULT_PROBE_TIMEOUT_MS;

  const runCheck = (): void => {
    timer = null;

    if (isStopped) {
      return;
    }

    // The next check is scheduled only from this probe's settlement — a probe that never settles
    // (half-open connection) would otherwise silently kill the monitor forever, so it is time-boxed.
    withTimeout(args.probe(), probeTimeoutMs, `control channel probe timed out after ${probeTimeoutMs}ms`)
      .then(handleSuccess)
      .catch(handleFailure);
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

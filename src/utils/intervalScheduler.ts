import { logger } from '@solncebro/trade-engine';

interface IntervalSchedulerHandle {
  stop: () => void;
}

interface IntervalSchedulerArgs {
  tickHandler: () => void | Promise<void>;
  intervalMs: number;
  contextLabel: string;
  heartbeatEveryNTicks?: number;
  heartbeatHandler?: (tickCount: number) => void;
  errorHandler?: (error: unknown) => void;
  runInitialTickSync?: boolean;
}

function startIntervalScheduler(args: IntervalSchedulerArgs): IntervalSchedulerHandle {
  let tickCount = 0;

  const handleError = (error: unknown): void => {
    if (args.errorHandler) {
      args.errorHandler(error);

      return;
    }

    logger.error({ error }, args.contextLabel);
  };

  const runTick = (): void => {
    tickCount += 1;

    try {
      const result = args.tickHandler();

      if (result instanceof Promise) {
        result.catch(handleError);
      }
    } catch (error: unknown) {
      handleError(error);
    }

    if (args.heartbeatEveryNTicks !== undefined && args.heartbeatHandler !== undefined && tickCount % args.heartbeatEveryNTicks === 0) {
      try {
        args.heartbeatHandler(tickCount);
      } catch (error: unknown) {
        handleError(error);
      }
    }
  };

  if (args.runInitialTickSync === true) {
    runTick();
  }

  const timer = setInterval(runTick, args.intervalMs);

  return {
    stop: () => {
      clearInterval(timer);
    },
  };
}

export { startIntervalScheduler };
export type { IntervalSchedulerArgs, IntervalSchedulerHandle };

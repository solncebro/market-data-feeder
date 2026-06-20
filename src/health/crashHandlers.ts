import type { FeederLogger } from '../server/feederServer.types.js';

interface CrashHandlerArgs {
  sendAlert: (message: string) => void | Promise<void>;
  logger: FeederLogger;
  exit: (code: number) => void;
  writeStderr: (line: string) => void;
  gracePeriodMs: number;
}

function formatFatalAlert(origin: string, error: Error): string {
  return `🛑 FATAL ${origin}: ${error.message}`;
}

function createCrashHandler(args: CrashHandlerArgs): (error: Error, origin: string) => void {
  let isHandling = false;

  return (error: Error, origin: string): void => {
    if (isHandling) {
      return;
    }

    isHandling = true;
    args.writeStderr(`[Feeder] FATAL ${origin}: ${error.message}`);

    if (error.stack !== undefined) {
      args.writeStderr(error.stack);
    }

    args.logger.error({ error, origin }, `[Feeder] fatal ${origin}`);

    let isExited = false;

    const exitOnce = (): void => {
      if (isExited) {
        return;
      }

      isExited = true;
      args.exit(1);
    };

    Promise.resolve(args.sendAlert(formatFatalAlert(origin, error)))
      .catch(() => undefined)
      .finally(exitOnce);

    setTimeout(exitOnce, args.gracePeriodMs).unref();
  };
}

function installCrashHandlers(args: CrashHandlerArgs): void {
  const handleFatalError = createCrashHandler(args);

  process.on('uncaughtException', (error: Error) => {
    handleFatalError(error, 'uncaughtException');
  });

  process.on('unhandledRejection', (reason: unknown) => {
    const error = reason instanceof Error ? reason : new Error(String(reason));
    handleFatalError(error, 'unhandledRejection');
  });
}

export { createCrashHandler, installCrashHandlers };
export type { CrashHandlerArgs };

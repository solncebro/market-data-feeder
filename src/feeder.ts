import { ExchangeConnector, logger } from '@solncebro/trade-engine';
import { escapeMarkdownV2WithFormatting } from '@solncebro/telegram-engine';

import type { KlineInterval } from './domain/marketData.types.js';
import type { FeederLogger } from './server/feederServer.types.js';
import { loadEnvConfig } from './config/env.js';
import { MarketDataManager } from './source/marketDataManager.js';
import { FeederServer } from './server/feederServer.js';
import { createRestartGuard } from './health/restartGuard.js';
import { createHealthMonitor } from './health/healthMonitor.js';
import { installCrashHandlers } from './health/crashHandlers.js';
import { runWithHardExit } from './utils/hardExit.js';
import { createTelegramControlBot } from './telegram/telegramControlBot.js';

const HEALTH_ALERT_DEDUP_MS = 300_000;
const HEALTH_RECOVERY_GRACE_MS = 120_000;
const HEALTH_BATCH_FLUSH_MS = 3000;
const RESTART_WINDOW_MS = 30 * 60_000;
const MAX_AUTO_RESTARTS = 3;
const CRASH_ALERT_GRACE_MS = 3000;
const SHUTDOWN_HARD_EXIT_MS = 15_000;
// On-disk log of auto-restart moments — survives a process restart so the loop guard
// (at most MAX_AUTO_RESTARTS within RESTART_WINDOW_MS) keeps working across restarts.
const RESTART_STATE_FILE = './feeder-restart-log.json';

const feederLogger: FeederLogger = {
  info: (payload, message) => logger.info(payload, message),
  warn: (payload, message) => logger.warn(payload, message),
  error: (payload, message) => logger.error(payload, message),
};

async function main(): Promise<void> {
  const envConfig = loadEnvConfig();

  // Late-bound: the alert channel lives on the bot, which needs the server, which needs the
  // health monitor — so the monitor is created first with a wrapper that resolves once the bot is up.
  let stopBot = async (): Promise<void> => undefined;
  let sendAlert = async (message: string): Promise<void> => {
    logger.warn({ message }, '[Feeder] health alert raised before bot was ready');
  };

  installCrashHandlers({
    sendAlert: (message) => sendAlert(escapeMarkdownV2WithFormatting(message)),
    logger: feederLogger,
    exit: (code) => process.exit(code),
    writeStderr: (line) => process.stderr.write(`${line}\n`),
    gracePeriodMs: CRASH_ALERT_GRACE_MS,
  });

  const restartGuard = createRestartGuard({
    filePath: RESTART_STATE_FILE,
    windowMs: RESTART_WINDOW_MS,
    maxRestarts: MAX_AUTO_RESTARTS,
    logger: feederLogger,
  });

  const shutdown = async (reason?: string, exitCode: number = 0): Promise<void> => {
    logger.info({ reason, exitCode }, '[Feeder] Shutting down');

    await runWithHardExit(
      async () => {
        await healthMonitor.shutdown();
        await stopBot();
        await server.shutdown();
        await exchangeConnector.disconnect();
      },
      SHUTDOWN_HARD_EXIT_MS,
      () => {
        process.stderr.write('[Feeder] shutdown timed out — forcing exit\n');
        process.exit(1);
      },
    );

    process.exit(exitCode);
  };

  const healthMonitor = createHealthMonitor({
    config: {
      alertDedupMs: HEALTH_ALERT_DEDUP_MS,
      recoveryGraceMs: HEALTH_RECOVERY_GRACE_MS,
      batchFlushMs: HEALTH_BATCH_FLUSH_MS,
    },
    sendAlert: (message) => sendAlert(message),
    restartGuard,
    onRestart: (reason) => {
      void shutdown(reason, 1).catch((error: unknown) => {
        logger.error({ error }, '[Feeder] auto-restart shutdown failed');
        process.exit(1);
      });
    },
    logger: feederLogger,
  });

  const exchangeConnector = new ExchangeConnector(
    envConfig.exchangeName,
    { apiKey: envConfig.exchangeApiKey, secret: envConfig.exchangeSecret },
    (message: string) => healthMonitor.report({ kind: 'transportNotify', message }),
    undefined,
    {
      onStreamStale: (event) => healthMonitor.report({ kind: 'klineStreamStale', interval: event.interval as KlineInterval, symbol: event.symbol, ageMs: event.ageMs }),
      onStreamRecovered: (event) => healthMonitor.report({ kind: 'klineStreamRecovered', interval: event.interval as KlineInterval, symbol: event.symbol }),
      onStreamRecoveryFailed: (event) => healthMonitor.report({ kind: 'klineStreamRecoveryFailed', interval: event.interval as KlineInterval, symbol: event.symbol, consecutiveFailCount: event.consecutiveFailCount }),
    },
  );

  await exchangeConnector.initialize();

  const server = new FeederServer({
    port: envConfig.port,
    host: envConfig.host,
    logger: feederLogger,
    createSource: (interval: KlineInterval) => new MarketDataManager(exchangeConnector, interval),
    onHealthEvent: (event) => healthMonitor.report(event),
  });

  await server.start();

  logger.info({ exchange: envConfig.exchangeName, port: server.getPort() }, `[Feeder] Market data feeder ready for ${envConfig.exchangeName} on ${envConfig.host}:${server.getPort()}`);

  const controlBot = createTelegramControlBot({
    botToken: envConfig.telegramBotToken,
    allowedChatIdList: envConfig.telegramAllowedChatIdList,
    statusProvider: server,
    exchangeName: envConfig.exchangeName,
    logger: feederLogger,
    onReboot: shutdown,
  });

  stopBot = controlBot.stop;
  sendAlert = controlBot.sendAlert;

  await controlBot.start();

  const readyStatus = server.getStatus();
  healthMonitor.report({ kind: 'feederReady', exchangeName: envConfig.exchangeName, host: readyStatus.host, port: readyStatus.port });

  process.on('SIGINT', () => {
    shutdown().catch((error: unknown) => {
      logger.error({ error }, '[Feeder] shutdown failed');
      process.exit(1);
    });
  });

  process.on('SIGTERM', () => {
    shutdown().catch((error: unknown) => {
      logger.error({ error }, '[Feeder] shutdown failed');
      process.exit(1);
    });
  });
}

main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);

  // Synchronous stderr write FIRST: the pino logger runs its transports in a worker thread, so a
  // logger.error() immediately followed by process.exit(1) is lost — the worker never flushes.
  // console.error writes to fd 2 synchronously, so a fatal startup error (e.g. a missing env var
  // like TELEGRAM_BOT_TOKEN) is always visible in the console instead of the process dying silently.
  console.error(`[Feeder] FATAL STARTUP ERROR: ${message}`);

  if (error instanceof Error && error.stack !== undefined) {
    console.error(error.stack);
  }

  logger.error({ error }, '[Feeder] fatal startup error');
  process.exit(1);
});

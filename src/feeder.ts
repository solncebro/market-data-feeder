import { ExchangeConnector, logger } from '@solncebro/trade-engine';

import type { KlineInterval } from './domain/marketData.types.js';
import type { FeederLogger } from './server/feederServer.types.js';
import { loadEnvConfig } from './config/env.js';
import { MarketDataManager } from './source/marketDataManager.js';
import { FeederServer } from './server/feederServer.js';

const feederLogger: FeederLogger = {
  info: (payload, message) => logger.info(payload, message),
  warn: (payload, message) => logger.warn(payload, message),
  error: (payload, message) => logger.error(payload, message),
};

async function main(): Promise<void> {
  const envConfig = loadEnvConfig();

  const exchangeConnector = new ExchangeConnector(envConfig.exchangeName, {
    apiKey: envConfig.exchangeApiKey,
    secret: envConfig.exchangeSecret,
  });

  await exchangeConnector.initialize();

  const server = new FeederServer({
    port: envConfig.port,
    host: envConfig.host,
    logger: feederLogger,
    createSource: (interval: KlineInterval) => new MarketDataManager(exchangeConnector, interval),
  });

  await server.start();

  logger.info({ exchange: envConfig.exchangeName, port: server.getPort() }, `[Feeder] Market data feeder ready for ${envConfig.exchangeName} on ${envConfig.host}:${server.getPort()}`);

  const shutdown = async (): Promise<void> => {
    logger.info({}, '[Feeder] Shutting down');

    await server.shutdown();
    await exchangeConnector.disconnect();
    process.exit(0);
  };

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
  logger.error({ error }, '[Feeder] fatal startup error');
  process.exit(1);
});

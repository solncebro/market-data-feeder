import 'dotenv/config';

import { ExchangeNameEnum } from '@solncebro/trade-engine';

import type { EnvConfig } from './env.types.js';

const DEFAULT_PORT = 7070;
const DEFAULT_HOST = '127.0.0.1';

function requireEnv(name: string): string {
  const value = process.env[name];

  if (value === undefined || value === '') {
    throw new Error(`Missing required env var: ${name}`);
  }

  return value;
}

function resolveExchangeName(raw: string): ExchangeNameEnum {
  if (raw === ExchangeNameEnum.Binance) {
    return ExchangeNameEnum.Binance;
  }

  if (raw === ExchangeNameEnum.Bybit) {
    return ExchangeNameEnum.Bybit;
  }

  throw new Error(`Invalid EXCHANGE_NAME "${raw}" — expected one of: ${ExchangeNameEnum.Binance}, ${ExchangeNameEnum.Bybit}`);
}

function resolvePort(): number {
  const raw = process.env.MARKET_DATA_FEEDER_PORT;

  if (raw === undefined || raw === '') {
    return DEFAULT_PORT;
  }

  const parsed = Number(raw);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Invalid MARKET_DATA_FEEDER_PORT "${raw}" — expected a positive integer`);
  }

  return parsed;
}

function loadEnvConfig(): EnvConfig {
  return {
    exchangeName: resolveExchangeName(requireEnv('EXCHANGE_NAME')),
    exchangeApiKey: requireEnv('EXCHANGE_API_KEY'),
    exchangeSecret: requireEnv('EXCHANGE_SECRET'),
    port: resolvePort(),
    host: process.env.MARKET_DATA_FEEDER_HOST ?? DEFAULT_HOST,
  };
}

export { loadEnvConfig };

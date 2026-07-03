import 'dotenv/config';

import { ExchangeNameEnum } from '@solncebro/trade-engine';

import type { EnvConfig } from './env.types.js';

const DEFAULT_PORT = 7070;
const DEFAULT_HOST = '127.0.0.1';

// Values are trimmed and whitespace-only counts as missing: a trailing space smuggled in by a shell
// or a systemd unit (e.g. in EXCHANGE_NAME) must not fail validation with a confusing message.
function readEnv(name: string): string | undefined {
  const raw = process.env[name];

  if (raw === undefined) {
    return undefined;
  }

  const trimmed = raw.trim();

  return trimmed === '' ? undefined : trimmed;
}

function requireEnv(name: string): string {
  const value = readEnv(name);

  if (value === undefined) {
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
  const raw = readEnv('MARKET_DATA_FEEDER_PORT');

  if (raw === undefined) {
    return DEFAULT_PORT;
  }

  const parsed = Number(raw);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Invalid MARKET_DATA_FEEDER_PORT "${raw}" — expected a positive integer`);
  }

  return parsed;
}

function resolveAllowedChatIdList(): string[] {
  const raw = requireEnv('TELEGRAM_ALLOWED_CHAT_IDS');
  const chatIdList = raw
    .split(',')
    .map((chatId) => chatId.trim())
    .filter((chatId) => chatId !== '');

  if (chatIdList.length === 0) {
    throw new Error('Invalid TELEGRAM_ALLOWED_CHAT_IDS — expected a comma-separated list of at least one chat id');
  }

  return chatIdList;
}

function loadEnvConfig(): EnvConfig {
  return {
    exchangeName: resolveExchangeName(requireEnv('EXCHANGE_NAME')),
    exchangeApiKey: requireEnv('EXCHANGE_API_KEY'),
    exchangeSecret: requireEnv('EXCHANGE_SECRET'),
    port: resolvePort(),
    host: readEnv('MARKET_DATA_FEEDER_HOST') ?? DEFAULT_HOST,
    telegramBotToken: requireEnv('TELEGRAM_BOT_TOKEN'),
    telegramAllowedChatIdList: resolveAllowedChatIdList(),
  };
}

export { loadEnvConfig };

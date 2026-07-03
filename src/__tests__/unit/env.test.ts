import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { loadEnvConfig } from '../../config/env.js';

const REQUIRED_ENV = {
  EXCHANGE_NAME: 'bybit',
  EXCHANGE_API_KEY: 'key',
  EXCHANGE_SECRET: 'secret',
  TELEGRAM_BOT_TOKEN: 'token',
  TELEGRAM_ALLOWED_CHAT_IDS: '1',
};

const ENV_KEY_LIST = [...Object.keys(REQUIRED_ENV), 'MARKET_DATA_FEEDER_PORT', 'MARKET_DATA_FEEDER_HOST'];

let savedEnv: Record<string, string | undefined> = {};

describe('loadEnvConfig', () => {
  beforeEach(() => {
    savedEnv = {};

    for (const key of ENV_KEY_LIST) {
      savedEnv[key] = process.env[key];
      delete process.env[key];
    }

    Object.assign(process.env, REQUIRED_ENV);
  });

  afterEach(() => {
    for (const key of ENV_KEY_LIST) {
      if (savedEnv[key] === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = savedEnv[key];
      }
    }
  });

  it('trims surrounding whitespace in values (a trailing space in EXCHANGE_NAME must not crash the feeder)', () => {
    process.env.EXCHANGE_NAME = ' bybit ';

    expect(loadEnvConfig().exchangeName).toBe('bybit');
  });

  it('treats a whitespace-only required value as missing', () => {
    process.env.TELEGRAM_BOT_TOKEN = '   ';

    expect(() => loadEnvConfig()).toThrow(/TELEGRAM_BOT_TOKEN/);
  });

  it('loads a plain valid configuration', () => {
    const config = loadEnvConfig();

    expect(config.exchangeName).toBe('bybit');
    expect(config.port).toBe(7070);
    expect(config.telegramAllowedChatIdList).toEqual(['1']);
  });
});

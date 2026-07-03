import { describe, expect, it } from 'vitest';

import { createTelegramControlBot } from '../../telegram/telegramControlBot.js';
import type { FeederStatusProvider } from '../../telegram/telegramControlBot.types.js';

const noopLogger = { info: () => undefined, warn: () => undefined, error: () => undefined };

const statusProvider: FeederStatusProvider = {
  getStatus: () => ({ host: '127.0.0.1', port: 7070, clientCount: 0, uptimeMs: 0, intervalStatusList: [] }),
  getStaleSymbolList: () => [],
  getSymbolDiagnostics: () => null,
};

describe('createTelegramControlBot', () => {
  it('throws when the allowed chat-id whitelist is empty (fail-closed)', () => {
    expect(() => createTelegramControlBot({
      botToken: 'test-token',
      allowedChatIdList: [],
      statusProvider,
      exchangeName: 'bybit',
      logger: noopLogger,
      onReboot: async () => undefined,
    })).toThrow();
  });

  it('stop resolves even when polling never started (Telegraf throws "Bot is not running!")', async () => {
    const bot = createTelegramControlBot({
      botToken: 'test-token',
      allowedChatIdList: ['1'],
      statusProvider,
      exchangeName: 'bybit',
      logger: noopLogger,
      onReboot: async () => undefined,
    });

    await expect(bot.stop()).resolves.toBeUndefined();
  });
});

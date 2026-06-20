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
});

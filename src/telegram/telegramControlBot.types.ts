import type { KlineInterval, StaleSymbolInfo } from '../domain/marketData.types.js';
import type { FeederLogger, FeederStatus, SymbolDiagnostics } from '../server/feederServer.types.js';

interface FeederStatusProvider {
  getStatus: () => FeederStatus;
  getStaleSymbolList: (interval: KlineInterval, limit: number) => StaleSymbolInfo[];
  getSymbolDiagnostics: (symbol: string, interval?: KlineInterval) => SymbolDiagnostics | null;
}

interface TelegramControlBotArgs {
  botToken: string;
  allowedChatIdList: string[];
  statusProvider: FeederStatusProvider;
  exchangeName: string;
  logger: FeederLogger;
  onReboot: () => Promise<void> | void;
}

interface TelegramControlBot {
  start: () => Promise<void>;
  stop: () => Promise<void>;
  sendAlert: (message: string) => Promise<void>;
}

type InputAction = 'symbolLookup';

export type { FeederStatusProvider, InputAction, TelegramControlBot, TelegramControlBotArgs };

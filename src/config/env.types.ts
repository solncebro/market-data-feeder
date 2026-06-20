import type { ExchangeNameEnum } from '@solncebro/trade-engine';

interface EnvConfig {
  exchangeName: ExchangeNameEnum;
  exchangeApiKey: string;
  exchangeSecret: string;
  port: number;
  host: string;
  telegramBotToken: string;
  telegramAllowedChatIdList: string[];
}

export type { EnvConfig };

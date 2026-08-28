import { normalizeSymbol } from '@solncebro/trade-engine';
import { Markup } from '@solncebro/telegram-engine';
import type { Context, InlineKeyboard } from '@solncebro/telegram-engine';
import {
  createBotRegistry,
  createBroadcaster,
  createCallbackEncoder,
  createInputStateManager,
  createKeyboardBuilder,
  createMenuRouter,
  escapeMarkdownV2WithFormatting,
  formatClickableText,
  registerBotCommands,
} from '@solncebro/telegram-engine';

import type { KlineInterval } from '@solncebro/market-data-feeder-lib';
import { formatIntervalButtonLabel, formatIntervalDetailMessage, formatOverviewMessage, formatStaleSymbolMessage, formatSymbolCard } from './statusFormatter.js';
import { MenuStep } from './menu.types.js';
import type { CallbackData } from './menu.types.js';
import { startChannelConnectivityMonitor } from './channelConnectivityMonitor.js';
import type { ChannelConnectivityMonitor } from './channelConnectivityMonitor.js';
import { createResilientStarter } from '../utils/resilientStarter.js';
import type { InputAction, TelegramControlBot, TelegramControlBotArgs } from './telegramControlBot.types.js';

const BENIGN_EDIT_ERROR_MARKER_LIST = [
  'message is not modified',
  'message to edit not found',
  'message to delete not found',
  "message can't be deleted",
  'query is too old',
  'MESSAGE_ID_INVALID',
];

function isBenignTelegramEditError(error: unknown): boolean {
  return error instanceof Error && BENIGN_EDIT_ERROR_MARKER_LIST.some((marker) => error.message.includes(marker));
}

const BOT_NAME = 'market-data-feeder';
const STALE_SYMBOL_DISPLAY_LIMIT = 15;
const CHANNEL_PROBE_RETRY_MS = 15_000;
const CHANNEL_PROBE_RECHECK_MS = 60_000;
const BOT_START_RETRY_MS = 60_000;

const BUTTON_OVERVIEW = '🛰️ Overview';
const BUTTON_WEBSOCKETS = '🔌 Websockets';
const BUTTON_STALE = '🕯️ Stale symbols';
const BUTTON_SYMBOL = '🔎 Symbol info';
const BUTTON_RESTART = '🔄 Restart';
const BUTTON_CLOSE = '✖️ Close menu';

const WELCOME_MESSAGE = '👋 Welcome to the market data feeder.\n\nThis is your control panel. Use the buttons below to check what is going on.';
const SYMBOL_PROMPT_MESSAGE = '🔎 Send a symbol to inspect, for example `BTCUSDT`.\n\nSend /menu to cancel.';
const MENU_CLOSED_MESSAGE = 'Menu closed. Send /menu to reopen.';

function resolveSymbolInput(rawText: string): string | null {
  const cleaned = normalizeSymbol(rawText).toUpperCase().replace(/[^A-Z0-9]/g, '');

  if (cleaned.length === 0) {
    return null;
  }

  return cleaned.endsWith('USDT') ? cleaned : `${cleaned}USDT`;
}

function createTelegramControlBot(args: TelegramControlBotArgs): TelegramControlBot {
  const { allowedChatIdList, botToken, exchangeName, logger, onReboot, statusProvider } = args;

  if (allowedChatIdList.length === 0) {
    throw new Error('Telegram allowedChatIdList is empty — refusing to start a bot that would accept every chat (fail-closed)');
  }

  const encoder = createCallbackEncoder<CallbackData>([
    { key: 'step', shortCode: 's', encode: String, decode: (value) => String(value) as MenuStep },
    { key: 'interval', shortCode: 'i', encode: String, decode: (value) => String(value) as KlineInterval },
  ]);
  const keyboardBuilder = createKeyboardBuilder(encoder);
  const inputStateManager = createInputStateManager<InputAction, CallbackData>();

  const mainReplyKeyboard = Markup.keyboard([
    [BUTTON_OVERVIEW, BUTTON_WEBSOCKETS, BUTTON_STALE],
    [BUTTON_SYMBOL, BUTTON_RESTART, BUTTON_CLOSE],
  ])
    .resize()
    .persistent();

  const menuRouter = createMenuRouter<MenuStep, CallbackData>({
    [MenuStep.Overview]: () => ({
      messageList: [formatOverviewMessage(statusProvider.getStatus(), exchangeName)],
      keyboard: keyboardBuilder.build([{ text: '🔄 Refresh', callbackData: { step: MenuStep.Overview } }]),
    }),
    [MenuStep.Websockets]: () => {
      const status = statusProvider.getStatus();

      if (status.intervalStatusList.length === 0) {
        return {
          messageList: ['🔌 Websockets\n\nNo active websockets yet. Nobody is subscribed.'],
          keyboard: keyboardBuilder.build([{ text: '🔄 Refresh', callbackData: { step: MenuStep.Websockets } }]),
        };
      }

      const buttonList: Array<{ text: string; callbackData: Partial<CallbackData> }> = status.intervalStatusList.map((intervalStatus) => ({
        text: formatIntervalButtonLabel(intervalStatus),
        callbackData: { step: MenuStep.IntervalDetail, interval: intervalStatus.interval },
      }));
      buttonList.push({ text: '🔄 Refresh', callbackData: { step: MenuStep.Websockets } });

      return {
        messageList: ['🔌 Websockets\n\nPick an interval to inspect.'],
        keyboard: keyboardBuilder.build(buttonList),
      };
    },
    [MenuStep.IntervalDetail]: (data) => {
      const interval = data.interval;

      if (interval === undefined) {
        return {
          messageList: ['No interval selected.'],
          keyboard: keyboardBuilder.build([{ text: '◀️ Back', callbackData: { step: MenuStep.Websockets } }]),
        };
      }

      const intervalStatus = statusProvider.getStatus().intervalStatusList.find((item) => item.interval === interval);

      if (intervalStatus === undefined) {
        return {
          messageList: [`Interval ${interval} is no longer active.`],
          keyboard: keyboardBuilder.build([{ text: '◀️ Back', callbackData: { step: MenuStep.Websockets } }]),
        };
      }

      return {
        messageList: [formatIntervalDetailMessage(intervalStatus, exchangeName)],
        keyboard: keyboardBuilder.build([
          { text: '🔄 Refresh', callbackData: { step: MenuStep.IntervalDetail, interval } },
          { text: '🕯️ Stale list', callbackData: { step: MenuStep.Stale, interval } },
          { text: '◀️ Back', callbackData: { step: MenuStep.Websockets } },
        ]),
      };
    },
    [MenuStep.StaleIntervals]: () => {
      const status = statusProvider.getStatus();

      if (status.intervalStatusList.length === 0) {
        return {
          messageList: ['🕯️ Stale symbols\n\nNo active websockets, so nothing to show.'],
          keyboard: keyboardBuilder.build([{ text: '🔄 Refresh', callbackData: { step: MenuStep.StaleIntervals } }]),
        };
      }

      const buttonList = status.intervalStatusList.map((intervalStatus) => ({
        text: `${intervalStatus.interval} (stale ${intervalStatus.staleCount})`,
        callbackData: { step: MenuStep.Stale, interval: intervalStatus.interval },
      }));

      return {
        messageList: ['🕯️ Stale symbols\n\nPick an interval.'],
        keyboard: keyboardBuilder.build(buttonList),
      };
    },
    [MenuStep.Stale]: (data) => {
      const interval = data.interval;

      if (interval === undefined) {
        return {
          messageList: ['No interval selected.'],
          keyboard: keyboardBuilder.build([{ text: '◀️ Back', callbackData: { step: MenuStep.StaleIntervals } }]),
        };
      }

      const staleSymbolList = statusProvider.getStaleSymbolList(interval, STALE_SYMBOL_DISPLAY_LIMIT);

      return {
        messageList: [formatStaleSymbolMessage(interval, staleSymbolList)],
        keyboard: keyboardBuilder.build([
          { text: '🔄 Refresh', callbackData: { step: MenuStep.Stale, interval } },
          { text: '◀️ Back', callbackData: { step: MenuStep.StaleIntervals } },
        ]),
      };
    },
  });

  const registry = createBotRegistry({
    allowedPeerList: allowedChatIdList,
    onLog: (message, payload) => logger.info(payload ?? {}, `[TelegramBot] ${message}`),
  });
  // Late-bound: the resilient starter is created below, but launch failures (polling death) must be
  // able to request a retry — createBot's onError is the only place they surface.
  let requestBotRestart: (() => void) | null = null;
  const instance = registry.register({
    botToken,
    botName: BOT_NAME,
    onError: (error) => {
      logger.error({ error }, '[TelegramBot] bot polling failed — a background relaunch will be scheduled');
      requestBotRestart?.();
    },
  });

  const sender = registry.createSender(BOT_NAME);
  // onLog is mandatory here: createBroadcaster swallows every per-recipient send error internally and
  // only surfaces it through onLog. Without it a failed alert (e.g. the startup readiness greeting) is
  // lost silently — exactly the kind of failure that must never be invisible.
  const broadcaster = sender !== undefined
    ? createBroadcaster({
      sender,
      recipientList: allowedChatIdList,
      onLog: (message, payload) => logger.error(payload ?? {}, `[TelegramBot] ${message}`),
    })
    : null;

  const sendAlert = async (message: string): Promise<void> => {
    if (broadcaster === null) {
      logger.error({}, '[TelegramBot] cannot send alert — sender unavailable');

      return;
    }

    await broadcaster.sendToAll(message, true);
  };

  const editScreen = async (context: Context, text: string, keyboard?: InlineKeyboard): Promise<void> => {
    const extra = keyboard !== undefined
      ? { parse_mode: 'MarkdownV2' as const, reply_markup: keyboard.reply_markup }
      : { parse_mode: 'MarkdownV2' as const };

    try {
      await context.editMessageText(escapeMarkdownV2WithFormatting(text), extra);
    } catch (error: unknown) {
      if (!isBenignTelegramEditError(error)) {
        throw error;
      }
    }
  };

  const sendStep = async (context: Context, step: MenuStep, data: Partial<CallbackData>, mode: 'reply' | 'edit'): Promise<void> => {
    const result = await menuRouter.handleStep(step, data);
    const text = result.messageList.join('\n\n');

    if (mode === 'edit') {
      await editScreen(context, text, result.keyboard);

      return;
    }

    await context.reply(escapeMarkdownV2WithFormatting(text), { parse_mode: 'MarkdownV2', reply_markup: result.keyboard.reply_markup });
  };

  const showMainMenu = async (context: Context): Promise<void> => {
    await context.reply(escapeMarkdownV2WithFormatting(WELCOME_MESSAGE), { parse_mode: 'MarkdownV2', reply_markup: mainReplyKeyboard.reply_markup });
  };

  const promptSymbolLookup = async (context: Context): Promise<void> => {
    const chatId = context.chat?.id;

    if (chatId === undefined) {
      return;
    }

    inputStateManager.set(String(chatId), { action: 'symbolLookup' });
    await context.reply(escapeMarkdownV2WithFormatting(SYMBOL_PROMPT_MESSAGE), { parse_mode: 'MarkdownV2' });
  };

  const closeMenu = async (context: Context): Promise<void> => {
    await context.reply(escapeMarkdownV2WithFormatting(MENU_CLOSED_MESSAGE), { parse_mode: 'MarkdownV2', reply_markup: Markup.removeKeyboard().reply_markup });
  };

  const triggerRestart = async (context: Context): Promise<void> => {
    logger.warn({ chatId: context.chat?.id }, '[TelegramBot] restart requested via control bot');
    await context.reply(escapeMarkdownV2WithFormatting('🔄 Restarting… the supervisor will bring me back.'), { parse_mode: 'MarkdownV2' });
    void Promise.resolve(onReboot()).catch((error: unknown) => {
      logger.error({ error }, '[TelegramBot] restart failed');
    });
  };

  const guard = (handler: (context: Context) => Promise<void>, keepInput = false) => async (context: Context): Promise<void> => {
    const chatId = context.chat?.id;

    if (chatId === undefined || !registry.accessControl.isAllowedPeer(String(chatId))) {
      return;
    }

    if (!keepInput) {
      inputStateManager.delete(String(chatId));
    }

    try {
      await handler(context);
    } catch (error: unknown) {
      logger.error({ error }, '[TelegramBot] reply handler failed');
    }
  };

  const messageHandler = async (context: Context): Promise<void> => {
    const chatId = context.chat?.id;

    if (chatId === undefined) {
      return;
    }

    const state = inputStateManager.get(String(chatId));

    if (state === undefined || state.action !== 'symbolLookup') {
      return;
    }

    const message = context.message;

    if (message === undefined || !('text' in message)) {
      return;
    }

    const rawText = message.text.trim();
    inputStateManager.delete(String(chatId));

    if (rawText.startsWith('/')) {
      return;
    }

    const symbol = resolveSymbolInput(rawText);

    if (symbol === null) {
      await context.reply(escapeMarkdownV2WithFormatting('That does not look like a symbol. Send something like BTCUSDT.'), { parse_mode: 'MarkdownV2' });

      return;
    }

    const diagnostics = statusProvider.getSymbolDiagnostics(symbol);

    if (diagnostics === null) {
      await context.reply(escapeMarkdownV2WithFormatting(`${formatClickableText(symbol)} is not loaded on any active interval.`), { parse_mode: 'MarkdownV2' });

      return;
    }

    await context.reply(escapeMarkdownV2WithFormatting(formatSymbolCard(diagnostics)), { parse_mode: 'MarkdownV2' });
  };

  const callbackQueryHandler = async (context: Context): Promise<void> => {
    const callbackQuery = context.callbackQuery;

    if (callbackQuery === undefined || !('data' in callbackQuery)) {
      return;
    }

    const data = encoder.decode(callbackQuery.data);

    if (data === null) {
      return;
    }

    await context.answerCbQuery().catch(() => undefined);

    const step = data.step;

    if (step === undefined) {
      return;
    }

    await sendStep(context, step, data, 'edit');
  };

  let connectivityMonitor: ChannelConnectivityMonitor | null = null;
  let isHandlersRegistered = false;
  let isCommandsRegistered = false;
  let isBotStopped = false;

  const registerReplyHandlers = (): void => {
    instance.bot.start(guard(showMainMenu));
    instance.bot.hears(BUTTON_OVERVIEW, guard((context) => sendStep(context, MenuStep.Overview, {}, 'reply')));
    instance.bot.hears(BUTTON_WEBSOCKETS, guard((context) => sendStep(context, MenuStep.Websockets, {}, 'reply')));
    instance.bot.hears(BUTTON_STALE, guard((context) => sendStep(context, MenuStep.StaleIntervals, {}, 'reply')));
    instance.bot.hears(BUTTON_SYMBOL, guard(promptSymbolLookup, true));
    instance.bot.hears(BUTTON_RESTART, guard(triggerRestart));
    instance.bot.hears(BUTTON_CLOSE, guard(closeMenu));
  };

  // Network phase, retried in the background by the resilient starter. Safe to re-run: the command
  // registration performs its network call (setMyCommands) BEFORE wiring any Telegraf middleware, so
  // a rejected attempt wires nothing and the isCommandsRegistered flag guards the wired-once success.
  const attemptNetworkStart = async (): Promise<void> => {
    // A hung network attempt can settle AFTER stop() — it must not relaunch polling then.
    if (isBotStopped) {
      return;
    }

    if (!isCommandsRegistered) {
      await registerBotCommands({
        bot: instance.bot,
        accessControl: registry.accessControl,
        commandConfigList: [
          {
            command: 'menu',
            description: 'Open the control menu',
            handler: guard(showMainMenu),
          },
          {
            command: 'status',
            description: 'Show feeder status',
            handler: guard(async (context) => {
              await context.reply(escapeMarkdownV2WithFormatting(formatOverviewMessage(statusProvider.getStatus(), exchangeName)), { parse_mode: 'MarkdownV2' });
            }),
          },
        ],
        callbackQueryHandler,
        messageHandler,
        onError: (message, payload) => logger.error(payload ?? {}, `[TelegramBot] ${message}`),
      });
      isCommandsRegistered = true;
    }

    if (isBotStopped) {
      return;
    }

    // Long-lived promise (resolves only when polling stops); launch failures surface via the
    // registered onError above, which schedules a background relaunch.
    registry.launchAll().catch((error: unknown) => {
      logger.error({ error }, '[TelegramBot] launch failed');
    });

    logger.info({ allowedChatCount: allowedChatIdList.length }, '[TelegramBot] control bot launched');
  };

  const starter = createResilientStarter({
    attempt: attemptNetworkStart,
    retryDelayMs: BOT_START_RETRY_MS,
    label: 'telegram control bot',
    logger,
  });
  requestBotRestart = () => starter.requestRetry();

  const start = async (): Promise<void> => {
    if (!isHandlersRegistered) {
      registerReplyHandlers();
      isHandlersRegistered = true;
    }

    if (connectivityMonitor === null) {
      connectivityMonitor = startChannelConnectivityMonitor({
        probe: async () => {
          await instance.bot.telegram.getMe();
        },
        logger,
        retryDelayMs: CHANNEL_PROBE_RETRY_MS,
        recheckIntervalMs: CHANNEL_PROBE_RECHECK_MS,
      });
    }

    // Never rejects: a Telegram outage must not take the feeder down with it.
    await starter.start();
  };

  const stop = async (): Promise<void> => {
    isBotStopped = true;
    starter.stop();
    connectivityMonitor?.stop();
    connectivityMonitor = null;

    try {
      await registry.stopAll('shutdown');
    } catch (error: unknown) {
      logger.warn({ error }, '[TelegramBot] stop skipped — polling was not running');
    }
  };

  return { start, stop, sendAlert };
}

export { createTelegramControlBot };

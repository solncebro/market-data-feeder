import { formatClickableText } from '@solncebro/telegram-engine';

import type { KlineInterval, MaValues, StaleSymbolInfo } from '../domain/marketData.types.js';
import type { FeederStatus, IntervalStatus, SymbolDiagnostics } from '../server/feederServer.types.js';

type HealthIndicator = '🟢' | '🟡' | '🔴' | '⚪';

const STALE_CRITICAL_PERCENT = 10;

function formatDuration(ms: number): string {
  if (ms <= 0) {
    return '0s';
  }

  const totalSeconds = Math.floor(ms / 1000);
  const days = Math.floor(totalSeconds / 86_400);
  const hours = Math.floor((totalSeconds % 86_400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  const partList: string[] = [];

  if (days > 0) {
    partList.push(`${days}d`);
  }

  if (hours > 0) {
    partList.push(`${hours}h`);
  }

  if (minutes > 0) {
    partList.push(`${minutes}m`);
  }

  if (partList.length === 0) {
    partList.push(`${seconds}s`);
  }

  return partList.join(' ');
}

function formatAge(timestampMs: number | undefined): string {
  if (timestampMs === undefined || timestampMs <= 0) {
    return 'never';
  }

  return `${formatDuration(Date.now() - timestampMs)} ago`;
}

function formatNumber(value: number): string {
  // toLocaleString caps at 3 fraction digits, which renders tiny meme-perp prices as a misleading
  // "0" — give small values enough dynamic precision instead.
  if (value !== 0 && Math.abs(value) < 0.001) {
    const fractionDigitCount = Math.min(12, Math.ceil(-Math.log10(Math.abs(value))) + 3);

    return value.toFixed(fractionDigitCount);
  }

  return value.toLocaleString('en-US');
}

function formatPrice(value: number | null): string {
  return value === null ? 'n/a' : formatNumber(value);
}

function formatVolume(value: number | null): string {
  return value === null ? 'n/a' : formatNumber(Math.round(value));
}

function resolveIntervalHealth(intervalStatus: IntervalStatus): HealthIndicator {
  if (intervalStatus.symbolCount === 0) {
    return '⚪';
  }

  if (intervalStatus.staleCount === 0) {
    return '🟢';
  }

  const stalePercent = (intervalStatus.staleCount / intervalStatus.symbolCount) * 100;

  return stalePercent > STALE_CRITICAL_PERCENT ? '🔴' : '🟡';
}

function formatLoadMode(intervalStatus: IntervalStatus): string {
  return intervalStatus.isAllLoaded ? 'all loaded' : 'on-demand';
}

function formatStreamState(intervalStatus: IntervalStatus): string {
  const { isStreamSilent, silenceMs } = intervalStatus.liveness;

  return isStreamSilent ? `🔴 silent for ${formatDuration(silenceMs)}` : '🟢 live';
}

function formatIntervalLine(intervalStatus: IntervalStatus): string {
  const health = resolveIntervalHealth(intervalStatus);

  return `${health} ${intervalStatus.interval} · ${formatLoadMode(intervalStatus)} · ${intervalStatus.symbolCount} symbols · ${intervalStatus.staleCount} stale`;
}

function formatIntervalButtonLabel(intervalStatus: IntervalStatus): string {
  const health = resolveIntervalHealth(intervalStatus);
  const liveLabel = intervalStatus.liveness.isStreamSilent ? 'silent' : 'live';

  return `${health} ${intervalStatus.interval} · ${liveLabel} · ${intervalStatus.symbolCount} sym`;
}

function formatOverviewMessage(status: FeederStatus, exchangeName: string): string {
  const lineList: string[] = [
    `🛰️ ${exchangeName.toUpperCase()} feeder overview`,
    `Address: ${status.host}:${status.port}`,
    `Uptime: ${formatDuration(status.uptimeMs)}`,
    `Clients connected: ${status.clientCount}`,
    '',
  ];

  if (status.intervalStatusList.length === 0) {
    lineList.push('No active websockets (nobody is subscribed yet).');

    return lineList.join('\n');
  }

  lineList.push(`Websockets (intervals): ${status.intervalStatusList.length}`);

  for (const intervalStatus of status.intervalStatusList) {
    lineList.push(formatIntervalLine(intervalStatus));
  }

  return lineList.join('\n');
}

function formatIntervalDetailMessage(intervalStatus: IntervalStatus, exchangeName: string): string {
  const subscribersLabel = `all×${intervalStatus.allSubscriberCount}, on-demand ${intervalStatus.refSymbolCount}`;

  return [
    `🔌 ${intervalStatus.interval} interval · ${exchangeName.toUpperCase()}`,
    `Load mode: ${formatLoadMode(intervalStatus)}`,
    `Stream: ${formatStreamState(intervalStatus)}`,
    `Last inbound: ${formatAge(intervalStatus.liveness.lastInboundAtMs)}`,
    `Subscribers: ${subscribersLabel}`,
    `Subscriptions (symbols): ${intervalStatus.subscriptionCount}`,
    `Symbols loaded: ${intervalStatus.symbolCount}`,
    `Klines buffered: ${formatNumber(intervalStatus.klineCount)}`,
    `Fresh symbols: ${intervalStatus.freshCount}`,
    `Stale symbols: ${intervalStatus.staleCount}`,
    `Persistent-stale symbols: ${intervalStatus.persistentStaleCount}`,
  ].join('\n');
}

function formatMaValues(maValues: MaValues): string {
  return `${formatPrice(maValues.ma25)} / ${formatPrice(maValues.ma50)} / ${formatPrice(maValues.ma100)} / ${formatPrice(maValues.ma200)}`;
}

function formatSymbolCard(diagnostics: SymbolDiagnostics): string {
  const stateLabel = diagnostics.isStale ? `🔴 stale, ${formatDuration(diagnostics.staleAgeMs ?? 0)} behind` : '🟢 fresh';

  return [
    `🔎 ${formatClickableText(diagnostics.symbol)} · ${diagnostics.interval}`,
    `Last price: ${formatPrice(diagnostics.lastPrice)}`,
    `Last candle open: ${formatAge(diagnostics.lastKlineOpenMs)}`,
    `MA25 / MA50 / MA100 / MA200: ${formatMaValues(diagnostics.maValues)}`,
    `24h volume (USDT): ${formatVolume(diagnostics.volume24hUsdt)}`,
    `Last update: ${formatAge(diagnostics.lastUpdateMs)}`,
    `State: ${stateLabel}`,
  ].join('\n');
}

function formatStaleSymbolMessage(interval: KlineInterval, staleSymbolList: StaleSymbolInfo[]): string {
  if (staleSymbolList.length === 0) {
    return `🕯️ Stale symbols · ${interval}\n\nNo stale symbols.`;
  }

  const lineList: string[] = [`🕯️ Stale symbols · ${interval} · ${staleSymbolList.length}`, ''];

  for (const staleSymbol of staleSymbolList) {
    lineList.push(`• ${formatClickableText(staleSymbol.symbol)} · ${formatDuration(staleSymbol.ageMs)} ago`);
  }

  return lineList.join('\n');
}

export {
  formatIntervalButtonLabel,
  formatIntervalDetailMessage,
  formatOverviewMessage,
  formatStaleSymbolMessage,
  formatSymbolCard,
  resolveIntervalHealth,
};

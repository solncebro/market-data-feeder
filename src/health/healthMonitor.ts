import { escapeMarkdownV2WithFormatting, formatClickableText } from '@solncebro/telegram-engine';

import type { HealthEvent, HealthMonitor, HealthMonitorArgs } from './healthMonitor.types.js';

const CRITICAL_TRANSPORT_MARKER_LIST = ['critical', 'fatal', 'max retries', 'exceeded', 'failed'];

function isCriticalTransportMessage(message: string): boolean {
  const lower = message.toLowerCase();

  return CRITICAL_TRANSPORT_MARKER_LIST.some((marker) => lower.includes(marker));
}

function formatSeconds(ms: number): number {
  return Math.round(ms / 1000);
}

interface SymbolEntry {
  interval: string;
  symbol: string;
  ageMs?: number;
}

function buildGroupedSymbolMessage(header: string, entryList: SymbolEntry[]): string {
  const groupByInterval = new Map<string, { symbolList: string[]; maxAgeMs: number }>();

  for (const entry of entryList) {
    const group = groupByInterval.get(entry.interval) ?? { symbolList: [], maxAgeMs: 0 };
    group.symbolList.push(entry.symbol);
    group.maxAgeMs = Math.max(group.maxAgeMs, entry.ageMs ?? 0);
    groupByInterval.set(entry.interval, group);
  }

  const lineList = [header];

  for (const [interval, group] of groupByInterval) {
    const lagLabel = group.maxAgeMs > 0 ? ` · lag up to ${formatSeconds(group.maxAgeMs)}s` : '';
    lineList.push('');
    lineList.push(`${interval} · ${group.symbolList.length} symbols${lagLabel}`);
    lineList.push(group.symbolList.map(formatClickableText).join(', '));
  }

  return lineList.join('\n');
}

function createHealthMonitor(args: HealthMonitorArgs): HealthMonitor {
  const { config, logger, onRestart, restartGuard, sendAlert } = args;
  const now = args.now ?? Date.now;

  const lastAlertAtByKey = new Map<string, number>();
  const degradedReasonByKey = new Map<string, string>();
  let restartTimer: ReturnType<typeof setTimeout> | null = null;

  const pendingStalledByKey = new Map<string, SymbolEntry>();
  const pendingRecoveredByKey = new Map<string, SymbolEntry>();
  const pendingRecoveryFailedByKey = new Map<string, SymbolEntry>();
  const pendingStuckByKey = new Map<string, SymbolEntry>();
  const pendingUnstuckByKey = new Map<string, SymbolEntry>();
  const pendingLoadedByKey = new Map<string, SymbolEntry>();
  const activeStallKeySet = new Set<string>();
  let batchTimer: ReturnType<typeof setTimeout> | null = null;

  const emitAlert = (message: string): void => {
    void Promise.resolve(sendAlert(escapeMarkdownV2WithFormatting(message))).catch((error: unknown) => {
      logger.error({ error }, '[Health] failed to send alert');
    });
  };

  const alertWithDedup = (dedupKey: string, message: string): void => {
    const lastAtMs = lastAlertAtByKey.get(dedupKey);

    if (lastAtMs !== undefined && now() - lastAtMs < config.alertDedupMs) {
      return;
    }

    lastAlertAtByKey.set(dedupKey, now());
    emitAlert(message);
  };

  const cancelRestartTimer = (): void => {
    if (restartTimer !== null) {
      clearTimeout(restartTimer);
      restartTimer = null;
    }
  };

  const flushPending = (pendingByKey: Map<string, SymbolEntry>, header: string): void => {
    if (pendingByKey.size === 0) {
      return;
    }

    emitAlert(buildGroupedSymbolMessage(header, Array.from(pendingByKey.values())));
    pendingByKey.clear();
  };

  const flushBatch = (): void => {
    batchTimer = null;

    if (pendingStalledByKey.size > 0) {
      for (const key of pendingStalledByKey.keys()) {
        activeStallKeySet.add(key);
      }
    }

    flushPending(pendingStalledByKey, '⚠️ Streams stalled');
    flushPending(pendingRecoveredByKey, '✅ Streams recovered');
    flushPending(pendingRecoveryFailedByKey, '🛑 Stream recovery failed');
    flushPending(pendingStuckByKey, '⚠️ Symbols stuck (no fresh candles)');
    flushPending(pendingUnstuckByKey, '✅ Symbols producing fresh candles again');
    flushPending(pendingLoadedByKey, '📥 Loaded on demand');
  };

  const scheduleBatchFlush = (): void => {
    if (batchTimer !== null) {
      return;
    }

    batchTimer = setTimeout(flushBatch, config.batchFlushMs);
  };

  const noteStreamStalled = (interval: string, symbol: string, ageMs: number | undefined): void => {
    const key = `${interval}:${symbol}`;

    if (activeStallKeySet.has(key)) {
      return;
    }

    pendingStalledByKey.set(key, { interval, symbol, ageMs });
    pendingRecoveredByKey.delete(key);
    scheduleBatchFlush();
  };

  const noteStreamRecovered = (interval: string, symbol: string): void => {
    const key = `${interval}:${symbol}`;
    const wasPending = pendingStalledByKey.delete(key);
    const wasActive = activeStallKeySet.delete(key);

    if (wasPending && !wasActive) {
      return;
    }

    pendingRecoveredByKey.set(key, { interval, symbol });
    scheduleBatchFlush();
  };

  const noteStreamRecoveryFailed = (interval: string, symbol: string): void => {
    const key = `${interval}:${symbol}`;
    pendingStalledByKey.delete(key);
    activeStallKeySet.delete(key);
    pendingRecoveryFailedByKey.set(key, { interval, symbol });
    scheduleBatchFlush();
  };

  const noteSymbolStuck = (interval: string, symbol: string): void => {
    pendingStuckByKey.set(`${interval}:${symbol}`, { interval, symbol });
    scheduleBatchFlush();
  };

  const noteSymbolUnstuck = (interval: string, symbol: string): void => {
    const key = `${interval}:${symbol}`;

    if (pendingStuckByKey.delete(key)) {
      return;
    }

    pendingUnstuckByKey.set(key, { interval, symbol });
    scheduleBatchFlush();
  };

  const noteSymbolLoaded = (interval: string, symbol: string): void => {
    pendingLoadedByKey.set(`${interval}:${symbol}`, { interval, symbol });
    scheduleBatchFlush();
  };

  const runEscalation = (): void => {
    restartTimer = null;

    if (degradedReasonByKey.size === 0) {
      return;
    }

    const reason = Array.from(degradedReasonByKey.values()).join('; ');

    if (!restartGuard.canRestart()) {
      emitAlert(`⛔ Degradation persists but the auto-restart limit for this window has been reached — stuck, manual intervention required. Reason: ${reason}`);

      return;
    }

    restartGuard.recordRestart();
    emitAlert(`🔄 Degradation not cleared within ${formatSeconds(config.recoveryGraceMs)}s — restarting. Reason: ${reason}`);
    onRestart(reason);
  };

  const setDegraded = (key: string, reason: string): void => {
    degradedReasonByKey.set(key, reason);

    if (restartTimer === null) {
      restartTimer = setTimeout(runEscalation, config.recoveryGraceMs);
    }
  };

  const clearDegraded = (key: string): boolean => {
    const wasDegraded = degradedReasonByKey.delete(key);
    lastAlertAtByKey.delete(key);

    if (degradedReasonByKey.size === 0) {
      cancelRestartTimer();
    }

    return wasDegraded;
  };

  const report = (event: HealthEvent): void => {
    switch (event.kind) {
      case 'streamSilent':
        setDegraded(`silent:${event.interval}`, `stream silent [${event.interval}] (silent for ${formatSeconds(event.silenceMs)}s)`);
        alertWithDedup(`silent:${event.interval}`, `🛑 Stream [${event.interval}] silent for ${formatSeconds(event.silenceMs)}s — the websocket may be dead. Waiting for recovery…`);

        return;

      case 'streamResumed':
        if (clearDegraded(`silent:${event.interval}`)) {
          emitAlert(`✅ Stream [${event.interval}] resumed (was silent for ${formatSeconds(event.silenceMs)}s).`);
        }

        return;

      case 'sourceMassStale':
        setDegraded(`massStale:${event.interval}`, `mass stale [${event.interval}] (${event.staleCount}/${event.symbolCount} symbols without fresh candles)`);
        alertWithDedup(`massStale:${event.interval}`, `🛑 Mass staleness on [${event.interval}]: ${event.staleCount}/${event.symbolCount} symbols have no fresh candles — escalating. Waiting for recovery…`);

        return;

      case 'sourceMassStaleRecovered':
        if (clearDegraded(`massStale:${event.interval}`)) {
          emitAlert(`✅ Mass staleness on [${event.interval}] cleared.`);
        }

        return;

      case 'klineStreamRecoveryFailed':
        noteStreamRecoveryFailed(event.interval, event.symbol);

        return;

      case 'klineStreamStale':
        noteStreamStalled(event.interval, event.symbol, event.ageMs);

        return;

      case 'klineStreamRecovered':
        noteStreamRecovered(event.interval, event.symbol);

        return;

      case 'transportNotify': {
        const safeMessage = event.message.replace(/[`*_~|]/g, '');

        if (isCriticalTransportMessage(safeMessage)) {
          setDegraded('transport', `critical transport failure (${safeMessage})`);
          alertWithDedup('transport', `🛑 Critical exchange transport failure: ${safeMessage}`);

          return;
        }

        alertWithDedup('transportNotify', `⚠️ Exchange transport: ${safeMessage}`);

        return;
      }

      case 'persistentStaleSymbol':
        noteSymbolStuck(event.interval, event.symbol);

        return;

      case 'persistentStaleRecovered':
        noteSymbolUnstuck(event.interval, event.symbol);

        return;

      case 'feederReady':
        emitAlert(`✅ ${event.exchangeName.toUpperCase()} feeder is up and serving on ${event.host}:${event.port}. Ready to accept clients.`);

        return;

      case 'intervalLoadStarted':
        emitAlert(`⏳ Loading the ${event.interval} interval (${event.symbolCount} symbols) for a client…`);

        return;

      case 'intervalLoadCompleted':
        emitAlert(`✅ The ${event.interval} interval is ready (${event.symbolCount} symbols).`);

        return;

      case 'symbolLoadCompleted':
        noteSymbolLoaded(event.interval, event.symbol);

        return;

      default:
        return;
    }
  };

  const shutdown = (): void => {
    cancelRestartTimer();

    if (batchTimer !== null) {
      clearTimeout(batchTimer);
      batchTimer = null;
    }

    flushBatch();
  };

  return { report, shutdown };
}

export { createHealthMonitor };

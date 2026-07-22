import { escapeMarkdownV2WithFormatting, formatClickableText } from '@solncebro/telegram-engine';

import type { HealthEvent, HealthMonitor, HealthMonitorArgs } from './healthMonitor.types.js';

const CRITICAL_TRANSPORT_MARKER_LIST = ['critical', 'fatal', 'max retries'];
const ALERT_DRAIN_TIMEOUT_MS = 2500;

function isCriticalTransportMessage(message: string): boolean {
  const lower = message.toLowerCase();

  return CRITICAL_TRANSPORT_MARKER_LIST.some((marker) => lower.includes(marker));
}

function isWatchdogSummaryMessage(message: string): boolean {
  const lower = message.toLowerCase();

  return lower.includes('kline') && (lower.includes('overdue') || lower.includes('recover'));
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
  // Per-key degradation start times: escalation fires only when the OLDEST active degradation has
  // outlived the grace, so a later degradation always gets its full grace even if an earlier one
  // armed the timer and then cleared.
  const degradedSinceMsByKey = new Map<string, number>();
  const inFlightSendSet = new Set<Promise<void>>();
  let restartTimer: ReturnType<typeof setTimeout> | null = null;

  const pendingRecoveryFailedByKey = new Map<string, SymbolEntry>();
  let batchTimer: ReturnType<typeof setTimeout> | null = null;

  const windowDegradedKeySet = new Set<string>();
  const windowRecoveredKeySet = new Set<string>();
  const unrecoveredByKey = new Map<string, SymbolEntry>();
  // Symbols whose history is incomplete and whose backfill has converged (the exchange serves no
  // more) — listed by name in the periodic digest until they recover, never an immediate alert.
  const stuckBackfillByKey = new Map<string, SymbolEntry>();

  const emitAlert = (message: string): void => {
    const sendPromise = Promise.resolve(sendAlert(escapeMarkdownV2WithFormatting(message))).catch((error: unknown) => {
      logger.error({ error }, '[Health] failed to send alert');
    });

    inFlightSendSet.add(sendPromise);
    sendPromise.finally(() => {
      inFlightSendSet.delete(sendPromise);
    });
  };

  const drainPendingSends = async (): Promise<void> => {
    if (inFlightSendSet.size === 0) {
      return;
    }

    const settled = Promise.allSettled(Array.from(inFlightSendSet));
    const timeout = new Promise<void>((resolve) => {
      setTimeout(resolve, ALERT_DRAIN_TIMEOUT_MS).unref();
    });

    await Promise.race([settled, timeout]);
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
    flushPending(pendingRecoveryFailedByKey, '🛑 Stream recovery failed');
  };

  const scheduleBatchFlush = (): void => {
    if (batchTimer !== null) {
      return;
    }

    batchTimer = setTimeout(flushBatch, config.batchFlushMs);
  };

  const emitDigest = (): void => {
    if (windowDegradedKeySet.size === 0 && windowRecoveredKeySet.size === 0 && unrecoveredByKey.size === 0 && stuckBackfillByKey.size === 0) {
      emitAlert('✅ Kline streams healthy — no issues since the last report.');

      return;
    }

    const lineList = [`📋 Kline streams report: ${windowDegradedKeySet.size} degraded, ${windowRecoveredKeySet.size} recovered.`];

    if (unrecoveredByKey.size > 0) {
      lineList.push('');
      lineList.push(buildGroupedSymbolMessage(`🛑 Still unrecovered (${unrecoveredByKey.size}):`, Array.from(unrecoveredByKey.values())));
    }

    if (stuckBackfillByKey.size > 0) {
      lineList.push('');
      lineList.push(buildGroupedSymbolMessage(`🕳️ Still under-filled (${stuckBackfillByKey.size}):`, Array.from(stuckBackfillByKey.values())));
    }

    emitAlert(lineList.join('\n'));
    windowDegradedKeySet.clear();
    windowRecoveredKeySet.clear();
  };

  const digestTimer = setInterval(emitDigest, config.digestIntervalMs);
  digestTimer.unref();

  const armEscalationTimer = (): void => {
    if (restartTimer !== null || degradedSinceMsByKey.size === 0) {
      return;
    }

    const oldestSinceMs = Math.min(...degradedSinceMsByKey.values());
    const delayMs = Math.max(0, oldestSinceMs + config.recoveryGraceMs - now());
    restartTimer = setTimeout(runEscalation, delayMs);
  };

  const runEscalation = (): void => {
    restartTimer = null;

    if (degradedReasonByKey.size === 0) {
      return;
    }

    const oldestSinceMs = Math.min(...degradedSinceMsByKey.values());

    // The degradation that armed this timer cleared meanwhile — the survivors get their full grace.
    if (now() - oldestSinceMs < config.recoveryGraceMs) {
      armEscalationTimer();

      return;
    }

    const reason = Array.from(degradedReasonByKey.values()).join('; ');

    if (!restartGuard.canRestart()) {
      alertWithDedup('restartBlocked', `⛔ Degradation persists but the auto-restart limit for this window has been reached — stuck, manual intervention required. Reason: ${reason}`);
      // Keep re-checking: the guard window rolls over, and a still-active degradation must restart
      // once it allows again — without this re-arm the process would stay degraded forever.
      restartTimer = setTimeout(runEscalation, config.recoveryGraceMs);

      return;
    }

    restartGuard.recordRestart();
    emitAlert(`🔄 Degradation not cleared within ${formatSeconds(config.recoveryGraceMs)}s — restarting. Reason: ${reason}`);
    onRestart(reason);
  };

  const setDegraded = (key: string, reason: string): void => {
    degradedReasonByKey.set(key, reason);

    // First report wins: a repeated critical transport message (not edge-triggered at the source)
    // must not push the escalation deadline forward.
    if (!degradedSinceMsByKey.has(key)) {
      degradedSinceMsByKey.set(key, now());
    }

    armEscalationTimer();
  };

  const clearDegraded = (key: string): boolean => {
    const wasDegraded = degradedReasonByKey.delete(key);
    degradedSinceMsByKey.delete(key);
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

      case 'klineStreamStale':
        windowDegradedKeySet.add(`${event.interval}:${event.symbol}`);

        return;

      case 'klineStreamRecovered': {
        const key = `${event.interval}:${event.symbol}`;
        windowRecoveredKeySet.add(key);
        unrecoveredByKey.delete(key);

        return;
      }

      case 'klineStreamRecoveryFailed': {
        const key = `${event.interval}:${event.symbol}`;
        windowDegradedKeySet.add(key);
        unrecoveredByKey.set(key, { interval: event.interval, symbol: event.symbol });
        pendingRecoveryFailedByKey.set(key, { interval: event.interval, symbol: event.symbol });
        scheduleBatchFlush();

        return;
      }

      case 'transportNotify': {
        const safeMessage = event.message.replace(/[`*_~|]/g, '');

        if (isCriticalTransportMessage(safeMessage)) {
          setDegraded('transport', `critical transport failure (${safeMessage})`);
          alertWithDedup('transport', `🛑 Critical exchange transport failure: ${safeMessage}`);

          return;
        }

        if (isWatchdogSummaryMessage(safeMessage)) {
          return;
        }

        alertWithDedup('transportNotify', `⚠️ Exchange transport: ${safeMessage}`);

        return;
      }

      case 'persistentStaleSymbol':
        windowDegradedKeySet.add(`${event.interval}:${event.symbol}`);

        return;

      case 'persistentStaleRecovered': {
        const key = `${event.interval}:${event.symbol}`;
        windowRecoveredKeySet.add(key);
        unrecoveredByKey.delete(key);

        return;
      }

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
        return;

      case 'symbolDelisted': {
        // A delisted or released symbol can never emit a recovery event — scrub it from the digest
        // bookkeeping so the "Still unrecovered" list does not repeat it forever.
        const key = `${event.interval}:${event.symbol}`;
        windowDegradedKeySet.delete(key);
        windowRecoveredKeySet.delete(key);
        unrecoveredByKey.delete(key);
        pendingRecoveryFailedByKey.delete(key);
        stuckBackfillByKey.delete(key);

        return;
      }

      case 'intervalReleased': {
        // The interval's source was torn down (last client left): its whole-source degradations can
        // never clear via streamResumed/massStaleRecovered — dropping them prevents a spurious
        // restart of a feeder whose "silent" interval simply no longer exists. Its per-symbol digest
        // keys are scrubbed for the same reason as a delisting.
        clearDegraded(`silent:${event.interval}`);
        clearDegraded(`massStale:${event.interval}`);

        const keyPrefix = `${event.interval}:`;

        for (const key of Array.from(windowDegradedKeySet)) {
          if (key.startsWith(keyPrefix)) {
            windowDegradedKeySet.delete(key);
          }
        }

        for (const key of Array.from(windowRecoveredKeySet)) {
          if (key.startsWith(keyPrefix)) {
            windowRecoveredKeySet.delete(key);
          }
        }

        for (const key of Array.from(unrecoveredByKey.keys())) {
          if (key.startsWith(keyPrefix)) {
            unrecoveredByKey.delete(key);
          }
        }

        for (const key of Array.from(pendingRecoveryFailedByKey.keys())) {
          if (key.startsWith(keyPrefix)) {
            pendingRecoveryFailedByKey.delete(key);
          }
        }

        for (const key of Array.from(stuckBackfillByKey.keys())) {
          if (key.startsWith(keyPrefix)) {
            stuckBackfillByKey.delete(key);
          }
        }

        return;
      }

      case 'symbolSyncAnomaly':
        // Not escalating: data keeps flowing; the guard defers the removals itself. The operator
        // must still hear about it immediately — a repeated anomaly means the exchange list is off.
        alertWithDedup(`symbolSyncAnomaly:${event.interval}`, `⚠️ Symbol sync anomaly on [${event.interval}]: the exchange list dropped ${event.deferredCount}/${event.symbolCount} loaded symbols — removals deferred until the next hourly sync confirms them.`);

        return;

      case 'symbolLoadFailed':
        windowDegradedKeySet.add(`${event.interval}:${event.symbol}`);

        return;

      case 'symbolBackfillStuck': {
        // Not an immediate alert and never escalating: the data still flows (with a short buffer).
        // The symbol is listed by name in the periodic digest until its history fills in.
        const key = `${event.interval}:${event.symbol}`;
        windowDegradedKeySet.add(key);
        stuckBackfillByKey.set(key, { interval: event.interval, symbol: event.symbol });

        return;
      }

      case 'symbolBackfillRecovered': {
        const key = `${event.interval}:${event.symbol}`;
        windowRecoveredKeySet.add(key);
        stuckBackfillByKey.delete(key);

        return;
      }

      default:
        return;
    }
  };

  const shutdown = async (): Promise<void> => {
    cancelRestartTimer();
    clearInterval(digestTimer);

    if (batchTimer !== null) {
      clearTimeout(batchTimer);
      batchTimer = null;
    }

    flushBatch();
    await drainPendingSends();
  };

  return { report, shutdown };
}

export { createHealthMonitor };

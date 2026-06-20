import type { KlineInterval } from '../domain/marketData.types.js';
import type { FeederLogger } from '../server/feederServer.types.js';
import type { RestartGuard } from './restartGuard.js';

type HealthEvent =
  | { kind: 'streamSilent'; interval: KlineInterval; silenceMs: number }
  | { kind: 'streamResumed'; interval: KlineInterval; silenceMs: number }
  | { kind: 'sourceMassStale'; interval: KlineInterval; staleCount: number; symbolCount: number }
  | { kind: 'sourceMassStaleRecovered'; interval: KlineInterval }
  | { kind: 'persistentStaleSymbol'; interval: KlineInterval; symbol: string }
  | { kind: 'persistentStaleRecovered'; interval: KlineInterval; symbol: string }
  | { kind: 'transportNotify'; message: string }
  | { kind: 'klineStreamStale'; interval: KlineInterval; symbol: string; ageMs?: number }
  | { kind: 'klineStreamRecovered'; interval: KlineInterval; symbol: string }
  | { kind: 'klineStreamRecoveryFailed'; interval: KlineInterval; symbol: string; consecutiveFailCount?: number }
  | { kind: 'feederReady'; exchangeName: string; host: string; port: number }
  | { kind: 'intervalLoadStarted'; interval: KlineInterval; symbolCount: number }
  | { kind: 'intervalLoadCompleted'; interval: KlineInterval; symbolCount: number }
  | { kind: 'symbolLoadCompleted'; interval: KlineInterval; symbol: string }
  | { kind: 'symbolLoadFailed'; interval: KlineInterval; symbol: string };

interface HealthMonitorConfig {
  alertDedupMs: number;
  recoveryGraceMs: number;
  batchFlushMs: number;
}

interface HealthMonitorArgs {
  config: HealthMonitorConfig;
  sendAlert: (message: string) => void | Promise<void>;
  restartGuard: RestartGuard;
  onRestart: (reason: string) => void;
  logger: FeederLogger;
  now?: () => number;
}

interface HealthMonitor {
  report: (event: HealthEvent) => void;
  shutdown: () => Promise<void>;
}

export type { HealthEvent, HealthMonitor, HealthMonitorArgs, HealthMonitorConfig };

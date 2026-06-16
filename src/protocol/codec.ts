import type { FeederMessage } from './messages.types.js';

const KNOWN_TYPE_SET: ReadonlySet<string> = new Set<string>([
  'subscribe',
  'unsubscribe',
  'snapshot',
  'klineClosed',
  'klineUpdated',
  'klineUpdatedTick',
  'symbolAdded',
  'symbolRemoved',
  'volume24h',
  'heartbeat',
]);

function encodeMessage(message: FeederMessage): string {
  return JSON.stringify(message);
}

function isFeederMessage(value: unknown): value is FeederMessage {
  if (typeof value !== 'object' || value === null) {
    return false;
  }

  if (!('type' in value)) {
    return false;
  }

  const messageType = value.type;

  return typeof messageType === 'string' && KNOWN_TYPE_SET.has(messageType);
}

function decodeMessage(raw: string): FeederMessage | null {
  let parsed: unknown;

  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }

  if (!isFeederMessage(parsed)) {
    return null;
  }

  return parsed;
}

export { decodeMessage, encodeMessage };

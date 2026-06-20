import { isKnownInterval } from '../domain/constants.js';

const MAX_SUBSCRIBE_SYMBOL_COUNT = 5000;
const SYMBOL_PATTERN = /^[A-Z0-9]{1,30}$/;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function isValidSymbol(value: unknown): boolean {
  return typeof value === 'string' && SYMBOL_PATTERN.test(value);
}

function isValidScope(scope: unknown): boolean {
  if (!isRecord(scope)) {
    return false;
  }

  if (scope.kind === 'all') {
    return true;
  }

  if (scope.kind !== 'symbols') {
    return false;
  }

  if (!Array.isArray(scope.symbolList) || scope.symbolList.length === 0 || scope.symbolList.length > MAX_SUBSCRIBE_SYMBOL_COUNT) {
    return false;
  }

  return scope.symbolList.every(isValidSymbol);
}

function isValidSubscribe(message: unknown): boolean {
  if (!isRecord(message)) {
    return false;
  }

  if (message.type !== 'subscribe') {
    return false;
  }

  if (typeof message.interval !== 'string' || !isKnownInterval(message.interval)) {
    return false;
  }

  if (!Array.isArray(message.events)) {
    return false;
  }

  return isValidScope(message.scope);
}

export { isValidSubscribe };

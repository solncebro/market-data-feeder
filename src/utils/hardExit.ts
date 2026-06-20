async function runWithHardExit(task: () => Promise<void>, timeoutMs: number, onTimeout: () => void): Promise<void> {
  const timer = setTimeout(onTimeout, timeoutMs);
  timer.unref();

  try {
    await task();
  } finally {
    clearTimeout(timer);
  }
}

export { runWithHardExit };

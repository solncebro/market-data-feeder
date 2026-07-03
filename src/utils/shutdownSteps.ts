interface ShutdownStep {
  label: string;
  run: () => Promise<unknown> | unknown;
}

// Every step runs even if an earlier one throws: a shutdown sequence must reach its end (and the
// caller's process.exit) no matter which subsystem misbehaves on the way down.
async function runShutdownStepsBestEffort(stepList: ShutdownStep[], onStepError: (label: string, error: unknown) => void): Promise<void> {
  for (const step of stepList) {
    try {
      await step.run();
    } catch (error: unknown) {
      onStepError(step.label, error);
    }
  }
}

export { runShutdownStepsBestEffort };
export type { ShutdownStep };

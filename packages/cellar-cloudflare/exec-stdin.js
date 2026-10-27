// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

/** Reserve one source before allocation; every exit path returns its reader. */
export function ownedStdin(stream) {
  const reader = stream.getReader();
  let released = false,
    cancellation;
  const release = () => {
    if (released) return;
    reader.releaseLock();
    released = true;
  };
  const cancel = (reason) => {
    if (released) return Promise.resolve();
    if (!cancellation) {
      cancellation = reader.cancel(reason);
      cancellation.catch(() => {});
    }
    return cancellation;
  };
  return {
    get cancelled() {
      return cancellation !== undefined;
    },
    release,
    cancel,
    async run(write, close, fail) {
      let failed = false;
      try {
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          await write(value);
        }
        await close();
      } catch (error) {
        failed = true;
        // Start native retirement before waiting for application cancellation.
        const retirement = fail(error);
        await Promise.allSettled([cancel(error), retirement]);
        throw error;
      } finally {
        try {
          await cancellation;
        } catch (error) {
          if (!failed) throw error;
        } finally {
          release();
        }
      }
    },
  };
}

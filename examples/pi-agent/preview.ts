import type {Progress} from './durable.js';

// A bounded, ordered, optional side channel. It carries previews, never secrets,
// thinking blocks or a durability acknowledgement. Losing it cannot fail a run.
export function configuredObserver(): ((task: string, event: Progress) => void) | undefined {
  const address = process.env.CELLAR_PI_EVENTS_URL;
  if (!address) return;
  const url = new URL(address);
  const token = process.env.CELLAR_PI_EVENTS_TOKEN;
  if (url.protocol !== 'http:' || url.hostname !== '127.0.0.1'
    || url.pathname !== '/internal/events' || !token) throw new Error('invalid local preview configuration');
  const queue: string[] = [];
  let sending = false;
  async function flush() {
    if (sending) return;
    sending = true;
    try {
      while (queue.length) {
        const body = queue.shift()!;
        try {
          const reply = await fetch(url, {method: 'POST', body,
            headers: {'authorization': `Bearer ${token}`, 'content-type': 'application/json'},
            signal: AbortSignal.timeout(750)});
          await reply.body?.cancel();
          if (!reply.ok) queue.length = 0;
        } catch { queue.length = 0; /* Do not accumulate timeouts behind a dead observer. */ }
      }
    } finally { sending = false; }
  }
  return (task, event) => {
    // Complete text snapshots allow a later delta to repair dropped previews.
    const body = JSON.stringify({task, ...event, ...(event.text ? {text: event.text.slice(0, 32000)} : {})});
    if (queue.length >= 32) queue.shift();
    queue.push(body);
    void flush();
  };
}

// Deterministic stand-in for private native controls. Production never uses a
// guest-provided clock or serializes this object in business metadata.
export function testBudget(clock = {now: 0}, cap = 5000, parent = Infinity) {
  const started = clock.now;
  let deadline = Math.min(started + cap, parent);
  return {
    released: 0,
    limit(timeout) { deadline = Math.min(deadline, started + timeout); },
    remaining() {
      if (clock.now >= deadline) throw Error("DeadlineExceeded");
      return deadline - clock.now;
    },
    release() { this.released++; },
  };
}

export function testBudgetControl(id = 1) {
  const budget = testBudget();
  return [id, () => budget.remaining(), value => budget.limit(value), () => budget.release()];
}

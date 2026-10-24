/** Bounds for the message graph before recursive codec work. */
export interface InputLimits {
  maxDepth?: number;
  maxElements?: number;
  maxMessageBytes?: number;
  /** Estimated decoded fields/objects and wire occurrences; not an RSS cap. */
  maxDecodedSlots?: number;
}

/** Resolve finite budgets before schema-driven graph and wire-size validation. */
export function inputLimits(limits?: InputLimits): Required<InputLimits>;

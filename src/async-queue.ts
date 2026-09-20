/**
 * A single-producer / single-consumer queue with backpressure.
 *
 * The producer (the parser) calls {@link push}; when the buffered size
 * reaches the high-water mark, `push` returns a promise that resolves once
 * the consumer has drained the queue below the mark. This is the mechanism
 * that keeps peak memory bounded by the high-water mark plus one item,
 * regardless of how much data still has to flow through.
 */
export class PushQueue<T> implements AsyncIterable<T> {
  #queue: T[] = [];
  #size = 0;
  #done = false;
  #error: { value: unknown } | null = null;
  #claimed = false;
  #onData: (() => void) | null = null;
  #onSpace: (() => void) | null = null;
  readonly #hwm: number;
  readonly #sizeOf: (item: T) => number;

  constructor(highWaterMark: number, sizeOf: (item: T) => number) {
    this.#hwm = Math.max(1, highWaterMark);
    this.#sizeOf = sizeOf;
  }

  /** Current buffered size (bytes for byte queues, count otherwise). */
  get size(): number {
    return this.#size;
  }

  /** True once {@link close} or {@link fail} has been called. */
  get closed(): boolean {
    return this.#done;
  }

  /**
   * Enqueue an item. Returns null while there is room, or a promise the
   * producer must await before pushing more.
   */
  push(item: T): Promise<void> | null {
    if (this.#done) throw new Error('PushQueue: push after close');
    this.#queue.push(item);
    this.#size += this.#sizeOf(item);
    this.#onData?.();
    this.#onData = null;
    if (this.#size >= this.#hwm) {
      return new Promise<void>((resolve) => {
        this.#onSpace = resolve;
      });
    }
    return null;
  }

  /** Signal end-of-stream: the consumer finishes after draining the queue. */
  close(): void {
    this.#done = true;
    this.#wake();
  }

  /** Signal end-of-stream with an error: the consumer throws after draining. */
  fail(error: unknown): void {
    this.#error = { value: error };
    this.#done = true;
    this.#wake();
  }

  #wake(): void {
    this.#onData?.();
    this.#onData = null;
    this.#onSpace?.();
    this.#onSpace = null;
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return this.#iterate();
  }

  async *#iterate(): AsyncGenerator<T> {
    if (this.#claimed) throw new Error('PushQueue is single-consumer');
    this.#claimed = true;
    for (;;) {
      const item = this.#queue.shift();
      if (item !== undefined) {
        this.#size -= this.#sizeOf(item);
        if (this.#onSpace && this.#size < this.#hwm) {
          const resume = this.#onSpace;
          this.#onSpace = null;
          resume();
        }
        yield item;
        continue;
      }
      if (this.#error) throw this.#error.value;
      if (this.#done) return;
      await new Promise<void>((resolve) => {
        this.#onData = resolve;
      });
    }
  }
}

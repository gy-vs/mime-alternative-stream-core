import { PushQueue } from './async-queue.js';
import { concatBytes } from './bytes.js';
import { isValidHeaderLine, parseContentType, parseHeaderLines } from './headers.js';
import { MimeParseError, MimePart, type ParseEvent } from './part.js';
import { BoundaryScanner } from './scanner.js';

const LF = 0x0a;
const CR = 0x0d;
const EMPTY = new Uint8Array(0);

/**
 * Explicit parser states, observable via {@link MimeParser.state}.
 *
 * ```
 * message-headers ──multipart root──▶ preamble ──delimiter──▶ part-headers
 *     │                                 ▲   │                     │
 *     └──single body──▶ part-body       │   └──close──▶ (pop)     ▼
 *                                     (pop)                 part-body ──delimiter──┐
 * header errors jump to skip-part (or epilogue at top level); ◀──────────────────────┘
 * the root closing delimiter leads to epilogue; end() leads to done.
 * ```
 */
export enum ParserState {
  /** Reading the top-level message headers. */
  MessageHeaders = 'message-headers',
  /** Discarding bytes up to the next delimiter of the innermost multipart. */
  Preamble = 'preamble',
  /** Accumulating one part's header section, line by line. */
  PartHeaders = 'part-headers',
  /** Streaming a leaf part's body while scanning for the delimiter. */
  PartBody = 'part-body',
  /** Error recovery: discarding a broken part up to the next delimiter. */
  SkipPart = 'skip-part',
  /** Root multipart is closed; discarding the epilogue until EOF. */
  Epilogue = 'epilogue',
  /** Terminal: `end()` has been fully processed. */
  Done = 'done',
}

export interface MimeParserOptions {
  /**
   * Per-body stream buffer in bytes. When the consumer of a part body falls
   * behind, `feed()` stops resolving once roughly this many body bytes are
   * buffered, so peak memory stays bounded independently of body size.
   * Default 64 KiB.
   */
  highWaterMark?: number;
  /**
   * Maximum size of one part's header section in bytes. Larger sections are
   * reported as `headers-too-large` and skipped. Default 256 KiB.
   */
  maxHeaderBytes?: number;
  /**
   * Maximum transport padding (SP/TAB) tolerated on a delimiter line.
   * Longer padding makes the line count as body content. Default 1024.
   */
  maxPaddingBytes?: number;
  /** Maximum number of unconsumed parse events. Default 16. */
  eventHighWaterMark?: number;
}

interface MultipartContext {
  boundary: string;
  part: MimePart;
}

/**
 * Incremental MIME multipart parser.
 *
 * Usage:
 * ```ts
 * const parser = new MimeParser();
 * const consumer = (async () => {
 *   for await (const event of parser.events()) {
 *     if (event.type === 'part-start' && event.part.body) {
 *       for await (const chunk of event.part.body) { ... }
 *     }
 *   }
 * })();
 * for (const chunk of chunks) await parser.feed(chunk); // await: backpressure
 * await parser.end();
 * await consumer;
 * ```
 *
 * Guarantees:
 * - `part-end` is emitted as soon as a part's closing delimiter is seen;
 *   children always end before their parents.
 * - `done` is the last event, emitted exactly once, and only after every
 *   body stream has been closed (ended or failed) by the parser.
 * - Peak buffering is bounded by `highWaterMark + one feed chunk +
 *   maxHeaderBytes + boundary length`, never by the size of a body.
 *
 * `feed()` calls must be serialized (await each one); concurrent calls throw.
 */
export class MimeParser {
  #state = ParserState.MessageHeaders;
  #stack: MultipartContext[] = [];
  #scanner: BoundaryScanner | null = null;

  // Header-section accumulation.
  #lines: Uint8Array[] = [];
  #partial: Uint8Array[] = [];
  #partialLen = 0;
  #lineStart = 0;
  #headerBytes = 0;
  #partStart = 0;

  // The leaf part currently receiving body bytes, if any.
  #currentPart: MimePart | null = null;
  #currentBody: PushQueue<Uint8Array> | null = null;
  #openBodies = 0;

  #events: PushQueue<ParseEvent>;
  #errors: MimeParseError[] = [];
  #root: MimePart | null = null;

  #fed = 0;
  #ended = false;
  #busy = false;
  #peak = 0;

  readonly #hwm: number;
  readonly #maxHeaderBytes: number;
  readonly #maxPadding: number;

  constructor(options: MimeParserOptions = {}) {
    this.#hwm = options.highWaterMark ?? 64 * 1024;
    this.#maxHeaderBytes = options.maxHeaderBytes ?? 256 * 1024;
    this.#maxPadding = options.maxPaddingBytes ?? 1024;
    this.#events = new PushQueue<ParseEvent>(options.eventHighWaterMark ?? 16, () => 1);
  }

  /** Current parser state. */
  get state(): ParserState {
    return this.#state;
  }

  /** Total input bytes consumed so far. */
  get offset(): number {
    return this.#fed;
  }

  /** Bytes currently buffered inside the parser (bodies, headers, scanner). */
  get bufferedBytes(): number {
    return (
      (this.#currentBody?.size ?? 0) +
      this.#headerBytes +
      (this.#scanner?.pendingBytes ?? 0)
    );
  }

  /** High-water mark of {@link bufferedBytes} observed so far. */
  get peakBufferedBytes(): number {
    return this.#peak;
  }

  /** The parse event stream. Single-consumer. */
  events(): AsyncIterable<ParseEvent> {
    return this.#events;
  }

  /**
   * Feed the next chunk of input. Resolves once the chunk has been consumed
   * and the parser is ready for more; while a body consumer is back-pressured
   * the returned promise stays pending, which is how the parser avoids
   * buffering unboundedly.
   */
  async feed(chunk: Uint8Array): Promise<void> {
    if (this.#ended) throw new Error('MimeParser: feed() after end()');
    if (this.#busy) {
      throw new Error('MimeParser: concurrent feed() calls are not supported; await each feed()');
    }
    if (chunk.length === 0) return;
    this.#busy = true;
    try {
      const base = this.#fed;
      this.#fed += chunk.length;
      let pos = 0;
      while (pos < chunk.length) {
        switch (this.#state) {
          case ParserState.MessageHeaders:
          case ParserState.PartHeaders:
            pos = await this.#readHeaders(chunk, pos, base);
            break;
          case ParserState.Preamble:
          case ParserState.PartBody:
          case ParserState.SkipPart:
            pos = await this.#readBody(chunk, pos, base);
            break;
          case ParserState.Epilogue:
          case ParserState.Done:
            pos = chunk.length; // trailing bytes after the root entity: ignored
            break;
        }
      }
    } finally {
      this.#busy = false;
    }
  }

  /**
   * Signal end of input. Flushes the boundary scanner, closes any open
   * streams (failing them with `unexpected-eof` when the message is
   * truncated) and emits the final `done` event.
   */
  async end(): Promise<void> {
    if (this.#ended) return;
    if (this.#busy) throw new Error('MimeParser: end() while a feed() is in progress');
    this.#ended = true;
    this.#busy = true;
    try {
      if (
        this.#scanner &&
        (this.#state === ParserState.PartBody ||
          this.#state === ParserState.Preamble ||
          this.#state === ParserState.SkipPart)
      ) {
        const r = this.#scanner.feed(EMPTY, true);
        if (this.#state === ParserState.PartBody) {
          for (const d of r.data) await this.#pushBody(d);
        }
        if (r.hit) await this.#delimiter(r.hit.closing, this.#fed);
      }
      switch (this.#state) {
        case ParserState.PartBody: {
          const part = this.#currentPart!;
          if (this.#stack.length === 0) {
            // Non-multipart message: the body simply runs to EOF.
            this.#currentPart = null;
            const body = this.#currentBody!;
            this.#currentBody = null;
            this.#openBodies--;
            body.close();
            await this.#emit({ type: 'part-end', part });
            this.#state = ParserState.Epilogue;
          } else {
            const error = new MimeParseError(
              'unexpected-eof',
              this.#fed,
              part.path,
              'end of input inside a part body',
            );
            await this.#emitError(error);
            this.#closeUnclosed(error);
          }
          break;
        }
        case ParserState.MessageHeaders:
        case ParserState.PartHeaders:
        case ParserState.Preamble:
        case ParserState.SkipPart: {
          const error = new MimeParseError(
            'unexpected-eof',
            this.#fed,
            this.#currentPath(),
            'end of input before the message was complete',
          );
          await this.#emitError(error);
          this.#closeUnclosed(error);
          break;
        }
        case ParserState.Epilogue:
        case ParserState.Done:
          break;
      }
      this.#state = ParserState.Done;
      await this.#emit({ type: 'done', message: this.#root, errors: this.#errors.slice() });
      this.#events.close();
    } finally {
      this.#busy = false;
    }
  }

  // ---------------------------------------------------------------- internals

  async #readHeaders(chunk: Uint8Array, pos: number, base: number): Promise<number> {
    while (pos < chunk.length) {
      const nl = chunk.indexOf(LF, pos);
      if (nl < 0) {
        if (this.#partialLen === 0) this.#lineStart = base + pos;
        const seg = chunk.slice(pos);
        this.#partial.push(seg);
        this.#partialLen += seg.length;
        this.#headerBytes += seg.length;
        this.#sample();
        if (this.#headerBytes > this.#maxHeaderBytes) {
          await this.#failHeaders(
            new MimeParseError(
              'headers-too-large',
              base + pos,
              this.#currentPath(),
              `header section exceeds ${this.#maxHeaderBytes} bytes`,
            ),
          );
        }
        return chunk.length;
      }
      const abs = this.#partialLen === 0 ? base + pos : this.#lineStart;
      let line =
        this.#partialLen === 0
          ? chunk.slice(pos, nl)
          : concatBytes(...this.#partial, chunk.subarray(pos, nl));
      this.#partial = [];
      this.#partialLen = 0;
      this.#headerBytes += nl - pos + 1;
      if (line.length > 0 && line[line.length - 1] === CR) {
        line = line.subarray(0, line.length - 1);
      }
      pos = nl + 1;
      if (line.length === 0) {
        await this.#finishHeaders();
        return pos;
      }
      if (!isValidHeaderLine(line, this.#lines.length > 0)) {
        await this.#failHeaders(
          new MimeParseError(
            'bad-header',
            abs,
            this.#currentPath(),
            `malformed header line ${JSON.stringify(preview(line))}`,
          ),
        );
        return pos;
      }
      this.#lines.push(line);
      if (this.#headerBytes > this.#maxHeaderBytes) {
        await this.#failHeaders(
          new MimeParseError(
            'headers-too-large',
            abs,
            this.#currentPath(),
            `header section exceeds ${this.#maxHeaderBytes} bytes`,
          ),
        );
        return pos;
      }
    }
    return pos;
  }

  async #finishHeaders(): Promise<void> {
    const headers = parseHeaderLines(this.#lines);
    this.#resetHeaders();
    const parent = this.#stack.length > 0 ? this.#stack[this.#stack.length - 1].part : null;
    const part = new MimePart({
      headers,
      parent,
      offset: this.#partStart,
      index: parent ? parent.children.length : 0,
    });
    if (parent) parent.children.push(part);
    else this.#root = part;

    const ct = part.contentType;
    const boundary = ct.type === 'multipart' ? ct.params.boundary : undefined;
    if (ct.type === 'multipart' && (boundary === undefined || boundary === '')) {
      await this.#emitError(
        new MimeParseError(
          'missing-boundary',
          part.offset,
          part.path,
          'multipart content type without a boundary parameter; treating the part as opaque',
        ),
      );
    }
    if (ct.type === 'multipart' && boundary !== undefined && boundary !== '') {
      this.#stack.push({ boundary, part });
      this.#scanner = new BoundaryScanner(boundary, this.#maxPadding);
      this.#state = ParserState.Preamble;
    } else {
      const queue = new PushQueue<Uint8Array>(this.#hwm, (c) => c.length);
      part.body = queue;
      this.#currentPart = part;
      this.#currentBody = queue;
      this.#openBodies++;
      const top = this.#stack.length > 0 ? this.#stack[this.#stack.length - 1] : null;
      this.#scanner = top ? new BoundaryScanner(top.boundary, this.#maxPadding) : null;
      this.#state = ParserState.PartBody;
    }
    await this.#emit({ type: 'part-start', part });
  }

  async #readBody(chunk: Uint8Array, pos: number, base: number): Promise<number> {
    const scanner = this.#scanner;
    if (!scanner) {
      // Non-multipart root: the rest of the input is the body, ended by EOF.
      if (this.#state === ParserState.PartBody) await this.#pushBody(chunk.slice(pos));
      return chunk.length;
    }
    const r = scanner.feed(chunk.subarray(pos));
    if (this.#state === ParserState.PartBody) {
      for (const d of r.data) await this.#pushBody(d);
    }
    // In Preamble and SkipPart the scanned bytes are discarded.
    pos += r.consumed;
    this.#sample();
    if (r.hit) await this.#delimiter(r.hit.closing, base + pos);
    return pos;
  }

  async #pushBody(data: Uint8Array): Promise<void> {
    const body = this.#currentBody;
    if (!body || data.length === 0) return;
    // Deliver in bounded slices so the queue never holds more than ~2×HWM,
    // even when a single feed chunk is huge. `data` is always a parser-owned
    // copy here, so the subarray views are safe to enqueue.
    for (let off = 0; off < data.length; off += this.#hwm) {
      const wait = body.push(data.subarray(off, Math.min(off + this.#hwm, data.length)));
      this.#sample();
      if (wait) await wait;
    }
  }

  async #delimiter(closing: boolean, nextOffset: number): Promise<void> {
    if (this.#state === ParserState.PartBody && this.#currentPart) {
      const part = this.#currentPart;
      const body = this.#currentBody!;
      this.#currentPart = null;
      this.#currentBody = null;
      this.#openBodies--;
      body.close();
      await this.#emit({ type: 'part-end', part });
    }
    if (closing) {
      const ctx = this.#stack.pop()!;
      await this.#emit({ type: 'part-end', part: ctx.part });
      if (this.#stack.length > 0) {
        const top = this.#stack[this.#stack.length - 1];
        this.#scanner = new BoundaryScanner(top.boundary, this.#maxPadding);
        this.#state = ParserState.Preamble;
      } else {
        this.#scanner = null;
        this.#state = ParserState.Epilogue;
      }
    } else {
      this.#scanner = null;
      this.#partStart = nextOffset;
      this.#state = ParserState.PartHeaders;
    }
  }

  async #failHeaders(error: MimeParseError): Promise<void> {
    await this.#emitError(error);
    this.#resetHeaders();
    if (this.#stack.length === 0) {
      // The top-level headers themselves are broken: nothing recoverable follows.
      this.#scanner = null;
      this.#state = ParserState.Epilogue;
    } else {
      const top = this.#stack[this.#stack.length - 1];
      this.#scanner = new BoundaryScanner(top.boundary, this.#maxPadding);
      this.#state = ParserState.SkipPart;
    }
  }

  #resetHeaders(): void {
    this.#lines = [];
    this.#partial = [];
    this.#partialLen = 0;
    this.#headerBytes = 0;
  }

  #closeUnclosed(error: MimeParseError): void {
    if (this.#currentBody) {
      this.#currentPart!.complete = false;
      this.#currentBody.fail(error);
      this.#currentBody = null;
      this.#currentPart = null;
      this.#openBodies--;
    }
    for (const ctx of this.#stack) ctx.part.complete = false;
    this.#stack = [];
  }

  #currentPath(): number[] {
    const path = this.#stack.slice(1).map((c) => c.part.index);
    if (this.#stack.length > 0) {
      path.push(this.#stack[this.#stack.length - 1].part.children.length);
    }
    return path;
  }

  async #emit(event: ParseEvent): Promise<void> {
    const wait = this.#events.push(event);
    if (wait) await wait;
  }

  async #emitError(error: MimeParseError): Promise<void> {
    this.#errors.push(error);
    await this.#emit({ type: 'error', error });
  }

  #sample(): void {
    const b = this.bufferedBytes;
    if (b > this.#peak) this.#peak = b;
  }
}

const previewDecoder = new TextDecoder();

function preview(line: Uint8Array): string {
  const text = previewDecoder.decode(line.subarray(0, 48));
  return line.length > 48 ? `${text}…` : text;
}

/**
 * Streaming MIME parser.
 *
 * Bytes are pushed in via {@link MimeParser.feed} (awaited, so backpressure
 * propagates to the producer). Structure is reported through a SAX-like
 * event protocol; part bodies are delivered through pausable async
 * iterators. Peak memory is bounded by a global byte budget and never
 * grows with the size of any single body.
 */

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

export type Header = { name: string; value: string };

export interface ContentTypeInfo {
  /** Raw header value ('' when the header was absent). */
  raw: string;
  /** Lowercased top-level type, e.g. 'multipart'. Defaults to 'text'. */
  type: string;
  /** Lowercased subtype, e.g. 'alternative'. Defaults to 'plain'. */
  subtype: string;
  /** Lowercased parameter names; values keep their case (boundary!). */
  params: Record<string, string>;
}

/** A node of the message tree: the message itself or any (nested) part. */
export interface MimeNode {
  readonly headers: Header[];
  readonly contentType: ContentTypeInfo;
  readonly isMultipart: boolean;
  /** Absolute byte offset where this node's header block starts. */
  readonly offset: number;
  /** Absolute byte offset where this node's body starts. */
  readonly bodyOffset: number;
  /**
   * Multipart nodes only: children are appended as their headers complete,
   * so the tree materialises incrementally while parsing.
   */
  readonly children: MimeNode[];
  /**
   * Leaf nodes only: pausable async iterator of body chunks. Not calling
   * `next()` applies backpressure to the parser. Breaking out of iteration
   * (`return()`) discards the remainder of this body.
   */
  readonly body: AsyncIterableIterator<Uint8Array> | null;
}

export type DiagnosticCode =
  /** A header line without a colon (and not a continuation) was skipped. */
  | 'malformed-header-line'
  /** Header block exceeded the configured cap; remainder treated as body. */
  | 'headers-too-large'
  /** A boundary appeared before the blank line terminating the headers. */
  | 'unterminated-headers'
  /** Content-Type claimed multipart/* but no boundary parameter exists. */
  | 'missing-content-type-boundary'
  /** A multipart body ended without any boundary delimiter at all. */
  | 'missing-boundary'
  /** Input ended in the middle of headers or a body (truncated message). */
  | 'unexpected-eof'
  /** Transport padding after a boundary exceeded the sanity limit. */
  | 'delimiter-line-too-long';

export interface MimeDiagnostic {
  code: DiagnosticCode;
  /** Absolute byte offset in the input stream. */
  offset: number;
  message: string;
  /** Node being parsed when the problem was found, when available. */
  node?: MimeNode;
}

/**
 * Event protocol, in emission order:
 *
 *   message-start
 *     (part-start ... part-end)*      // pre-order; children nest inside parents
 *   (diagnostic)*                     // anywhere; parsing continues
 *   message-end                       // only after every child stream closed
 *
 * `part-end` for a multipart node is emitted only after the `part-end` of
 * every descendant. `message-end` is always the last event.
 */
export type ParseEvent =
  | { type: 'message-start'; node: MimeNode; offset: number }
  | { type: 'part-start'; node: MimeNode; parent: MimeNode; offset: number }
  | { type: 'part-end'; node: MimeNode; offset: number }
  | { type: 'diagnostic'; diagnostic: MimeDiagnostic; offset: number }
  | { type: 'message-end'; node: MimeNode; offset: number };

export interface MimeParserOptions {
  /**
   * Event sink. Invoked synchronously in emission order. If it returns a
   * promise the parser does NOT wait for it (flow control belongs to the
   * body iterators); rejections are collected in {@link MimeParser.handlerErrors}.
   */
  onEvent?: (event: ParseEvent) => void | Promise<unknown>;
  /**
   * Global budget in bytes for body data buffered across ALL open body
   * streams. The parser stalls (feed() stays pending) while this much data
   * is queued unconsumed. Default 256 KiB.
   */
  highWaterMark?: number;
  /** Per header-block cap in bytes. Default 64 KiB. */
  maxHeaderBytes?: number;
  /** Maximum size of a single chunk pushed to a body iterator. Default 64 KiB. */
  chunkSize?: number;
}

// ---------------------------------------------------------------------------
// Legacy API (kept for backward compatibility)
// ---------------------------------------------------------------------------

export function parseHeaders(input: string): Header[] {
  const out: Record<string, string> = {};
  for (const line of input.split(/\r?\n/)) {
    const at = line.indexOf(':');
    if (at > 0) out[line.slice(0, at).toLowerCase()] = line.slice(at + 1).trim();
  }
  return Object.entries(out).map(([name, value]) => ({ name, value }));
}

export class MimeStream {
  #buffer = '';
  feed(chunk: string) {
    this.#buffer += chunk;
    const at = this.#buffer.indexOf('\r\n\r\n');
    if (at < 0) return [];
    const head = this.#buffer.slice(0, at);
    this.#buffer = this.#buffer.slice(at + 4);
    return [{ headers: parseHeaders(head), body: this.#buffer }];
  }
}

// ---------------------------------------------------------------------------
// Byte utilities
// ---------------------------------------------------------------------------

const CR = 0x0d;
const LF = 0x0a;
const TAB = 0x09;
const SP = 0x20;
const DASH = 0x2d;

const EMPTY = new Uint8Array(0);
const utf8 = new TextEncoder();

/** Encode an ASCII-ish string (boundary, pattern) as bytes. */
function ascii(s: string): Uint8Array {
  const out = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) out[i] = s.charCodeAt(i) & 0xff;
  return out;
}

/** Decode bytes as latin1 (headers are byte-oriented; no utf-8 mangling). */
function latin1(b: Uint8Array): string {
  let s = '';
  for (let i = 0; i < b.length; i += 8192) {
    s += String.fromCharCode(...b.subarray(i, i + 8192));
  }
  return s;
}

function concatBytes(a: Uint8Array, b: Uint8Array): Uint8Array {
  if (a.length === 0) return b;
  if (b.length === 0) return a;
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}

/** First index of `needle` in `haystack` at or after `from`, or -1. */
function indexOfBytes(haystack: Uint8Array, from: number, needle: Uint8Array): number {
  let i = haystack.indexOf(needle[0], from);
  while (i >= 0 && i + needle.length <= haystack.length) {
    let ok = true;
    for (let j = 1; j < needle.length; j++) {
      if (haystack[i + j] !== needle[j]) { ok = false; break; }
    }
    if (ok) return i;
    i = haystack.indexOf(needle[0], i + 1);
  }
  return -1;
}

const noop = () => undefined;

// ---------------------------------------------------------------------------
// Header / Content-Type helpers
// ---------------------------------------------------------------------------

/**
 * Fold one raw header line into `headers`. Returns false for a malformed
 * line (no colon, or a continuation with nothing to continue).
 */
function addHeaderLine(headers: Header[], line: string): boolean {
  if (line.length === 0) return true;
  const c = line.charCodeAt(0);
  if (c === SP || c === TAB) {
    const last = headers[headers.length - 1];
    if (!last) return false;
    const v = line.trim();
    if (v.length > 0) last.value += ' ' + v;
    return true;
  }
  const at = line.indexOf(':');
  if (at <= 0) return false;
  headers.push({ name: line.slice(0, at).trim().toLowerCase(), value: line.slice(at + 1).trim() });
  return true;
}

function headerValue(headers: Header[], name: string): string | undefined {
  for (const h of headers) if (h.name === name) return h.value;
  return undefined;
}

/** Parse a Content-Type header value. Absent header defaults to text/plain. */
export function parseContentType(raw: string | undefined): ContentTypeInfo {
  if (raw === undefined) return { raw: '', type: 'text', subtype: 'plain', params: {} };
  // Split on ';' outside of quoted strings.
  const segs: string[] = [];
  let cur = '';
  let quoted = false;
  let escaped = false;
  for (const ch of raw) {
    if (escaped) { cur += ch; escaped = false; continue; }
    if (quoted && ch === '\\') { cur += ch; escaped = true; continue; }
    if (ch === '"') { quoted = !quoted; cur += ch; continue; }
    if (ch === ';' && !quoted) { segs.push(cur); cur = ''; continue; }
    cur += ch;
  }
  segs.push(cur);
  const main = segs[0] ?? '';
  const slash = main.indexOf('/');
  let type = '';
  let subtype = '';
  if (slash > 0) {
    type = main.slice(0, slash).trim().toLowerCase();
    subtype = main.slice(slash + 1).trim().toLowerCase();
  } else {
    type = main.trim().toLowerCase();
  }
  const params: Record<string, string> = {};
  for (const seg of segs.slice(1)) {
    const eq = seg.indexOf('=');
    if (eq <= 0) continue;
    const name = seg.slice(0, eq).trim().toLowerCase();
    let value = seg.slice(eq + 1).trim();
    if (value.length >= 2 && value.startsWith('"') && value.endsWith('"')) {
      value = value.slice(1, -1).replace(/\\(.)/g, '$1');
    }
    if (name) params[name] = value;
  }
  return { raw, type, subtype, params };
}

/**
 * Classify a line that starts with `--boundary`: 'close' for `--boundary--`,
 * 'next' for a bare `--boundary`, both allowing trailing transport padding
 * (SP/TAB). Returns null when the line is not a valid delimiter.
 */
function classifyDelimiterLine(line: Uint8Array, dash: Uint8Array): 'next' | 'close' | null {
  if (line.length < dash.length) return null;
  for (let i = 0; i < dash.length; i++) if (line[i] !== dash[i]) return null;
  let i = dash.length;
  let closing = false;
  if (i + 1 < line.length && line[i] === DASH && line[i + 1] === DASH) {
    closing = true;
    i += 2;
  }
  for (; i < line.length; i++) {
    if (line[i] !== SP && line[i] !== TAB) return null;
  }
  return closing ? 'close' : 'next';
}

// ---------------------------------------------------------------------------
// BufferBudget: global backpressure across all open body streams
// ---------------------------------------------------------------------------

class BufferBudget {
  buffered = 0;
  peak = 0;
  private waiters: Array<{ n: number; grant: () => void }> = [];

  constructor(readonly limit: number) {}

  /** Resolves once `n` bytes fit in the budget. A chunk larger than the
   *  limit is granted as soon as the budget is empty (progress guarantee). */
  acquire(n: number): Promise<void> | void {
    if (this.waiters.length === 0 && (this.buffered + n <= this.limit || this.buffered === 0)) {
      this.add(n);
      return;
    }
    return new Promise<void>((resolve) => {
      this.waiters.push({ n, grant: () => { this.add(n); resolve(); } });
    });
  }

  private add(n: number): void {
    this.buffered += n;
    if (this.buffered > this.peak) this.peak = this.buffered;
  }

  release(n: number): void {
    this.buffered -= n;
    while (this.waiters.length > 0) {
      const w = this.waiters[0];
      if (this.buffered + w.n > this.limit && this.buffered !== 0) break;
      this.waiters.shift();
      w.grant();
    }
  }
}

// ---------------------------------------------------------------------------
// BodyStream: pausable async iterator + parser-side sink
// ---------------------------------------------------------------------------

class BodyStream implements AsyncIterableIterator<Uint8Array> {
  private queue: Uint8Array[] = [];
  private done = false;
  private error: unknown = null;
  private waiters: Array<() => void> = [];

  constructor(private readonly budget: BufferBudget) {}

  /** Parser side: push a chunk, awaiting budget (backpressure). */
  async write(chunk: Uint8Array): Promise<void> {
    if (this.done || chunk.length === 0) return;
    await this.budget.acquire(chunk.length);
    if (this.done) { this.budget.release(chunk.length); return; }
    this.queue.push(chunk);
    this.notify();
  }

  /** Parser side: no more data for this body. */
  async end(): Promise<void> {
    this.close();
  }

  close(error?: unknown): void {
    if (this.done) return;
    this.done = true;
    if (error !== undefined && error !== null) this.error = error;
    this.notify();
  }

  private notify(): void {
    const waiters = this.waiters.splice(0);
    for (const w of waiters) w();
  }

  next(): Promise<IteratorResult<Uint8Array>> {
    return new Promise((resolve, reject) => {
      const attempt = (): void => {
        const chunk = this.queue.shift();
        if (chunk !== undefined) {
          this.budget.release(chunk.length);
          resolve({ value: chunk, done: false });
          return;
        }
        if (this.error !== null) {
          const e = this.error;
          this.error = null;
          reject(e);
          return;
        }
        if (this.done) {
          resolve({ value: undefined, done: true });
          return;
        }
        this.waiters.push(attempt);
      };
      attempt();
    });
  }

  /** Consumer abandoned the body: release buffered bytes, drop future writes. */
  async return(): Promise<IteratorResult<Uint8Array>> {
    if (!this.done) {
      this.done = true;
      for (const c of this.queue) this.budget.release(c.length);
      this.queue.length = 0;
      this.notify();
    }
    return { value: undefined, done: true };
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<Uint8Array> {
    return this;
  }
}

// ---------------------------------------------------------------------------
// Internal plumbing shared by the parser and every multipart level
// ---------------------------------------------------------------------------

interface ByteSink {
  write(chunk: Uint8Array): Promise<void>;
  end(): Promise<void>;
}

interface ParserContext {
  readonly budget: BufferBudget;
  readonly maxHeaderBytes: number;
  readonly chunkSize: number;
  emit(event: ParseEvent): void;
  diag(code: DiagnosticCode, offset: number, message: string, node?: MimeNode): void;
}

class MimeNodeImpl {
  readonly children: MimeNode[] = [];
  body: BodyStream | null = null;
  readonly isMultipart: boolean;
  bodyOffset = -1;

  constructor(
    readonly headers: Header[],
    readonly contentType: ContentTypeInfo,
    readonly offset: number,
  ) {
    this.isMultipart = contentType.type === 'multipart';
  }
}

// ---------------------------------------------------------------------------
// MultipartScanner: one nesting level. Splits its byte stream at the
// boundary and feeds each encapsulated part into a child sink (a BodyStream
// for leaf parts, another MultipartScanner for nested multiparts).
//
// States: Preamble -> (PartHeaders -> PartBody -> DelimiterLine)* -> Epilogue
//         -> Done. DelimiterLine may fall back to the state it came from when
//         a candidate turns out to be body content (e.g. a nested boundary
//         that merely shares a prefix with this level's boundary).
// ---------------------------------------------------------------------------

const enum ScannerState {
  Preamble,
  PartHeaders,
  PartBody,
  DelimiterLine,
  Epilogue,
  Done,
}

const MAX_DELIMITER_PADDING = 256;

class MultipartScanner implements ByteSink {
  private state: ScannerState = ScannerState.Preamble;
  private buf: Uint8Array = EMPTY;
  private pos = 0;
  private consumed = 0;
  private atStart = true;

  // Current part under construction.
  private headers: Header[] = [];
  private headerBytes = 0;
  private partOffset = 0;
  private node: MimeNodeImpl | null = null;
  private sink: ByteSink | null = null;

  // Candidate delimiter (kept so it can be re-emitted as body on a false match).
  private candidate: number[] = [];
  private candidateClosing = false;
  private resumeState: ScannerState = ScannerState.Preamble;

  private readonly dash: Uint8Array; // "--boundary"
  private readonly pat: Uint8Array;  // "\n--boundary"

  constructor(
    boundary: string,
    private readonly baseOffset: number,
    private readonly ctx: ParserContext,
    private readonly parentNode: MimeNodeImpl,
  ) {
    this.dash = ascii('--' + boundary);
    this.pat = concatBytes(ascii('\n'), this.dash);
  }

  private absOffset(): number {
    return this.baseOffset + this.consumed;
  }

  private remaining(): number {
    return this.buf.length - this.pos;
  }

  private consume(n: number): void {
    this.pos += n;
    this.consumed += n;
    if (n > 0) this.atStart = false;
  }

  /** Release the consumed prefix; copy when the remainder is small so a
   *  tiny carry never pins a large producer buffer. */
  private compact(): void {
    if (this.pos === 0) return;
    const rest = this.buf.subarray(this.pos);
    this.buf = rest.length <= 4096 ? rest.slice() : rest;
    this.pos = 0;
  }

  async write(chunk: Uint8Array): Promise<void> {
    if (this.state === ScannerState.Done || chunk.length === 0) return;
    this.buf = this.pos === 0
      ? (this.buf.length === 0 ? chunk : concatBytes(this.buf, chunk))
      : concatBytes(this.buf.subarray(this.pos), chunk);
    this.pos = 0;
    await this.run();
    this.compact();
  }

  async end(): Promise<void> {
    if (this.state === ScannerState.Done) return;
    switch (this.state) {
      case ScannerState.Preamble:
        if (this.consumed + this.remaining() > 0) {
          this.ctx.diag('missing-boundary', this.absOffset(),
            'multipart body ended before any boundary delimiter', this.parentNode);
        }
        break;

      case ScannerState.PartHeaders: {
        if (this.remaining() > 0) {
          const lineOffset = this.absOffset();
          let end = this.buf.length;
          if (end > this.pos && this.buf[end - 1] === CR) end--;
          const line = this.buf.subarray(this.pos, end);
          this.consume(this.remaining());
          const cls = classifyDelimiterLine(line, this.dash);
          if (cls !== null) {
            this.beginPart();
            this.ctx.diag('unterminated-headers', lineOffset,
              'boundary encountered before blank line; part has no body', this.node ?? undefined);
            await this.endSegment();
          } else {
            if (line.length > 0 && !addHeaderLine(this.headers, latin1(line))) {
              this.ctx.diag('malformed-header-line', lineOffset, 'header line without colon');
            }
            if (this.headers.length > 0) {
              this.ctx.diag('unexpected-eof', this.absOffset(),
                'end of data inside part headers', undefined);
              this.beginPart();
              await this.endSegment();
            }
          }
        } else if (this.headers.length > 0) {
          this.ctx.diag('unexpected-eof', this.absOffset(),
            'end of data inside part headers', undefined);
          this.beginPart();
          await this.endSegment();
        }
        break;
      }

      case ScannerState.DelimiterLine: {
        // Stream cut right after a candidate delimiter. This is the normal
        // way a well-formed nested multipart ends (the parent delimiter's
        // CRLF is not part of this stream), so be lenient.
        const start = this.candidate[0] === CR ? 1 : 0;
        const extra = this.candidate.slice(start + this.pat.length);
        const paddingOnly = (a: number[]) => a.every((b) => b === SP || b === TAB);
        const isClose = extra.length >= 2 && extra[0] === DASH && extra[1] === DASH
          && paddingOnly(extra.slice(2));
        const isNext = extra.length === 0 || paddingOnly(extra)
          || (extra.length === 1 && extra[0] === CR);
        const candidate = Uint8Array.from(this.candidate);
        this.candidate.length = 0;
        if (isClose || isNext) {
          if (this.resumeState === ScannerState.PartBody) await this.endSegment();
        } else {
          // Not a delimiter after all: the bytes were body content.
          if (this.resumeState === ScannerState.PartBody) {
            await this.emitBody(candidate);
            this.ctx.diag('unexpected-eof', this.absOffset(),
              'end of data inside part body; missing closing boundary', this.node ?? undefined);
            await this.endSegment();
          }
        }
        break;
      }

      case ScannerState.PartBody: {
        const rest = this.buf.subarray(this.pos);
        this.consume(this.remaining());
        if (rest.length > 0) await this.emitBody(rest);
        this.ctx.diag('unexpected-eof', this.absOffset(),
          'end of data inside part body; missing closing boundary', this.node ?? undefined);
        await this.endSegment();
        break;
      }

      case ScannerState.Epilogue:
        break;
    }
    this.state = ScannerState.Done;
  }

  private async run(): Promise<void> {
    for (;;) {
      let progress = false;
      switch (this.state) {
        case ScannerState.Preamble: progress = this.scanPreamble(); break;
        case ScannerState.PartHeaders: progress = await this.scanHeaders(); break;
        case ScannerState.PartBody: progress = await this.scanBody(); break;
        case ScannerState.DelimiterLine: progress = await this.scanDelimiterLine(); break;
        case ScannerState.Epilogue:
          this.consume(this.remaining());
          progress = false;
          break;
        case ScannerState.Done:
          return;
      }
      if (!progress) return;
    }
  }

  // -- Preamble ------------------------------------------------------------

  private scanPreamble(): boolean {
    const rem = this.remaining();
    if (rem === 0) return false;
    // A delimiter at the very start of the stream needs no leading LF.
    if (this.atStart) {
      const n = Math.min(rem, this.dash.length);
      let match = n > 0;
      for (let i = 0; i < n && match; i++) {
        if (this.buf[this.pos + i] !== this.dash[i]) match = false;
      }
      if (match) {
        if (n < this.dash.length) return false; // wait for more bytes
        this.candidate = Array.from(this.dash);
        this.resumeState = ScannerState.Preamble;
        this.consume(this.dash.length);
        this.state = ScannerState.DelimiterLine;
        return true;
      }
    }
    const idx = indexOfBytes(this.buf, this.pos, this.pat);
    if (idx >= 0) {
      this.candidate = Array.from(this.pat);
      this.resumeState = ScannerState.Preamble;
      this.consume(idx - this.pos + this.pat.length); // discard preamble + pattern
      this.state = ScannerState.DelimiterLine;
      return true;
    }
    const drop = rem - this.holdbackLength();
    if (drop > 0) this.consume(drop); // discard preamble
    return false;
  }

  // -- Part headers ----------------------------------------------------------

  private async scanHeaders(): Promise<boolean> {
    const idx = this.buf.indexOf(LF, this.pos);
    if (idx < 0) {
      if (this.headerBytes + this.remaining() > this.ctx.maxHeaderBytes) {
        this.ctx.diag('headers-too-large', this.absOffset(),
          'part headers exceed limit; treating remainder as body');
        this.beginPart();
        return true;
      }
      return false;
    }
    const lineOffset = this.absOffset();
    let end = idx;
    if (end > this.pos && this.buf[end - 1] === CR) end--;
    const line = this.buf.subarray(this.pos, end);
    this.headerBytes += idx - this.pos + 1;
    this.consume(idx - this.pos + 1);

    const cls = classifyDelimiterLine(line, this.dash);
    if (cls !== null) {
      // Boundary before the blank line: recover with an empty body.
      this.beginPart();
      this.ctx.diag('unterminated-headers', lineOffset,
        'boundary encountered before blank line; part has no body', this.node ?? undefined);
      await this.endSegment();
      if (cls === 'close') {
        this.state = ScannerState.Epilogue;
      } else {
        this.headers = [];
        this.headerBytes = 0;
        this.partOffset = this.absOffset();
        this.state = ScannerState.PartHeaders;
      }
      return true;
    }
    if (line.length === 0) {
      this.beginPart();
      return true;
    }
    if (!addHeaderLine(this.headers, latin1(line))) {
      this.ctx.diag('malformed-header-line', lineOffset, 'header line without colon');
    }
    if (this.headerBytes > this.ctx.maxHeaderBytes) {
      this.ctx.diag('headers-too-large', lineOffset,
        'part headers exceed limit; treating remainder as body');
      this.beginPart();
    }
    return true;
  }

  private beginPart(): void {
    const headers = this.headers;
    this.headers = [];
    this.headerBytes = 0;
    const ct = parseContentType(headerValue(headers, 'content-type'));
    const node = new MimeNodeImpl(headers, ct, this.partOffset);
    node.bodyOffset = this.absOffset();
    this.parentNode.children.push(node);
    this.node = node;
    if (ct.type === 'multipart') {
      const boundary = ct.params['boundary'];
      if (boundary) {
        this.sink = new MultipartScanner(boundary, this.absOffset(), this.ctx, node);
      } else {
        this.ctx.diag('missing-content-type-boundary', node.offset,
          'multipart part without boundary parameter; treating body as opaque', node);
        node.body = new BodyStream(this.ctx.budget);
        this.sink = node.body;
      }
    } else {
      node.body = new BodyStream(this.ctx.budget);
      this.sink = node.body;
    }
    this.ctx.emit({ type: 'part-start', node, parent: this.parentNode, offset: node.offset });
    this.state = ScannerState.PartBody;
  }

  private async endSegment(): Promise<void> {
    const sink = this.sink;
    const node = this.node;
    this.sink = null;
    this.node = null;
    if (sink) await sink.end(); // cascades into nested scanners
    if (node) this.ctx.emit({ type: 'part-end', node, offset: this.absOffset() });
  }

  // -- Part body -------------------------------------------------------------

  private async scanBody(): Promise<boolean> {
    const rem = this.remaining();
    if (rem === 0) return false;
    const idx = indexOfBytes(this.buf, this.pos, this.pat);
    if (idx < 0) {
      const emitLen = rem - this.holdbackLength();
      if (emitLen <= 0) return false;
      const data = this.buf.subarray(this.pos, this.pos + emitLen);
      this.consume(emitLen);
      await this.emitBody(data);
      return true;
    }
    // Candidate delimiter at idx. A CR immediately before it belongs to the
    // delimiter (RFC 2046), unless the candidate turns out to be a false match.
    let bodyEnd = idx;
    let strippedCR = false;
    if (bodyEnd > this.pos && this.buf[bodyEnd - 1] === CR) {
      bodyEnd--;
      strippedCR = true;
    }
    const body = this.buf.subarray(this.pos, bodyEnd);
    this.candidate = strippedCR ? [CR, ...this.pat] : Array.from(this.pat);
    this.candidateClosing = false;
    this.resumeState = ScannerState.PartBody;
    this.consume(idx - this.pos + this.pat.length);
    this.state = ScannerState.DelimiterLine;
    if (body.length > 0) await this.emitBody(body);
    return true;
  }

  /**
   * Length of the buffer suffix that may still grow into a delimiter: the
   * longest suffix that is a proper prefix of the pattern, plus a trailing
   * CR (which may precede a pattern arriving in the next chunk).
   */
  private holdbackLength(): number {
    const rem = this.remaining();
    const max = Math.min(this.pat.length - 1, rem);
    for (let l = max; l >= 1; l--) {
      const off = this.buf.length - l;
      let ok = true;
      for (let j = 0; j < l; j++) {
        if (this.buf[off + j] !== this.pat[j]) { ok = false; break; }
      }
      if (ok) {
        // A CR immediately before the held suffix may be the delimiter's CR.
        if (off > this.pos && this.buf[off - 1] === CR) return l + 1;
        return l;
      }
    }
    if (rem > 0 && this.buf[this.buf.length - 1] === CR) return 1;
    return 0;
  }

  private async emitBody(data: Uint8Array): Promise<void> {
    const sink = this.sink;
    if (!sink || data.length === 0) return;
    const cs = this.ctx.chunkSize;
    for (let off = 0; off < data.length; off += cs) {
      // slice() copies: queued chunks must not pin a larger producer buffer.
      await sink.write(data.slice(off, Math.min(off + cs, data.length)));
    }
  }

  // -- Delimiter line ----------------------------------------------------------

  private async scanDelimiterLine(): Promise<boolean> {
    for (;;) {
      const rem = this.remaining();
      if (rem === 0) return false;
      const b = this.buf[this.pos];
      if (!this.candidateClosing && b === DASH) {
        if (rem < 2) return false;
        if (this.buf[this.pos + 1] !== DASH) return await this.invalidDelimiter();
        this.candidate.push(DASH, DASH);
        this.candidateClosing = true;
        this.consume(2);
        continue;
      }
      if (b === SP || b === TAB) {
        if (this.candidate.length > this.pat.length + MAX_DELIMITER_PADDING) {
          this.ctx.diag('delimiter-line-too-long', this.absOffset(),
            'transport padding after boundary too long', this.node ?? undefined);
          return await this.invalidDelimiter();
        }
        this.candidate.push(b);
        this.consume(1);
        continue;
      }
      if (b === LF) {
        this.consume(1);
        return await this.validDelimiter(this.candidateClosing);
      }
      if (b === CR) {
        if (rem < 2) return false;
        if (this.buf[this.pos + 1] !== LF) return await this.invalidDelimiter();
        this.consume(2);
        return await this.validDelimiter(this.candidateClosing);
      }
      return await this.invalidDelimiter();
    }
  }

  private async validDelimiter(closing: boolean): Promise<boolean> {
    this.candidate.length = 0;
    this.candidateClosing = false;
    if (this.resumeState === ScannerState.PartBody) await this.endSegment();
    if (closing) {
      this.state = ScannerState.Epilogue;
    } else {
      this.headers = [];
      this.headerBytes = 0;
      this.partOffset = this.absOffset();
      this.state = ScannerState.PartHeaders;
    }
    return true;
  }

  /** False match (e.g. a nested boundary sharing a prefix): the candidate
   *  bytes are body content; resume scanning right after them. */
  private async invalidDelimiter(): Promise<boolean> {
    const resume = this.resumeState;
    if (resume === ScannerState.PartBody && this.candidate.length > 0) {
      await this.emitBody(Uint8Array.from(this.candidate));
    }
    this.candidate.length = 0;
    this.candidateClosing = false;
    this.state = resume;
    return true;
  }
}

// ---------------------------------------------------------------------------
// MimeParser: message headers, then either a root MultipartScanner
// (multipart/* message) or a single BodyStream (leaf message).
// ---------------------------------------------------------------------------

const enum ParserState {
  MessageHeaders,
  Body,
  Done,
}

export class MimeParser {
  private state: ParserState = ParserState.MessageHeaders;
  private buf: Uint8Array = EMPTY;
  private pos = 0;
  private consumed = 0;
  private headers: Header[] = [];
  private headerBytes = 0;
  private pendingDash: Uint8Array | null = null;
  private sink: ByteSink | null = null;
  private messageNode: MimeNodeImpl | null = null;
  private tail: Promise<void> = Promise.resolve();

  private readonly budget: BufferBudget;
  private readonly maxHeaderBytes: number;
  private readonly chunkSize: number;
  private readonly onEvent: (e: ParseEvent) => void | Promise<unknown>;
  private readonly ctx: ParserContext;

  /** Errors thrown (or rejected) by the user's onEvent handler. */
  readonly handlerErrors: unknown[] = [];

  constructor(options: MimeParserOptions = {}) {
    this.onEvent = options.onEvent ?? (() => undefined);
    this.budget = new BufferBudget(options.highWaterMark ?? 256 * 1024);
    this.maxHeaderBytes = options.maxHeaderBytes ?? 64 * 1024;
    this.chunkSize = Math.max(1, Math.min(options.chunkSize ?? 64 * 1024, this.budget.limit));
    this.ctx = {
      budget: this.budget,
      maxHeaderBytes: this.maxHeaderBytes,
      chunkSize: this.chunkSize,
      emit: (e) => this.dispatch(e),
      diag: (code, offset, message, node) =>
        this.dispatch({ type: 'diagnostic', diagnostic: { code, offset, message, node }, offset }),
    };
  }

  /** The message node; available from the 'message-start' event onward. */
  get message(): MimeNode | null {
    return this.messageNode;
  }

  get stats(): { bufferedBytes: number; peakBufferedBytes: number; bytesConsumed: number } {
    return {
      bufferedBytes: this.budget.buffered,
      peakBufferedBytes: this.budget.peak,
      bytesConsumed: this.consumed,
    };
  }

  /** 'headers' | 'body' | 'done' */
  get parserState(): 'headers' | 'body' | 'done' {
    return this.state === ParserState.MessageHeaders ? 'headers'
      : this.state === ParserState.Body ? 'body'
      : 'done';
  }

  /**
   * Push one chunk. The returned promise resolves when the chunk has been
   * fully processed; it stays pending while consumers are not draining body
   * streams (backpressure). Await it to keep memory bounded.
   */
  feed(chunk: Uint8Array | string): Promise<void> {
    const bytes = typeof chunk === 'string' ? utf8.encode(chunk) : chunk;
    const p = this.tail.then(() => this.write(bytes));
    this.tail = p.then(noop, noop);
    return p;
  }

  /** Signal end of input. Resolves after all streams are closed and the
   *  final events (including 'message-end') have been dispatched. */
  end(): Promise<void> {
    const p = this.tail.then(() => this.finish());
    this.tail = p.then(noop, noop);
    return p;
  }

  private dispatch(e: ParseEvent): void {
    try {
      const r = this.onEvent(e) as Promise<unknown> | undefined;
      if (r && typeof r.catch === 'function') {
        r.catch((err) => this.handlerErrors.push(err));
      }
    } catch (err) {
      this.handlerErrors.push(err);
    }
  }

  private currentOffset(): number {
    return this.consumed - (this.buf.length - this.pos);
  }

  private async write(chunk: Uint8Array): Promise<void> {
    if (this.state === ParserState.Done) throw new Error('MimeParser: feed() after end()');
    this.consumed += chunk.length;
    if (this.sink) {
      await this.sink.write(chunk);
      return;
    }
    this.buf = this.pos === 0
      ? (this.buf.length === 0 ? chunk : concatBytes(this.buf, chunk))
      : concatBytes(this.buf.subarray(this.pos), chunk);
    this.pos = 0;
    await this.runMessageHeaders();
  }

  private async runMessageHeaders(): Promise<void> {
    for (;;) {
      const idx = this.buf.indexOf(LF, this.pos);
      if (idx < 0) {
        if (this.headerBytes + (this.buf.length - this.pos) > this.maxHeaderBytes) {
          this.ctx.diag('headers-too-large', this.currentOffset(),
            'message headers exceed limit; treating remainder as body');
          await this.routeMessage();
        }
        return;
      }
      let end = idx;
      if (end > this.pos && this.buf[end - 1] === CR) end--;
      const line = this.buf.subarray(this.pos, end);
      const lineOffset = this.currentOffset();
      // A boundary line before the blank line: recover by starting the body.
      if (this.pendingDash && classifyDelimiterLine(line, this.pendingDash) !== null) {
        this.ctx.diag('unterminated-headers', lineOffset,
          'boundary encountered before blank line; message headers truncated');
        await this.routeMessage();
        return;
      }
      this.headerBytes += idx - this.pos + 1;
      this.pos = idx + 1;
      if (line.length === 0) {
        await this.routeMessage();
        return;
      }
      if (!addHeaderLine(this.headers, latin1(line))) {
        this.ctx.diag('malformed-header-line', lineOffset, 'header line without colon');
      } else {
        const last = this.headers[this.headers.length - 1];
        if (last.name === 'content-type') {
          const ct = parseContentType(last.value);
          if (ct.type === 'multipart' && ct.params['boundary']) {
            this.pendingDash = ascii('--' + ct.params['boundary']);
          }
        }
      }
      if (this.headerBytes > this.maxHeaderBytes) {
        this.ctx.diag('headers-too-large', lineOffset,
          'message headers exceed limit; treating remainder as body');
        await this.routeMessage();
        return;
      }
    }
  }

  private async routeMessage(): Promise<void> {
    const headers = this.headers;
    this.headers = [];
    const ct = parseContentType(headerValue(headers, 'content-type'));
    const node = new MimeNodeImpl(headers, ct, 0);
    node.bodyOffset = this.currentOffset();
    this.messageNode = node;
    if (ct.type === 'multipart') {
      const boundary = ct.params['boundary'];
      if (boundary) {
        this.sink = new MultipartScanner(boundary, this.currentOffset(), this.ctx, node);
      } else {
        this.ctx.diag('missing-content-type-boundary', 0,
          'multipart message without boundary parameter; treating body as opaque', node);
        node.body = new BodyStream(this.budget);
        this.sink = node.body;
      }
    } else {
      node.body = new BodyStream(this.budget);
      this.sink = node.body;
    }
    this.dispatch({ type: 'message-start', node, offset: 0 });
    this.state = ParserState.Body;
    const rest = this.buf.subarray(this.pos);
    this.buf = EMPTY;
    this.pos = 0;
    if (rest.length > 0) await this.sink.write(rest);
  }

  private async finish(): Promise<void> {
    if (this.state === ParserState.Done) return;
    this.state = ParserState.Done;
    if (!this.messageNode) {
      // EOF while still reading message headers.
      if (this.consumed === 0) {
        this.ctx.diag('unexpected-eof', 0, 'empty input: no message');
        return;
      }
      this.ctx.diag('unexpected-eof', this.currentOffset(),
        'end of data inside message headers');
      this.buf = EMPTY; // discard the unterminated header fragment
      this.pos = 0;
      await this.routeMessage();
    }
    if (this.sink) await this.sink.end();
    if (this.messageNode) {
      this.dispatch({ type: 'message-end', node: this.messageNode, offset: this.consumed });
    }
  }
}

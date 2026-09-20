import { concatBytes, indexOfBytes, suffixPrefixOverlap } from './bytes.js';

const LF = 0x0a;
const CR = 0x0d;
const DASH = 0x2d;
const SP = 0x20;
const TAB = 0x09;

const EMPTY = new Uint8Array(0);
const NEEDMORE = Symbol('needmore');

export interface ScanResult {
  /** Body bytes now known not to be part of a delimiter, in order. */
  data: Uint8Array[];
  /** Set when a delimiter line was found and consumed. */
  hit: { closing: boolean } | null;
  /**
   * How many bytes of the chunk passed to {@link BoundaryScanner.feed} were
   * consumed. Bytes beyond this offset belong to whatever comes after the
   * delimiter and must be reprocessed by the caller.
   */
  consumed: number;
}

interface Hit {
  kind: 'hit';
  bodyEnd: number;
  delimEnd: number;
  closing: boolean;
}

interface Hold {
  kind: 'hold';
  holdFrom: number;
}

/**
 * Incremental scanner for one MIME boundary.
 *
 * A delimiter is `\n--<boundary>` optionally followed by `--` (closing),
 * transport padding (SP/TAB) and a line ending. The CRLF preceding a
 * delimiter belongs to the delimiter, not to the body (RFC 2046). The
 * scanner also accepts a delimiter at the very start of the stream (the
 * first boundary of a multipart entity) and tolerates bare-LF line endings.
 *
 * Memory is bounded: at most `boundary.length + maxPadding + 2` bytes are
 * held back between feeds, no matter how large the scanned body is. A
 * boundary candidate followed by more than `maxPadding` bytes of padding is
 * treated as body content.
 *
 * A scanner is single-use per body: once a delimiter is reported, create a
 * new scanner for the next body.
 */
export class BoundaryScanner {
  readonly boundary: string;
  readonly #needle: Uint8Array; // "\n--" + boundary
  readonly #dashBoundary: Uint8Array; // "--" + boundary
  readonly #maxPadding: number;
  #pending: Uint8Array = EMPTY;
  #atStart = true;
  #finished = false;

  constructor(boundary: string, maxPadding = 1024) {
    this.boundary = boundary;
    const bytes = new TextEncoder().encode(boundary);
    const dashBoundary = new Uint8Array(bytes.length + 2);
    dashBoundary[0] = DASH;
    dashBoundary[1] = DASH;
    dashBoundary.set(bytes, 2);
    this.#dashBoundary = dashBoundary;
    const needle = new Uint8Array(dashBoundary.length + 1);
    needle[0] = LF;
    needle.set(dashBoundary, 1);
    this.#needle = needle;
    this.#maxPadding = maxPadding;
  }

  /** Bytes currently held back as a potential delimiter prefix. */
  get pendingBytes(): number {
    return this.#pending.length;
  }

  /**
   * Scan the next chunk. Set `eof` on the final call to flush: a complete
   * delimiter at the very end of input is still honoured; a trailing
   * partial candidate is emitted as body.
   */
  feed(chunk: Uint8Array, eof = false): ScanResult {
    if (this.#finished) throw new Error('BoundaryScanner: feed after delimiter');
    const pending = this.#pending;
    const buf = pending.length > 0 ? concatBytes(pending, chunk) : chunk;
    const base = buf.length - chunk.length;
    const r = this.#scan(buf, eof);
    if (r.kind === 'hit') {
      this.#finished = true;
      this.#pending = EMPTY;
      return {
        data: r.bodyEnd > 0 ? [buf.slice(0, r.bodyEnd)] : [],
        hit: { closing: r.closing },
        consumed: Math.min(chunk.length, Math.max(0, r.delimEnd - base)),
      };
    }
    const data = r.holdFrom > 0 ? [buf.slice(0, r.holdFrom)] : [];
    // Copy: a subarray view would keep the whole (possibly huge) chunk alive.
    this.#pending = buf.slice(r.holdFrom);
    return { data, hit: null, consumed: chunk.length };
  }

  #scan(buf: Uint8Array, eof: boolean): Hit | Hold {
    const needle = this.#needle;
    let i = 0;
    if (this.#atStart) {
      this.#atStart = false;
      const dash = this.#dashBoundary;
      const n = Math.min(buf.length, dash.length);
      let k = 0;
      while (k < n && buf[k] === dash[k]) k++;
      if (k === dash.length) {
        // The stream starts with "--boundary": a delimiter with a virtual CRLF.
        const v = this.#validate(buf, dash.length, eof);
        if (v === NEEDMORE) {
          this.#atStart = true;
          return { kind: 'hold', holdFrom: 0 };
        }
        if (v) return { kind: 'hit', bodyEnd: 0, delimEnd: v.end, closing: v.closing };
        i = dash.length; // false alarm: the leading "--boundary" is body content
      } else if (k === buf.length && !eof) {
        // Everything so far is a proper prefix of "--boundary": wait for more.
        this.#atStart = true;
        return { kind: 'hold', holdFrom: 0 };
      }
    }
    for (;;) {
      const m = indexOfBytes(buf, needle, i);
      if (m < 0) {
        if (eof) return { kind: 'hold', holdFrom: buf.length };
        let hold = suffixPrefixOverlap(buf, needle);
        if (hold > 0) {
          // Keep a CR just before the held suffix: it may belong to the
          // delimiter's CRLF rather than to the body.
          if (buf[buf.length - hold - 1] === CR) hold++;
        } else if (buf.length > 0 && buf[buf.length - 1] === CR) {
          hold = 1; // may combine with "\n--boundary" in the next chunk
        }
        return { kind: 'hold', holdFrom: buf.length - hold };
      }
      const v = this.#validate(buf, m + needle.length, eof);
      if (v === NEEDMORE) {
        const bodyEnd = m > 0 && buf[m - 1] === CR ? m - 1 : m;
        return { kind: 'hold', holdFrom: bodyEnd };
      }
      if (v) {
        const bodyEnd = m > 0 && buf[m - 1] === CR ? m - 1 : m;
        return { kind: 'hit', bodyEnd, delimEnd: v.end, closing: v.closing };
      }
      i = m + 1; // false alarm: the matched bytes are body content
    }
  }

  /**
   * Inspect what follows a matched `--boundary` at index `p`.
   * Returns the delimiter extent, null for a false alarm, or NEEDMORE when
   * the buffer ends before the decision can be made.
   */
  #validate(
    buf: Uint8Array,
    p: number,
    eof: boolean,
  ): { end: number; closing: boolean } | null | typeof NEEDMORE {
    let closing = false;
    if (p >= buf.length) return eof ? { end: p, closing } : NEEDMORE;
    let c = buf[p];
    if (c === DASH) {
      if (p + 1 >= buf.length) return eof ? null : NEEDMORE;
      if (buf[p + 1] !== DASH) return null;
      closing = true;
      p += 2;
      if (p >= buf.length) return eof ? { end: p, closing } : NEEDMORE;
      c = buf[p];
    }
    if (c === SP || c === TAB) {
      let j = p;
      while (j < buf.length && (buf[j] === SP || buf[j] === TAB)) {
        if (j - p + 1 > this.#maxPadding) return null; // padding run too long: body
        j++;
      }
      if (j >= buf.length) return eof ? { end: j, closing } : NEEDMORE;
      p = j;
      c = buf[p];
    }
    if (c === CR) {
      if (p + 1 >= buf.length) return eof ? { end: p + 1, closing } : NEEDMORE;
      if (buf[p + 1] !== LF) return null;
      return { end: p + 2, closing };
    }
    if (c === LF) return { end: p + 1, closing };
    return null;
  }
}

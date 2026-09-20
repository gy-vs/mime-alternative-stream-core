import { collectBytes } from './bytes.js';
import { parseContentType, type ContentType, type Header } from './headers.js';

export type MimeErrorCode =
  | 'bad-header'
  | 'headers-too-large'
  | 'missing-boundary'
  | 'unexpected-eof';

/**
 * A recoverable parse problem, delivered through the `error` event.
 * Carries the absolute byte offset in the input stream and the index path
 * of the part that was being parsed (e.g. [2, 0] is the first child of the
 * root's third child).
 */
export class MimeParseError extends Error {
  readonly code: MimeErrorCode;
  readonly offset: number;
  readonly partPath: readonly number[];

  constructor(code: MimeErrorCode, offset: number, partPath: readonly number[], detail: string) {
    const where = partPath.length > 0 ? ` (part ${partPath.join('.')})` : '';
    super(`${code} at byte ${offset}${where}: ${detail}`);
    this.name = 'MimeParseError';
    this.code = code;
    this.offset = offset;
    this.partPath = partPath;
  }
}

export interface MimePartInit {
  headers: Header[];
  parent: MimePart | null;
  /** Absolute byte offset of this part's header section in the input. */
  offset: number;
  /** Index among the parent's children. */
  index: number;
}

/**
 * One node of the parsed message tree. Multipart nodes expose `children`;
 * leaf nodes expose a streaming `body`. Nodes appear in `parent.children`
 * as soon as their headers are parsed, so previously completed nodes stay
 * available even when a later sibling is corrupt.
 */
export class MimePart {
  readonly headers: readonly Header[];
  readonly parent: MimePart | null;
  readonly offset: number;
  readonly index: number;
  readonly children: MimePart[] = [];
  /**
   * False when the input ended before this part was fully parsed
   * (see the `unexpected-eof` diagnostic).
   */
  complete = true;
  /**
   * Leaf parts only: the pausable body stream. Null for multipart
   * containers. Single-consumer; bytes not yet read are buffered only up to
   * the parser's high-water mark.
   */
  body: AsyncIterable<Uint8Array> | null = null;

  #contentType: ContentType | undefined;

  constructor(init: MimePartInit) {
    this.headers = init.headers;
    this.parent = init.parent;
    this.offset = init.offset;
    this.index = init.index;
  }

  get contentType(): ContentType {
    if (!this.#contentType) this.#contentType = parseContentType(this.header('content-type'));
    return this.#contentType;
  }

  get isMultipart(): boolean {
    return this.contentType.type === 'multipart';
  }

  /** The boundary parameter of a multipart content type, if present. */
  get boundary(): string | undefined {
    return this.contentType.params.boundary;
  }

  /** e.g. "multipart/alternative" */
  get mediaType(): string {
    const ct = this.contentType;
    return ct.subtype ? `${ct.type}/${ct.subtype}` : ct.type;
  }

  /** Index path from the root to this part, e.g. [2, 0]. */
  get path(): number[] {
    const path: number[] = [];
    let node: MimePart | null = this;
    while (node.parent) {
      path.unshift(node.index);
      node = node.parent;
    }
    return path;
  }

  get depth(): number {
    let d = 0;
    let node = this.parent;
    while (node) {
      d++;
      node = node.parent;
    }
    return d;
  }

  /** First header with the given (case-insensitive) name, if any. */
  header(name: string): string | undefined {
    const lower = name.toLowerCase();
    for (const h of this.headers) {
      if (h.name === lower) return h.value;
    }
    return undefined;
  }

  /** Drain the body stream into a single buffer. Throws for containers. */
  async bytes(): Promise<Uint8Array> {
    if (!this.body) throw new Error('MimePart: multipart containers have no body stream');
    return collectBytes(this.body);
  }

  /** Drain the body stream and decode it as UTF-8. */
  async text(): Promise<string> {
    return new TextDecoder().decode(await this.bytes());
  }
}

export interface PartStartEvent {
  type: 'part-start';
  part: MimePart;
}

export interface PartEndEvent {
  type: 'part-end';
  part: MimePart;
}

export interface ErrorEvent {
  type: 'error';
  error: MimeParseError;
}

export interface DoneEvent {
  type: 'done';
  /** The root of the message tree, or null when the top-level headers failed. */
  message: MimePart | null;
  /** Every diagnostic reported during the parse. */
  errors: readonly MimeParseError[];
}

/**
 * The parser's event protocol, in emission order:
 * `part-start` (headers complete; body stream live) → `part-end` (delimiter
 * seen; children always end before parents) → `error` (recoverable
 * diagnostics, any time) → `done` (exactly once, last, and only after every
 * body stream has been closed).
 */
export type ParseEvent = PartStartEvent | PartEndEvent | ErrorEvent | DoneEvent;

import type { Header } from './headers.js';

export type { Header };

/** Legacy string-based header parsing (kept for backward compatibility). */
export function parseHeaders(input: string): Header[] {
  const out: Record<string, string> = {};
  for (const line of input.split(/\r?\n/)) {
    const at = line.indexOf(':');
    if (at > 0) out[line.slice(0, at).toLowerCase()] = line.slice(at + 1).trim();
  }
  return Object.entries(out).map(([name, value]) => ({ name, value }));
}

/**
 * @deprecated The original whole-message splitter. Use {@link MimeParser}
 * for incremental, multipart-aware parsing with backpressure.
 */
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

export { PushQueue } from './async-queue.js';
export { collectBytes } from './bytes.js';
export { isValidHeaderLine, parseContentType, parseHeaderLines } from './headers.js';
export type { ContentType } from './headers.js';
export { MimeParseError, MimePart } from './part.js';
export type {
  DoneEvent,
  ErrorEvent,
  MimeErrorCode,
  ParseEvent,
  PartEndEvent,
  PartStartEvent,
} from './part.js';
export { MimeParser, ParserState } from './parser.js';
export type { MimeParserOptions } from './parser.js';
export { BoundaryScanner } from './scanner.js';
export type { ScanResult } from './scanner.js';

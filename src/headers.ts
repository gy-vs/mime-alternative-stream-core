import { concatBytes } from './bytes.js';

export interface Header {
  name: string;
  value: string;
}

export interface ContentType {
  /** Lowercased top-level type, e.g. "multipart". Empty when unparseable. */
  type: string;
  /** Lowercased subtype, e.g. "alternative". */
  subtype: string;
  /** Lowercased parameter names mapped to unquoted values. */
  params: Record<string, string>;
  /** The raw header value, when present. */
  raw: string | undefined;
}

const SP = 0x20;
const TAB = 0x09;
const COLON = 0x3a;

const decoder = new TextDecoder();

function isTokenChar(c: number): boolean {
  return (
    (c >= 0x30 && c <= 0x39) || // 0-9
    (c >= 0x41 && c <= 0x5a) || // A-Z
    (c >= 0x61 && c <= 0x7a) || // a-z
    c === 0x21 || // !
    c === 0x23 || // #
    c === 0x24 || // $
    c === 0x25 || // %
    c === 0x26 || // &
    c === 0x27 || // '
    c === 0x2a || // *
    c === 0x2b || // +
    c === 0x2d || // -
    c === 0x2e || // .
    c === 0x5e || // ^
    c === 0x5f || // _
    c === 0x60 || // `
    c === 0x7c || // |
    c === 0x7e // ~
  );
}

/**
 * Validate one unfolded-or-not header line (already stripped of its line
 * ending). Lines starting with SP/TAB are RFC 5322 continuations and are
 * only valid when a previous header exists.
 */
export function isValidHeaderLine(line: Uint8Array, hasPrevious: boolean): boolean {
  if (line.length === 0) return false;
  if (line[0] === SP || line[0] === TAB) return hasPrevious;
  const colon = line.indexOf(COLON);
  if (colon <= 0) return false;
  for (let i = 0; i < colon; i++) {
    if (!isTokenChar(line[i])) return false;
  }
  return true;
}

/**
 * Build the header list from raw lines, unfolding continuations.
 * Assumes lines were validated with {@link isValidHeaderLine}.
 */
export function parseHeaderLines(lines: readonly Uint8Array[]): Header[] {
  const headers: Header[] = [];
  for (const line of lines) {
    const first = line[0];
    if ((first === SP || first === TAB) && headers.length > 0) {
      headers[headers.length - 1].value += ' ' + decoder.decode(line).trim();
      continue;
    }
    const colon = line.indexOf(COLON);
    if (colon <= 0) continue; // defensive; the parser validates first
    headers.push({
      name: decoder.decode(line.subarray(0, colon)).trim().toLowerCase(),
      value: decoder.decode(line.subarray(colon + 1)).trim(),
    });
  }
  return headers;
}

/** Split a header value on ";" while respecting quoted strings. */
function splitParams(raw: string): string[] {
  const out: string[] = [];
  let cur = '';
  let quoted = false;
  let escaped = false;
  for (const ch of raw) {
    if (escaped) {
      cur += ch;
      escaped = false;
    } else if (ch === '\\' && quoted) {
      cur += ch;
      escaped = true;
    } else if (ch === '"') {
      quoted = !quoted;
      cur += ch;
    } else if (ch === ';' && !quoted) {
      out.push(cur);
      cur = '';
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out;
}

/**
 * Parse a Content-Type header value. Missing or malformed values fall back
 * to the RFC 2045 default of text/plain. RFC 2231 parameter continuations
 * are not expanded.
 */
export function parseContentType(raw: string | undefined): ContentType {
  if (!raw) return { type: 'text', subtype: 'plain', params: {}, raw };
  const segments = splitParams(raw);
  const main = segments[0].trim().toLowerCase();
  const slash = main.indexOf('/');
  const type = slash > 0 ? main.slice(0, slash).trim() : '';
  const subtype = slash > 0 ? main.slice(slash + 1).trim() : '';
  const params: Record<string, string> = {};
  for (const segment of segments.slice(1)) {
    const eq = segment.indexOf('=');
    if (eq < 0) continue;
    const key = segment.slice(0, eq).trim().toLowerCase();
    let value = segment.slice(eq + 1).trim();
    if (value.length >= 2 && value.startsWith('"') && value.endsWith('"')) {
      value = value.slice(1, -1).replace(/\\(.)/g, '$1');
    }
    if (key) params[key] = value;
  }
  return { type, subtype, params, raw };
}

/** Short printable preview of a raw line, for diagnostics. */
export function previewBytes(line: Uint8Array): string {
  const text = decoder.decode(line.subarray(0, 48));
  return line.length > 48 ? `${text}…` : text;
}

/** Internal: used by the parser when joining a split line. */
export { concatBytes };

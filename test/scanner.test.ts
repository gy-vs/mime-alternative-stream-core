import { describe, expect, it } from 'vitest';
import { BoundaryScanner } from '../src/index.js';

const enc = new TextEncoder();
const dec = new TextDecoder();

function bytes(s: string): Uint8Array {
  return enc.encode(s);
}

function text(parts: Uint8Array[]): string {
  return parts.map((p) => dec.decode(p)).join('');
}

describe('BoundaryScanner', () => {
  it('finds a delimiter and excludes the preceding CRLF from the body', () => {
    const s = new BoundaryScanner('b');
    const r = s.feed(bytes('hello\r\n--b\r\nrest'));
    expect(text(r.data)).toBe('hello');
    expect(r.hit).toEqual({ closing: false });
    expect(bytes('rest')).toEqual(bytes('hello\r\n--b\r\nrest').subarray(r.consumed));
  });

  it('detects the closing delimiter', () => {
    const s = new BoundaryScanner('b');
    const r = s.feed(bytes('x\r\n--b--\r\n'));
    expect(text(r.data)).toBe('x');
    expect(r.hit).toEqual({ closing: true });
  });

  it('matches a boundary at the very start of the stream', () => {
    const s = new BoundaryScanner('b');
    const r = s.feed(bytes('--b\r\n'));
    expect(text(r.data)).toBe('');
    expect(r.hit).toEqual({ closing: false });
  });

  it('splits a delimiter across many tiny chunks', () => {
    const s = new BoundaryScanner('boundary');
    const input = bytes('body\r\n--boundary--\r\n');
    const out: string[] = [];
    let hit: { closing: boolean } | null = null;
    for (let i = 0; i < input.length; i++) {
      const r = s.feed(input.subarray(i, i + 1));
      out.push(text(r.data));
      if (r.hit) hit = r.hit;
    }
    expect(out.join('')).toBe('body');
    expect(hit).toEqual({ closing: true });
  });

  it('does not treat --boundaryXYZ as a delimiter', () => {
    const s = new BoundaryScanner('b');
    const r = s.feed(bytes('keep\r\n--bXYZ and more\r\n--b\r\n'));
    expect(text(r.data)).toBe('keep\r\n--bXYZ and more');
    expect(r.hit).toEqual({ closing: false });
  });

  it('skips transport padding on the delimiter line', () => {
    const s = new BoundaryScanner('b');
    const r = s.feed(bytes('x\r\n--b \t \r\nnext'));
    expect(text(r.data)).toBe('x');
    expect(r.hit).toEqual({ closing: false });
  });

  it('treats padding beyond the cap as body content', () => {
    const s = new BoundaryScanner('b', 8);
    const r = s.feed(bytes('x\r\n--b          \r\n--b--\r\n'));
    // the padded line is body; the real closing delimiter is still found
    expect(text(r.data)).toBe('x\r\n--b          ');
    expect(r.hit).toEqual({ closing: true });
  });

  it('honours a complete delimiter at EOF without a trailing line ending', () => {
    const s = new BoundaryScanner('b');
    const r1 = s.feed(bytes('x\r\n--b'));
    const r2 = s.feed(new Uint8Array(0), true);
    expect(text([...r1.data, ...r2.data])).toBe('x');
    expect(r2.hit).toEqual({ closing: false });
  });

  it('flushes a trailing partial candidate as body at EOF', () => {
    const s = new BoundaryScanner('boundary');
    const r1 = s.feed(bytes('ends with\r\n--boun'));
    const r2 = s.feed(new Uint8Array(0), true);
    expect(text([...r1.data, ...r2.data])).toBe('ends with\r\n--boun');
    expect(r2.hit).toBeNull();
  });

  it('keeps holdback bounded regardless of body size', () => {
    const s = new BoundaryScanner('b');
    const chunk = new Uint8Array(64 * 1024).fill(0x61);
    let emitted = 0;
    for (let i = 0; i < 100; i++) {
      const r = s.feed(chunk);
      emitted += r.data.reduce((n, d) => n + d.length, 0);
      expect(s.pendingBytes).toBeLessThanOrEqual(4); // "\n--b" minus one, plus CR
    }
    expect(emitted).toBe(100 * chunk.length - s.pendingBytes);
  });
});

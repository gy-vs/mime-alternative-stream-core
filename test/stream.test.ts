import { describe, expect, it } from 'vitest';
import { createHash } from 'node:crypto';
import {
  MimeParser,
  type MimeNode,
  type ParseEvent,
} from '../src/index.js';

const enc = new TextEncoder();
const dec = new TextDecoder();
const bytes = (s: string) => enc.encode(s);
const text = (b: Uint8Array) => dec.decode(b);

function collect() {
  const events: ParseEvent[] = [];
  const parser = new MimeParser({ onEvent: (e) => events.push(e) });
  return { parser, events };
}

async function readAll(body: AsyncIterableIterator<Uint8Array>): Promise<Uint8Array> {
  const parts: Uint8Array[] = [];
  for await (const c of body) parts.push(c);
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let o = 0;
  for (const p of parts) { out.set(p, o); o += p.length; }
  return out;
}

function eventTypes(events: ParseEvent[]) {
  return events.map((e) => e.type);
}

function diagnostics(events: ParseEvent[]) {
  return events.filter((e) => e.type === 'diagnostic')
    .map((e) => (e as Extract<ParseEvent, { type: 'diagnostic' }>).diagnostic);
}

const SIMPLE =
  'From: alice@example.com\r\n' +
  'Content-Type: multipart/mixed; boundary="BB"\r\n' +
  '\r\n' +
  'this is the preamble\r\n' +
  '--BB\r\n' +
  'Content-Type: text/plain\r\n' +
  '\r\n' +
  'hello\r\n' +
  '--BB\r\n' +
  'Content-Type: text/html\r\n' +
  '\r\n' +
  '<b>hi</b>\r\n' +
  '--BB--\r\n' +
  'this is the epilogue';

describe('basic multipart parsing', () => {
  it('parses parts, skips preamble/epilogue, emits events in order', async () => {
    const { parser, events } = collect();
    await parser.feed(bytes(SIMPLE));
    await parser.end();

    expect(eventTypes(events)).toEqual([
      'message-start', 'part-start', 'part-end', 'part-start', 'part-end', 'message-end',
    ]);
    const msg = parser.message!;
    expect(msg.contentType.type).toBe('multipart');
    expect(msg.children).toHaveLength(2);
    expect(text(await readAll(msg.children[0].body!))).toBe('hello');
    expect(text(await readAll(msg.children[1].body!))).toBe('<b>hi</b>');
    // message-end carries the total byte count
    const end = events[events.length - 1];
    expect(end.type).toBe('message-end');
    expect(end.offset).toBe(bytes(SIMPLE).length);
    expect(diagnostics(events)).toEqual([]);
  });

  it('produces the identical tree when fed byte by byte', async () => {
    const { parser, events } = collect();
    const all = bytes(SIMPLE);
    for (let i = 0; i < all.length; i++) {
      await parser.feed(all.subarray(i, i + 1));
    }
    await parser.end();

    expect(eventTypes(events)).toEqual([
      'message-start', 'part-start', 'part-end', 'part-start', 'part-end', 'message-end',
    ]);
    const msg = parser.message!;
    expect(text(await readAll(msg.children[0].body!))).toBe('hello');
    expect(text(await readAll(msg.children[1].body!))).toBe('<b>hi</b>');
    expect(diagnostics(events)).toEqual([]);
  });

  it('is insensitive to chunking: random splits parse identically', async () => {
    const NESTED_MSG =
      'Content-Type: multipart/mixed; boundary=OUT\r\n' +
      '\r\n' +
      '--OUT\r\n' +
      'Content-Type: multipart/alternative; boundary=IN\r\n' +
      '\r\n' +
      '--IN\r\n' +
      'Content-Type: text/plain\r\n' +
      '\r\n' +
      'plain text\r\n' +
      '--IN\r\n' +
      'Content-Type: text/html\r\n' +
      '\r\n' +
      '<b>html</b>\r\n' +
      '--IN--\r\n' +
      '--OUT\r\n' +
      'Content-Type: application/octet-stream\r\n' +
      '\r\n' +
      '<binary>\r\n' +
      '--OUT--\r\n';
    const all = bytes(NESTED_MSG);
    let seed = 42;
    const rand = () => (seed = (seed * 1103515245 + 12345) & 0x7fffffff);
    for (let round = 0; round < 20; round++) {
      const { parser, events } = collect();
      let off = 0;
      while (off < all.length) {
        const n = 1 + (rand() % 97);
        await parser.feed(all.subarray(off, Math.min(off + n, all.length)));
        off += n;
      }
      await parser.end();
      const msg = parser.message!;
      const [alt, bin] = msg.children;
      expect(msg.children).toHaveLength(2);
      expect(text(await readAll(alt.children[0].body!))).toBe('plain text');
      expect(text(await readAll(alt.children[1].body!))).toBe('<b>html</b>');
      expect(text(await readAll(bin.body!))).toBe('<binary>');
      expect(diagnostics(events)).toEqual([]);
      expect(events[events.length - 1].type).toBe('message-end');
    }
  });

  it('rejects feed() after end()', async () => {
    const { parser } = collect();
    await parser.feed(bytes('Content-Type: text/plain\r\n\r\nhi'));
    await parser.end();
    await expect(parser.feed(bytes('x'))).rejects.toThrow('after end');
  });

  it('accepts LF-only line endings', async () => {
    const { parser, events } = collect();
    await parser.feed(bytes(SIMPLE.replace(/\r\n/g, '\n')));
    await parser.end();
    const msg = parser.message!;
    expect(msg.children).toHaveLength(2);
    expect(text(await readAll(msg.children[0].body!))).toBe('hello');
    expect(diagnostics(events)).toEqual([]);
  });

  it('parses a non-multipart message as a single leaf body', async () => {
    const { parser, events } = collect();
    await parser.feed(bytes('Content-Type: text/plain\r\n\r\nhello world'));
    await parser.end();
    expect(eventTypes(events)).toEqual(['message-start', 'message-end']);
    expect(text(await readAll(parser.message!.body!))).toBe('hello world');
  });
});

describe('nesting', () => {
  const NESTED =
    'Content-Type: multipart/mixed; boundary=OUT\r\n' +
    '\r\n' +
    '--OUT\r\n' +
    'Content-Type: multipart/alternative; boundary=IN\r\n' +
    '\r\n' +
    '--IN\r\n' +
    'Content-Type: text/plain\r\n' +
    '\r\n' +
    'plain text\r\n' +
    '--IN\r\n' +
    'Content-Type: text/html\r\n' +
    '\r\n' +
    '<b>html</b>\r\n' +
    '--IN--\r\n' +
    '--OUT\r\n' +
    'Content-Type: application/octet-stream\r\n' +
    '\r\n' +
    '<binary>\r\n' +
    '--OUT--\r\n';

  it('builds the nested tree and orders events pre-order', async () => {
    const { parser, events } = collect();
    await parser.feed(bytes(NESTED));
    await parser.end();

    const msg = parser.message!;
    expect(msg.children).toHaveLength(2);
    const [alt, bin] = msg.children;
    expect(alt.isMultipart).toBe(true);
    expect(alt.contentType.subtype).toBe('alternative');
    expect(alt.children).toHaveLength(2);
    expect(text(await readAll(alt.children[0].body!))).toBe('plain text');
    expect(text(await readAll(alt.children[1].body!))).toBe('<b>html</b>');
    expect(text(await readAll(bin.body!))).toBe('<binary>');

    // pre-order: parent starts before children, ends after them
    expect(eventTypes(events)).toEqual([
      'message-start',
      'part-start',           // alternative
      'part-start', 'part-end', // plain
      'part-start', 'part-end', // html
      'part-end',             // alternative
      'part-start', 'part-end', // binary
      'message-end',
    ]);
    // parents are wired correctly
    const starts = events.filter((e) => e.type === 'part-start') as Array<
      Extract<ParseEvent, { type: 'part-start' }>>;
    expect(starts[1].parent).toBe(alt);
    expect(starts[0].parent).toBe(msg);
    expect(diagnostics(events)).toEqual([]);
  });

  it('handles a nested boundary that has the outer boundary as a prefix', async () => {
    const msg =
      'Content-Type: multipart/mixed; boundary=abc\r\n' +
      '\r\n' +
      '--abc\r\n' +
      'Content-Type: multipart/alternative; boundary=abcdef\r\n' +
      '\r\n' +
      '--abcdef\r\n' +
      'Content-Type: text/plain\r\n' +
      '\r\n' +
      'one\r\n' +
      '--abcdef\r\n' +
      'Content-Type: text/plain\r\n' +
      '\r\n' +
      'two\r\n' +
      '--abcdef--\r\n' +
      '--abc\r\n' +
      'Content-Type: text/plain\r\n' +
      '\r\n' +
      'outer two\r\n' +
      '--abc--\r\n';
    const { parser, events } = collect();
    await parser.feed(bytes(msg));
    await parser.end();

    const root = parser.message!;
    expect(root.children).toHaveLength(2);
    const inner = root.children[0];
    expect(inner.children).toHaveLength(2);
    expect(text(await readAll(inner.children[0].body!))).toBe('one');
    expect(text(await readAll(inner.children[1].body!))).toBe('two');
    expect(text(await readAll(root.children[1].body!))).toBe('outer two');
    expect(diagnostics(events)).toEqual([]);
  });
});

describe('empty parts', () => {
  it('parses parts with no headers and/or no body', async () => {
    const msg =
      'Content-Type: multipart/mixed; boundary=b\r\n' +
      '\r\n' +
      '--b\r\n' +   // part 1: no headers, no body
      '\r\n' +
      '\r\n' +
      '--b\r\n' +   // part 2: headers, no body
      'X-A: 1\r\n' +
      '\r\n' +
      '\r\n' +
      '--b--\r\n';
    const { parser, events } = collect();
    await parser.feed(bytes(msg));
    await parser.end();

    const root = parser.message!;
    expect(root.children).toHaveLength(2);
    expect(root.children[0].headers).toHaveLength(0);
    expect(text(await readAll(root.children[0].body!))).toBe('');
    expect(root.children[1].headers).toEqual([{ name: 'x-a', value: '1' }]);
    expect(text(await readAll(root.children[1].body!))).toBe('');
    expect(diagnostics(events)).toEqual([]);
  });

  it('handles a multipart with only a closing delimiter', async () => {
    const { parser, events } = collect();
    await parser.feed(bytes('Content-Type: multipart/mixed; boundary=b\r\n\r\n--b--\r\n'));
    await parser.end();
    expect(parser.message!.children).toHaveLength(0);
    expect(eventTypes(events)).toEqual(['message-start', 'message-end']);
  });
});

describe('early termination', () => {
  it('reports unexpected-eof mid-body, keeps completed nodes, still finishes', async () => {
    const truncated =
      'Content-Type: multipart/mixed; boundary=b\r\n' +
      '\r\n' +
      '--b\r\n' +
      'Content-Type: text/plain\r\n' +
      '\r\n' +
      'complete\r\n' +
      '--b\r\n' +
      'Content-Type: text/plain\r\n' +
      '\r\n' +
      'partial data with no closing boundary';
    const { parser, events } = collect();
    await parser.feed(bytes(truncated));
    await parser.end();

    const root = parser.message!;
    expect(root.children).toHaveLength(2);
    expect(text(await readAll(root.children[0].body!))).toBe('complete');
    expect(text(await readAll(root.children[1].body!))).toBe('partial data with no closing boundary');

    const diags = diagnostics(events);
    expect(diags).toHaveLength(1);
    expect(diags[0].code).toBe('unexpected-eof');
    expect(diags[0].offset).toBe(bytes(truncated).length);
    // completion event still produced, and it is last
    expect(events[events.length - 1].type).toBe('message-end');
    expect(eventTypes(events)).toContain('part-end');
  });

  it('reports unexpected-eof inside message headers', async () => {
    const { parser, events } = collect();
    await parser.feed(bytes('Content-Type: text/plain\r\nX-Unfinished: 1'));
    await parser.end();
    const diags = diagnostics(events);
    expect(diags.map((d) => d.code)).toContain('unexpected-eof');
    // salvaged message node still emitted
    expect(eventTypes(events)).toEqual(['diagnostic', 'message-start', 'message-end']);
    expect(parser.message!.contentType.type).toBe('text');
  });

  it('reports unexpected-eof inside part headers and salvages them', async () => {
    const truncated =
      'Content-Type: multipart/mixed; boundary=b\r\n' +
      '\r\n' +
      '--b\r\n' +
      'Content-Type: text/plain\r\n';
    const { parser, events } = collect();
    await parser.feed(bytes(truncated));
    await parser.end();
    const diags = diagnostics(events);
    expect(diags.map((d) => d.code)).toContain('unexpected-eof');
    const root = parser.message!;
    expect(root.children).toHaveLength(1);
    expect(root.children[0].headers).toEqual([{ name: 'content-type', value: 'text/plain' }]);
    expect(text(await readAll(root.children[0].body!))).toBe('');
  });

  it('diagnoses a multipart body with no boundary at all', async () => {
    const { parser, events } = collect();
    await parser.feed(bytes('Content-Type: multipart/mixed; boundary=b\r\n\r\njust junk, no delimiter'));
    await parser.end();
    expect(diagnostics(events).map((d) => d.code)).toContain('missing-boundary');
    expect(parser.message!.children).toHaveLength(0);
  });
});

describe('corrupted part headers', () => {
  it('skips a malformed header line, keeps good nodes, reports the offset', async () => {
    const badLine = 'this line has no colon';
    const msg =
      'Content-Type: multipart/mixed; boundary=b\r\n' +
      '\r\n' +
      '--b\r\n' +
      'Content-Type: text/plain\r\n' +
      '\r\n' +
      'first\r\n' +
      '--b\r\n' +
      'Content-Type: text/plain\r\n' +
      badLine + '\r\n' +
      'X-Ok: yes\r\n' +
      '\r\n' +
      'second\r\n' +
      '--b\r\n' +
      'Content-Type: text/plain\r\n' +
      '\r\n' +
      'third\r\n' +
      '--b--\r\n';
    const { parser, events } = collect();
    await parser.feed(bytes(msg));
    await parser.end();

    const root = parser.message!;
    expect(root.children).toHaveLength(3);
    // previously successful nodes preserved
    expect(text(await readAll(root.children[0].body!))).toBe('first');
    // corrupted part keeps the valid headers around the bad line
    expect(root.children[1].headers).toEqual([
      { name: 'content-type', value: 'text/plain' },
      { name: 'x-ok', value: 'yes' },
    ]);
    expect(text(await readAll(root.children[1].body!))).toBe('second');
    // later parts unaffected
    expect(text(await readAll(root.children[2].body!))).toBe('third');

    const diags = diagnostics(events);
    expect(diags).toHaveLength(1);
    expect(diags[0].code).toBe('malformed-header-line');
    expect(diags[0].offset).toBe(msg.indexOf(badLine));
  });

  it('recovers when a boundary arrives before the blank line', async () => {
    const msg =
      'Content-Type: multipart/mixed; boundary=b\r\n' +
      '\r\n' +
      '--b\r\n' +
      'X-A: 1\r\n' +
      '--b--\r\n';
    const { parser, events } = collect();
    await parser.feed(bytes(msg));
    await parser.end();
    const diags = diagnostics(events);
    expect(diags.map((d) => d.code)).toEqual(['unterminated-headers']);
    const root = parser.message!;
    expect(root.children).toHaveLength(1);
    expect(root.children[0].headers).toEqual([{ name: 'x-a', value: '1' }]);
    expect(text(await readAll(root.children[0].body!))).toBe('');
    expect(events[events.length - 1].type).toBe('message-end');
  });

  it('caps oversized header blocks and treats the rest as body', async () => {
    const big = 'x'.repeat(300);
    const msg =
      'Content-Type: multipart/mixed; boundary=b\r\n' +
      '\r\n' +
      '--b\r\n' +
      `X-Big: ${big}\r\n` +
      '\r\n' +
      'body\r\n' +
      '--b--\r\n';
    const events: ParseEvent[] = [];
    const parser = new MimeParser({
      maxHeaderBytes: 128,
      onEvent: (e) => events.push(e),
    });
    await parser.feed(bytes(msg));
    await parser.end();
    expect(diagnostics(events).map((d) => d.code)).toContain('headers-too-large');
    const part = parser.message!.children[0];
    const body = text(await readAll(part.body!));
    expect(body).toContain('body');
  });
});

describe('backpressure', () => {
  it('stalls feed() while the consumer is paused, resumes on read', async () => {
    const chunk = 'x'.repeat(64);
    const bodyText = chunk.repeat(40); // 2560 bytes
    const msg =
      'Content-Type: multipart/mixed; boundary=b\r\n' +
      '\r\n' +
      '--b\r\n' +
      'Content-Type: application/octet-stream\r\n' +
      '\r\n' +
      bodyText +
      '\r\n--b--\r\n';
    const all = bytes(msg);
    const bodyStart = msg.indexOf(bodyText); // ASCII-only, so string offset == byte offset

    const events: ParseEvent[] = [];
    const parser = new MimeParser({
      highWaterMark: 256,
      chunkSize: 64,
      onEvent: (e) => events.push(e),
    });

    await parser.feed(all.subarray(0, bodyStart));
    const part = (events.find((e) => e.type === 'part-start') as Extract<ParseEvent, { type: 'part-start' }>).node;

    // Fill the budget: 4 x 64 bytes, nobody is reading.
    let off = bodyStart;
    for (let i = 0; i < 4; i++) {
      await parser.feed(all.subarray(off, off + 64));
      off += 64;
    }
    expect(parser.stats.bufferedBytes).toBe(256);

    // The next feed must stay pending while the consumer is paused.
    let stalledResolved = false;
    const stalled = parser.feed(all.subarray(off, off + 64)).then(() => { stalledResolved = true; });
    off += 64;
    await new Promise((r) => setTimeout(r, 30));
    expect(stalledResolved).toBe(false);

    // Start consuming: the stalled feed unblocks.
    const reading = readAll(part.body!);
    await stalled;

    // Feed the rest; the consumer keeps up.
    await parser.feed(all.subarray(off));
    await parser.end();

    expect(text(await reading)).toBe(bodyText);
    expect(parser.stats.peakBufferedBytes).toBeLessThanOrEqual(256);
    expect(events[events.length - 1].type).toBe('message-end');
  });
});

describe('large attachments', () => {
  it('streams a 50MB body with bounded peak memory', async () => {
    const SIZE = 50 * 1024 * 1024;
    const boundary = 'BOUNDARY-1f2d3c4b';
    // Deterministic pseudo-random payload.
    const payload = new Uint8Array(SIZE);
    let s = 123456789;
    for (let i = 0; i < SIZE; i++) {
      s = (s * 1103515245 + 12345) & 0x7fffffff;
      payload[i] = s & 0xff;
    }
    // Plant byte sequences that look almost like the delimiter.
    const plant = (offset: number, data: string) => payload.set(bytes(data), offset);
    plant(1_000_000, `\r\n--${boundary}X not a delimiter\r\n`);
    plant(2_000_000, `\r\n--${boundary} \t junk-after-padding\r\n`);
    plant(3_000_000, `\r\n--${boundary}\rX`);
    plant(SIZE - 100, `\n--${boundary.slice(0, 5)}`);

    const head = bytes(
      `Content-Type: multipart/mixed; boundary=${boundary}\r\n` +
      '\r\n' +
      `--${boundary}\r\n` +
      'Content-Type: application/octet-stream\r\n' +
      '\r\n',
    );
    const tail = bytes(`\r\n--${boundary}--\r\n`);

    const events: ParseEvent[] = [];
    let resolveBody!: (b: AsyncIterableIterator<Uint8Array>) => void;
    const bodyReady = new Promise<AsyncIterableIterator<Uint8Array>>((r) => { resolveBody = r; });
    const parser = new MimeParser({
      onEvent: (e) => {
        events.push(e);
        if (e.type === 'part-start' && e.node.body) resolveBody(e.node.body);
      },
    });

    const feeding = (async () => {
      await parser.feed(head);
      const CHUNK = 64 * 1024;
      for (let off = 0; off < payload.length; off += CHUNK) {
        await parser.feed(payload.subarray(off, Math.min(off + CHUNK, payload.length)));
      }
      await parser.feed(tail);
      await parser.end();
    })();

    const body = await bodyReady;
    const hash = createHash('sha256');
    let received = 0;
    for await (const c of body) {
      received += c.length;
      hash.update(c);
    }
    await feeding;

    expect(received).toBe(SIZE);
    expect(hash.digest('hex')).toBe(createHash('sha256').update(payload).digest('hex'));
    // Peak buffer stays at the budget, not at body size.
    expect(parser.stats.peakBufferedBytes).toBeLessThanOrEqual(256 * 1024);
    expect(parser.stats.bytesConsumed).toBe(head.length + SIZE + tail.length);
    expect(events[events.length - 1].type).toBe('message-end');
    expect(diagnostics(events)).toEqual([]);
  }, 30_000);
});

describe('completion guarantees', () => {
  it('emits message-end only after every child stream has closed', async () => {
    const { parser, events } = collect();
    await parser.feed(bytes(NESTED_FOR_COMPLETION));
    await parser.end();

    const endIdx = events.findIndex((e) => e.type === 'message-end');
    expect(endIdx).toBe(events.length - 1);
    // every part-end precedes message-end; children end before their parent
    const ends = events
      .map((e, i) => ({ e, i }))
      .filter(({ e }) => e.type === 'part-end') as Array<{ e: Extract<ParseEvent, { type: 'part-end' }>; i: number }>;
    for (const { e, i } of ends) expect(i).toBeLessThan(endIdx);
    const nodeEnd = new Map<MimeNode, number>();
    for (const { e, i } of ends) nodeEnd.set(e.node, i);
    const starts = events.filter((e) => e.type === 'part-start') as Array<
      Extract<ParseEvent, { type: 'part-start' }>>;
    for (const s of starts) {
      if (s.node.isMultipart) {
        for (const child of s.node.children) {
          expect(nodeEnd.get(child)!).toBeLessThan(nodeEnd.get(s.node)!);
        }
      }
    }
    // all body streams are already closed at message-end: draining what is
    // still buffered must reach done without any further parser input
    const msg = parser.message!;
    const walk = (n: MimeNode): MimeNode[] => [n, ...n.children.flatMap(walk)];
    for (const n of walk(msg)) {
      if (n.body) {
        let r = await n.body.next();
        while (!r.done) r = await n.body.next();
        expect(r.done).toBe(true);
      }
    }
  });
});

const NESTED_FOR_COMPLETION =
  'Content-Type: multipart/mixed; boundary=O\r\n' +
  '\r\n' +
  '--O\r\n' +
  'Content-Type: multipart/alternative; boundary=I\r\n' +
  '\r\n' +
  '--I\r\n' +
  '\r\n' +
  'a\r\n' +
  '--I\r\n' +
  '\r\n' +
  'b\r\n' +
  '--I--\r\n' +
  '--O\r\n' +
  '\r\n' +
  'c\r\n' +
  '--O--\r\n';

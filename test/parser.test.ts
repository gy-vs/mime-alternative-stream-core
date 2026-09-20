import { describe, expect, it } from 'vitest';
import {
  collectBytes,
  MimeParseError,
  MimeParser,
  ParserState,
  type MimePart,
  type ParseEvent,
} from '../src/index.js';

const enc = new TextEncoder();
const dec = new TextDecoder();

function bytes(s: string): Uint8Array {
  return enc.encode(s);
}

const tick = () => new Promise<void>((r) => setImmediate(r));

interface RunResult {
  parser: MimeParser;
  events: ParseEvent[];
  bodies: Map<MimePart, string>;
  bodyErrors: Map<MimePart, unknown>;
  root: MimePart | null;
  errors: MimeParseError[];
}

/**
 * Drive a parse to completion: feed the chunks, drain every body inline as
 * parts start, and collect the full event log.
 */
async function run(
  chunks: Uint8Array[] | Uint8Array,
  options?: ConstructorParameters<typeof MimeParser>[0],
): Promise<RunResult> {
  const parser = new MimeParser(options);
  const events: ParseEvent[] = [];
  const bodies = new Map<MimePart, string>();
  const bodyErrors = new Map<MimePart, unknown>();
  const consumer = (async () => {
    for await (const ev of parser.events()) {
      events.push(ev);
      if (ev.type === 'part-start' && ev.part.body) {
        try {
          bodies.set(ev.part, dec.decode(await collectBytes(ev.part.body)));
        } catch (e) {
          bodyErrors.set(ev.part, e);
        }
      }
    }
  })();
  for (const c of Array.isArray(chunks) ? chunks : [chunks]) await parser.feed(c);
  await parser.end();
  await consumer;
  const last = events[events.length - 1];
  if (!last || last.type !== 'done') throw new Error('last event must be done');
  return { parser, events, bodies, bodyErrors, root: last.message, errors: [...last.errors] };
}

function types(events: ParseEvent[]): string[] {
  return events.map((e) => e.type);
}

const NESTED = [
  'Content-Type: multipart/mixed; boundary="mix"\r',
  'Subject: nested demo\r',
  '\r',
  'this preamble is ignored\r',
  '--mix\r',
  'Content-Type: multipart/alternative; boundary="alt"\r',
  '\r',
  '--alt\r',
  'Content-Type: text/plain\r',
  '\r',
  'hello plain\r',
  '--alt\r',
  'Content-Type: text/html\r',
  '\r',
  '<b>hello</b>\r',
  '--alt--\r',
  '--mix\r',
  'Content-Type: multipart/related; boundary="rel"\r',
  '\r',
  '--rel\r',
  'Content-Type: text/html\r',
  '\r',
  '<img src="cid:i1">\r',
  '--rel\r',
  'Content-Type: image/png\r',
  '\r',
  'PNGDATA\r',
  '--rel--\r',
  '--mix\r',
  'Content-Type: application/octet-stream\r',
  '\r',
  'ATTACH\r',
  '--mix--\r',
  'epilogue is ignored',
].join('\n');

describe('multipart tree building', () => {
  it('parses a simple multipart/mixed message fed in one chunk', async () => {
    const msg = bytes(
      'Content-Type: multipart/mixed; boundary="b"\r\n' +
        '\r\n' +
        '--b\r\nContent-Type: text/plain\r\n\r\nfirst\r\n' +
        '--b\r\nContent-Type: text/html\r\n\r\n<i>second</i>\r\n' +
        '--b--\r\n',
    );
    const { events, bodies, root, errors } = await run(msg);
    expect(errors).toEqual([]);
    expect(types(events)).toEqual([
      'part-start',
      'part-start',
      'part-end',
      'part-start',
      'part-end',
      'part-end',
      'done',
    ]);
    expect(root!.mediaType).toBe('multipart/mixed');
    expect(root!.children).toHaveLength(2);
    expect(bodies.get(root!.children[0])).toBe('first');
    expect(bodies.get(root!.children[1])).toBe('<i>second</i>');
    expect(root!.children[0].parent).toBe(root);
    expect(root!.children[1].path).toEqual([1]);
  });

  it('builds nested alternative / related / mixed trees', async () => {
    const { events, bodies, root, errors } = await run(bytes(NESTED));
    expect(errors).toEqual([]);
    expect(events[events.length - 1].type).toBe('done');

    expect(root!.mediaType).toBe('multipart/mixed');
    expect(root!.children.map((c) => c.mediaType)).toEqual([
      'multipart/alternative',
      'multipart/related',
      'application/octet-stream',
    ]);
    const [alt, rel, attach] = root!.children;
    expect(alt.children.map((c) => c.mediaType)).toEqual(['text/plain', 'text/html']);
    expect(bodies.get(alt.children[0])).toBe('hello plain');
    expect(bodies.get(alt.children[1])).toBe('<b>hello</b>');
    expect(rel.children.map((c) => c.mediaType)).toEqual(['text/html', 'image/png']);
    expect(bodies.get(rel.children[1])).toBe('PNGDATA');
    expect(bodies.get(attach)).toBe('ATTACH');
    expect(rel.path).toEqual([1]);
    expect(rel.children[0].path).toEqual([1, 0]);
    expect(rel.children[0].depth).toBe(2);

    // children end before their parents; root ends before done
    expect(types(events)).toEqual([
      'part-start', // root
      'part-start', // alternative
      'part-start', // plain
      'part-end',
      'part-start', // html
      'part-end',
      'part-end', // alternative
      'part-start', // related
      'part-start', // html
      'part-end',
      'part-start', // png
      'part-end',
      'part-end', // related
      'part-start', // attachment
      'part-end',
      'part-end', // root
      'done',
    ]);
  });

  it('parses a non-multipart message as a single body running to EOF', async () => {
    const { events, bodies, root } = await run(
      bytes('Content-Type: text/plain\r\nX-A: 1\r\n\r\njust a body\r\nwith lines'),
    );
    expect(types(events)).toEqual(['part-start', 'part-end', 'done']);
    expect(root!.isMultipart).toBe(false);
    expect(bodies.get(root!)).toBe('just a body\r\nwith lines');
  });
});

describe('chunk splitting', () => {
  it('parses byte-by-byte identically to one-shot', async () => {
    const msg = bytes(NESTED);
    const oneShot = await run(msg);
    const oneByteChunks: Uint8Array[] = [];
    for (let i = 0; i < msg.length; i++) oneByteChunks.push(msg.subarray(i, i + 1));
    const drip = await run(oneByteChunks);
    expect(types(drip.events)).toEqual(types(oneShot.events));
    expect(drip.errors).toEqual([]);
    // bodies in part-start order (Map preserves insertion order)
    expect([...drip.bodies.values()]).toEqual([...oneShot.bodies.values()]);
  });

  it('handles boundaries split across random chunk sizes', async () => {
    const msg = bytes(NESTED);
    let seed = 0x2f6e2b1;
    const rand = () => (seed = (seed * 1103515245 + 12345) & 0x7fffffff);
    for (let round = 0; round < 5; round++) {
      const chunks: Uint8Array[] = [];
      let off = 0;
      while (off < msg.length) {
        const n = 1 + (rand() % 97);
        chunks.push(msg.subarray(off, Math.min(off + n, msg.length)));
        off += n;
      }
      const r = await run(chunks);
      expect(r.errors).toEqual([]);
      expect(r.root!.children.map((c) => c.mediaType)).toEqual([
        'multipart/alternative',
        'multipart/related',
        'application/octet-stream',
      ]);
      expect(r.bodies.get(r.root!.children[2])).toBe('ATTACH');
    }
  });
});

describe('empty parts, preamble and epilogue', () => {
  it('handles empty bodies, empty header sections and empty multiparts', async () => {
    const msg = bytes(
      'Content-Type: multipart/mixed; boundary=b\r\n' +
        '\r\n' +
        '--b\r\n' + // part 0: no headers…
        '\r\n' + // …empty header section…
        '\r\n' + // …and empty body
        '--b\r\n' +
        'Content-Type: text/plain\r\n' +
        '\r\n' + // part 1: headers but empty body
        '\r\n' +
        '--b\r\n' +
        'Content-Type: multipart/alternative; boundary=inner\r\n' +
        '\r\n' +
        '--inner--\r\n' + // part 2: multipart with zero children
        '--b--\r\n',
    );
    const { bodies, root, errors } = await run(msg);
    expect(errors).toEqual([]);
    expect(root!.children).toHaveLength(3);
    expect(root!.children[0].headers).toEqual([]);
    expect(bodies.get(root!.children[0])).toBe('');
    expect(bodies.get(root!.children[1])).toBe('');
    expect(root!.children[2].isMultipart).toBe(true);
    expect(root!.children[2].children).toEqual([]);
  });

  it('skips preamble and epilogue content', async () => {
    const msg = bytes(
      'Content-Type: multipart/mixed; boundary=b\r\n\r\n' +
        'Hello, this is the preamble.\r\n' +
        'It mentions --other and even --b without a line start: x--b\r\n' +
        '--not-the-boundary\r\n' +
        '--b\r\nContent-Type: text/plain\r\n\r\nreal\r\n' +
        '--b--\r\n' +
        'Epilogue here.\r\n--b\r\n--b--\r\nanything goes',
    );
    const { bodies, root, errors } = await run(msg);
    expect(errors).toEqual([]);
    expect(root!.children).toHaveLength(1);
    expect(bodies.get(root!.children[0])).toBe('real');
  });

  it('keeps the CRLF attached to the delimiter out of the body', async () => {
    const msg = bytes(
      'Content-Type: multipart/mixed; boundary=b\r\n\r\n' +
        '--b\r\n\r\nbody\r\n--b--\r\n',
    );
    const { bodies, root } = await run(msg);
    expect(bodies.get(root!.children[0])).toBe('body');
  });
});

describe('diagnostics and recovery', () => {
  it('recovers from a corrupt part header and reports the byte offset', async () => {
    const msg =
      'Content-Type: multipart/mixed; boundary=b\r\n\r\n' +
      '--b\r\nContent-Type: text/plain\r\n\r\nfirst\r\n' +
      '--b\r\nThis line is not a header\r\nContent-Type: text/plain\r\n\r\nsecond\r\n' +
      '--b\r\nContent-Type: text/plain\r\n\r\nthird\r\n' +
      '--b--\r\n';
    const { events, bodies, root, errors } = await run(bytes(msg));
    expect(errors).toHaveLength(1);
    const err = errors[0];
    expect(err).toBeInstanceOf(MimeParseError);
    expect(err.code).toBe('bad-header');
    expect(err.offset).toBe(msg.indexOf('This line is not a header'));
    expect(err.partPath).toEqual([1]);
    // previously and subsequently parsed nodes survive; the corrupt one is skipped
    expect(root!.children).toHaveLength(2);
    expect(bodies.get(root!.children[0])).toBe('first');
    expect(bodies.get(root!.children[1])).toBe('third');
    expect(types(events)).toContain('error');
    expect(events[events.length - 1].type).toBe('done');
  });

  it('reports header sections exceeding the configured cap', async () => {
    const big = 'X-Pad: ' + 'y'.repeat(200) + '\r\n';
    const msg =
      'Content-Type: multipart/mixed; boundary=b\r\n\r\n' +
      `--b\r\n${big}\r\nbody\r\n` +
      '--b\r\nContent-Type: text/plain\r\n\r\nok\r\n' +
      '--b--\r\n';
    const { bodies, root, errors } = await run(bytes(msg), { maxHeaderBytes: 128 });
    expect(errors).toHaveLength(1);
    expect(errors[0].code).toBe('headers-too-large');
    expect(root!.children).toHaveLength(1);
    expect(bodies.get(root!.children[0])).toBe('ok');
  });

  it('treats a multipart part without boundary as an opaque body', async () => {
    const msg =
      'Content-Type: multipart/mixed; boundary=b\r\n\r\n' +
      '--b\r\nContent-Type: multipart/mixed\r\n\r\n' +
      '--fake\r\nlooks nested but is not\r\n--fake--\r\n' +
      '--b\r\nContent-Type: text/plain\r\n\r\nafter\r\n' +
      '--b--\r\n';
    const { bodies, root, errors } = await run(bytes(msg));
    expect(errors).toHaveLength(1);
    expect(errors[0].code).toBe('missing-boundary');
    expect(root!.children).toHaveLength(2);
    expect(bodies.get(root!.children[0])).toContain('--fake');
    expect(bodies.get(root!.children[1])).toBe('after');
  });

  it('reports premature end of input and fails the open body stream', async () => {
    const msg =
      'Content-Type: multipart/mixed; boundary=b\r\n\r\n' +
      '--b\r\nContent-Type: text/plain\r\n\r\ncomplete\r\n' +
      '--b\r\nContent-Type: text/plain\r\n\r\ntruncated body with no closing';
    const { events, bodies, bodyErrors, root, errors } = await run(bytes(msg));
    expect(errors).toHaveLength(1);
    expect(errors[0].code).toBe('unexpected-eof');
    expect(errors[0].offset).toBe(msg.length);
    // first part intact, second part present but incomplete
    expect(root!.children).toHaveLength(2);
    expect(bodies.get(root!.children[0])).toBe('complete');
    expect(root!.children[0].complete).toBe(true);
    expect(root!.children[1].complete).toBe(false);
    expect(root!.complete).toBe(false);
    const failure = bodyErrors.get(root!.children[1]);
    expect(failure).toBeInstanceOf(MimeParseError);
    expect((failure as MimeParseError).code).toBe('unexpected-eof');
    expect(events[events.length - 1].type).toBe('done');
  });

  it('reports end of input inside a nested multipart', async () => {
    const msg =
      'Content-Type: multipart/mixed; boundary=o\r\n\r\n' +
      '--o\r\nContent-Type: multipart/alternative; boundary=i\r\n\r\n' +
      '--i\r\nContent-Type: text/plain\r\n\r\ninner\r\n--i--\r\n';
    // outer closing delimiter never arrives
    const { root, errors } = await run(bytes(msg));
    expect(errors).toHaveLength(1);
    expect(errors[0].code).toBe('unexpected-eof');
    expect(root!.complete).toBe(false);
    // the fully parsed inner multipart is still intact
    expect(root!.children[0].complete).toBe(true);
    expect(root!.children[0].children).toHaveLength(1);
  });
});

describe('event protocol guarantees', () => {
  it('emits a completed part as soon as its delimiter arrives', async () => {
    const parser = new MimeParser();
    const events: ParseEvent[] = [];
    const consumer = (async () => {
      for await (const ev of parser.events()) {
        events.push(ev);
        if (ev.type === 'part-start' && ev.part.body) await collectBytes(ev.part.body);
      }
    })();
    await parser.feed(
      bytes(
        'Content-Type: multipart/mixed; boundary=b\r\n\r\n' +
          '--b\r\nContent-Type: text/plain\r\n\r\nearly\r\n--b\r\n',
      ),
    );
    await tick();
    // only the tail of the message has not been fed, yet part 0 has ended
    expect(types(events)).toEqual(['part-start', 'part-start', 'part-end']);
    await parser.feed(bytes('Content-Type: text/plain\r\n\r\nlate\r\n--b--\r\n'));
    await parser.end();
    await consumer;
    expect(types(events)).toEqual([
      'part-start',
      'part-start',
      'part-end',
      'part-start',
      'part-end',
      'part-end',
      'done',
    ]);
  });

  it('emits done exactly once, last, after all body streams closed', async () => {
    const parser = new MimeParser({ highWaterMark: 256 });
    const events: ParseEvent[] = [];
    const bodies: AsyncIterable<Uint8Array>[] = [];
    const consumer = (async () => {
      for await (const ev of parser.events()) {
        events.push(ev);
        if (ev.type === 'part-start' && ev.part.body) bodies.push(ev.part.body);
      }
    })();
    const big = 'z'.repeat(4096);
    const msg = bytes(
      'Content-Type: multipart/mixed; boundary=b\r\n\r\n' +
        `--b\r\nContent-Type: text/plain\r\n\r\n${big}\r\n--b--\r\n`,
    );
    // The producer stalls on backpressure because nobody drains the body.
    let feedDone = false;
    const fed = parser.feed(msg).then(() => {
      feedDone = true;
    });
    await tick();
    await tick();
    expect(feedDone).toBe(false);
    expect(types(events)).not.toContain('done');
    // Drain the body: the parser unblocks, closes the stream, then finishes.
    const collected = collectBytes(bodies[0]);
    await fed;
    await parser.end();
    expect((await collected).length).toBe(4096);
    await consumer;
    expect(types(events)).toEqual(['part-start', 'part-start', 'part-end', 'part-end', 'done']);
    expect(events.filter((e) => e.type === 'done')).toHaveLength(1);
  });

  it('exposes the parser state', async () => {
    const parser = new MimeParser();
    expect(parser.state).toBe(ParserState.MessageHeaders);
    await parser.feed(bytes('Content-Type: text/plain\r\n\r\nabc'));
    expect(parser.state).toBe(ParserState.PartBody);
    await parser.end();
    expect(parser.state).toBe(ParserState.Done);
    await expect(parser.feed(bytes('x'))).rejects.toThrow(/after end/);
  });

  it('handles empty input as a diagnostic plus done', async () => {
    const { events, root, errors } = await run([]);
    expect(root).toBeNull();
    expect(errors).toHaveLength(1);
    expect(errors[0].code).toBe('unexpected-eof');
    expect(errors[0].offset).toBe(0);
    expect(events[events.length - 1].type).toBe('done');
  });

  it('rejects concurrent feed calls and a second event consumer', async () => {
    const parser = new MimeParser({ highWaterMark: 64 });
    const body = 'x'.repeat(4096);
    const msg = bytes(
      `Content-Type: multipart/mixed; boundary=b\r\n\r\n--b\r\n\r\n${body}\r\n--b--\r\n`,
    );
    // Nobody consumes: the feed parks on body backpressure.
    const parked = parser.feed(msg);
    await tick();
    await expect(parser.feed(bytes('more'))).rejects.toThrow(/concurrent/);
    // Attach the real consumer, then a second one which must be rejected.
    const first = (async () => {
      for await (const ev of parser.events()) {
        if (ev.type === 'part-start' && ev.part.body) await collectBytes(ev.part.body);
      }
    })();
    const second = parser.events()[Symbol.asyncIterator]();
    await expect(second.next()).rejects.toThrow(/single-consumer/);
    await parked;
    await parser.end();
    await first;
  });
});

describe('backpressure', () => {
  it('stops feeding when the consumer pauses and buffers only a bounded amount', async () => {
    const parser = new MimeParser({ highWaterMark: 1024 });
    const events: ParseEvent[] = [];
    const consumer = (async () => {
      for await (const ev of parser.events()) events.push(ev);
    })();
    const body = 'x'.repeat(16 * 1024);
    const msg = bytes(
      'Content-Type: multipart/mixed; boundary=b\r\n\r\n' +
        `--b\r\nContent-Type: text/plain\r\n\r\n${body}\r\n--b--\r\n`,
    );
    // Nobody reads the body: feed() must park instead of buffering it all.
    let feedResolved = false;
    const fed = parser.feed(msg).then(() => {
      feedResolved = true;
    });
    await tick();
    await tick();
    expect(feedResolved).toBe(false);
    expect(parser.bufferedBytes).toBeLessThan(4096); // ≈ HWM + scanner holdback
    expect(parser.peakBufferedBytes).toBeLessThan(4096);
    expect(types(events)).toEqual(['part-start', 'part-start']);

    // Start consuming: the parser unblocks and the parse completes.
    const start = events[1];
    if (start.type !== 'part-start' || !start.part.body) throw new Error('expected leaf part');
    const got = collectBytes(start.part.body);
    await fed;
    await parser.end();
    expect(dec.decode(await got)).toBe(body);
    await consumer;
    expect(events[events.length - 1].type).toBe('done');
  });

  it('streams a 50 MiB attachment with bounded peak buffering', async () => {
    const SIZE = 50 * 1024 * 1024;
    const boundary = 'Bb7f3a9c';
    // Adversarial but deterministic filler: CRs, LFs, dashes and near-boundaries.
    const pattern = bytes('lorem-ipsum\r\n--dolor sit amet\n--Bb7f3a9\r\n--\r\n');
    const body = new Uint8Array(SIZE);
    for (let off = 0; off < SIZE; off += pattern.length) {
      body.set(pattern.subarray(0, Math.min(pattern.length, SIZE - off)), off);
    }
    const head = bytes(
      `Content-Type: multipart/mixed; boundary="${boundary}"\r\n\r\n` +
        `--${boundary}\r\nContent-Type: application/octet-stream\r\n\r\n`,
    );
    const tail = bytes(`\r\n--${boundary}--\r\n`);

    const parser = new MimeParser();
    let received = 0;
    let corrupt = 0;
    const eventTypes: string[] = [];
    const consumer = (async () => {
      for await (const ev of parser.events()) {
        eventTypes.push(ev.type);
        if (ev.type === 'part-start' && ev.part.body) {
          for await (const chunk of ev.part.body) {
            for (let i = 0; i < chunk.length; i++) {
              if (chunk[i] !== body[received + i]) corrupt++;
            }
            received += chunk.length;
          }
        }
      }
    })();
    const CHUNK = 64 * 1024;
    await parser.feed(head);
    for (let off = 0; off < SIZE; off += CHUNK) {
      await parser.feed(body.subarray(off, Math.min(off + CHUNK, SIZE)));
    }
    await parser.feed(tail);
    await parser.end();
    await consumer;

    expect(corrupt).toBe(0);
    expect(received).toBe(SIZE);
    expect(eventTypes).toEqual(['part-start', 'part-start', 'part-end', 'part-end', 'done']);
    // Peak buffering must not grow with the body: HWM (64 KiB) + one chunk + slack.
    expect(parser.peakBufferedBytes).toBeLessThan(1024 * 1024);
  }, 30_000);
});

describe('headers and content types', () => {
  it('unfolds continuations, matches names case-insensitively, parses quoted boundaries', async () => {
    const msg =
      'Content-Type: multipart/mixed;\r\n\tboundary="q;b"\r\nX-Empty:\r\n\r\n' +
      '--q;b\r\ncontent-type: TEXT/PLAIN; charset="utf-8"\r\n\r\nhi\r\n' +
      '--q;b--\r\n';
    const { bodies, root, errors } = await run(bytes(msg));
    expect(errors).toEqual([]);
    expect(root!.boundary).toBe('q;b');
    expect(root!.header('x-empty')).toBe('');
    const leaf = root!.children[0];
    expect(leaf.contentType.params.charset).toBe('utf-8');
    expect(leaf.mediaType).toBe('text/plain');
    expect(bodies.get(leaf)).toBe('hi');
  });

  it('accepts LF-only line endings', async () => {
    const msg =
      'Content-Type: multipart/mixed; boundary=b\n\n' +
      '--b\nContent-Type: text/plain\n\none\n--b\nContent-Type: text/plain\n\ntwo\n--b--\n';
    const { bodies, root, errors } = await run(bytes(msg));
    expect(errors).toEqual([]);
    expect(root!.children).toHaveLength(2);
    expect(bodies.get(root!.children[0])).toBe('one');
    expect(bodies.get(root!.children[1])).toBe('two');
  });

  it('keeps duplicate headers and defaults to text/plain', async () => {
    const msg = bytes(
      'Content-Type: multipart/mixed; boundary=b\r\n\r\n' +
        '--b\r\nX-Tag: a\r\nX-Tag: b\r\n\r\nhi\r\n--b--\r\n',
    );
    const { root } = await run(msg);
    const leaf = root!.children[0];
    expect(leaf.headers.filter((h) => h.name === 'x-tag')).toHaveLength(2);
    expect(leaf.mediaType).toBe('text/plain');
  });
});

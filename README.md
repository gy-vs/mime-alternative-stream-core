# MIME stream core

TypeScript library for parsing MIME messages — including nested
`multipart/alternative`, `multipart/mixed` and `multipart/related`
structures — as a **stream**: input arrives in arbitrary byte chunks, parts
are emitted as soon as they are complete, and bodies are delivered through
pausable async iterators with backpressure, so a 50&nbsp;MiB attachment does
not require 50&nbsp;MiB of memory.

Run `npm install`, then `npm test` and `npm run build`.

## Quick start

```ts
import { MimeParser } from 'mime-alternative-stream-core';

const parser = new MimeParser({ highWaterMark: 64 * 1024 });

const consumer = (async () => {
  for await (const event of parser.events()) {
    switch (event.type) {
      case 'part-start':
        if (event.part.body) {
          // Leaf part: stream the body. Not reading applies backpressure.
          for await (const chunk of event.part.body) sink.write(chunk);
        }
        break;
      case 'part-end':   // event.part is fully parsed
        break;
      case 'error':      // recoverable diagnostic, see below
        report(event.error);
        break;
      case 'done':       // final event; event.message is the tree root
        break;
    }
  }
})();

for await (const chunk of socket) await parser.feed(chunk); // await: backpressure
await parser.end();
await consumer;
```

The result is a tree of `MimePart` nodes: multipart nodes expose
`children`; leaf nodes expose a streaming `body`. Nodes are added to
`parent.children` as soon as their headers parse, so successfully parsed
nodes survive a corrupt sibling.

## Parse states

`parser.state` exposes an explicit state machine:

```
message-headers ──multipart root──▶ preamble ──delimiter──▶ part-headers
    │                                ▲   │                        │
    └──single body──▶ part-body      │   └──close──▶ (pop ctx)    ▼
                                    (pop ctx)               part-body ──delimiter──┐
                                      ▲                                              │
                                      └──────────────────────────────────────────────┘
```

- **message-headers / part-headers** — header lines are accumulated (with
  unfolding) until a blank line; the section is capped at `maxHeaderBytes`
  (default 256&nbsp;KiB).
- **preamble** — bytes before the first delimiter of a multipart (and
  between a nested multipart's closing delimiter and the parent's next
  delimiter, i.e. its epilogue) are discarded.
- **part-body** — body bytes stream to the current part while a
  `BoundaryScanner` looks for `\n--boundary`. The CRLF preceding a
  delimiter belongs to the delimiter (RFC 2046). Boundaries may be split
  across chunks at any byte; the scanner holds back at most
  `boundary.length + maxPaddingBytes + 2` bytes.
- **skip-part** — error recovery: after a corrupt header section, bytes are
  discarded up to the next delimiter of the enclosing multipart, then
  parsing resumes with the next sibling.
- **epilogue** — the root entity is closed; everything up to EOF is
  discarded.
- **done** — terminal, after `end()` has been processed.

Nested multiparts are handled with a boundary context stack: a part whose
`Content-Type` is `multipart/*` with a `boundary` parameter pushes a new
context; its closing delimiter `--boundary--` pops it.

## Event protocol

Events are delivered through the single-consumer async iterable
`parser.events()`, in this order:

| event        | when                                                        |
| ------------ | ----------------------------------------------------------- |
| `part-start` | a part's headers are complete; `part.body` is live for leaf parts |
| `part-end`   | the part's closing delimiter was seen; children always end before their parents |
| `error`      | a recoverable problem was diagnosed (parsing continues)     |
| `done`       | exactly once, last, and **only after every body stream has been closed** |

`done` carries `{ message, errors }`: the root of the tree (or `null` if
the top-level headers failed) and every diagnostic reported.

## Backpressure

`feed()` returns a promise that resolves when the chunk has been consumed
*and* the parser is ready for more. Each body is a `PushQueue` with a
high-water mark (`highWaterMark`, default 64&nbsp;KiB); when the consumer
stops reading, the queue fills, `feed()` stops resolving, and the producer
naturally pauses. Bodies are delivered to the queue in slices of at most
`highWaterMark`, so **peak buffering is bounded by roughly
`2 × highWaterMark + maxHeaderBytes + boundary length` — independent of any
single body size or feed chunk size**. `parser.bufferedBytes` and
`parser.peakBufferedBytes` expose the live and peak usage.

`feed()` calls must be serialized; concurrent calls throw.

## Diagnostics

Errors are reported, not thrown (misuse such as `feed()` after `end()`
still throws). Each `MimeParseError` carries:

- `code` — `bad-header`, `headers-too-large`, `missing-boundary`, or
  `unexpected-eof`;
- `offset` — absolute byte offset in the input stream;
- `partPath` — index path of the part being parsed, e.g. `[2, 0]`.

Recovery: a part with corrupt headers is skipped to the next delimiter of
its enclosing multipart; previously and subsequently parsed nodes remain in
the tree. A `multipart/*` part without a boundary is treated as an opaque
leaf body. On premature EOF, open body streams fail with `unexpected-eof`
and affected parts (and their ancestors) are marked `complete === false`;
`done` still fires.

## Guarantees and limits

- Parts are emitted as early as possible: `part-end` fires the moment the
  closing delimiter is seen, before any later bytes arrive.
- `done` is emitted exactly once, as the last event, after all body streams
  have been closed (ended or failed) by the parser.
- Both CRLF and bare-LF line endings are accepted; a delimiter at the very
  start of a multipart entity is recognized, as is one at EOF without a
  trailing line ending.
- Delimiter lines with more than `maxPaddingBytes` (default 1024) of
  transport padding are treated as body content, keeping holdback bounded.
- `Content-Transfer-Encoding` is not decoded; bodies are delivered raw.
  RFC 2231 parameter continuations are not expanded.

## Legacy API

`parseHeaders(string)` and `MimeStream` (the original whole-message
splitter) are kept for backward compatibility; `MimeStream` is deprecated.

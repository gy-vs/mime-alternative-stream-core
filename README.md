# MIME stream core

Streaming MIME parser for nested `multipart/*` messages (alternative, mixed,
related, …). Bytes are pushed in as they arrive; the message tree is built
incrementally and complete parts are emitted as early as possible. Body
content flows through pausable async iterators with backpressure, so peak
memory stays bounded regardless of attachment size.

Run `npm install`, then `npm test` and `npm run build`.

## Usage

```ts
import { MimeParser } from 'mime-alternative-stream-core';

const parser = new MimeParser({
  onEvent(event) {
    if (event.type === 'part-start' && event.node.body) {
      // consume the body as it streams — do NOT await this from onEvent
      drain(event.node.body);
    }
  },
});

async function drain(body: AsyncIterableIterator<Uint8Array>) {
  for await (const chunk of body) { /* ... */ }
}

for await (const chunk of source) {
  await parser.feed(chunk);   // await: applies backpressure
}
await parser.end();           // resolves after 'message-end'
```

## Parse states

The parser is a small state machine; nested multiparts recurse, one
`MultipartScanner` per level.

```
MimeParser:   MessageHeaders → Body → Done

MultipartScanner (one per nesting level):
  Preamble ──boundary──▶ PartHeaders ──blank line──▶ PartBody
     ▲                      │boundary early            │boundary
     │                      ▼ (recover, empty body)    ▼
     └──────────── next ── DelimiterLine ── close ──▶ Epilogue ──▶ Done
                           │ invalid continuation (e.g. nested
                           └─ boundary sharing a prefix) → resume
                             previous state, bytes are body
```

Delimiter recognition works across arbitrary chunk splits: at most
`boundary.length + 4` bytes are held back per level. A candidate that turns
out not to be a delimiter (invalid continuation byte) is re-emitted as body
content, so nested boundaries that share a prefix with an outer boundary
(`abc` / `abcdef`) parse correctly.

## Event protocol

```
message-start
  (part-start … part-end)*     pre-order; children nest inside parents
(diagnostic)*                  anywhere; parsing continues
message-end                    always last
```

- `part-start` fires as soon as a part's headers are complete. The node is
  already linked into the tree (`parent.children`).
- Leaf nodes expose `node.body`, a pausable `AsyncIterableIterator<Uint8Array>`.
- `part-end` fires when the part's body stream is closed; for multipart
  nodes, only after every descendant's `part-end`.
- `message-end` fires only after **all** child streams are closed.
- Events are dispatched synchronously, in order. An `onEvent` that returns
  a promise is not awaited; use the body iterators for flow control.

## Backpressure and memory

- `feed()` resolves only after its chunk is fully processed. While any
  consumer is paused, `feed()` stays pending — the producer naturally stops.
- All open body streams share one global byte budget (`highWaterMark`,
  default 256 KiB). Peak buffered bytes never exceed it, independent of any
  single body's size (verified with a 50 MB attachment).
- Per-level carry is bounded by the boundary length; header blocks are
  capped (`maxHeaderBytes`, default 64 KiB).
- Breaking out of `for await` on a body (`return()`) discards its remainder
  and releases its budget. Consume bodies as parts arrive; do not wait for
  `message-end` before reading them.

## Diagnostics

Recoverable problems produce `diagnostic` events with an absolute byte
`offset`; previously completed nodes are always preserved.

| code | meaning |
| --- | --- |
| `malformed-header-line` | header line without a colon skipped |
| `headers-too-large` | header block over cap; remainder treated as body |
| `unterminated-headers` | boundary arrived before the blank line |
| `missing-content-type-boundary` | multipart/* without boundary; body opaque |
| `missing-boundary` | multipart body contained no delimiter |
| `unexpected-eof` | input ended mid-headers or mid-body |
| `delimiter-line-too-long` | transport padding over sanity limit |

`parser.stats` exposes `bufferedBytes`, `peakBufferedBytes` and
`bytesConsumed` for monitoring.

## Legacy API

`parseHeaders` and `MimeStream` (whole-message, string based) are kept for
backward compatibility.

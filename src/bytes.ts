/** Byte-level helpers shared by the scanner and the parser. */

/** Concatenate byte arrays into a freshly allocated array. */
export function concatBytes(...parts: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const p of parts) total += p.length;
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}

/**
 * First index of `needle` in `buf` at or after `from`, or -1.
 * Only full occurrences are reported; a partial occurrence touching the end
 * of `buf` is left for the caller's holdback logic.
 */
export function indexOfBytes(buf: Uint8Array, needle: Uint8Array, from: number): number {
  const last = buf.length - needle.length;
  let i = buf.indexOf(needle[0], from);
  while (i >= 0 && i <= last) {
    let j = 1;
    while (j < needle.length && buf[i + j] === needle[j]) j++;
    if (j === needle.length) return i;
    i = buf.indexOf(needle[0], i + 1);
  }
  return -1;
}

/**
 * Length of the longest suffix of `buf` that is a *proper* prefix of
 * `needle` — i.e. how many trailing bytes might grow into a full match once
 * more input arrives.
 */
export function suffixPrefixOverlap(buf: Uint8Array, needle: Uint8Array): number {
  const max = Math.min(buf.length, needle.length - 1);
  for (let k = max; k > 0; k--) {
    const off = buf.length - k;
    let j = 0;
    while (j < k && buf[off + j] === needle[j]) j++;
    if (j === k) return k;
  }
  return 0;
}

/** Drain an async byte stream into a single freshly allocated array. */
export async function collectBytes(body: AsyncIterable<Uint8Array>): Promise<Uint8Array> {
  const parts: Uint8Array[] = [];
  let total = 0;
  for await (const chunk of body) {
    parts.push(chunk);
    total += chunk.length;
  }
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}

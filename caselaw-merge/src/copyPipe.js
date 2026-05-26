// Source → target COPY pipe.
//
// Used by phases moving large row counts where chunked INSERTs would be
// slow (phase 3 cases ≈ 940k rows). For each source we open two COPY
// streams — one TO STDOUT on the source connection, one FROM STDIN on the
// target — and pipe them together. PostgreSQL's text-format COPY round-
// trips all built-in types losslessly, so no per-row JS marshalling.
//
// Expects to be called from inside the target transaction (the COPY FROM
// runs on that client). The source side runs against a dedicated source
// client — caller's responsibility to release it.

import { pipeline } from 'node:stream/promises';
import copyStreams from 'pg-copy-streams';

const { to: copyTo, from: copyFrom } = copyStreams;

/**
 * Stream rows from a SELECT on `sourceClient` into `targetClient` via COPY.
 *
 *   sourceSelectSQL: a parenthesised SELECT, e.g. `(SELECT a, b FROM t)`
 *   targetCopyDest:  `<table>(col1, col2, ...)` form
 *
 * Returns rows-copied row count parsed from the target COPY result.
 */
export async function copyBetween(sourceClient, sourceSelectSQL, targetClient, targetCopyDest) {
  const readable = sourceClient.query(copyTo(`COPY ${sourceSelectSQL} TO STDOUT`));
  const writable = targetClient.query(copyFrom(`COPY ${targetCopyDest} FROM STDIN`));
  await pipeline(readable, writable);
  return writable.rowCount;
}

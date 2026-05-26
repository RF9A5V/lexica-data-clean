// Per spec §D.3: each phase brackets its work with
//   BEGIN;
//     SELECT pg_advisory_xact_lock(<phase_number>);
//     -- phase work
//   COMMIT;
//
// `pg_advisory_xact_lock` is automatically released at COMMIT/ROLLBACK so two
// concurrent runs of the same phase will serialize. We use the phase id as the
// lock key so different phases never contend (a later phase running while an
// earlier one is mid-flight would fail downstream FKs anyway).

export async function acquirePhaseLock(client, phaseId) {
  await client.query('SELECT pg_advisory_xact_lock($1::bigint)', [phaseId]);
}

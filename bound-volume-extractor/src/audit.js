import { readdir, readFile, writeFile } from 'fs/promises';
import path from 'path';

/**
 * Audit log: one JSON file per batch under audit/<batch_id>.json. Records
 * everything needed to (a) understand what was parsed, (b) reproduce a
 * parse, and (c) reverse it later — even though the actual reverse step
 * happens at the source-DB import boundary, the audit row is the system
 * of record for "what was claimed."
 */

export async function writeAudit(auditDir, record) {
  const file = path.join(auditDir, `${record.batch_id}.json`);
  await writeFile(file, JSON.stringify(record, null, 2));
  return file;
}

export async function listAudits(auditDir) {
  const files = await readdir(auditDir);
  const records = [];
  for (const f of files) {
    if (!f.endsWith('.json')) continue;
    try {
      const content = await readFile(path.join(auditDir, f), 'utf8');
      records.push(JSON.parse(content));
    } catch {
      // skip malformed
    }
  }
  records.sort((a, b) => (a.parsed_at || '').localeCompare(b.parsed_at || ''));
  return records;
}

export async function readAudit(auditDir, batchId) {
  const file = path.join(auditDir, `${batchId}.json`);
  const content = await readFile(file, 'utf8');
  return JSON.parse(content);
}

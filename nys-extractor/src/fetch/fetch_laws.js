import path from 'path';
import fs from 'fs/promises';
import { ensureDir, writeJson } from '../lib/utils.js';

// Cache layout:
//   data/cache/laws/<LAWID>.json    — full=true tree response
//   data/cache/repealed.json        — list of repealed sections
//   data/cache/repealed/<LAW>/<LOC>__<DATE>.json  — repealed section text

export async function fetchAllLawTrees(client, { cacheDir, force = false, only = null } = {}) {
  const dir = path.join(cacheDir, 'laws');
  await ensureDir(dir);

  const laws = await client.listLaws();
  const filtered = only ? laws.filter((l) => only.includes(l.lawId)) : laws;
  console.log(`fetching ${filtered.length} of ${laws.length} laws...`);

  const results = [];
  for (const law of filtered) {
    const lawId = law.lawId;
    const cachePath = path.join(dir, `${lawId}.json`);
    if (!force) {
      try {
        await fs.access(cachePath);
        results.push({ lawId, cached: true });
        continue;
      } catch {}
    }
    process.stdout.write(`  ${lawId} ... `);
    try {
      const json = await client.getLawTreeFull(lawId);
      await writeJson(cachePath, json);
      console.log('ok');
      results.push({ lawId, cached: false });
    } catch (e) {
      console.log(`FAIL: ${e.message}`);
      results.push({ lawId, error: e.message });
    }
  }
  return results;
}

export async function fetchRepealed(client, { cacheDir, force = false } = {}) {
  const indexPath = path.join(cacheDir, 'repealed.json');
  let index;
  if (!force) {
    try { index = JSON.parse(await fs.readFile(indexPath, 'utf8')); } catch {}
  }
  if (!index) {
    console.log('fetching /laws/repealed index...');
    index = await client.listRepealed();
    await writeJson(indexPath, index);
  }
  console.log(`  ${index.length} repealed sections`);

  const dir = path.join(cacheDir, 'repealed');
  await ensureDir(dir);
  const results = [];
  for (const entry of index) {
    const { lawId, locationId, publishedDate } = entry;
    const safeLoc = String(locationId).replace(/[/\\]/g, '_');
    const lawDir = path.join(dir, lawId);
    await ensureDir(lawDir);
    const cachePath = path.join(lawDir, `${safeLoc}__${publishedDate}.json`);
    if (!force) {
      try {
        await fs.access(cachePath);
        results.push({ lawId, locationId, cached: true });
        continue;
      } catch {}
    }
    try {
      const json = await client.getDocument(lawId, locationId, { date: publishedDate });
      await writeJson(cachePath, { ...json, _meta: entry });
      results.push({ lawId, locationId, cached: false });
    } catch (e) {
      results.push({ lawId, locationId, error: e.message });
    }
  }
  return results;
}

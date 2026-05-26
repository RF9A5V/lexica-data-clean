import fs from 'fs/promises';
import path from 'path';

export function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export async function ensureDir(p) {
  await fs.mkdir(p, { recursive: true });
}

export async function readJson(p) {
  const text = await fs.readFile(p, 'utf8');
  return JSON.parse(text);
}

export async function writeJson(p, obj) {
  await ensureDir(path.dirname(p));
  await fs.writeFile(p, JSON.stringify(obj, null, 2));
}

export async function appendLine(p, line) {
  await ensureDir(path.dirname(p));
  await fs.appendFile(p, line + '\n');
}

export function getIn(obj, pathArr, dflt) {
  let cur = obj;
  for (const k of pathArr) {
    if (cur == null) return dflt;
    cur = cur[k];
  }
  return cur === undefined ? dflt : cur;
}

// Kebab-case a free-form string (used for sort keys, NOT for canonical_ids).
export function kebab(s) {
  return String(s)
    .toLowerCase()
    .replace(/&/g, 'and')
    .replace(/[^\w\s-]/g, '')
    .trim()
    .replace(/\s+/g, '-');
}

export function padSort(parts) {
  return parts.map((p) => String(p).padStart(6, '0')).join('.');
}

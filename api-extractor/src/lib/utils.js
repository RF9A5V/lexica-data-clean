import fs from 'fs/promises';
import fsSync from 'fs';
import path from 'path';

export function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

export function getEnv(name, fallback) {
  const v = process.env[name];
  return (v === undefined || v === '') ? fallback : v;
}

export function getIn(obj, pathArr, fallback) {
  if (!obj || !Array.isArray(pathArr)) return fallback;
  for (const key of pathArr) {
    if (obj && Object.prototype.hasOwnProperty.call(obj, key)) {
      obj = obj[key];
    } else {
      return fallback;
    }
  }
  return obj;
}

export function ensureDir(dir) {
  return fs.mkdir(dir, { recursive: true });
}

export async function appendLine(filePath, line) {
  await ensureDir(path.dirname(filePath));
  await fs.appendFile(filePath, line + '\n', 'utf8');
}

export function padSort(parts) {
  // pad numeric parts for lexicographic sort
  return parts.map((p) => {
    const m = String(p).match(/^(\d+)([\w\W]*)$/);
    if (m) {
      const n = m[1].padStart(6, '0');
      return n + (m[2] || '');
    }
    return String(p);
  }).join('.');
}

export function stableId(...parts) {
  return parts.filter(Boolean).join(':').toLowerCase();
}

export function canonicalCitation(lawId, sectionNum) {
  if (lawId && sectionNum) return `${lawId} § ${sectionNum}`;
  if (lawId) return String(lawId);
  return undefined;
}

export function readNDJSON(filePath) {
  const content = fsSync.readFileSync(filePath, 'utf8');
  return content.trim().split('\n').map(line => JSON.parse(line));
}


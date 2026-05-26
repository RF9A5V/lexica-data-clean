import { sleep, getIn } from '../lib/utils.js';

export class NysenateClient {
  constructor({ baseUrl, apiKey, rps = 3, retryMax = 5, retryBase = 500 } = {}) {
    this.baseUrl = (baseUrl || process.env.NYSENATE_BASE_URL || 'https://legislation.nysenate.gov/api/3').replace(/\/$/, '');
    this.apiKey = apiKey || process.env.NYSENATE_API_KEY;
    this.rps = parseFloat(process.env.NYSENATE_RATE_RPS || rps);
    this.retryMax = parseInt(process.env.NYSENATE_RETRY_MAX || retryMax, 10);
    this.retryBase = retryBase;
    this.lastAt = 0;
  }

  async throttle() {
    if (!this.rps || this.rps <= 0) return;
    const minInterval = 1000 / this.rps;
    const elapsed = Date.now() - this.lastAt;
    if (elapsed < minInterval) await sleep(minInterval - elapsed);
    this.lastAt = Date.now();
  }

  async request(pathname, { query = {}, retry = 0 } = {}) {
    await this.throttle();
    const url = new URL(this.baseUrl + pathname);
    if (this.apiKey) url.searchParams.set('key', this.apiKey);
    for (const [k, v] of Object.entries(query)) {
      if (v !== undefined && v !== null && v !== '') url.searchParams.set(k, v);
    }

    const res = await fetch(url.toString(), { headers: { accept: 'application/json' } });
    if (!res.ok) {
      if ((res.status === 429 || res.status >= 500) && retry < this.retryMax) {
        await sleep(this.retryBase * Math.pow(2, retry));
        return this.request(pathname, { query, retry: retry + 1 });
      }
      const body = await res.text().catch(() => '');
      throw new Error(`HTTP ${res.status} for ${url}: ${body.slice(0, 300)}`);
    }
    return res.json();
  }

  async listLaws() {
    const j = await this.request('/laws', { query: { limit: 500 } });
    return getIn(j, ['result', 'items'], []);
  }

  async getLawTreeFull(lawId, { date } = {}) {
    return this.request(`/laws/${encodeURIComponent(lawId)}`, { query: { full: 'true', date } });
  }

  async getDocument(lawId, locationId, { date } = {}) {
    return this.request(
      `/laws/${encodeURIComponent(lawId)}/${encodeURIComponent(locationId)}`,
      { query: { date } },
    );
  }

  // Pulls /laws/repealed paginating until exhausted.
  async listRepealed() {
    const all = [];
    let offset = 1;
    const size = 1000;
    while (true) {
      const j = await this.request('/laws/repealed', { query: { limit: size, offset } });
      const items = getIn(j, ['result', 'items'], []);
      all.push(...items);
      const total = j.total ?? items.length;
      if (offset + items.length > total || items.length === 0) break;
      offset += items.length;
    }
    return all;
  }
}

import { sleep, getEnv, getIn } from '../lib/utils.js';

export class NysenateClient {
  constructor(config) {
    this.config = config;
    this.baseUrl = getEnv(config.baseUrlEnv || 'NYSENATE_BASE_URL', config.defaultBaseUrl);
    this.apiKey = process.env[config.apiKeyEnv || 'NYSENATE_API_KEY'];
    this.rps = parseFloat(getEnv(config.rate?.rpsEnv || 'NYSENATE_RATE_RPS', String(config.rate?.defaultRps ?? 3)));
    this.retryMax = parseInt(getEnv(config.retry?.maxEnv || 'NYSENATE_RETRY_MAX', String(config.retry?.defaultMax ?? 5)));
    this.retryBase = config.retry?.baseDelayMs ?? 500;
    this.lastRequestAt = 0;
  }

  async throttle() {
    if (!this.rps || this.rps <= 0) return;
    const minInterval = 1000 / this.rps;
    const now = Date.now();
    const elapsed = now - this.lastRequestAt;
    if (elapsed < minInterval) {
      await sleep(minInterval - elapsed);
    }
    this.lastRequestAt = Date.now();
  }

  async request(path, { query = {}, headers = {}, retry = 0 } = {}) {
    await this.throttle();
    const url = new URL(this.baseUrl.replace(/\/$/, '') + path);
    // API key commonly required as `key`
    if (this.apiKey) url.searchParams.set('key', this.apiKey);
    for (const [k, v] of Object.entries(query)) {
      if (v !== undefined && v !== null && v !== '') url.searchParams.set(k, v);
    }

    const res = await fetch(url.toString(), {
      headers: { 'accept': 'application/json', ...headers },
    });

    if (!res.ok) {
      if ((res.status === 429 || res.status >= 500) && retry < this.retryMax) {
        const delay = this.retryBase * Math.pow(2, retry);
        await sleep(delay);
        return this.request(path, { query, headers, retry: retry + 1 });
      }
      const bodyText = await res.text().catch(() => '');
      throw new Error(`HTTP ${res.status} ${res.statusText} for ${url} :: ${bodyText.slice(0,200)}`);
    }

    const json = await res.json();
    return json;
  }

  extractArray(json) {
    // Try multiple nesting levels defined in config
    const df = this.config.response?.dataField;
    if (Array.isArray(df)) {
      const maybe = getIn(json, df, undefined);
      if (Array.isArray(maybe)) return maybe;
      if (maybe && Array.isArray(maybe.items)) return maybe.items; // common fallback
    }
    // Fallbacks
    if (Array.isArray(json.items)) return json.items;
    if (Array.isArray(json.result)) return json.result;
    return [];
  }

  extractTotalPages(json) {
    const tf = this.config.response?.totalPagesField;
    const v = Array.isArray(tf) ? getIn(json, tf, undefined) : undefined;
    return typeof v === 'number' ? v : undefined;
  }

  // List all laws
  async listLaws() {
    const p = this.config.endpoints.lawsIndex;
    const json = await this.request(p);
    return this.extractArray(json);
  }

  // Get law structure (hierarchy without full text)
  async getLawStructure(lawId) {
    const raw = this.config.endpoints.lawStructure;
    const path = raw.replace('{lawId}', encodeURIComponent(lawId));
    const json = await this.request(path);
    return json;
  }

  // Get law tree, optionally with full text included
  async getLawTree(lawId, { full = false, date } = {}) {
    const base = `/laws/${encodeURIComponent(lawId)}`;
    const json = await this.request(base, { query: { full: full ? 'true' : undefined, date } });
    return json;
  }

  // Get a specific document (node) with details/text
  async getDocument(lawId, docType, docId) {
    // Root LAW uses the base endpoint
    if (String(docType).toUpperCase() === 'LAW') {
      const p = (this.config.endpoints.lawStructure || `/laws/{lawId}`).replace('{lawId}', encodeURIComponent(lawId));
      const json = await this.request(p, { query: { view: 'full' } });
      return json;
    }
    const raw = this.config.endpoints.document;
    const path = raw
      .replace('{lawId}', encodeURIComponent(lawId))
      .replace('{docType}', encodeURIComponent(String(docType).toUpperCase()))
      .replace('{docId}', encodeURIComponent(String(docId)));
    const json = await this.request(path);
    return json;
  }

  // List sub-documents (children) of a node with pagination
  async listSubDocuments(lawId, docType, docId, { page = 1, size = 100 } = {}) {
    const raw = this.config.endpoints.subDocuments;
    const path = raw
      .replace('{lawId}', encodeURIComponent(lawId))
      .replace('{docType}', encodeURIComponent(String(docType).toUpperCase()))
      .replace('{docId}', encodeURIComponent(String(docId)))
      .replace('{?page,size}', '');
    const json = await this.request(path, { query: { page, size } });
    const items = this.extractArray(json);
    const totalPages = this.extractTotalPages(json);
    return { items, totalPages };
  }

  // List root-level documents under a law (e.g., Titles/Articles)
  async listRootDocuments(lawId, { page = 1, size = 100 } = {}) {
    const raw = this.config.endpoints.rootDocuments;
    const path = raw
      .replace('{lawId}', encodeURIComponent(lawId))
      .replace('{?page,size}', '');
    const json = await this.request(path, { query: { page, size } });
    const items = this.extractArray(json);
    const totalPages = this.extractTotalPages(json);
    return { items, totalPages };
  }
}

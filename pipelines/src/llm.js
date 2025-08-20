// Simple provider-agnostic LLM wrapper (OpenAI default)
// Reads prompts and schemas from ../prompts and ../schemas
// Exports: generatePass1(opinionText)

const fs = require('fs');
const path = require('path');

const seed = 2177750;

function readFile(rel) {
  const p = path.join(__dirname, '..', rel);
  return fs.readFileSync(p, 'utf8');
}

function sleep(ms) {
  return new Promise(res => setTimeout(res, ms));
}

function getOpenAI() {
  let OpenAI;
  try {
    OpenAI = require('openai');
  } catch (e) {
    throw new Error('OpenAI SDK not installed. Please run: npm i openai');
  }
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) throw new Error('Missing OPENAI_API_KEY in environment');
  return new OpenAI({ apiKey });
}

async function chatJson(promptText, userPayload, opts = {}) {
  const client = getOpenAI();
  const model = process.env.OPENAI_MODEL || opts.model || 'gpt-5-mini';
  const temperature = opts.temperature ?? 1;
  const maxRetries = Number.parseInt(process.env.OPENAI_MAX_RETRIES || opts.maxRetries || '4', 10);
  const baseDelayMs = Number.parseInt(process.env.OPENAI_RETRY_BASE_MS || opts.baseDelayMs || '500', 10);
  const messages = [
    { role: 'system', content: promptText + '\n\nReturn ONLY valid JSON. No prose.' },
    { role: 'user', content: userPayload }
  ];
  let attempt = 0;
  while (true) {
    try {
      const resp = await client.chat.completions.create({
        model,
        messages,
        response_format: { type: 'json_object' },
        seed: seed,
      });
      const content = resp.choices?.[0]?.message?.content || '{}';
      try {
        return JSON.parse(content);
      } catch (e) {
        const m = content.match(/\{[\s\S]*\}$/);
        if (m) return JSON.parse(m[0]);
        throw new Error('LLM returned non-JSON content');
      }
    } catch (e) {
      attempt += 1;
      const status = e?.status || e?.response?.status;
      const retriable = status === 429 || (status >= 500 && status < 600) || e.code === 'ECONNRESET' || e.code === 'ETIMEDOUT' || e.code === 'ENOTFOUND';
      if (!retriable || attempt > maxRetries) {
        throw e;
      }
      const jitter = Math.floor(Math.random() * 200);
      const delay = baseDelayMs * Math.pow(2, attempt - 1) + jitter;
      await sleep(delay);
    }
  }
}

function chunkText(t, maxChars = 35000) {
  if (!t) return '';
  if (t.length <= maxChars) return t;
  return t.slice(0, maxChars);
}

async function generateUnified(opinionText, opts = {}) {
  // Always use unified_prompt with minimal-mapped output enforced by minimal schema
  const prompt = readFile('prompts/unified_prompt.md');
  const schema = readFile('schemas/unified_minimal_schema.json');

  const systemPrompt = prompt;
  
  // Build case context section if provided
  let caseContext = '';
  if (opts.caseContext) {
    const { jurisdiction_name, court_name, decision_date } = opts.caseContext;
    caseContext = `Case Context:\n`;
    if (jurisdiction_name) caseContext += `- Jurisdiction: ${jurisdiction_name}\n`;
    if (court_name) caseContext += `- Court: ${court_name}\n`;
    if (decision_date) caseContext += `- Decision Date: ${decision_date}\n`;
    caseContext += `\n`;
  }
  
  let user = `JSON Schema (strict):\n${schema}\n\n${caseContext}Opinion Text (may be truncated):\n${chunkText(opinionText)}`;
  if (opts.retryPrompt) {
    user = `${opts.retryPrompt}\n\n${user}`;
  }
  const result = await chatJson(systemPrompt, user, opts);
  return result;
}

module.exports = {
  generateUnified,
};

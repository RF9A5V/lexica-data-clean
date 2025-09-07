import 'dotenv/config';
import OpenAI from 'openai';
import { TaxonomyResponseSchema } from '../util/schema.js';

const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

export async function fetchEnrichment({ systemPrompt, userPrompt, model }) {
  const resp = await client.chat.completions.create({
    model,
    temperature: 0,
    messages: [
      { role: 'system', content: systemPrompt },
      { role: 'user', content: userPrompt },
    ],
  });

  const choice = resp.choices?.[0]?.message?.content ?? '';
  let parsed;
  try {
    parsed = JSON.parse(choice);
  } catch (e) {
    throw new Error(`Model did not return valid JSON: ${choice.slice(0, 200)}...`);
  }
  const result = TaxonomyResponseSchema.parse(parsed);
  const usage = resp.usage || {};
  return { result, usage };
}

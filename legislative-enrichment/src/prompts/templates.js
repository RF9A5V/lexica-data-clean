export function buildSystemPrompt() {
  return [
    'You are a legal taxonomy assistant. From the provided statute section text, extract concise taxonomy terms and a short digest.',
    'Follow these categories and definitions:',
    '- field_of_law: the primary legal domains implicated (e.g., criminal law, administrative law, contracts).',
    '- doctrines: named doctrines, rules, or statutory schemes relevant to applying the law.',
    '- distinguishing_factors: concrete factual or contextual attributes that materially affect application.',
    'Write a brief fifth-grade reading level digest (80–120 words).',
    'Return ONLY valid JSON with keys: field_of_law, doctrines, distinguishing_factors, digest.',
  ].join(' ');
}

export function buildUserPrompt({ lawId, label, text }) {
  return [
    `Law: ${lawId}`,
    label ? `Label: ${label}` : null,
    '',
    'Section text:',
    text,
    '',
    'Output JSON schema:',
    '{',
    '  "field_of_law": string[],',
    '  "doctrines": string[],',
    '  "distinguishing_factors": string[],',
    '  "digest": string',
    '}',
    '',
    'Rules:',
    '- Use short, generalizable phrases for taxonomy terms.',
    '- Avoid duplication and over-specific wording.',
    '- Do not include extraneous keys or commentary.',
  ].filter(Boolean).join('\n');
}

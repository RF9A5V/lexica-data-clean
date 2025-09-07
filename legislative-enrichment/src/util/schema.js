import { z } from 'zod';

// Taxonomy response for statute sections, Phase 1
// We expect three categories plus a digest string.
export const TaxonomyResponseSchema = z.object({
  field_of_law: z.array(z.string().min(1)).default([]),
  doctrines: z.array(z.string().min(1)).default([]),
  distinguishing_factors: z.array(z.string().min(1)).default([]),
  digest: z.string().min(20),
});

export const KeywordsSeedSchema = z.object({
  version: z.string().min(1),
  keywords: z.array(z.string().min(1)).min(1),
});

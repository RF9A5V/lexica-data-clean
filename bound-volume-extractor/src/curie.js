/**
 * CURIE generation for bound-volume cases and opinions.
 *
 * Mirrors the logic in `co-collection/src/utils/curieGeneration.js` so the
 * bound-volume-extractor stays self-contained (no cross-package import).
 * Keep the two implementations behaviorally compatible — ingestion downstream
 * relies on these CURIEs being identical to what `co-collection` would
 * produce for the same case data.
 *
 * Case CURIE:    `<reporter-norm>:<volume>:<first-page>:<name-slug>`
 *                with `:NN` collision-disambiguation suffix on duplicates.
 *   e.g.,        `ny3d:30:1:myers-v-schneiderman`
 *                `ad3d:157:417:koeppel-v-volkswagen-group-of-america-inc`
 *
 * Opinion CURIE: `<caseCurie>#<opinion-index>-<type>[-<author-slug>]`
 *   e.g.,        `ad3d:157:417:olatujoye-people-v#0-memorandum`
 *                `ad3d:157:1072:jacobson-v-blaise#0-memorandum-lynch`
 */

/**
 * Normalize a reporter string ("NY3d", "AD3d", "Misc 3d") to its CURIE form
 * (lowercase, no spaces). Matches `normalizeCitation` in co-collection.
 */
export function reporterToCurieNorm(reporter) {
  return (reporter || '').toLowerCase().replace(/\s+/g, '');
}

/**
 * Slug a free-form case-name string into CURIE-safe form: lowercase ASCII
 * letters/digits joined by hyphens, leading/trailing hyphens stripped.
 */
export function slugName(name) {
  if (!name) return '';
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

/**
 * Build the BASE case CURIE (without the collision suffix). Caller is
 * responsible for tracking duplicates and applying `appendOccurrence`.
 */
export function caseCurieBase(volumeMeta, volumePage, nameAbbreviation) {
  if (!volumeMeta?.reporter || !volumeMeta?.volume || !volumePage) return null;
  const reporter = reporterToCurieNorm(volumeMeta.reporter);
  const slug = slugName(nameAbbreviation || '');
  const base = `${reporter}:${volumeMeta.volume}:${volumePage}`;
  return slug ? `${base}:${slug}` : base;
}

/**
 * Append a 2-digit zero-padded occurrence suffix when disambiguating a
 * collision group. Occurrence 1 (the first) gets the bare CURIE; 2nd
 * onward gets `:02`, `:03`, etc.
 */
export function appendOccurrence(baseCurie, occurrence) {
  if (occurrence <= 1) return baseCurie;
  return `${baseCurie}:${String(occurrence).padStart(2, '0')}`;
}

/**
 * Generate an opinion CURIE deterministically from its parent case CURIE,
 * its zero-based index in the case's `opinions` array, and the opinion's
 * type/author. Matches `generateOpinionCurie` in co-collection.
 */
export function opinionCurie(caseCurie, opinionIndex, opinionType, author) {
  if (!caseCurie || !Number.isInteger(opinionIndex) || opinionIndex < 0) return null;
  const typeSlug = slugTypeForCurie(opinionType);
  const authorSlug = slugAuthor(author);
  const tail = authorSlug ? `${opinionIndex}-${typeSlug}-${authorSlug}` : `${opinionIndex}-${typeSlug}`;
  return `${caseCurie}#${tail}`;
}

function slugTypeForCurie(type) {
  if (!type) return 'opinion';
  const slug = String(type).toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
  return slug || 'opinion';
}

function slugAuthor(author) {
  if (!author) return '';
  // Take the surname (text before the first comma).
  const surname = String(author).split(',')[0] || '';
  return surname.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
}

/**
 * Walk a parser's case list and assign final CURIEs in-place, handling
 * collisions via occurrence-suffix. Returns the same list (mutated) for
 * caller convenience plus a summary `{caseCurieCount, collisionCount}`.
 *
 * Cases with no derivable CURIE (missing volume_page, etc.) get
 * `case_curie: null` and are skipped for opinion CURIEs as well.
 *
 * Also synthesizes `file_name` on every case as `<padded-page>-<padded-occ>`
 * (e.g. `0904-01`, `0904-02`). The format mirrors the CAP-imported cohort's
 * file_name and gives stacked-memo cases sharing a page caption a stable,
 * content-derivable disambiguator — without it those cases collapse on the
 * Phase-1 case_curie hash (see docs/planning/case-curie-content-derived.md).
 * Pre-existing `c.file_name` is preserved if a caller already set one.
 */
export function assignCuries(cases, volumeMeta) {
  const baseCounts = new Map();    // base CURIE → next occurrence number
  const pageCounts = new Map();    // volume_page → next position-on-page
  let collisionCount = 0;
  let caseCount = 0;
  for (const c of cases) {
    const page = c.volume_page ?? c.first_page;

    // file_name: <4-digit page>-<2-digit position-on-page>. Independent of
    // curie collision counting because two cases can share a page without
    // sharing a curie base (different names) yet still benefit from the
    // disambiguator on the content-hash side.
    if (c.file_name == null && page != null) {
      const pos = (pageCounts.get(String(page)) || 0) + 1;
      pageCounts.set(String(page), pos);
      c.file_name = `${String(page).padStart(4, '0')}-${String(pos).padStart(2, '0')}`;
    }

    const base = caseCurieBase(volumeMeta, page, c.name);
    if (!base) {
      c.case_curie = null;
      for (const op of c.opinions || []) op.curie = null;
      continue;
    }
    const occ = (baseCounts.get(base) || 0) + 1;
    baseCounts.set(base, occ);
    if (occ > 1) collisionCount++;
    c.case_curie = appendOccurrence(base, occ);
    caseCount++;
    for (let i = 0; i < (c.opinions || []).length; i++) {
      const op = c.opinions[i];
      op.curie = opinionCurie(c.case_curie, i, op.opinion_type, op.author);
    }
  }
  return { caseCount, collisionCount };
}

# nys-extractor

Fetches NYS consolidated and unconsolidated laws from the
[NYSenate Open Legislation API](https://legislation.nysenate.gov/static/docs/html/laws.html)
and loads them into `nys_legislative` Postgres for use by `co-collection`.

## Pipeline

```
fetch     : NYSenate API → data/cache/laws/<LAWID>.json + data/cache/repealed/...
transform : data/cache/* → data/ndjson/nys.ndjson  (units + aliases)
load      : data/ndjson/nys.ndjson → nys_legislative Postgres
```

## CURIE strategy

The case-side citation extractor (`co-collection/scripts/extractLegislativeCitationsFromCaselaw.js`)
emits CURIEs like `nys:penal-law-120.05` based on the kebab-cased law name as it
appears in the case opinion. The API uses 3-letter `lawId` codes (PEN, CPLR, FCT).

`configs/law_aliases.json` maps each `lawId` → primary kebab form + aliases.
For each section the loader writes:

- `units.canonical_id` = `nys:<primary>-<sectionNumber>` (one row)
- `unit_aliases` = one row per additional kebab form (e.g. `nys:pl-120.05`, `nys:cpl-440.30`)

`co-collection`'s `resolveLegislativeCitations.js` and
`seedGlobalNormsFromLegislative.js` UNION `units.canonical_id` with
`unit_aliases.alias`, so all forms resolve.

## Running

```bash
# 1. cache the API responses (resumable; skips files that already exist)
node src/index.js fetch                    # all 137 laws + repealed sections
node src/index.js fetch --only=PEN,CPL     # subset for testing

# 2. transform cache → NDJSON
node src/index.js transform

# 3. load NDJSON → Postgres (truncates by default)
node src/index.js load --verbose

# all-in-one
node src/index.js all --only=ABC --verbose
```

## Lazy version history

`units.published_dates TEXT[]` stores the API's snapshot history (weekly back
to 2014-09-22). Use this to lazily fetch historical text on demand later — no
bulk pull required at load time.

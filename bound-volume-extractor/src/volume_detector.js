/**
 * Detect bound-volume metadata (reporter series, volume number, court) from
 * the PDF's first few pages. NY official reports follow consistent title-page
 * conventions:
 *   "REPORTS OF CASES DECIDED IN THE COURT OF APPEALS ... VOLUME 38"  → NY3d, Court of Appeals
 *   "OFFICIAL REPORTS APPELLATE DIVISION ... THIRD SERIES VOLUME 216" → AD3d, Appellate Division
 *   "MISCELLANEOUS REPORTS ... THIRD SERIES VOLUME 78"                → Misc 3d, trial courts
 *
 * Returns { reporter, volume, court, source_db } or null if undetectable.
 */

// Title-page layout for all three series (verified against 30 NY3d, 157
// AD3d, 57 Misc 3d):
//   <descriptive header>
//   VOLUME <N>
//   <reporter title>
//   3d SERIES
//   <year>
// VOLUME comes BEFORE the reporter title; "3d SERIES" is digit-d, not the
// word "THIRD". Patterns capture the volume number and require the
// reporter title to follow within a short window.
const REPORTER_PATTERNS = [
  {
    reporter: 'NY3d',
    court: 'Court of Appeals',
    source_db: 'ny_reporter',
    re: /VOLUME\s+(\d+)[\s\S]{0,200}?(?:NEW\s+YORK\s+REPORTS|COURT\s+OF\s+APPEALS)[\s\S]{0,100}?3d\s+SERIES/i,
  },
  {
    reporter: 'AD3d',
    court: 'Appellate Division',
    source_db: 'ny_appellate_division',
    re: /VOLUME\s+(\d+)[\s\S]{0,200}?APPELLATE\s+DIVISION\s+REPORTS[\s\S]{0,100}?3d\s+SERIES/i,
  },
  {
    reporter: 'Misc 3d',
    court: 'Trial Courts',
    source_db: 'ny_trial_courts',
    re: /VOLUME\s+(\d+)[\s\S]{0,200}?MISCELLANEOUS\s+REPORTS[\s\S]{0,100}?3d\s+SERIES/i,
  },
];

export function detectVolume(firstPagesText) {
  const haystack = firstPagesText.replace(/\s+/g, ' ');
  for (const p of REPORTER_PATTERNS) {
    const m = haystack.match(p.re);
    if (m) {
      return {
        reporter: p.reporter,
        volume: parseInt(m[1], 10),
        court: p.court,
        source_db: p.source_db,
      };
    }
  }
  return null;
}

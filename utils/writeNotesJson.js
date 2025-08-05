import fs from "fs/promises";
import path from "path";

export async function writeNotesJson(notesArr, titleNum, outDirBase = __dirname) {
  const outDir = path.join(outDirBase, `title_${titleNum}`);
  await fs.mkdir(outDir, { recursive: true });
  await fs.writeFile(
    path.join(outDir, 'notes_text.json'),
    JSON.stringify(notesArr, null, 2),
    'utf8'
  );
  console.log(`Wrote ${notesArr.length} notes to ${path.join(outDir, 'notes_text.json')}`);

  // Flag for review: unresolved or malformed refs
  const flagged = notesArr.filter(note => {
    // Flag if heading or content suggests a short title or amendment, but all ref arrays are empty
    const h = (note.heading || '').toLowerCase();
    const c = (note.content || '').toLowerCase();
    const isShortTitle = h.includes('short title') || c.includes('may be cited as');
    const hasNoRefs = (!note.enactedRefs || note.enactedRefs.length === 0) &&
                     (!note.amendedRefs || note.amendedRefs.length === 0) &&
                     (!note.repealedRefs || note.repealedRefs.length === 0);
    return isShortTitle && hasNoRefs;
  }).map(note => ({
    identifier: note.identifier,
    documentId: note.documentId,
    heading: note.heading,
    content: note.content,
    attemptedExtraction: {
      enacted: note.enacted,
      amended: note.amended,
      repealed: note.repealed
    },
    status: 'unresolved'
  }));
  if (flagged.length > 0) {
    await fs.writeFile(
      path.join(outDir, 'notes_review.json'),
      JSON.stringify(flagged, null, 2),
      'utf8'
    );
    console.log(`Flagged ${flagged.length} notes for review in ${path.join(outDir, 'notes_review.json')}`);
  }
}
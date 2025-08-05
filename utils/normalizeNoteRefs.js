// Normalizes references in notesArr, adding normalized fields to each note object
export function normalizeNoteRefs(notesArr, titleNum) {
  function normalizeSectionRef(str, noteObj) {
    const results = [];
    console.log('--- Normalizing string:', str);
    // Handle 'this section'
    if (/^this section$/i.test(str.trim())) {
      console.log('Matched: this section →', noteObj.identifier);
      results.push(noteObj.identifier);
    }
    // Handle 'sections X, Y, and Z of Title N' (allow trailing context)
    let multiMatch = str.match(/sections ([\w\d\s, and]+) of Title (\d+)(?:, [^\]]+)?/i);
    if (multiMatch) {
      const sectionsStr = multiMatch[1];
      const title = multiMatch[2].padStart(2, '0');
      const sectionParts = sectionsStr.split(/,| and /i).map(s => s.trim()).filter(Boolean);
      const refs = sectionParts.map(section => `/us/usc/t${title}/s${section.replace(/\s+/g, '').toLowerCase()}`);
      console.log('Matched: plural sections of Title', title, '→', refs);
      results.push(...refs);
    }
    // Handle 'sections X, Y, and Z of this title' (allow trailing context)
    multiMatch = str.match(/sections ([\w\d\s, and]+) of this title(?:, [^\]]+)?/i);
    if (multiMatch && typeof titleNum === 'string') {
      const sectionsStr = multiMatch[1];
      const sectionParts = sectionsStr.split(/,| and /i).map(s => s.trim()).filter(Boolean);
      const refs = sectionParts.map(section => `/us/usc/t${titleNum}/s${section.replace(/\s+/g, '').toLowerCase()}`);
      console.log('Matched: plural sections of this title', titleNum, '→', refs);
      results.push(...refs);
    }
    // Handle singular 'section X of Title Y'
    let m = str.match(/section ([\w\d]+) of Title (\d+)/i);
    if (m) {
      const section = m[1].replace(/\s+/g, '').toLowerCase();
      const title = m[2].padStart(2, '0');
      const ref = `/us/usc/t${title}/s${section}`;
      console.log('Matched: singular section of Title', title, '→', ref);
      results.push(ref);
    }
    // Handle singular 'section X of this title'
    m = str.match(/section ([\w\d]+) of this title/i);
    if (m && typeof titleNum === 'string') {
      const section = m[1].replace(/\s+/g, '').toLowerCase();
      const ref = `/us/usc/t${titleNum}/s${section}`;
      console.log('Matched: singular section of this title', titleNum, '→', ref);
      results.push(ref);
    }
    console.log('Final normalized refs:', results);
    return results;
  }
  for (const noteObj of notesArr) {
    // Always set normalized refs as arrays (even if empty)
    let enactedRefs = [];
    let enactedNotesRefs = [];
    let amendedRefs = [];
    let repealedRefs = [];
    if (noteObj.enacted && Array.isArray(noteObj.enacted)) {
      enactedRefs = noteObj.enacted.flatMap(str => normalizeSectionRef(str, noteObj)).filter(Boolean);
      enactedNotesRefs = noteObj.enacted
        .map(str => {
          let m = str.match(/provisions set out as notes under section ([\w\d]+) of Title (\d+)/i);
          if (m) {
            const section = m[1].replace(/\s+/g, '').toLowerCase();
            const title = m[2].padStart(2, '0');
            return `/us/usc/t${title}/s${section}`;
          }
          m = str.match(/provisions set out as notes under section ([\w\d]+) of this title/i);
          if (m && typeof titleNum === 'string') {
            const section = m[1].replace(/\s+/g, '').toLowerCase();
            return `/us/usc/t${titleNum}/s${section}`;
          }
          return null;
        })
        .filter(Boolean);
    }
    if (noteObj.amended && Array.isArray(noteObj.amended)) {
      amendedRefs = noteObj.amended.flatMap(str => normalizeSectionRef(str, noteObj)).filter(Boolean);
    }
    if (noteObj.repealed && Array.isArray(noteObj.repealed)) {
      repealedRefs = noteObj.repealed.flatMap(str => normalizeSectionRef(str, noteObj)).filter(Boolean);
    }
    // Only keep refs for short title notes
    const heading = (noteObj.heading || '').replace(/[^a-z]/gi, '').toLowerCase();
    if (heading.startsWith('shorttitle')) {
      noteObj.enactedRefs = enactedRefs;
      noteObj.amendedRefs = amendedRefs;
      noteObj.repealedRefs = repealedRefs;
      noteObj.enactedNotesRefs = enactedNotesRefs;
    } else {
      delete noteObj.enactedRefs;
      delete noteObj.amendedRefs;
      delete noteObj.repealedRefs;
      delete noteObj.enactedNotesRefs;
    }
    // Remove raw enacted/amended/repealed fields after normalization
    delete noteObj.enacted;
    delete noteObj.amended;
    delete noteObj.repealed;
  }
}
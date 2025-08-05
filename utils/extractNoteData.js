import { XMLSerializer } from "xmldom-qsa";

function getOuterXML(node) {
  return new XMLSerializer().serializeToString(node);
}

// Extracts all relevant fields from a <note> element node
export function extractNoteData(note) {
  // Always defined for the note
  let skippedLegalRefs = [];
  let ancestor = note;
  let identifier = null;
  while (ancestor && !identifier) {
    identifier = ancestor.getAttribute && ancestor.getAttribute('identifier');
    ancestor = ancestor.parentNode;
  }

  const refNodes = Array.from(note.getElementsByTagName('ref'));
  const refs = refNodes.map(ref => ref.getAttribute('href')).filter(Boolean);
  let heading = null;
  const headingElem = note.getElementsByTagName('heading')[0];
  if (headingElem && headingElem.textContent) {
    heading = headingElem.textContent.trim();
  }
  let content = '';
  for (let i = 0; i < note.childNodes.length; i++) {
    const child = note.childNodes[i];
    if (child.nodeType === 1 && child.nodeName === 'heading') continue;
    if (child.textContent) content += child.textContent.trim() + ' ';
  }
  content = content.trim();
  // Extract tables as per user preference
  const tableNodes = Array.from(note.getElementsByTagName('table'));
  let tables = [];
  for (const table of tableNodes) {
    let columns = [];
    let rows = [];
    let headerRow = null;
    let headerCells = [];
    const thead = table.getElementsByTagName('thead')[0];
    if (thead) {
      const trNodes = Array.from(thead.getElementsByTagName('tr'));
      for (const tr of trNodes) {
        const ths = Array.from(tr.getElementsByTagName('th'));
        if (ths.length > 1) {
          headerRow = tr;
          headerCells = ths;
          break;
        }
      }
      if (!headerRow) {
        for (const tr of trNodes) {
          const tds = Array.from(tr.getElementsByTagName('td'));
          if (tds.length > 1) {
            headerRow = tr;
            headerCells = tds;
            break;
          }
        }
      }
    }
    // Fallback: search in <tbody> or all <tr> in table
    if (!headerRow) {
      const allTrs = Array.from(table.getElementsByTagName('tr'));
      for (const tr of allTrs) {
        const ths = Array.from(tr.getElementsByTagName('th'));
        if (ths.length > 1) {
          headerRow = tr;
          headerCells = ths;
          break;
        }
      }
      if (!headerRow) {
        for (const tr of allTrs) {
          const tds = Array.from(tr.getElementsByTagName('td'));
          if (tds.length > 1) {
            headerRow = tr;
            headerCells = tds;
            break;
          }
        }
      }
    }
    columns = headerCells.map(cell => cell.textContent.trim().replace(/\s+/g, ' '));
    let allRows = Array.from(table.getElementsByTagName('tr'));
    let dataRows = allRows;
    if (headerRow) {
      dataRows = allRows.filter(tr => tr !== headerRow);
    }
    for (const row of dataRows) {
      const cells = Array.from(row.getElementsByTagName('td'));
      if (cells.length > 0) {
        rows.push(cells.map(cell => cell.textContent.trim().replace(/\s+/g, ' ')));
      }
    }
    if (columns.length > 0 && rows.length > 0) {
      tables.push({ columns, rows });
    }
  }
  // Short title and ref extraction
  let shortTitle = null;
  const topicAttr = note.getAttribute && note.getAttribute('topic');
  let enacted = null, amended = null, repealed = null;
  if ((topicAttr && topicAttr.toLowerCase().includes('shorttitle')) || (heading && heading.toLowerCase().includes('short title'))) {
    let quotedText = null;
    const quotedContentElem = note.getElementsByTagName('quotedContent')[0];
    if (quotedContentElem && quotedContentElem.textContent) {
      quotedText = quotedContentElem.textContent.trim();
    } else {
      const citedMatch = content.match(/may be cited as ([“\"][^”\"]+[”\"])/i);
      if (citedMatch && citedMatch[1]) {
        quotedText = citedMatch[1].trim();
      }
    }
    let bracketMatch = quotedText && quotedText.match(/^([“\"])?This Act \[(.+?)\] may be cited as (.+)[”\"]?\.?$/i);
    if (!bracketMatch && quotedContentElem && quotedContentElem.textContent) {
      bracketMatch = quotedContentElem.textContent.match(/^([“\"])?This Act \[(.+?)\] may be cited as (.+)[”\"]?\.?$/i);
    }
    if (bracketMatch) {
      const actions = bracketMatch[2];
      const actionRegex = /\b(enacting|amending|repealing) ([^\],]+(?:,[^\],]+)*)(?=,\s*(enacting|amending|repealing)|\]|$)/gi;
      let match;
      let enactedList = [], amendedList = [], repealedList = [];
      const contextTerms = [
        'Judiciary', 'Judicial Procedure', 'Banks and Banking', 'Internal Revenue Code',
        'Education', 'Labor', 'Transportation', 'Agriculture', 'Commerce', 'Public Health', 'and', 'or'
      ];
      function isSectionReference(str) {
        return /section|title|chapter|subchapter|usc|stat|note/i.test(str);
      }
      while ((match = actionRegex.exec(actions)) !== null) {
        const action = match[1].toLowerCase();
        // 1. Extract full plural section references from the entire action string
        let pluralPattern = /(sections [\w\d\s, and]+ of (?:Title \d+|this title)(?:, [^\]]+)?)/gi;
        let pluralArgs = [];
        let actionRemainder = match[2];
        let pluralMatch;
        while ((pluralMatch = pluralPattern.exec(actionRemainder)) !== null) {
          pluralArgs.push(pluralMatch[1].trim());
        }
        // Remove plural matches from the action string
        let cleaned = actionRemainder;
        pluralArgs.forEach(p => {
          cleaned = cleaned.replace(p, '');
        });
        // 2. Split the remainder as usual
        let nonPluralArgs = cleaned
          .split(/,| and /)
          .map(s => s.trim())
          .filter(Boolean);
        // Flatten and strictly filter all args
        let allArgs = [...pluralArgs, ...nonPluralArgs]
          .flatMap(arg => arg.split(/,| and /).map(s => s.trim()).filter(Boolean));
        // Only allow substrings that match strict legal reference pattern and do not contain amending/repealing/enacting anywhere
        const strictLegalRefPattern = /^(sections? [\w\d\-, and]+ of (this title|Title \d+)|section [\w\d\-]+ of (this title|Title \d+)|chapter [\w\d\-]+ of (this title|Title \d+)|subchapter [\w\d\-]+ of (this title|Title \d+)|title \d+)$/i;
        let args = allArgs.filter(arg => {
          const lower = arg.toLowerCase();
          if (lower.includes('amending') || lower.includes('repealing') || lower.includes('enacting')) {
            console.log('Skipping phrase with embedded action:', arg);
            return false;
          }
          if (strictLegalRefPattern.test(arg) && isSectionReference(arg) && !contextTerms.some(term => arg === term)) {
            return true;
          } else {
            console.log('Skipping non-legal-ref phrase:', arg);
            skippedLegalRefs.push(arg);
            return false;
          }
        });
        if (action === 'enacting') enactedList.push(...args);
        if (action === 'amending') amendedList.push(...args);
        if (action === 'repealing') repealedList.push(...args);
      }
      if (enactedList.length > 0) enacted = enactedList;
      if (amendedList.length > 0) amended = amendedList;
      if (repealedList.length > 0) repealed = repealedList;
      let titleMatch = bracketMatch[3].match(/[‘\"]([^’\"]+)[’\"]/);
      if (titleMatch) {
        shortTitle = titleMatch[1].trim();
      } else {
        shortTitle = bracketMatch[3].trim();
      }
    } else if (quotedText) {
      let titleMatch = quotedText.match(/[‘\"]([^’\"]+)[’\"]/);
      if (titleMatch) {
        shortTitle = titleMatch[1].trim();
      }
    }
  }
  // Construct note object
  const noteObj = { identifier };
  noteObj.documentId = note.getAttribute && note.getAttribute('id');
  noteObj.refs = Array.from(new Set(refs));
  if (heading) noteObj.heading = heading;
  if (content) noteObj.content = content;
  if (tables.length > 0) noteObj.tables = tables;
  if (shortTitle) noteObj.shortTitle = shortTitle;
  // Attach as temporary fields for normalization
  if (enacted) noteObj.enacted = enacted;
  // Flag if any skipped legal refs
  if (skippedLegalRefs && skippedLegalRefs.length > 0) {
    noteObj.hasSkippedLegalRefs = true;
    noteObj.skippedLegalRefs = skippedLegalRefs;
  }
  if (amended) noteObj.amended = amended;
  if (repealed) noteObj.repealed = repealed;
  return noteObj;
}
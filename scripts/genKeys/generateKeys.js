import fs from 'fs';
import readline from 'readline';
import path from 'path';

/**
 * Returns an async iterator over parsed NDJSON objects from a file
 * @param {string} filename - Path to the NDJSON file
 */
async function* ndjsonIterator(filename) {
  const fileStream = fs.createReadStream(filename);
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });
  for await (const line of rl) {
    if (line.trim() === '') continue;
    try {
      yield JSON.parse(line);
    } catch (err) {
      console.warn('Skipping invalid JSON line:', line);
    }
  }
}

// Hierarchy order for stack logic
const HIERARCHY = ['title', 'subtitle', 'part', 'subpart', 'chapter', 'subchapter'];
const SECTION_LEVELS = [
  'section', 'subsection', 'paragraph', 'subparagraph', 'clause', 'subclause', 'item'
];
const HEADING_LEVELS = {
  section: '#',
  subsection: '##',
  paragraph: '###',
  subparagraph: '####',
  clause: '#####',
  subclause: '######',
  item: null // handled as list
};

function getLocalId(identifier) {
  const parts = identifier.split('/');
  return parts[parts.length - 1];
}
function getSectionNum(identifier) {
  const match = identifier.match(/\/s(\w+)$/i);
  return match ? match[1].toLowerCase() : null;
}
function getLevel(type) {
  return SECTION_LEVELS.indexOf(type);
}
function renderMarkdown(element, depth = 0) {
  let out = '';
  const headingLevel = HEADING_LEVELS[element.type];
  const id = getLocalId(element.identifier);
  let headingText = `(${id})`;
  if (element.type === 'section') {
    headingText = `Section ${id}`;
  }
  if (element.heading) {
    headingText += `: ${element.heading}`;
  }
  if (element.type === 'item') {
    if (element.content) {
      out += `  `.repeat(depth - 1) + `1. ${element.content}\n`;
    } else {
      out += `  `.repeat(depth - 1) + `1. (${id})\n`;
    }
  } else {
    out += `${headingLevel} ${headingText}\n`;
    if (element.content) {
      out += `${element.content}\n`;
    }
  }
  if (element.children && element.children.length > 0) {
    const childIsItem = element.children[0]?.type === 'item';
    if (childIsItem) {
      element.children.forEach(child => {
        out += renderMarkdown(child, depth + 1);
      });
    } else {
      element.children.forEach(child => {
        out += renderMarkdown(child, depth + 1);
      });
    }
  }
  return out;
}

const DIR_PREFIX = {
  title: 'title_',
  subtitle: 'subtitle_',
  chapter: 'chapter_',
  subchapter: 'subchapter_',
  part: 'part_',
  subpart: 'subpart_'
};

// Extracts the number from the identifier for a given type
function extractNumFromIdentifier(identifier, type) {
  const parts = identifier.split('/').filter(Boolean);
  let code = '';
  switch (type) {
    case 'title': code = 't'; break;
    case 'subtitle': code = 'st'; break;
    case 'part': code = 'pt'; break;
    case 'subpart': code = 'spt'; break;
    case 'chapter': code = 'ch'; break;
    case 'subchapter': code = 'sch'; break;
    default: return null;
  }
  const match = parts.find(p => p.startsWith(code));
  if (!match) return null;
  if (type === 'part') {
    // ptIII -> III (Roman numerals)
    const m = match.match(/^pt([IVXLCDM]+)$/i);
    if (m) {
      console.log(`[DEBUG] Extracted part: ${m[1]} from ${match}`);
      return m[1];
    }
    return null;
  } else if (type === 'subpart') {
    // sptA -> A (letters only)
    const m = match.match(/^spt([A-Za-z]+)$/);
    if (m) {
      console.log(`[DEBUG] Extracted subpart: ${m[1]} from ${match}`);
      return m[1];
    }
    return null;
  } else {
    const numMatch = match.match(/(\d+)$/);
    return numMatch ? numMatch[1] : null;
  }
}

async function ensureDirAndWriteContent(dir, content) {
  await fs.promises.mkdir(dir, { recursive: true });
  const filePath = path.join(dir, 'content.txt');
  await fs.promises.writeFile(filePath, content, 'utf8');
}

function getHierarchyLevel(type) {
  return HIERARCHY.indexOf(type);
}

async function main() {
  const filename = process.argv[2];
  if (!filename) {
    console.error('Usage: node generateKeys.js <ndjsonfile>');
    process.exit(1);
  }

  // Will be set when we encounter the first title element
  let clearedTitleDir = false;
  let titleDir = null;

  // Stack: [{ type, dir, num }]
  const stack = [];
  // For section and below, we need to collect and nest children.
  let currentSection = null;
  let sectionStack = [];
  for await (const obj of ndjsonIterator(filename)) {
    // Hierarchy elements (title, subtitle, part, subpart, chapter, subchapter) - handle content.txt
    if (HIERARCHY.includes(obj.type) && obj.contentType === 'heading') {
      const level = getHierarchyLevel(obj.type);
      const num = extractNumFromIdentifier(obj.identifier, obj.type);
      if (!num) continue;
      // If this is the first title, clear its directory before proceeding
      if (!clearedTitleDir && obj.type === 'title') {
        titleDir = path.join('data', 'keywords', `${DIR_PREFIX.title}${num}`);
        try {
          await fs.promises.rm(titleDir, { recursive: true, force: true });
          console.log(`[INFO] Cleared directory: ${titleDir}`);
        } catch (err) {
          if (err.code !== 'ENOENT') throw err;
        }
        clearedTitleDir = true;
      }
      // Pop stack until we find the parent
      while (stack.length && getHierarchyLevel(stack[stack.length - 1].type) >= level) {
        // Before popping, flush currentSection if present
        if (currentSection) {
          const secNum = getSectionNum(currentSection.identifier);
          const secDir = stack.length ? stack[stack.length - 1].dir : titleDir;
          const outPath = path.join(secDir, `section_${secNum}.md`);
          await fs.promises.mkdir(secDir, { recursive: true });
          const md = renderMarkdown(currentSection);
          await fs.promises.writeFile(outPath, md, 'utf8');
          console.log(`[DEBUG] Flushed section before hierarchy pop: ${outPath}`);
          currentSection = null;
        }
        stack.pop();
      }
      // Build directory path
      const parentDir = stack.length ? stack[stack.length - 1].dir : path.join('data', 'keywords');
      const dirName = `${DIR_PREFIX[obj.type]}${num}`;
      const dir = path.join(parentDir, dirName);
      // Push this element onto the stack
      stack.push({ type: obj.type, dir, num });
      // Write content.txt
      await ensureDirAndWriteContent(dir, obj.content || '');
      console.log(`Wrote: ${path.join(dir, 'content.txt')}`);
      continue;
    }
    // Section and below (section, subsection, ...)
    if (!SECTION_LEVELS.includes(obj.type)) continue;
    // Map to track open elements by identifier
    if (!globalThis._elementMap) globalThis._elementMap = {};
    const elementMap = globalThis._elementMap;
    // Section start
    if (obj.type === 'section') {
      // Write previous section if exists
      if (currentSection) {
        const secNum = getSectionNum(currentSection.identifier);
        const secDir = stack.length ? stack[stack.length - 1].dir : titleDir;
        const outPath = path.join(secDir, `section_${secNum}.md`);
        await fs.promises.mkdir(secDir, { recursive: true });
        const md = renderMarkdown(currentSection);
        await fs.promises.writeFile(outPath, md, 'utf8');
        console.log(`[DEBUG] Wrote section: ${outPath}`);
      }
      // Start new section or update if already exists
      let el = elementMap[obj.identifier];
      if (!el) {
        el = {
          ...obj,
          heading: obj.contentType === 'heading' ? obj.content : undefined,
          content: obj.contentType === 'body' ? obj.content : undefined,
          children: []
        };
        elementMap[obj.identifier] = el;
      } else {
        if (obj.contentType === 'heading') el.heading = obj.content;
        if (obj.contentType === 'body') el.content = obj.content;
      }
      currentSection = el;
      sectionStack = [currentSection];
      continue;
    }
    // Lower-level elements (subsection, paragraph, etc.)
    const depth = getLevel(obj.type);
    // Pop sectionStack to correct parent
    while (sectionStack.length > 1 && getLevel(sectionStack[sectionStack.length - 1].type) >= depth) {
      sectionStack.pop();
    }
    // Find or create element by identifier
    let el = elementMap[obj.identifier];
    if (!el) {
      el = {
        ...obj,
        heading: obj.contentType === 'heading' ? obj.content : undefined,
        content: obj.contentType === 'body' ? obj.content : undefined,
        children: []
      };
      elementMap[obj.identifier] = el;
      // Attach to parent
      sectionStack[sectionStack.length - 1].children.push(el);
      sectionStack.push(el);
    } else {
      if (obj.contentType === 'heading') el.heading = obj.content;
      if (obj.contentType === 'body') el.content = obj.content;
      // If not already on stack, attach to parent
      if (!sectionStack.includes(el)) {
        sectionStack[sectionStack.length - 1].children.push(el);
        sectionStack.push(el);
      }
    }
  }
  // Write last section
  if (currentSection) {
    const secNum = getSectionNum(currentSection.identifier);
    const secDir = stack.length ? stack[stack.length - 1].dir : titleDir;
    const outPath = path.join(secDir, `section_${secNum}.md`);
    await fs.promises.mkdir(secDir, { recursive: true });
    const md = renderMarkdown(currentSection);
    await fs.promises.writeFile(outPath, md, 'utf8');
    console.log(`[DEBUG] Wrote section: ${outPath}`);
  }
}

if (process.argv[1] === new URL(import.meta.url).pathname) {
  main();
}

// Robust NY statute subsection parser using a token scanner + small stack
// Levels and examples:
//  - subdivision: 1.  3-a.  12-aaaa.
//  - paragraph: (a)
//  - subparagraph: (i)
//  - clause: (A)
//  - item: (1)

const RX_SUBDIV = /^(?:\s*)(?<label>\d+(?:-[a-z]+)*)\.\s/gm;
const RX_PARA = /^(?:\s*)\((?<label>[a-z])\)\s/gm;
const RX_SUBPARA = /^(?:\s*)\((?<label>i|ii|iii|iv|v|vi|vii|viii|ix|x|xi|xii|xiii|xiv|xv|xvi|xvii|xviii|xix|xx)\)\s/gm;
const RX_CLAUSE = /^(?:\s*)\((?<label>[A-Z])\)\s/gm;
const RX_ITEM = /^(?:\s*)\((?<label>\d+)\)\s/gm;

const SCANNERS = [
  { name: 'subdivision', rx: RX_SUBDIV, rank: 1 },
  { name: 'subparagraph', rx: RX_SUBPARA, rank: 2 }, // Higher priority than paragraph
  { name: 'paragraph', rx: RX_PARA, rank: 3 },
  { name: 'clause', rx: RX_CLAUSE, rank: 4 },
  { name: 'item', rx: RX_ITEM, rank: 5 },
];

function normalize(text) {
  if (!text) return '';
  return text.replace(/\\n/g, '\n').replace(/\r\n?/g, '\n');
}

function stripCaption(text) {
  // Jump to first subdivision marker like `1.` or `3-a.` if present
  const m = /(\d+(?:-[a-z]+)*)\.\s/.exec(text);
  return m ? text.slice(m.index) : text;
}

function scanTokens(text) {
  const tokens = [];
  const usedPositions = new Set();
  
  for (const { name, rx, rank } of SCANNERS) {
    rx.lastIndex = 0; // Reset regex state for global flag
    let m;
    while ((m = rx.exec(text)) !== null) {
      // Skip if this position is already used by a higher-priority (lower rank) token
      if (!usedPositions.has(m.index)) {
        tokens.push({ start: m.index, end: m.index + m[0].length, name, rank, label: m.groups?.label });
        usedPositions.add(m.index);
        
        // Check for inline child identifiers immediately after this token
        if (rank === 1) { // Only check after subdivision tokens
          const remainingText = text.slice(m.index + m[0].length);
          for (const { name: childName, rx: childRx, rank: childRank } of SCANNERS) {
            if (childRank <= rank) continue; // Only look for child patterns
            
            const childMatch = remainingText.match(childRx);
            if (childMatch && childMatch.index === 0) {
              const childStart = m.index + m[0].length;
              const childEnd = childStart + childMatch[0].length;
              
              if (!usedPositions.has(childStart)) {
                tokens.push({ 
                  start: childStart, 
                  end: childEnd, 
                  name: childName, 
                  rank: childRank, 
                  label: childMatch.groups?.label 
                });
                usedPositions.add(childStart);
              }
              break; // Only find the first inline child
            }
          }
        }
      }
    }
  }
  tokens.sort((a, b) => a.start - b.start);
  return tokens;
}

function buildTree(tokens, text) {
  const root = { type: 'section_body', label: null, text: '', children: [] };
  const rankOf = { section_body: 0, subdivision: 1, subparagraph: 2, paragraph: 3, clause: 4, item: 5 };
  const stack = [root];
  let lastIdx = 0;

  function attach(parent, node) {
    parent.children.push(node);
  }

  for (const tok of tokens) {
    // Interstitial text belongs to current node
    const inter = text.slice(lastIdx, tok.start);
    if (inter.trim()) {
      stack[stack.length - 1].text += inter.replace(/\n[ \t]+/g, '\n');
    }

    const node = { type: tok.name, label: tok.label, text: '', children: [] };
    
    // Special handling for subparagraphs that appear without a direct paragraph parent
    if (tok.name === 'subparagraph') {
      // Look for the most recent paragraph in the stack to attach this subparagraph to
      let paragraphParent = null;
      for (let i = stack.length - 1; i >= 0; i--) {
        if (stack[i].type === 'paragraph') {
          paragraphParent = stack[i];
          break;
        }
      }
      
      if (paragraphParent) {
        // Attach to the paragraph parent and set stack to paragraph level
        attach(paragraphParent, node);
        // Adjust stack to be at paragraph level + this subparagraph
        const paragraphIndex = stack.indexOf(paragraphParent);
        stack.length = paragraphIndex + 1; // Trim stack to paragraph level
        stack.push(node);
        lastIdx = tok.end;
        continue;
      }
    }
    
    // Pop until the top's rank is less than new rank
    // Special case: paragraphs should pop subparagraphs since they're at the same hierarchical level
    while (stack.length > 1 && 
           (rankOf[stack[stack.length - 1].type] >= tok.rank ||
            (tok.name === 'paragraph' && stack[stack.length - 1].type === 'subparagraph'))) {
      stack.pop();
    }
    attach(stack[stack.length - 1], node);
    stack.push(node);
    lastIdx = tok.end;
  }

  // Trailing text
  const tail = text.slice(lastIdx);
  if (tail.trim()) {
    stack[stack.length - 1].text += tail.replace(/\n[ \t]+/g, '\n');
  }

  // Return the tree structure
  return root;
}

function flattenTree(root, sectionNumber = null) {
  const out = [];
  // Create global tracking for identifiers to prevent duplicate tokenization
  const globalTokenizedIdentifiers = new Map();
  
  function walk(n, path) {
    if (n.type !== 'section_body') {
      // Include current node's label in its own marker
      const currentPath = n.type === 'section_body' ? path : [...path, n.label];
      const marker = currentPath.filter(x => x != null && x !== '').join('.');
      
      // For inline token generation, create a full hierarchical marker that includes section number
      const fullMarkerForTokens = sectionNumber ? [sectionNumber, ...currentPath].filter(x => x != null && x !== '').join('.') : marker;
      
      // Create entry for this node regardless of whether it has text content
      let nodeText = '';
      
      if (n.text && n.text.trim()) {
        // Node has text content - extract inline elements and replace with tokens
        const { cleanText, extractedSubparagraphs } = extractNestedSubparagraphs(n.text, marker, sectionNumber, globalTokenizedIdentifiers);
        nodeText = cleanText.trim();
        
        // Add extracted subparagraphs to output
        for (const sub of extractedSubparagraphs) {
          out.push(sub);
        }
        
        // If node also has children, append their tokens
        if (n.children && Array.isArray(n.children) && n.children.length > 0) {
          const childTokens = n.children.map(child => {
            return generateHierarchicalToken(child.type || 'unknown', child.label, marker, sectionNumber);
          });
          if (nodeText) nodeText += '\n';
          nodeText += childTokens.join('\n');
        }
      } else if (n.children && Array.isArray(n.children) && n.children.length > 0) {
        // Node has no text but has children - create interpolatable tokens
        const childTokens = n.children.map(child => {
          return generateHierarchicalToken(child.type || 'unknown', child.label, marker, sectionNumber);
        });
        nodeText = childTokens.join('\n');
      }
      
      // Only create an entry if we have a valid marker and type
      const classifiedType = classifyLevel(n.type);
      if (classifiedType && marker) {
        out.push({ type: classifiedType, marker, text: nodeText });
      }
    }
    const nextPath = n.type === 'section_body' ? path : [...path, n.label];
    if (n.children && Array.isArray(n.children)) {
      for (const c of n.children) walk(c, nextPath);
    }
  }
  walk(root, []);
  return out;
}

/**
 * Generate a hierarchical token with full parent context
 * @param {string} elementType - The type of element (paragraph, subparagraph, etc.)
 * @param {string} elementLabel - The label of the element (a, i, etc.)
 * @param {string} parentMarker - The full hierarchical marker of the parent
 * @param {string} sectionNumber - The section number (required for validation)
 * @returns {string} The formatted token
 */
function generateHierarchicalToken(elementType, elementLabel, parentMarker, sectionNumber) {
  if (!sectionNumber) {
    // Fallback: extract section number from parent marker if available
    if (parentMarker && parentMarker.includes('.')) {
      sectionNumber = parentMarker.split('.')[0];
    } else {
      // Last resort: use parent marker as section number
      sectionNumber = parentMarker || 'UNKNOWN';
    }
  }
  
  // Always ensure section number is included in the path
  let fullPath;
  if (parentMarker && parentMarker.trim() !== '') {
    // Always prepend section number if not already present
    if (parentMarker.startsWith(sectionNumber + '.') || parentMarker === sectionNumber) {
      // Parent marker already includes section number or is the section number
      fullPath = `${parentMarker}.${elementLabel}`;
    } else {
      // Parent marker doesn't include section number, prepend it
      fullPath = `${sectionNumber}.${parentMarker}.${elementLabel}`;
    }
  } else {
    // No parent marker, use section number as base
    fullPath = `${sectionNumber}.${elementLabel}`;
  }
  
  const tokenType = (elementType || 'UNKNOWN').toUpperCase();
  return `{{${tokenType}_${fullPath}}}`;
}

// Extract nested elements from text and replace with tokens
function extractNestedSubparagraphs(text, parentMarker, sectionNumber, globalTokenizedIdentifiers = null) {
  const extractedElements = [];
  let cleanText = text;
  
  // Extract nested elements that appear inline within already-parsed elements
  const patterns = [
    { 
      name: 'paragraph', 
      regex: /(?:^|\s)\((?<label>[a-z])\)\s+((?:(?!\s*\([a-z]\)|(?:^|\n)\s*\([ivx]+\)|(?:^|\n)\s*\([A-Z]\)|(?:^|\n)\s*\(\d+\)|(?:^|\n)\s*\d+\.).)*)/gs,
      tokenPrefix: 'PARAGRAPH'
    },
    { 
      name: 'subparagraph', 
      regex: /(?:^|\s)\((?<label>i|ii|iii|iv|v|vi|vii|viii|ix|x|xi|xii|xiii|xiv|xv|xvi|xvii|xviii|xix|xx)\)\s+((?:(?!\s*\([ivx]+\)|(?:^|\n)\s*\([a-z]\)\s|(?:^|\n)\s*\d+\.).)*)/gs,
      tokenPrefix: 'SUBPARAGRAPH'
    },
    { 
      name: 'clause', 
      regex: /(?:^|\s)\((?<label>[A-Z])\)\s+((?:(?!\s*\([A-Z]\)|(?:^|\n)\s*\([ivx]+\)|(?:^|\n)\s*\([a-z]\)\s|(?:^|\n)\s*\d+\.).)*)/gs,
      tokenPrefix: 'CLAUSE'
    },
    { 
      name: 'item', 
      regex: /(?:^|\s)\((?<label>\d+)\)\s+((?:(?!\s*\(\d+\)|(?:^|\n)\s*\([A-Z]\)|(?:^|\n)\s*\([ivx]+\)|(?:^|\n)\s*\([a-z]\)\s|(?:^|\n)\s*\d+\.).)*)/gs,
      tokenPrefix: 'ITEM'
    }
  ];
  
  // Use global tracking if provided, otherwise create local tracking
  const tokenizedIdentifiers = globalTokenizedIdentifiers || new Map();
  
  // Process each pattern type in hierarchical order (deepest first)
  for (const pattern of patterns) {
    const matches = [];
    let match;
    
    // Initialize tracking for this pattern type
    if (!tokenizedIdentifiers.has(pattern.name)) {
      tokenizedIdentifiers.set(pattern.name, new Set());
    }
    const seenLabels = tokenizedIdentifiers.get(pattern.name);
    
    // Collect all matches for this pattern
    while ((match = pattern.regex.exec(cleanText)) !== null) {
      const content = match[2] ? match[2].trim() : match[1].trim();
      const label = match.groups.label;
      
      // Only process if we haven't seen this label before in this context
      if (content && content.length > 0 && !seenLabels.has(label)) {
        // Check if match starts with whitespace (not beginning of string)
        const startsWithWhitespace = match[0].match(/^\s/);
        const leadingWhitespace = startsWithWhitespace ? match[0].match(/^\s+/)[0] : '';
        
        matches.push({
          fullMatch: match[0],
          label: label,
          content: content,
          index: match.index,
          pattern: pattern,
          leadingWhitespace: leadingWhitespace
        });
        
        // Mark this label as seen
        seenLabels.add(label);
      }
    }
    
    // Process matches in reverse order to maintain text positions
    for (let i = matches.length - 1; i >= 0; i--) {
      const { fullMatch, label, content, pattern: matchPattern, leadingWhitespace } = matches[i];
      
      // Create token for replacement with full hierarchical path
      const elementMarker = parentMarker ? `${parentMarker}.${label}` : label;
      const token = generateHierarchicalToken(matchPattern.name, label, parentMarker, sectionNumber);
      
      // Replace the content with token in parent text, preserving leading whitespace
      const replacement = leadingWhitespace + token;
      cleanText = cleanText.replace(fullMatch, replacement);
      
      // Create element entry
      extractedElements.unshift({
        type: matchPattern.name,
        marker: elementMarker,
        text: content,
        token: token,
        parentMarker: parentMarker
      });
    }
  }
  
  return { cleanText, extractedSubparagraphs: extractedElements };
}

export function parseSubsections(raw, sectionNumber = null) {
  const text = stripCaption(normalize(String(raw || '')));
  
  const tokens = scanTokens(text);
  
  if (tokens.length === 0) {
    return [];
  }

  const root = buildTree(tokens, text);
  return flattenTree(root, sectionNumber);
}

export function classifyLevel(level) {
  switch (level) {
    case 'subdivision': return 'subsection';
    case 'paragraph': return 'paragraph';
    case 'subparagraph': return 'subparagraph';
    case 'clause': return 'clause';
    case 'item': return 'item';
    default: return null;
  }
}

// Split a section's combined text into caption and body.
// Example input: "§ 35. Direct interstate ... 1. Authorization. ..."
// Returns { caption: "Direct interstate ...", body: "1. Authorization. ..." }
export function splitCaptionAndBody(raw) {
  if (!raw) {
    return { caption: '', body: '' };
  }
  
  const text = normalize(String(raw));
  if (!text || !text.trim()) {
    return { caption: '', body: '' };
  }
  
  // Try to match leading "§ <num>. <caption>. <rest>"
  const m = text.match(/^\s*§\s*\d+[a-z-]*\.\s*(.+?)\.\s*([\s\S]*)$/s);
  if (m) {
    return { caption: (m[1] || '').trim(), body: (m[2] || '').trim() };
  }
  // Fallback: strip just the section marker and split at first subdivision token
  const n = text.replace(/^\s*§\s*\d+[a-z-]*\.\s*/m, '');
  const tok = scanTokens(n);
  if (tok.length > 0) {
    const first = tok[0].start;
    return { caption: n.slice(0, first).trim(), body: n.slice(first).trim() };
  }
  // No markers; treat entire as caption, empty body
  return { caption: n.trim(), body: '' };
}

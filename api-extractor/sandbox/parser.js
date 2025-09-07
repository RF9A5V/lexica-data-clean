import getSectionText from "./db.js";
import parseEnglishToNumber from "./parseEnglishToNumber.js";

/*
  Note to future self:

  replacer/withReplace is meant to build two things:
  - The exact value in the target string to replace
  - The token you want to replace that string with

  This lets us have fine grained control over how to generate tokens depending on the tokenization use case
  Like (extended)? citations or paragraphs and clauses

  text is the target text we want to replace on
  item is a regex.exec object. Check out matchGenerator to see how we generally build this thing
*/
const UNIT_DEPTH = [
  { 
    unitType: "subdivision", 
    regex: /(?<id>\d+-?\w*)\.\s+(?<text>.+)(?:\n|$)/g, 
    replacer: (text, item) => {
      const exactValue = text.slice(item.startIndex, item.startIndex + item.length);
      const token = `{SUBDIVISION_${item.groups.id}}\n`;

      return {
        token: token.trim(),
        text: text.replace(exactValue, token)
      }
    } 
  },
  { 
    unitType: "paragraph", 
    regex: /(?<!(paragraphs)|(paragraph))\((?<id>\w+)\)\s+(?!of)(?<text>[\w\s",:\\\.\-\/]+)/g,
    cleanerRegex: /(?<!(paragraphs)|(paragraph))\s*\((?<id>[\w]+)\)\s+(?!of)/g,
    // I hate my life
    postCleanRegex: /^\s*\((?<id>\w+)\)(?<text>.+)(?:\.|\n)/gm,
    replacer: (text, item) => {
      const exactValue = text.slice(item.startIndex, item.startIndex + item.length);

      const token = `\n{PARAGRAPH_${item.groups.id}}\n`;

      return {
        token: token.trim(),
        text: text.replace(exactValue, token)
      }
    } 
  }
];

//(?<!(paragraphs)|(paragraph))\s\(([\w+])\)\s+(?!of)([\w\s]|(\{\#\d+(\_\d+)?\}))+




// Look up the return value from regex.exec if confused
function* matchGenerator(text, regex) {
  let iter;
  while ((iter = regex.exec(text)) !== null) {
    yield {
      startIndex: iter.index,
      groups: iter.groups,
      length: iter[0].length,
      rawMatch: iter[0]
    }
  }
}

function getMatchesAndReplace(text, regex, replaceWith) {
  const matches = [];

  if(!replaceWith) {
    replaceWith = (text, item) => {
      const exactValue = text.slice(item.startIndex, item.startIndex + item.length);
      const token = `\n\n{UNSUPPLIED_TOKEN}`;
      
      return {
        token,
        text: text.replace(exactValue, token)
      }
    }
  }

  for(const match of matchGenerator(text, regex)) {
    matches.push(match);
  }

  for (let i = matches.length - 1; i >= 0; i--) {
    let replaceObj = replaceWith(text, matches[i]);
    text = replaceObj.text;
    matches[i].token = replaceObj.token;
  }

  return { text, matches };
}

function tokenizeSectionCitations(text) {
  const UNIT_TYPES = ["subdivision", "paragraph", "section"]
  const SECTION_CITATION_REGEX = /(?<prefix>(?<subunit>subdivision|paragraph) (?<subunitId>[\w,\s]+) of )?(?<sectionMarker>sections?|§|;|,|and|or)\s+(?<unitId>\d+(\.\d+)?(\(\w\))*)+(\([\w\s]+\))?/g;
  let citationReplObj = getMatchesAndReplace(text, SECTION_CITATION_REGEX, (text, item) => {

    let { sectionMarker } = item.groups;
    if(sectionMarker[sectionMarker.length - 1] === "s") {
      sectionMarker = sectionMarker.slice(0, sectionMarker.length - 1);
    }

    const targetMatch = item.groups.subunit || UNIT_TYPES.includes(sectionMarker) ? item.rawMatch : item.groups.unitId;
    let subunitIdentifiers = [null]; 
    
    if(item.groups.subunitId) {
      subunitIdentifiers = item.groups.subunitId.split(",").map(id => parseEnglishToNumber(id));
    }

    const tokens = subunitIdentifiers.map(
      suId => `{#${item.groups.unitId.replace(".", "_")}${suId ? "(" + suId + ")" : ""}}`
    );

    return {
      tokens, text: text.replace(targetMatch, tokens.join(", "))
    }
  })

  return {
    matches: citationReplObj.matches,
    text: citationReplObj.text
  }
}

function cleanText(text) {
  const lines = text.split("\\n");
  const expectedHeader = lines[0].slice(0, lines[0].lastIndexOf(".")).trim();
  lines[0] = lines[0].slice(lines[0].lastIndexOf(".") + 1);
  let remainder = lines.map(l => l.trim()).filter(l => l).join(" ");

  let replaceObj = getMatchesAndReplace(remainder, /\s\d+-?\w*\.\s/g, (text, item) => {
    const exactValue = text.slice(item.startIndex, item.startIndex + item.length);
    const spacer = `\n\n${exactValue.trim()} `;

    return {
      token: spacer,
      text: text.replace(exactValue, spacer)
    }
  })

  return {
    header: expectedHeader,
    text: replaceObj.text
  }
}

function tokenizeText(text, unitIndex) {
  const { unitType, regex, replacer, cleanerRegex, postCleanRegex } = UNIT_DEPTH[unitIndex];

  if(cleanerRegex) {
    let cleanedObj = getMatchesAndReplace(text, cleanerRegex, (text, item) => {
      return {
        text: text.slice(0, item.startIndex) + "\n" + text.slice(item.startIndex),
        token: null
      };
    });
    text = cleanedObj.text;
  }

  const detailExtractorRegex = postCleanRegex ?? regex;

  let tokenizedObj = getMatchesAndReplace(text, detailExtractorRegex, replacer);
  let matches = tokenizedObj.matches;

  const nextUnit = UNIT_DEPTH[unitIndex + 1];

  if(nextUnit) {
    for(let match of matches) {
      if(nextUnit.regex.test(match.groups.text)) {
        const internalReplObj = tokenizeText(match.groups.text, unitIndex + 1);

        const romanRegex = /(?<id>[xiv]+)/g;

        let currentItem;
        let replacementMatches = [];

        for(let submatch of internalReplObj.matches) {
          if(!currentItem) {
            currentItem = submatch;
          }
          else {
            if(romanRegex.test(submatch.groups.id)) {
              internalReplObj.tokenized = internalReplObj.tokenized.replace(`\n\n${submatch.token}`, "");

              // console.log(submatch)

              // Fuck it, we'll just assume roman numerals refer to subparagraphs
              submatch.token = submatch.token[0] + "SUB" + submatch.token.slice(1); 
              if(!currentItem.matches) {
                currentItem.matches = [];
              }
              currentItem.matches.push(submatch);
              
              if(!currentItem.tokenizedText) {
                currentItem.tokenizedText = currentItem.groups.text;
              }
              currentItem.tokenizedText += `\n\n${submatch.token}`;
              
              continue;
            }
            else {
              currentItem = submatch;
            }
          }

          replacementMatches.push(currentItem);
        }

        match.matches = replacementMatches;
        match.tokenizedText = internalReplObj.tokenized.trim();
      }
    }
  }

  // Pull out any mismatched subparagraph paragraphs

  const tokenMap = {};
  matches.forEach(m => { tokenMap[m.token] = 1 });

  const tokens = [];
  for(let match of matchGenerator(tokenizedObj.text, /(?<token>\{.+\})/g)) {
    tokens.push(match);
  }

  // if(unitIndex > 0) {
  //   console.log(tokenMap, tokens, tokenizedObj.text)
  // }
  
  for(let i = tokens.length - 1; i >= 0; i --) {
    if(!tokenMap[tokens[i].groups.token]) {
      tokenizedObj.text = tokenizedObj.text.replace(`${tokens[i].groups.token}`, "");
    }
  }
  
  return {
    tokenized: tokenizedObj.text,
    matches
  };
}

async function getTokenizedText(sectionId) {
  const rawText = await getSectionText(sectionId);

  if (rawText) {
    let { header, text: cleanedText } = cleanText(rawText);
    const { matches: citations, text: citationTokenizedText } = tokenizeSectionCitations(cleanedText);

    let { tokenized, matches } = tokenizeText(citationTokenizedText, 0);

    return {
      text: tokenized,
      matches,
      citations
    }
  }
}

// main();

// Example usage
export { getTokenizedText };
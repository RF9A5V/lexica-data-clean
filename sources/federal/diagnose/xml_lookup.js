import fs from "fs/promises";
import * as cheerio from "cheerio";

// Given an identifier and xmlPath, returns XML for previous sibling, the node, and next sibling (if they exist), labeled for debugging
export async function getXmlElementByIdentifier(xmlPath, identifier) {
  const xmlContent = await fs.readFile(xmlPath, "utf8");
  const $ = cheerio.load(xmlContent, { xmlMode: true });
  let elem = $(`[identifier='${identifier}']`).first();
  if (!elem.length && identifier) {
    // Fallback: try to find section-level identifier
    const sectionId = identifier.split("/").slice(0, 5).join("/");
    elem = $(`[identifier='${sectionId}']`).first();
    if (!elem.length) {
      return { error: `No XML element found for identifier '${identifier}' or section '${sectionId}' in ${xmlPath}` };
    }
  }
  let out = [];
  let prev = elem.prev();
  let next = elem.next();
  if (prev.length && prev[0].tagName !== undefined) {
    out.push("<!-- Previous sibling -->\n" + $.xml(prev));
  }
  out.push("<!-- Target node -->\n" + $.xml(elem));
  if (next.length && next[0].tagName !== undefined) {
    out.push("<!-- Next sibling -->\n" + $.xml(next));
  }
  return { xml: out.join("\n\n") };
}

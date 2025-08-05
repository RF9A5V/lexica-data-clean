import fs from "fs/promises";
import path from "path";
import { DOMParser } from "xmldom-qsa";

// Usage: node query_xml_by_id.js <xmlfile> <element_id>
const [,, xmlFile, elemId] = process.argv;
if (!xmlFile || !elemId) {
  console.error("Usage: node query_xml_by_id.js <xmlfile> <element_id>");
  process.exit(1);
}

async function main() {
  const xmlPath = path.resolve(xmlFile);
  const xmlContent = await fs.readFile(xmlPath, "utf8");
  const doc = new DOMParser().parseFromString(xmlContent, "text/xml");
  // Use XPath to find element by id
  const node = doc.getElementById(elemId);
  if (!node) {
    // fallback: search all elements for matching id attribute
    const all = doc.getElementsByTagName("*");
    for (let i = 0; i < all.length; i++) {
      if (all[i].getAttribute && all[i].getAttribute("id") === elemId) {
        return printNode(all[i]);
      }
    }
    console.error(`Element with id '${elemId}' not found.`);
    process.exit(2);
  } else {
    printNode(node);
  }
}

function printNode(node) {
  // Print tag name, attributes, and text content
  const tag = node.tagName;
  const attrs = Array.from(node.attributes || []).map(a => `${a.name}='${a.value}'`).join(" ");
  const text = node.textContent.trim();
  console.log(`Tag: <${tag}>`);
  if (attrs) console.log(`Attributes: ${attrs}`);
  if (text) console.log(`Text: ${text.substring(0, 500)}${text.length > 500 ? '...' : ''}`);
  // Print the outer XML
  console.log("--- Outer XML ---");
  console.log(node.toString());
}

main();

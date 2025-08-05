import fs from "fs/promises";
import path from "path";
import { DOMParser } from "xmldom-qsa";

const fileArg = process.argv[2];
if (!fileArg) {
  console.error("Usage: node dump_xml_structure.js <xmlfile>");
  process.exit(1);
}

async function main() {
  const xmlPath = path.resolve(fileArg);
  const xmlContent = await fs.readFile(xmlPath, "utf8");
  const doc = new DOMParser().parseFromString(xmlContent, "text/xml");
  const main = doc.getElementsByTagName("main")[0];
  if (!main) {
    console.log("No <main> element found.");
    return;
  }
  console.log("Root children of <main>:");
  for (let child = main.firstChild; child; child = child.nextSibling) {
    if (child.nodeType === 1) {
      console.log(`- <${child.localName}>`);
      // Count and show tag names of this child's children
      const tagCounts = {};
      for (let sub = child.firstChild; sub; sub = sub.nextSibling) {
        if (sub.nodeType === 1) {
          tagCounts[sub.localName] = (tagCounts[sub.localName] || 0) + 1;
        }
      }
      if (Object.keys(tagCounts).length > 0) {
        console.log(`  Children: ${Object.entries(tagCounts).map(([k,v]) => `<${k}>:${v}`).join(", ")}`);
      }
    }
  }
}

main();

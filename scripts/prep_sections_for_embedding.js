import fs from "fs/promises";
import path from "path";
import readline from "readline";

// --- Stop words and helpers ---
const STOP_WORDS = new Set([
  "the", "and", "of", "to", "in", "a", "for", "is", "on", "that", "by", "with", "as", "at", "from", "or", "an", "be", "this", "which", "are", "it", "was", "not", "have", "has", "but", "their", "they", "will", "can", "if", "all", "any", "such", "may", "shall"
]);

function removeStopWords(text) {
  return text
    .split(/\s+/)
    .filter(word => !STOP_WORDS.has(word.toLowerCase()))
    .join(" ");
}

// Simple chunker: split by ~500 words
function splitTextToChunks(text, chunkSize = 500) {
  const words = text.split(/\s+/);
  const chunks = [];
  for (let i = 0; i < words.length; i += chunkSize) {
    chunks.push(words.slice(i, i + chunkSize).join(" "));
  }
  return chunks;
}

const fullAutoDir = path.resolve("./");
const finalDir = path.join(fullAutoDir, "final");
const outputPath = path.join(finalDir, "sections_cleaned.ndjson");

async function processSectionFile(sectionFile, titleNum, output) {
  const fileStream = await fs.open(sectionFile);
  const rl = readline.createInterface({ input: fileStream.createReadStream() });

  for await (const line of rl) {
    if (!line.trim()) continue;
    const section = JSON.parse(line);
    const content = section.content || "";
    const heading = section.heading || "";
    const identifier = section.identifier || section.num || section.path || "";

    // Prepend heading to content for embedding
    const fullText = heading ? heading + "\n" + content : content;
    const chunks = splitTextToChunks(fullText);
    for (let i = 0; i < chunks.length; i++) {
      const cleaned = removeStopWords(chunks[i]);
      const record = {
        titleNum,
        identifier,
        chunk_index: i,
        total_chunks: chunks.length,
        content: cleaned
      };
      await output.write(JSON.stringify(record) + "\n");
    }
  }
  await rl.close();
  await fileStream.close();
}

async function main() {
  await fs.mkdir(finalDir, { recursive: true });
  const dirEntries = await fs.readdir(fullAutoDir, { withFileTypes: true });
  const titleDirs = dirEntries.filter(
    e => e.isDirectory() && /^title_\d+$/.test(e.name)
  );

  const output = await fs.open(outputPath, "w");

  for (const dir of titleDirs) {
    const titleNum = dir.name.match(/^title_(\d+)$/)[1];
    const sectionFile = path.join(fullAutoDir, dir.name, "section_text.ndjson");
    try {
      await fs.access(sectionFile);
      await processSectionFile(sectionFile, titleNum, output);
      console.log(`Processed ${sectionFile}`);
    } catch (e) {
      console.warn(`Skipping ${sectionFile}: ${e.message}`);
    }
  }

  await output.close();
  console.log(`All cleaned section chunks written to ${outputPath}`);
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});

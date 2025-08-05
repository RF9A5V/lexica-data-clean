import fs from "fs/promises";
import path from "path";
import fetch from "node-fetch";

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const MODEL = "gpt-3.5-turbo-0125"; // Use latest mini model

async function queryLLM(prompt) {
  const response = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${OPENAI_API_KEY}`,
    },
    body: JSON.stringify({
      model: MODEL,
      messages: [
        { role: "system", content: "You are a legal reference extraction assistant. Given a short title or editorial note from US law, extract arrays of legal references that were enacted, amended, or repealed by the cited law. If you cannot confidently extract, return the boolean false." },
        { role: "user", content: prompt },
      ],
      temperature: 0.0,
    }),
  });
  if (!response.ok) throw new Error(`OpenAI API error: ${response.statusText}`);
  const data = await response.json();
  const content = data.choices[0].message.content.trim();
  try {
    // Try to parse as JSON
    return JSON.parse(content);
  } catch (e) {
    // If not JSON, check if it's the boolean false
    if (content.toLowerCase() === "false") return false;
    throw new Error("Unexpected LLM output: " + content);
  }
}

async function main() {
  const reviewFile = process.argv[2];
  if (!reviewFile) throw new Error("Usage: node review_notes_refs.js <notes_review.json>");
  const reviewPath = path.resolve(reviewFile);
  const notes = JSON.parse(await fs.readFile(reviewPath, "utf8"));

  let updated = false;
  for (const note of notes) {
    if (note.status === "resolved" && note.llmResult) continue;
    // Build prompt
    const prompt = `Extract enactedRefs, amendedRefs, and repealedRefs as arrays of strings from the following note. If you cannot, return false.\n---\nHeading: ${note.heading}\nContent: ${note.content}\nAttempted Extraction: ${JSON.stringify(note.attemptedExtraction)}`;
    try {
      const result = await queryLLM(prompt);
      note.llmResult = result;
      note.status = "resolved";
      updated = true;
      console.log(`LLM result for ${note.identifier}:`, result);
    } catch (e) {
      note.llmResult = false;
      note.status = "unresolved";
      console.error(`Error for ${note.identifier}:`, e.message);
    }
    // Write after each note for safety
    await fs.writeFile(reviewPath, JSON.stringify(notes, null, 2), "utf8");
  }
  if (updated) {
    console.log("Review file updated with LLM results.");
  } else {
    console.log("No unresolved notes found or all already resolved.");
  }
}

main().catch(e => { console.error(e); process.exit(1); });

const allowedTypes = [
  "title", "subtitle", "chapter", "subchapter", "part", "subpart", "section", "subsection", "paragraph", "subparagraph", "clause", "subclause", "item", "heading", "content", "chapeau"
];
export function typeCheck(data) {
  if (!data.type) return "Missing type.";
  if (!allowedTypes.includes(data.type)) {
    return `Type '${data.type}' is not in allowed set.`;
  }
  return null;
}

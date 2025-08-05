export function contentCheck(data) {
  if (!data.content || !data.content.trim()) {
    return "Content is empty or whitespace.";
  }
  return null;
}

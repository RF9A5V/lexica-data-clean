// Checks that contentType is either 'heading' or 'body'
export function contentTypeCheck(data) {
  if (!data.contentType) return "Missing contentType.";
  if (data.contentType !== "heading" && data.contentType !== "body") {
    return `Invalid contentType: '${data.contentType}'. Expected 'heading' or 'body'.`;
  }
  return null;
}

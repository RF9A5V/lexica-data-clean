export function identifierFormatCheck(data) {
  if (!data.identifier) return "Missing identifier.";
  const pattern = /^\/us\/usc\/t\d{1,2}(\/.*)?$/;
  if (!pattern.test(data.identifier)) {
    return `Identifier '${data.identifier}' does not match expected format.`;
  }
  return null;
}

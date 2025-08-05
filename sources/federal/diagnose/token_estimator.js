import { encode } from "gpt-3-encoder";

export function estimateTokens(str) {
  try {
    return encode(str).length;
  } catch {
    // Fallback: rough estimate (1 token ~= 4 chars)
    return Math.ceil(str.length / 4);
  }
}

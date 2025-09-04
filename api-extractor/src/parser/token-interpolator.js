/**
 * Token Interpolator
 * Handles reinterpolation of tokens back to original text
 */

class TokenInterpolator {
  constructor() {
    this.tokenPattern = /{{([A-Z_]+)_([^_}]+)_([^_}]+)}}/g;
  }

  /**
   * Reinterpolate tokens in text with child element content
   */
  reinterpolateText(tokenizedText, childElements) {
    if (!tokenizedText || childElements.length === 0) {
      return tokenizedText;
    }

    let result = tokenizedText;
    
    // Create lookup map for efficient token replacement
    const tokenMap = new Map();
    for (const child of childElements) {
      tokenMap.set(child.token, child.text);
    }

    // Replace all tokens with their corresponding text
    result = result.replace(this.tokenPattern, (match, type, sectionId, number) => {
      const replacement = tokenMap.get(match);
      return replacement !== undefined ? replacement : match;
    });

    return result;
  }

  /**
   * Recursively reinterpolate tokens including nested children
   */
  reinterpolateTextRecursive(tokenizedText, childElements) {
    if (!tokenizedText || childElements.length === 0) {
      return tokenizedText;
    }

    let result = tokenizedText;
    
    // Process each child element
    for (const child of childElements) {
      let childText = child.text;
      
      // Recursively process nested children
      if (child.children && child.children.length > 0) {
        childText = this.reinterpolateTextRecursive(childText, child.children);
      }
      
      // Replace token with processed child text
      result = result.replace(child.token, childText);
    }

    return result;
  }

  /**
   * Find all tokens in text
   */
  findTokens(text) {
    const tokens = [];
    let match;
    
    this.tokenPattern.lastIndex = 0;
    while ((match = this.tokenPattern.exec(text)) !== null) {
      tokens.push({
        token: match[0],
        type: match[1],
        sectionId: match[2],
        number: match[3],
        position: match.index
      });
    }

    return tokens;
  }

  /**
   * Validate that all tokens in text can be resolved
   */
  validateTokenResolution(tokenizedText, childElements) {
    const tokensInText = this.findTokens(tokenizedText);
    const availableTokens = new Set(childElements.map(child => child.token));
    
    const unresolvedTokens = tokensInText.filter(token => !availableTokens.has(token.token));
    
    return {
      isValid: unresolvedTokens.length === 0,
      unresolvedTokens,
      totalTokens: tokensInText.length,
      resolvedTokens: tokensInText.length - unresolvedTokens.length
    };
  }

  /**
   * Build hierarchical structure from flat child elements
   */
  buildHierarchy(childElements) {
    const hierarchy = [];
    const elementMap = new Map();
    
    // First pass: create map of all elements
    for (const element of childElements) {
      elementMap.set(element.token, { ...element, children: [] });
    }
    
    // Second pass: build parent-child relationships
    for (const element of childElements) {
      const current = elementMap.get(element.token);
      
      // Find parent based on token hierarchy
      const parent = this.findParentElement(element, elementMap);
      if (parent) {
        parent.children.push(current);
      } else {
        hierarchy.push(current);
      }
    }
    
    return hierarchy;
  }

  /**
   * Find parent element based on token hierarchy
   */
  findParentElement(element, elementMap) {
    const tokenInfo = this.parseToken(element.token);
    if (!tokenInfo) return null;
    
    // Look for potential parents with same section but different type/number
    for (const [token, candidate] of elementMap) {
      if (token === element.token) continue;
      
      const candidateInfo = this.parseToken(token);
      if (!candidateInfo || candidateInfo.sectionId !== tokenInfo.sectionId) continue;
      
      // Check if candidate could be a parent based on hierarchy rules
      if (this.isParentChild(candidateInfo, tokenInfo)) {
        return candidate;
      }
    }
    
    return null;
  }

  /**
   * Determine if one element is parent of another based on legislative hierarchy
   */
  isParentChild(parentInfo, childInfo) {
    const hierarchy = ['SUBSECTION', 'PARAGRAPH', 'SUBPARAGRAPH', 'CLAUSE', 'ITEM'];
    
    const parentLevel = hierarchy.indexOf(parentInfo.type);
    const childLevel = hierarchy.indexOf(childInfo.type);
    
    // Parent must be at higher level (lower index)
    if (parentLevel >= childLevel) return false;
    
    // Additional logic for specific parent-child relationships
    // This would need to be expanded based on actual legislative structure rules
    return true;
  }

  /**
   * Parse token into components
   */
  parseToken(token) {
    const match = token.match(/^{{([A-Z_]+)_([^_}]+)_([^_}]+)}}$/);
    if (!match) return null;
    
    return {
      type: match[1],
      sectionId: match[2],
      number: match[3]
    };
  }

  /**
   * Generate statistics about token interpolation
   */
  getInterpolationStats(tokenizedText, childElements) {
    const tokens = this.findTokens(tokenizedText);
    const validation = this.validateTokenResolution(tokenizedText, childElements);
    
    const typeStats = {};
    for (const token of tokens) {
      typeStats[token.type] = (typeStats[token.type] || 0) + 1;
    }
    
    return {
      totalTokens: tokens.length,
      resolvedTokens: validation.resolvedTokens,
      unresolvedTokens: validation.unresolvedTokens.length,
      resolutionRate: tokens.length > 0 ? validation.resolvedTokens / tokens.length : 1,
      typeDistribution: typeStats,
      childElementCount: childElements.length
    };
  }

  /**
   * Debug token replacement process
   */
  debugTokenReplacement(tokenizedText, childElements) {
    const steps = [];
    let currentText = tokenizedText;
    
    steps.push({
      step: 0,
      description: 'Initial tokenized text',
      text: currentText,
      tokens: this.findTokens(currentText)
    });
    
    for (let i = 0; i < childElements.length; i++) {
      const child = childElements[i];
      const beforeReplacement = currentText;
      currentText = currentText.replace(child.token, child.text);
      
      steps.push({
        step: i + 1,
        description: `Replaced ${child.token}`,
        text: currentText,
        replacedToken: child.token,
        replacementText: child.text,
        changed: beforeReplacement !== currentText
      });
    }
    
    return steps;
  }
}

export { TokenInterpolator };

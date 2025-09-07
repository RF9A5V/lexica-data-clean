function parseEnglishToNumber(text) {

    if(parseInt(text)) {
        return parseInt(text);
    }

    // Normalize input: replace spaces with dashes and convert to lowercase
    const normalized = text.toLowerCase().replace(/\s+/g, '-');
    
    // Maps for number words
    const basicNumbers = {
      'zero': 0, 'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
      'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10,
      'eleven': 11, 'twelve': 12, 'thirteen': 13, 'fourteen': 14, 'fifteen': 15,
      'sixteen': 16, 'seventeen': 17, 'eighteen': 18, 'nineteen': 19
    };
    
    const tens = {
      'twenty': 20, 'thirty': 30, 'forty': 40, 'fifty': 50,
      'sixty': 60, 'seventy': 70, 'eighty': 80, 'ninety': 90
    };
    
    const multipliers = {
      'hundred': 100, 'thousand': 1000, 'million': 1000000, 'billion': 1000000000
    };
    
    // Split on dashes
    const parts = normalized.split('-');
    let result = 0;
    let currentNumber = 0;
    
    for (let i = 0; i < parts.length; i++) {
      const part = parts[i];
      
      if (basicNumbers.hasOwnProperty(part)) {
        currentNumber += basicNumbers[part];
      } else if (tens.hasOwnProperty(part)) {
        currentNumber += tens[part];
      } else if (multipliers.hasOwnProperty(part)) {
        const multiplier = multipliers[part];
        currentNumber *= multiplier;
        result += currentNumber;
        currentNumber = 0;
      } else {
        // Handle compound tens like "twenty-one"
        if (i > 0 && tens.hasOwnProperty(parts[i-1]) && basicNumbers.hasOwnProperty(part)) {
          currentNumber += basicNumbers[part] - (basicNumbers[part] > 9 ? 10 : 0);
        } else {
          throw new Error(`Unknown number word: ${part}`);
        }
      }
    }
    
    result += currentNumber;
    return result;
  }

  export default parseEnglishToNumber;
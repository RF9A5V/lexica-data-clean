const fs = require('fs');
const path = require('path');
const Ajv = require('ajv');

function loadSchema(relPath) {
  const p = path.join(__dirname, '..', relPath);
  const data = fs.readFileSync(p, 'utf8');
  return JSON.parse(data);
}

const ajv = new Ajv({ allErrors: true, strict: false });

let unifiedValidate = null;

function getUnifiedValidator() {
  if (!unifiedValidate) {
    // Use unified minimal schema across all cases
    const schema = loadSchema('schemas/unified_minimal_schema.json');
    unifiedValidate = ajv.compile(schema);
  }
  return unifiedValidate;
}

function validateUnified(payload, opts = {}) {
  const validator = getUnifiedValidator();
  const valid = validator(payload);
  return { valid, errors: validator.errors };
}

module.exports = {
  getUnifiedValidator,
  validateUnified,
};

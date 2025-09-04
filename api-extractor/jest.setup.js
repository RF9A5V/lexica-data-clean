// Jest setup file for NYSenate API extractor tests
const dotenv = require('dotenv');

// Load environment variables for testing
dotenv.config();

// Set test timeout to 30 seconds for API calls
jest.setTimeout(30000);

// Global test configuration
global.testConfig = {
  apiTimeout: 10000,
  cacheEnabled: true,
  verboseLogging: process.env.NODE_ENV === 'test'
};

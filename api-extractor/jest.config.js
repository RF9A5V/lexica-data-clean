module.exports = {
  testEnvironment: 'node',
  transform: {},
  transformIgnorePatterns: [],
  testMatch: [
    '**/__tests__/**/*.test.js'
  ],
  collectCoverageFrom: [
    'src/**/*.js',
    '!src/index.js'
  ],
  setupFilesAfterEnv: ['<rootDir>/jest.setup.js'],
  testTimeout: 30000
};

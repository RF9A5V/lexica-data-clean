const fs = require('fs/promises');
const path = require('path');

async function analyzeGlobalCostReport() {
  const reportPath = path.resolve(__dirname, '../../data/costEstimate/global_cost_report.txt');
  const data = await fs.readFile(reportPath, 'utf8');
  const lines = data.split('\n');

  // Only consider lines that match the pattern: ...: <number> tokens
  const tokenCounts = [];
  for (const line of lines) {
    const match = line.match(/: (\d+) tokens$/);
    if (match && !line.startsWith('Grand total')) {
      tokenCounts.push(Number(match[1]));
    }
  }

  if (tokenCounts.length === 0) {
    console.log('No token counts found in the report.');
    return;
  }

  const min = Math.min(...tokenCounts);
  const max = Math.max(...tokenCounts);
  const avg = tokenCounts.reduce((a, b) => a + b, 0) / tokenCounts.length;

  console.log(`Lowest token count: ${min}`);
  console.log(`Highest token count: ${max}`);
  console.log(`Average token count: ${avg.toFixed(2)}`);
}

analyzeGlobalCostReport().catch(err => {
  console.error('Error analyzing global cost report:', err);
});
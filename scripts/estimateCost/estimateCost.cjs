const fs = require('fs/promises');
const path = require('path');
const { encoding_for_model } = require('tiktoken');

// Change this if you want to use a different OpenAI model's tokenizer
const MODEL = 'gpt-3.5-turbo';
const keywordsRoot = path.resolve(__dirname, '../../data/keywords');
const costEstimateRoot = path.resolve(__dirname, '../../data/costEstimate');

async function countTokens(text, encoding) {
  return encoding.encode(text).length;
}

async function ensureDir(dirPath) {
  await fs.mkdir(dirPath, { recursive: true });
}

async function processDirectory(srcDir, destDir, aggregate) {
  await ensureDir(destDir);
  const entries = await fs.readdir(srcDir, { withFileTypes: true });
  let reportLines = [];
  let dirTotal = 0;
  const encoding = encoding_for_model(MODEL);

  for (const entry of entries) {
    const srcPath = path.join(srcDir, entry.name);
    const destPath = path.join(destDir, entry.name);
    if (entry.isDirectory()) {
      await processDirectory(srcPath, destPath, aggregate);
    } else if (entry.isFile() && entry.name.endsWith('.md')) {
      const content = await fs.readFile(srcPath, 'utf8');
      const tokens = await countTokens(content, encoding);
      reportLines.push(`${entry.name}: ${tokens} tokens`);
      dirTotal += tokens;
      if (aggregate) {
        aggregate.files.push({ path: srcPath, tokens });
        aggregate.total += tokens;
      }
    }
  }

  if (reportLines.length > 0) {
    reportLines.push(`\nTotal tokens: ${dirTotal} tokens`);
    const reportPath = path.join(destDir, 'cost_report.txt');
    await fs.writeFile(reportPath, reportLines.join('\n'), 'utf8');
    console.log(`Wrote report: ${reportPath}`);
  }
}

(async () => {
  try {
    const aggregate = { total: 0, files: [] };
    await processDirectory(keywordsRoot, costEstimateRoot, aggregate);
    // Write global aggregation report
    const aggLines = aggregate.files.map(f => `${f.path}: ${f.tokens} tokens`);
    aggLines.push(`\nGrand total tokens: ${aggregate.total} tokens`);
    const aggReportPath = path.join(costEstimateRoot, 'global_cost_report.txt');
    await fs.writeFile(aggReportPath, aggLines.join('\n'), 'utf8');
    console.log(`Wrote global aggregation report: ${aggReportPath}`);
    console.log('Token estimation complete.');
  } catch (err) {
    console.error('Error during token estimation:', err);
  }
})();

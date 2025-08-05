// Simple progress bar for Node.js
export function renderProgressBar(current, total, barLength = 40) {
  const percent = current / total;
  const filledLength = Math.round(barLength * percent);
  const bar = "█".repeat(filledLength) + "-".repeat(barLength - filledLength);
  const pct = (percent * 100).toFixed(1);
  process.stdout.write(`\r[${bar}] ${current}/${total} (${pct}%)`);
  if (current === total) {
    process.stdout.write("\n");
  }
}

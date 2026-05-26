import { config } from './config.js';

const LEVELS = { DEBUG: 10, INFO: 20, WARN: 30, ERROR: 40 };

function emit(level, prefix, msg, extra) {
  if (LEVELS[level] < LEVELS[config.logLevel]) return;
  const ts = new Date().toISOString();
  const tag = prefix ? `[${prefix}] ` : '';
  const line = `${ts} ${level.padEnd(5)} ${tag}${msg}`;
  const stream = level === 'ERROR' || level === 'WARN' ? process.stderr : process.stdout;
  stream.write(line + '\n');
  if (extra) stream.write('  ' + JSON.stringify(extra) + '\n');
}

export function makeLogger(prefix = '') {
  return {
    debug: (msg, extra) => emit('DEBUG', prefix, msg, extra),
    info: (msg, extra) => emit('INFO', prefix, msg, extra),
    warn: (msg, extra) => emit('WARN', prefix, msg, extra),
    error: (msg, extra) => emit('ERROR', prefix, msg, extra),
    child: (sub) => makeLogger(prefix ? `${prefix}/${sub}` : sub),
  };
}

export const logger = makeLogger();

import 'dotenv/config';

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

export const config = {
  source: {
    ny_supreme: requireEnv('SOURCE_NY_SUPREME_URL'),
    ny_appellate: requireEnv('SOURCE_NY_APPELLATE_URL'),
    ny_trial: requireEnv('SOURCE_NY_TRIAL_URL'),
  },
  admin: requireEnv('ADMIN_URL'),
  target: requireEnv('TARGET_URL'),
  targetDbName: requireEnv('MERGE_TARGET_DB'),
  logLevel: (process.env.LOG_LEVEL || 'INFO').toUpperCase(),
};

export const SOURCE_REFS = ['ny_supreme', 'ny_appellate', 'ny_trial'];

export function parseCliArgs(argv = process.argv.slice(2)) {
  const args = {
    phase: null,
    only: null,
    from: null,
    to: null,
    dryRun: false,
    force: false,
    yes: false,
  };
  for (const arg of argv) {
    if (arg === '--dry-run') args.dryRun = true;
    else if (arg === '--force') args.force = true;
    else if (arg === '--yes' || arg === '-y') args.yes = true;
    else if (arg.startsWith('--phase=')) args.phase = Number(arg.split('=')[1]);
    else if (arg.startsWith('--only=')) args.only = arg.split('=')[1].split(',').map(Number);
    else if (arg.startsWith('--from=')) args.from = Number(arg.split('=')[1]);
    else if (arg.startsWith('--to=')) args.to = Number(arg.split('=')[1]);
    else throw new Error(`Unknown arg: ${arg}`);
  }
  return args;
}

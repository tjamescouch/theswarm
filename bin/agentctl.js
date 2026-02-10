#!/usr/bin/env node

/**
 * agentctl — CLI for managing the agentctl-swarm supervisor.
 *
 * Commands:
 *   start      Boot the supervisor and spawn daemon fleet
 *   status     Show swarm status
 *   scale N    Scale to N total daemons
 *   assign     Assign task to specific agent
 *   broadcast  Broadcast task to all idle agents
 *   stop       Graceful shutdown (or send SIGINT/SIGTERM)
 *
 * Zero dependencies — plain Node.js arg parsing.
 */

import { Supervisor } from '../lib/supervisor.js';
import { AgentChatBus, EventEmitterBus } from '../lib/message-bus.js';
import { Logger, attachLogger } from '../lib/logger.js';

// ============ Arg parsing ============

function parseArgs(argv) {
  const args = argv.slice(2);
  const command = args[0] || 'help';
  const flags = {};
  const positional = [];

  for (let i = 1; i < args.length; i++) {
    const arg = args[i];
    if (arg.startsWith('--')) {
      const key = arg.slice(2);
      const next = args[i + 1];
      if (next && !next.startsWith('--')) {
        flags[key] = next;
        i++;
      } else {
        flags[key] = true;
      }
    } else if (arg.startsWith('-') && arg.length === 2) {
      const shortMap = {
        n: 'count',
        m: 'max-active',
        r: 'role',
        s: 'server',
        c: 'channels',
        o: 'output',
        j: 'json',
      };
      const key = shortMap[arg[1]] || arg[1];
      const next = args[i + 1];
      if (next && !next.startsWith('-')) {
        flags[key] = next;
        i++;
      } else {
        flags[key] = true;
      }
    } else {
      positional.push(arg);
    }
  }

  return { command, flags, positional };
}

// ============ Output helpers ============

function printTable(rows, headers) {
  const widths = headers.map((h, i) =>
    Math.max(h.length, ...rows.map(r => String(r[i] || '').length))
  );

  const sep = widths.map(w => '-'.repeat(w)).join('--+-');
  const line = (vals) =>
    vals.map((v, i) => String(v || '').padEnd(widths[i])).join('  | ');

  console.log(line(headers));
  console.log(sep);
  rows.forEach(r => console.log(line(r)));
}

// ============ Commands ============

async function cmdStart(flags) {
  const count = parseInt(flags.count || '3');
  const maxActive = parseInt(flags['max-active'] || '5');
  const role = flags.role || 'builder';
  const channels = (flags.channels || '#agents').split(',');
  const server = flags.server || process.env.AGENTCHAT_SERVER || null;
  const persist = !!flags.persist;
  const basePath = flags['base-path'] || undefined;

  // AgentAuth sidecar config
  const agentauthConfig = flags['agentauth-config'] || process.env.AGENTAUTH_CONFIG || null;
  const agentauthPort = parseInt(flags['agentauth-port'] || process.env.AGENTAUTH_PORT || '9999');

  // Choose bus
  let messageBus;
  if (server) {
    messageBus = new AgentChatBus({
      server,
      identity: flags.identity || null,
      name: flags.name || null,
    });
  } else {
    messageBus = new EventEmitterBus();
    console.log('No --server specified; using local EventEmitterBus (no network).');
  }

  const config = {
    count,
    maxActive,
    role,
    channels,
    persist,
    basePath,
    messageBus,
    repo: flags.repo || null,
    agentauth: agentauthConfig ? { configPath: agentauthConfig, port: agentauthPort } : null,
    tokenBudget: parseInt(flags['token-budget'] || '0'),
    heartbeatIntervalMs: parseInt(flags['heartbeat-interval'] || '30000'),
    maxTaskDurationMs: parseInt(flags['max-task-duration'] || '1800000'),
  };

  const sup = new Supervisor(config);

  // Set up file logger
  const logDir = flags['log-dir'] || config.logDir || undefined;
  let logger = null;
  if (logDir !== 'none') {
    logger = new Logger({
      dir: sup.logDir,
      maxSizeBytes: parseInt(flags['log-max-size'] || String(10 * 1024 * 1024)),
      maxFiles: parseInt(flags['log-max-files'] || '5'),
      stdout: !!(flags.verbose || flags.v),
    });
    logger.open();
    attachLogger(sup, logger);
  }

  // Verbose console output (when not using file logger stdout mode)
  if ((flags.verbose || flags.v) && !logger) {
    sup.on('log', (entry) => {
      console.log(`[${entry.ts}] ${entry.event}`, JSON.stringify(entry));
    });
  }

  // Signal handlers
  let stopping = false;

  const gracefulStop = async () => {
    if (stopping) return;
    stopping = true;
    console.log('\nShutting down...');
    await sup.stop();
    if (logger) logger.close();
    console.log('Swarm stopped.');
    process.exit(0);
  };

  process.on('SIGINT', gracefulStop);
  process.on('SIGTERM', gracefulStop);

  // SIGHUP — reload config from flags or env
  process.on('SIGHUP', () => {
    console.log('SIGHUP received — reloading config...');
    const newConfig = {};
    const envMaxActive = process.env.AGENTCTL_MAX_ACTIVE;
    const envBudget = process.env.AGENTCTL_TOKEN_BUDGET;
    const envHeartbeat = process.env.AGENTCTL_HEARTBEAT_INTERVAL;

    if (envMaxActive) newConfig.maxActive = parseInt(envMaxActive);
    if (envBudget) newConfig.tokenBudget = parseInt(envBudget);
    if (envHeartbeat) newConfig.heartbeatIntervalMs = parseInt(envHeartbeat);

    if (Object.keys(newConfig).length > 0) {
      sup.reloadConfig(newConfig);
      console.log('Config reloaded:', newConfig);
    } else {
      console.log('No env vars set for reload (AGENTCTL_MAX_ACTIVE, AGENTCTL_TOKEN_BUDGET, AGENTCTL_HEARTBEAT_INTERVAL).');
    }
  });

  try {
    await sup.start();
    const status = sup.status();
    console.log(`Swarm started: ${status.total} daemons, ${status.idle} idle, role=${role}`);
    if (server) {
      console.log(`Connected to ${server}`);
    }
    console.log(`Channels: ${channels.join(', ')}`);
    console.log('Press Ctrl+C to stop. Send SIGHUP to reload config.');
  } catch (err) {
    console.error(`Failed to start: ${err.message}`);
    process.exit(1);
  }

  // Keep process alive
  const keepAlive = setInterval(() => {
    // Periodic status check (for log rotation, etc.)
  }, 60000);

  // Unref so process can exit when stop() is called
  keepAlive.unref();

  // Return supervisor for testing
  return sup;
}

async function cmdStatus(flags) {
  // Status reads from a running supervisor via pidfile
  // For now, this command is only useful when run in the same process
  console.log('Status command requires a running supervisor.');
  console.log('Use `agentctl start` in another terminal, then check logs.');
  console.log('In a future version, status will connect via IPC socket.');
}

async function cmdScale(flags, positional) {
  const target = parseInt(positional[0]);
  if (isNaN(target) || target < 0) {
    console.error('Usage: agentctl scale <number>');
    console.error('  Scale the swarm to the target number of daemons.');
    process.exit(1);
  }
  console.log(`Scale command: target=${target}`);
  console.log('Scale requires a running supervisor. Use IPC in future version.');
}

async function cmdAssign(flags, positional) {
  const agentId = positional[0];
  const prompt = positional.slice(1).join(' ') || flags.prompt;
  if (!agentId || !prompt) {
    console.error('Usage: agentctl assign <agentId> <prompt...>');
    process.exit(1);
  }
  console.log(`Assign: agent=${agentId}, prompt="${prompt}"`);
  console.log('Assign requires a running supervisor. Use IPC in future version.');
}

async function cmdBroadcast(flags, positional) {
  const prompt = positional.join(' ') || flags.prompt;
  if (!prompt) {
    console.error('Usage: agentctl broadcast <prompt...>');
    process.exit(1);
  }
  console.log(`Broadcast: prompt="${prompt}"`);
  console.log('Broadcast requires a running supervisor. Use IPC in future version.');
}

function cmdHelp() {
  console.log(`agentctl — manage AI agent swarms

Usage: agentctl <command> [options]

Commands:
  start                 Boot supervisor and spawn daemon fleet
  status                Show swarm status
  scale <N>             Scale to N total daemons
  assign <id> <prompt>  Assign task to specific agent
  broadcast <prompt>    Broadcast task to all idle agents
  stop                  Graceful shutdown
  help                  Show this help

Start options:
  -n, --count <N>             Number of daemons (default: 3)
  -m, --max-active <N>        Max concurrent active agents (default: 5)
  -r, --role <role>           Agent role (default: builder)
  -s, --server <url>          AgentChat server URL (or AGENTCHAT_SERVER env)
  -c, --channels <ch1,ch2>    Channels to join (default: #agents)
      --name <name>           Agent identity name
      --identity <path>       Path to identity JSON file
      --repo <url>            Git repo to clone into workspaces
      --persist               Keep workspaces on shutdown
      --base-path <dir>       Workspace base directory
      --token-budget <N>      Total token budget (0 = unlimited)
      --heartbeat-interval <ms>  Heartbeat interval (default: 30000)
      --max-task-duration <ms>   Max task duration (default: 1800000)
      --agentauth-config <path>  AgentAuth config JSON (enables sidecar)
      --agentauth-port <port>    AgentAuth proxy port (default: 9999)
      --verbose               Print all log events

Config reload (SIGHUP):
  Set env vars then send SIGHUP to the process:
    AGENTCTL_MAX_ACTIVE=10 kill -HUP <pid>

Environment:
  AGENTCHAT_SERVER     Default server URL for --server
  AGENTAUTH_CONFIG     Default config path for --agentauth-config
  AGENTAUTH_PORT       Default port for --agentauth-port
  AGENTCTL_MAX_ACTIVE  Reload: max active agents
  AGENTCTL_TOKEN_BUDGET  Reload: token budget
  AGENTCTL_HEARTBEAT_INTERVAL  Reload: heartbeat interval (ms)
`);
}

// ============ Main ============

const { command, flags, positional } = parseArgs(process.argv);

switch (command) {
  case 'start':
    cmdStart(flags);
    break;
  case 'status':
    cmdStatus(flags);
    break;
  case 'scale':
    cmdScale(flags, positional);
    break;
  case 'assign':
    cmdAssign(flags, positional);
    break;
  case 'broadcast':
    cmdBroadcast(flags, positional);
    break;
  case 'stop':
    console.log('Send SIGTERM or SIGINT to the running supervisor process.');
    console.log('  kill <pid>  or  Ctrl+C in the terminal');
    break;
  case 'help':
  case '--help':
  case '-h':
    cmdHelp();
    break;
  default:
    console.error(`Unknown command: ${command}`);
    console.error('Run `agentctl help` for usage.');
    process.exit(1);
}

// Export for testing
export { parseArgs, cmdStart };

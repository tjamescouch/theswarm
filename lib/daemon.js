/**
 * Daemon
 * A lightweight idle process that listens for tasks on agentchat
 * and promotes to a full agent session when work is available.
 * One daemon per swarm slot.
 */

import { EventEmitter } from 'events';
import { spawn as spawnProcess } from 'child_process';
import fs from 'fs';
import path from 'path';

/** Daemon states */
export const DaemonState = {
  IDLE: 'idle',
  PROMOTING: 'promoting',
  ACTIVE: 'active',
  DEMOTING: 'demoting',
  CRASHED: 'crashed',
};

/**
 * @typedef {object} DaemonConfig
 * @property {string} agentId - agentchat agent ID
 * @property {string} name - agent name
 * @property {string} workspace - workspace directory path
 * @property {string} [role] - agent role (default: 'builder')
 * @property {string[]} [channels] - agentchat channels to join (default: ['#agents'])
 * @property {number} [heartbeatIntervalMs] - heartbeat interval (default: 30000)
 */

export class Daemon extends EventEmitter {
  /**
   * @param {DaemonConfig} config
   */
  constructor(config) {
    super();

    if (!config.agentId) throw new Error('daemon requires agentId');
    if (!config.name) throw new Error('daemon requires name');
    if (!config.workspace) throw new Error('daemon requires workspace');

    this.agentId = config.agentId;
    this.name = config.name;
    this.workspace = config.workspace;
    this.role = config.role || 'builder';
    this.channels = config.channels || ['#agents'];
    this.heartbeatIntervalMs = config.heartbeatIntervalMs || 30000;
    this.agentauthUrl = config.agentauthUrl || null;

    this.state = DaemonState.IDLE;
    this.currentTask = null;
    this.claudeProcess = null;
    this._heartbeatTimer = null;
  }

  /**
   * Start the daemon — begin sending heartbeats and listening.
   * Does NOT connect to agentchat (that's the supervisor's responsibility
   * to wire up message routing).
   */
  start() {
    this.state = DaemonState.IDLE;
    this._startHeartbeat();
    this.emit('started', { agentId: this.agentId });
  }

  /**
   * Stop the daemon — cleanup timers, kill active process if any.
   */
  stop() {
    this._stopHeartbeat();

    if (this.claudeProcess) {
      this.claudeProcess.kill('SIGTERM');
      this.claudeProcess = null;
    }

    this.state = DaemonState.IDLE;
    this.currentTask = null;
    this.emit('stopped', { agentId: this.agentId });
  }

  /**
   * Evaluate whether a task matches this daemon's role.
   * @param {object} task
   * @param {string} task.role - required role
   * @param {string} [task.component] - component name
   * @returns {boolean}
   */
  matchesRole(task) {
    if (!task || !task.role) return this.role === 'general';
    return task.role === this.role || this.role === 'general';
  }

  /**
   * Handle an incoming message from the work channel.
   * Evaluates task announcements and ASSIGN messages.
   * @param {object} msg
   */
  handleMessage(msg) {
    if (this.state !== DaemonState.IDLE) return;

    // Check for ASSIGN directed at this daemon
    if (msg.type === 'ASSIGN' && msg.agentId === this.agentId) {
      this._requestPromotion(msg.task);
      return;
    }

    // Check for task announcements that match our role
    if (msg.type === 'TASK_AVAILABLE' && this.matchesRole(msg.task)) {
      this.emit('claim', {
        agentId: this.agentId,
        component: msg.task.component,
        role: this.role,
      });
    }
  }

  /**
   * Request promotion from supervisor.
   * Emits 'promote-request' — supervisor must call approvePromotion() or denyPromotion().
   * @param {object} task
   */
  _requestPromotion(task) {
    this.state = DaemonState.PROMOTING;
    this.currentTask = task;

    this.emit('promote-request', {
      agentId: this.agentId,
      task,
    });
  }

  /**
   * Supervisor approves promotion — spawn the claude session.
   * @param {object} task
   * @param {string} task.prompt - the task prompt
   * @param {string} [task.id] - task identifier
   * @param {string} [task.component] - component being built
   */
  approvePromotion(task) {
    if (this.state !== DaemonState.PROMOTING) {
      this.emit('error', { agentId: this.agentId, error: 'Cannot promote: not in promoting state' });
      return;
    }

    this.currentTask = task || this.currentTask;

    // Write task context before starting
    this._writeContext(`# Active Task\n\nTask: ${this.currentTask.component || 'unknown'}\nPrompt: ${this.currentTask.prompt || 'none'}\nStarted: ${new Date().toISOString()}\n`);

    // Spawn claude session
    this._spawnClaude(this.currentTask.prompt || 'Execute the assigned task.');
  }

  /**
   * Supervisor denies promotion — return to idle.
   * @param {string} [reason]
   */
  denyPromotion(reason) {
    if (this.state !== DaemonState.PROMOTING) return;

    this.state = DaemonState.IDLE;
    this.currentTask = null;

    this.emit('unclaim', {
      agentId: this.agentId,
      reason: reason || 'promotion denied',
    });
  }

  /**
   * Spawn a claude -p session in the workspace.
   * @param {string} prompt
   */
  /**
   * Build a sanitized environment for spawned agent processes.
   * Removes sensitive keys and injects agentauth proxy URL if configured.
   * @returns {object} sanitized env vars
   */
  _buildSanitizedEnv() {
    const SCRUB_PATTERNS = [
      /^ANTHROPIC_API_KEY$/i,
      /^OPENAI_API_KEY$/i,
      /^GITHUB_TOKEN$/i,
      /^GH_TOKEN$/i,
      /^AWS_SECRET_ACCESS_KEY$/i,
      /^AWS_SESSION_TOKEN$/i,
      /^STRIPE_SECRET_KEY$/i,
      /^DATABASE_URL$/i,
      /^DB_PASSWORD$/i,
      /^SECRET_KEY$/i,
      /^PRIVATE_KEY$/i,
      /^API_KEY$/i,
      /^API_SECRET$/i,
    ];

    const env = {};
    for (const [key, value] of Object.entries(process.env)) {
      const shouldScrub = SCRUB_PATTERNS.some((p) => p.test(key));
      if (!shouldScrub) {
        env[key] = value;
      }
    }

    // Inject agentauth proxy URL so agents route through the proxy
    if (this.agentauthUrl) {
      env.AGENTAUTH_PROXY_URL = this.agentauthUrl;
    }

    return env;
  }

  _spawnClaude(prompt) {
    this.state = DaemonState.ACTIVE;
    this._stopHeartbeat(); // Active agents don't send idle heartbeats

    const args = ['-p', prompt, '--cwd', this.workspace];

    // Use sanitized env when agentauth is configured, otherwise inherit
    const spawnOpts = {
      cwd: this.workspace,
      stdio: ['pipe', 'pipe', 'pipe'],
    };
    if (this.agentauthUrl) {
      spawnOpts.env = this._buildSanitizedEnv();
    }

    try {
      this.claudeProcess = spawnProcess('claude', args, spawnOpts);

      this.emit('promoted', {
        agentId: this.agentId,
        pid: this.claudeProcess.pid,
        task: this.currentTask,
      });

      let stdout = '';
      let stderr = '';

      this.claudeProcess.stdout.on('data', (data) => {
        const chunk = data.toString();
        stdout += chunk;
        this.emit('output', { agentId: this.agentId, data: chunk, stream: 'stdout' });
      });

      this.claudeProcess.stderr.on('data', (data) => {
        const chunk = data.toString();
        stderr += chunk;
        this.emit('output', { agentId: this.agentId, data: chunk, stream: 'stderr' });
      });

      this.claudeProcess.on('exit', (code, signal) => {
        this.claudeProcess = null;
        this._handleClaudeExit(code, signal, stdout, stderr);
      });

      this.claudeProcess.on('error', (err) => {
        this.claudeProcess = null;
        this._handleClaudeError(err);
      });
    } catch (err) {
      this._handleClaudeError(err);
    }
  }

  /**
   * Handle claude process exit — begin demotion.
   */
  _handleClaudeExit(code, signal, stdout, stderr) {
    this.state = DaemonState.DEMOTING;

    const success = code === 0;
    const result = {
      agentId: this.agentId,
      task: this.currentTask,
      exitCode: code,
      signal,
      success,
      output: stdout.slice(-2000), // Last 2000 chars
    };

    // Save context for crash recovery
    this._writeContext(
      `# Last Task\n\n` +
      `Task: ${this.currentTask?.component || 'unknown'}\n` +
      `Result: ${success ? 'SUCCESS' : 'FAIL'}\n` +
      `Exit code: ${code}\n` +
      `Completed: ${new Date().toISOString()}\n` +
      `\n## Output (last 500 chars)\n\n${stdout.slice(-500)}\n`
    );

    if (success) {
      this.emit('done', result);
    } else {
      this.emit('fail', result);
    }

    // Complete demotion
    this.state = DaemonState.IDLE;
    this.currentTask = null;
    this._startHeartbeat();

    this.emit('demoted', { agentId: this.agentId });
  }

  /**
   * Handle claude spawn/runtime error.
   */
  _handleClaudeError(err) {
    this.state = DaemonState.CRASHED;

    this._writeContext(
      `# Crash\n\nError: ${err.message}\nTime: ${new Date().toISOString()}\n`
    );

    this.emit('fail', {
      agentId: this.agentId,
      task: this.currentTask,
      exitCode: null,
      signal: null,
      success: false,
      error: err.message,
    });

    this.emit('crashed', { agentId: this.agentId, error: err.message });
  }

  /**
   * Write context.md to workspace.
   * @param {string} content
   */
  _writeContext(content) {
    try {
      fs.writeFileSync(path.join(this.workspace, 'context.md'), content);
    } catch {
      // Best effort — don't crash daemon on write failure
    }
  }

  /**
   * Start periodic heartbeat.
   */
  _startHeartbeat() {
    this._stopHeartbeat();
    this._heartbeatTimer = setInterval(() => {
      this.emit('heartbeat', {
        agentId: this.agentId,
        status: this.state,
      });
    }, this.heartbeatIntervalMs);
  }

  /**
   * Stop periodic heartbeat.
   */
  _stopHeartbeat() {
    if (this._heartbeatTimer) {
      clearInterval(this._heartbeatTimer);
      this._heartbeatTimer = null;
    }
  }

  /**
   * Get current daemon info.
   */
  info() {
    return {
      agentId: this.agentId,
      name: this.name,
      role: this.role,
      state: this.state,
      workspace: this.workspace,
      channels: this.channels,
      currentTask: this.currentTask,
      hasClaude: !!this.claudeProcess,
      pid: this.claudeProcess?.pid || null,
    };
  }
}

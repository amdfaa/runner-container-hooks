import * as core from '@actions/core'
import * as crypto from 'crypto'
import * as fs from 'fs'
import { join } from 'path'
import { sleep } from './utils'
import { execPodStepOutput, getPodByName } from './index'

interface RpcStatusResponse {
  // null when no job has run yet (server is in initial 'idle' state). The
  // Python server sets _job_id = None at startup and only assigns a value
  // when /exec accepts a job.
  id: string | null
  // 'starting'/'killing' are transient server-side states (job being launched
  // or torn down); the poll loop treats them like 'running' and keeps waiting.
  status: 'idle' | 'starting' | 'running' | 'killing' | 'completed' | 'failed'
  exit_code: number | null
}

// Fixed port for the in-pod server. Each pod has its own network namespace, so
// there are no collisions, and a NetworkPolicy only has to allow one port.
export const RPC_PORT = 8765

// How long the heartbeat may be silent. One constant feeds both sides: the hook
// gives up after this long without an answered heartbeat, and the server (via
// RPC_HEARTBEAT_TIMEOUT_SECS) kills the step after this long without receiving
// one. Keeping them equal means the two sides agree on when the step is lost.
export const RPC_HEARTBEAT_GRACE_SECONDS = 120

// Where the binary is baked into the runner image, and where it is copied to on
// the shared work volume (the job pod sees RUNNER_TEMP as /__w/_temp).
export const DEFAULT_RPC_SERVER_IMAGE_PATH = '/home/runner/k8s/rpc-server'
export const RPC_SERVER_CONTAINER_PATH = '/__w/_temp/rpc-server'
// The server records the running step's process-group id here (job pod path).
export const RPC_PGID_FILE = '/tmp/rpc-logs/job.pgid'

const HEARTBEAT_INTERVAL_MS = 3000
const HEARTBEAT_GRACE_MS = RPC_HEARTBEAT_GRACE_SECONDS * 1000
// The log poll starts fast and backs off while the step is quiet.
export const LOG_POLL_MIN_INTERVAL_MS = 200
export const LOG_POLL_MAX_INTERVAL_MS = 1000
const HEALTH_POLL_INTERVAL_MS = 1000
const HEALTH_TIMEOUT_MS = 15000
const MAX_DEPLOY_ATTEMPTS = 3
const FETCH_TIMEOUT_MS = 10000
// Longer than undici's own 10s connect timeout, so a host that never answers
// fails with a connect error (safe to fall back from) rather than an abort that
// could also mean the server accepted the step but its reply was lost.
const EXEC_FETCH_TIMEOUT_MS = 20000
const KILL_STEP_GROUP_TIMEOUT_MS = 10000
const KILL_TIMEOUT_MS = 5000
const DIAGNOSTIC_TIMEOUT_MS = 15000
const DIAGNOSTIC_EXEC_TIMEOUT_MS = 10000

function rpcUrl(podIp: string, port: number, path: string): string {
  const host = podIp.includes(':') ? `[${podIp}]` : podIp
  return `http://${host}:${port}${path}`
}

async function healthCheck(podIp: string, port: number): Promise<boolean> {
  // Health-check from the hook side via outbound HTTP. Avoids depending on
  // any in-pod tool (curl/wget/nc/python) — the binary listens on the pod IP
  // and the hook process can reach it directly.
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), 2000)
  try {
    const resp = await fetch(rpcUrl(podIp, port, '/health'), {
      signal: controller.signal
    })
    return resp.ok
  } catch {
    return false
  } finally {
    clearTimeout(timer)
  }
}

/**
 * Copy the server binary from the runner image onto the shared work volume so
 * the job pod can run it. Throws if the image does not ship the binary.
 */
function copyRpcServerBinary(): void {
  const source =
    process.env.ACTIONS_RUNNER_RPC_SERVER_PATH || DEFAULT_RPC_SERVER_IMAGE_PATH
  if (!fs.existsSync(source)) {
    throw new Error(`RPC server binary not found at ${source}`)
  }
  const destination = join(process.env.RUNNER_TEMP as string, 'rpc-server')
  fs.copyFileSync(source, destination)
  fs.chmodSync(destination, 0o755)
}

/**
 * Start the in-pod server in the job container and wait until it answers
 * /health. Retries the start up to MAX_DEPLOY_ATTEMPTS times. Throws when the
 * binary is missing or the server never becomes healthy; the caller then falls
 * back to the exec launcher. Never logs the token or the start command.
 */
export async function deployRpcServer(
  podName: string,
  containerName: string,
  token: string
): Promise<{ podIp: string; port: number }> {
  const pod = await getPodByName(podName)
  const podIp = pod.status?.podIP
  if (!podIp) {
    throw new Error(`Pod ${podName} has no IP address`)
  }

  copyRpcServerBinary()

  const attemptErrors: string[] = []
  for (let attempt = 1; attempt <= MAX_DEPLOY_ATTEMPTS; attempt++) {
    core.debug(
      `Starting RPC server (attempt ${attempt}/${MAX_DEPLOY_ATTEMPTS})`
    )

    if (attempt > 1) {
      // A server left over from a failed attempt still holds the fixed port.
      // The `[_]` keeps this shell's own command line from matching the pattern.
      try {
        await execPodStepOutput(
          ['sh', '-c', `pkill -f '[_]temp/rpc-server' 2>/dev/null; true`],
          podName,
          containerName
        )
      } catch (err) {
        core.debug(
          `RPC cleanup failed: ${err instanceof Error ? err.message : err}`
        )
      }
    }

    // --daemonize: the server prints its startup lines, then forks into the
    // background and detaches its stdio so this exec returns.
    try {
      const { exitCode, stdout } = await execPodStepOutput(
        [
          'env',
          `RPC_HEARTBEAT_TIMEOUT_SECS=${RPC_HEARTBEAT_GRACE_SECONDS}`,
          RPC_SERVER_CONTAINER_PATH,
          '--port',
          String(RPC_PORT),
          '--token',
          token,
          '--daemonize'
        ],
        podName,
        containerName
      )
      const oomLine = stdout.match(/^rpc-server: oom_score_adj .*$/m)
      if (oomLine) {
        core.info(oomLine[0])
      }
      const listening = /^RPC server listening on \[::\]:(\d+)$/m.exec(stdout)
      if (
        exitCode !== 0 ||
        !listening ||
        parseInt(listening[1], 10) !== RPC_PORT
      ) {
        throw new Error(
          `server did not report port ${RPC_PORT} (exit ${exitCode}): ${stdout || '(empty)'}`
        )
      }
    } catch (err) {
      const msg = `attempt ${attempt}: server start failed: ${err instanceof Error ? err.message : err}`
      core.warning(msg)
      attemptErrors.push(msg)
      continue
    }

    const startTime = Date.now()
    while (Date.now() - startTime < HEALTH_TIMEOUT_MS) {
      if (await healthCheck(podIp, RPC_PORT)) {
        core.debug(`RPC server healthy after ${Date.now() - startTime}ms`)
        return { podIp, port: RPC_PORT }
      }
      await sleep(HEALTH_POLL_INTERVAL_MS)
    }

    let serverLog = ''
    try {
      const logResult = await execPodStepOutput(
        [
          'sh',
          '-c',
          'tail -20 /tmp/rpc-server.log 2>/dev/null || echo "(no log)"'
        ],
        podName,
        containerName
      )
      serverLog = logResult.stdout
    } catch {
      // best effort
    }
    const msg = `attempt ${attempt}: health check timed out on port ${RPC_PORT}${serverLog ? `. Server log:\n${serverLog}` : ''}`
    core.warning(msg)
    attemptErrors.push(msg)
  }

  throw new Error(
    `RPC server failed after ${MAX_DEPLOY_ATTEMPTS} attempts:\n${attemptErrors.join('\n')}`
  )
}

interface PodFailureDiagnostic {
  cause:
    | 'container-oom'
    | 'pod-evicted'
    | 'node-failure'
    | 'rpc-process-died'
    | 'unknown'
  exitCode: number
  message: string
}

async function diagnosePodFailure(
  podName: string,
  containerName: string
): Promise<PodFailureDiagnostic> {
  const pod = await getPodByName(podName)
  const phase = pod.status?.phase
  const podReason = pod.status?.reason
  const podMessage = pod.status?.message

  const containerStatus = pod.status?.containerStatuses?.find(
    cs => cs.name === containerName
  )

  // Container terminated — check reason (OOMKilled, Error, etc.)
  const terminated = containerStatus?.state?.terminated
  if (terminated) {
    if (terminated.reason === 'OOMKilled') {
      // Force 137 — some CRI runtimes report exitCode 0 for OOMKilled
      const oomExitCode = terminated.exitCode || 137
      return {
        cause: 'container-oom',
        exitCode: oomExitCode,
        message: `Container "${containerName}" was OOMKilled (exit code ${oomExitCode}). The process exceeded the container memory limit.`
      }
    }
    return {
      cause: 'unknown',
      exitCode: terminated.exitCode ?? -1,
      message: `Container "${containerName}" terminated: ${terminated.reason ?? 'unknown reason'} (exit code ${terminated.exitCode ?? -1})${terminated.message ? `. ${terminated.message}` : ''}`
    }
  }

  // Pod-level failure (eviction, preemption, etc.)
  if (phase === 'Failed') {
    if (podReason === 'Evicted') {
      return {
        cause: 'pod-evicted',
        exitCode: -1,
        message: `Pod was evicted${podMessage ? `: ${podMessage}` : ''}`
      }
    }
    return {
      cause: 'unknown',
      exitCode: -1,
      message: `Pod failed: ${podReason ?? 'unknown reason'}${podMessage ? `. ${podMessage}` : ''}`
    }
  }

  // Pod phase Unknown — typically node failure
  if (phase === 'Unknown') {
    return {
      cause: 'node-failure',
      exitCode: -1,
      message: `Pod is in Unknown phase — possible node failure${podMessage ? `. ${podMessage}` : ''}`
    }
  }

  // Container still running — RPC server process likely crashed inside it
  if (phase === 'Running' && containerStatus?.state?.running) {
    let serverLog = ''
    try {
      let execTimer: ReturnType<typeof setTimeout>
      const result = await Promise.race([
        execPodStepOutput(
          [
            'sh',
            '-c',
            'tail -20 /tmp/rpc-server.log 2>/dev/null || echo "(no server log)"'
          ],
          podName,
          containerName
        ),
        new Promise<never>((_, reject) => {
          execTimer = setTimeout(
            () => reject(new Error('diagnostic exec timeout')),
            DIAGNOSTIC_EXEC_TIMEOUT_MS
          )
        })
      ]).finally(() => clearTimeout(execTimer))
      serverLog = result.stdout
    } catch {
      // Diagnostic exec failed — proceed without server log
    }
    return {
      cause: 'rpc-process-died',
      exitCode: -1,
      message: `RPC server process died while container is still running (likely process-level OOM or crash)${serverLog ? `\nServer log:\n${serverLog}` : ''}`
    }
  }

  return {
    cause: 'unknown',
    exitCode: -1,
    message: `Pod in unexpected state: phase=${phase}, reason=${podReason}${podMessage ? `. ${podMessage}` : ''}`
  }
}

/**
 * Thrown when /exec could not reach the server on any attempt, so the step was
 * never started. The caller may safely run the step another way. Errors after
 * the server accepted the step are plain Errors and must fail the step.
 */
export class RpcUnreachableError extends Error {
  constructor(message: string) {
    super(message)
    // This repo compiles to ES5, where extending Error loses the prototype
    // chain unless it is restored by hand.
    Object.setPrototypeOf(this, RpcUnreachableError.prototype)
  }
}

// Error codes that mean the TCP connection was never established, so the
// request cannot have reached the server. ECONNRESET and response timeouts are
// deliberately absent: those can happen after the server has the request.
const NEVER_CONNECTED_CODES = new Set([
  'ECONNREFUSED',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'ENOTFOUND',
  'EADDRNOTAVAIL',
  'UND_ERR_CONNECT_TIMEOUT'
])

function isNeverConnected(err: unknown): boolean {
  // fetch() throws TypeError('fetch failed') with the socket error in `cause`;
  // with several addresses the cause is an AggregateError of the attempts.
  const cause = (err as { cause?: { code?: string; errors?: unknown[] } })
    ?.cause
  if (!cause) return false
  const causes = cause.errors?.length ? cause.errors : [cause]
  return causes.every(c =>
    NEVER_CONNECTED_CODES.has((c as { code?: string })?.code ?? '')
  )
}

/**
 * Build the shell command that kills the step's process group after the server
 * died: SIGTERM, a short wait, then SIGKILL. Does nothing if the pgid file is
 * missing (the step already ended) or empty.
 */
export function buildKillStepGroupCommand(
  pgidFile: string = RPC_PGID_FILE
): string {
  // `kill -s SIG -- -PGID` works in dash and bash; busybox rejects the `--`
  // and takes `kill -s SIG -PGID`. Try one, then the other.
  return (
    `P=$(cat ${pgidFile} 2>/dev/null); ` +
    `case "$P" in ''|*[!0-9]*) exit 0;; esac; ` +
    `k() { kill -s "$1" -- "-$P" 2>/dev/null || kill -s "$1" "-$P" 2>/dev/null; }; ` +
    `k TERM; sleep 1; k KILL; exit 0`
  )
}

// Best effort, bounded: the cleanup hook remains the backstop if this fails.
async function killStepGroup(
  podName: string,
  containerName: string
): Promise<void> {
  try {
    await execPodStepOutput(
      ['sh', '-c', buildKillStepGroupCommand()],
      podName,
      containerName,
      KILL_STEP_GROUP_TIMEOUT_MS
    )
  } catch (err) {
    core.debug(`Failed to kill the step process group: ${err}`)
  }
}

export async function rpcPodStep(
  podIp: string,
  port: number,
  scriptPath: string,
  token: string,
  podName: string,
  containerName: string
): Promise<number> {
  const id = crypto.randomUUID()
  const headers = { 'X-Auth-Token': token, 'Content-Type': 'application/json' }

  // Retry /exec on network errors and 5xx; throw immediately on 4xx
  const maxExecAttempts = 3
  const execRetryDelayMs = 1000
  // True while every failed attempt was a connect failure. Any HTTP response,
  // or an error that may have come after the request was sent, clears it.
  let neverReachedServer = true
  for (let attempt = 1; attempt <= maxExecAttempts; attempt++) {
    let execResp: Response
    try {
      execResp = await fetch(rpcUrl(podIp, port, '/exec'), {
        method: 'POST',
        headers,
        body: JSON.stringify({ id, path: scriptPath }),
        signal: AbortSignal.timeout(EXEC_FETCH_TIMEOUT_MS)
      })
    } catch (fetchErr) {
      if (!isNeverConnected(fetchErr)) {
        neverReachedServer = false
      }
      if (attempt < maxExecAttempts) {
        core.debug(
          `RPC /exec network error (attempt ${attempt}/${maxExecAttempts}): ${fetchErr}`
        )
        await sleep(execRetryDelayMs)
        continue
      }
      const message = `RPC /exec network error after ${maxExecAttempts} attempts: ${fetchErr}`
      throw neverReachedServer
        ? new RpcUnreachableError(message)
        : new Error(message)
    }

    // The server answered, so it is reachable and may have taken the step.
    neverReachedServer = false
    if (execResp.ok) {
      break
    }

    if (execResp.status >= 400 && execResp.status < 500) {
      const body = await execResp.text().catch(() => '')

      if (
        execResp.status === 409 &&
        body.includes('A job is already running')
      ) {
        // The RPC server still has a prior job in 'running' state. This
        // almost always means the prior step was cancelled by GitHub Actions
        // (matrix fail-fast, manual cancel, workflow concurrency override,
        // or run-level cancellation) but the in-pod subprocess kept running
        // — the cancel signal only reached the hook process. Subsequent
        // post-cleanup steps cannot start until that prior subprocess exits.
        // The hook's SIGTERM/SIGINT handler is supposed to /kill it; seeing
        // 409 here means that path didn't fire (e.g. SIGKILL, hook crash).
        let stuckCtx = ''
        try {
          const statusResp = await fetch(rpcUrl(podIp, port, '/status'), {
            headers: { 'X-Auth-Token': token },
            signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
          })
          if (statusResp.ok) {
            const s = (await statusResp.json()) as RpcStatusResponse
            stuckCtx = ` (prior job id=${s.id ?? '?'}, status=${s.status ?? '?'})`
          }
        } catch {
          // Best-effort diagnostic; ignore failures
        }
        throw new Error(
          `Step cannot start: workflow pod still has a prior step in flight${stuckCtx}. ` +
            `This is the expected state after GitHub Actions cancels a step ` +
            `(matrix fail-fast, manual cancel, or workflow concurrency override) ` +
            `while its subprocess was still running inside the pod. Check earlier ` +
            `in the job log (or the workflow-run page) for the actual cancellation.`
        )
      }

      throw new Error(`RPC /exec failed (${execResp.status}): ${body}`)
    }

    if (attempt < maxExecAttempts) {
      core.debug(
        `RPC /exec server error ${execResp.status} (attempt ${attempt}/${maxExecAttempts}), retrying`
      )
      await sleep(execRetryDelayMs)
      continue
    }

    const body = await execResp.text().catch(() => '')
    throw new Error(
      `RPC /exec failed after ${maxExecAttempts} attempts (${execResp.status}): ${body}`
    )
  }

  let lastSuccessfulHeartbeat = Date.now()
  const heartbeatInterval = setInterval(async () => {
    try {
      const resp = await fetch(rpcUrl(podIp, port, '/heartbeat'), {
        method: 'POST',
        headers: { 'X-Auth-Token': token },
        signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
      })
      if (resp.ok) {
        lastSuccessfulHeartbeat = Date.now()
      } else if (Date.now() - lastSuccessfulHeartbeat > HEARTBEAT_GRACE_MS) {
        clearInterval(heartbeatInterval)
      }
    } catch {
      if (Date.now() - lastSuccessfulHeartbeat > HEARTBEAT_GRACE_MS) {
        clearInterval(heartbeatInterval)
      }
    }
  }, HEARTBEAT_INTERVAL_MS)

  let stdoutOffset = 0
  let stderrOffset = 0
  let exitCode = -1
  let pollIntervalMs = LOG_POLL_MIN_INTERVAL_MS

  try {
    while (true) {
      if (Date.now() - lastSuccessfulHeartbeat > HEARTBEAT_GRACE_MS) {
        let diagnosticMsg = `RPC heartbeat failed for ${RPC_HEARTBEAT_GRACE_SECONDS}s — infrastructure failure`
        try {
          let diagTimer: ReturnType<typeof setTimeout>
          const diagnostic = await Promise.race([
            diagnosePodFailure(podName, containerName),
            new Promise<never>((_, reject) => {
              diagTimer = setTimeout(
                () => reject(new Error('diagnostic timed out')),
                DIAGNOSTIC_TIMEOUT_MS
              )
            })
          ]).finally(() => clearTimeout(diagTimer))
          diagnosticMsg = diagnostic.message
          core.error(`Step failed: ${diagnostic.message}`)
          if (diagnostic.cause === 'rpc-process-died') {
            // The server is gone but the step may still be running; stop it
            // so it does not keep going after the step is reported failed.
            await killStepGroup(podName, containerName)
          }
        } catch (diagErr) {
          core.debug(`Pod failure diagnosis failed: ${diagErr}`)
          core.error(diagnosticMsg)
        }
        throw new Error(diagnosticMsg)
      }

      // Log streaming — stdout and stderr in parallel. Script steps merge
      // stderr into stdout (`exec 2>&1` in the entry script), so stderr only
      // carries what the shell printed before that line ran.
      let receivedBytes = false
      try {
        const [stdoutData, stderrData] = await Promise.all([
          fetchLogStream(podIp, port, token, 'stdout', stdoutOffset),
          fetchLogStream(podIp, port, token, 'stderr', stderrOffset)
        ])
        if (stdoutData.byteLength > 0) {
          process.stdout.write(Buffer.from(stdoutData))
          stdoutOffset += stdoutData.byteLength
          receivedBytes = true
        }
        if (stderrData.byteLength > 0) {
          process.stderr.write(Buffer.from(stderrData))
          stderrOffset += stderrData.byteLength
          receivedBytes = true
        }
      } catch {
        core.debug('log fetch failed, will retry')
      }

      // Status polling
      try {
        const statusResp = await fetch(rpcUrl(podIp, port, '/status'), {
          headers: { 'X-Auth-Token': token },
          signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
        })
        if (statusResp.ok) {
          const status = (await statusResp.json()) as RpcStatusResponse
          if (status.status === 'completed' || status.status === 'failed') {
            exitCode = status.exit_code ?? -1
            break
          }
        }
      } catch {
        core.debug('status fetch failed, will retry')
      }

      // Poll fast while output flows; back off to the maximum while quiet.
      if (receivedBytes) {
        pollIntervalMs = LOG_POLL_MIN_INTERVAL_MS
      }
      await sleep(pollIntervalMs)
      if (!receivedBytes) {
        pollIntervalMs = Math.min(pollIntervalMs * 2, LOG_POLL_MAX_INTERVAL_MS)
      }
    }

    // Final log flush — drain all remaining data from both streams
    try {
      let flushing = true
      while (flushing) {
        const [stdoutData, stderrData] = await Promise.all([
          fetchLogStream(podIp, port, token, 'stdout', stdoutOffset),
          fetchLogStream(podIp, port, token, 'stderr', stderrOffset)
        ])
        flushing = false
        if (stdoutData.byteLength > 0) {
          process.stdout.write(Buffer.from(stdoutData))
          stdoutOffset += stdoutData.byteLength
          flushing = true
        }
        if (stderrData.byteLength > 0) {
          process.stderr.write(Buffer.from(stderrData))
          stderrOffset += stderrData.byteLength
          flushing = true
        }
      }
    } catch {
      core.debug('final log flush failed')
    }

    return exitCode
  } finally {
    clearInterval(heartbeatInterval)
  }
}

/**
 * Tell the in-pod RPC server to terminate the currently running job.
 *
 * Called from the hook's SIGTERM/SIGINT handler when GitHub Actions cancels
 * the step. Without this, the user's subprocess (e.g. pytest) keeps running
 * in the workflow pod and post-cleanup steps fail with 409 "A job is already
 * running" until something else (heartbeat watchdog, pod eviction) kills it.
 *
 * Always best-effort: never throws, swallows network errors, and uses a tight
 * timeout because the process is about to exit.
 */
export async function killRpcJob(
  podIp: string,
  port: number,
  token: string
): Promise<void> {
  try {
    await fetch(rpcUrl(podIp, port, '/kill'), {
      method: 'POST',
      headers: { 'X-Auth-Token': token },
      signal: AbortSignal.timeout(KILL_TIMEOUT_MS)
    })
  } catch (err) {
    core.debug(`Failed to send /kill on cancellation: ${err}`)
  }
}

async function fetchLogStream(
  podIp: string,
  port: number,
  token: string,
  stream: 'stdout' | 'stderr',
  offset: number
): Promise<ArrayBuffer> {
  const resp = await fetch(
    rpcUrl(podIp, port, `/logs?stream=${stream}&offset=${offset}`),
    {
      headers: { 'X-Auth-Token': token },
      signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
    }
  )
  if (!resp.ok) {
    return new ArrayBuffer(0)
  }
  return resp.arrayBuffer()
}

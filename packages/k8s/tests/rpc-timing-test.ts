// Long-running behaviour of rpcPodStep, driven by fake timers so minutes of
// "step time" run instantly: quiet steps, heartbeat blips, the heartbeat grace
// and the idle log-poll backoff.

const mockExecPodStepOutput = jest.fn()
const mockGetPodByName = jest.fn()

jest.mock('../src/k8s', () => ({
  execPodStep: jest.fn(),
  execPodStepOutput: (...args) => mockExecPodStepOutput(...args),
  getPodByName: (...args) => mockGetPodByName(...args)
}))

jest.mock('@actions/core', () => ({
  debug: jest.fn(),
  info: jest.fn(),
  warning: jest.fn(),
  error: jest.fn()
}))

// A real, timer-based sleep, so fake timers control every wait in the loop.
jest.mock('../src/k8s/utils', () => ({
  sleep: async (ms: number) => new Promise(resolve => setTimeout(resolve, ms))
}))

import * as core from '@actions/core'
import {
  LOG_POLL_MAX_INTERVAL_MS,
  LOG_POLL_MIN_INTERVAL_MS,
  RPC_HEARTBEAT_GRACE_SECONDS,
  rpcPodStep
} from '../src/k8s/rpc'

const POD_IP = '10.0.0.1'
const MINUTE_MS = 60 * 1000
const GRACE_MS = RPC_HEARTBEAT_GRACE_SECONDS * 1000

function jsonResponse(status: number, body: unknown): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    text: async () => JSON.stringify(body),
    json: async () => body,
    arrayBuffer: async () => new ArrayBuffer(0)
  } as unknown as Response
}

function bytesResponse(text: string): Response {
  const bytes = new TextEncoder().encode(text)
  return {
    ok: true,
    status: 200,
    arrayBuffer: async () => bytes.buffer
  } as unknown as Response
}

describe('rpcPodStep over time', () => {
  let startedAt: number
  let fetchMock: jest.Mock
  let stdoutWrite: jest.SpyInstance

  // Milliseconds of fake time since the test began.
  const elapsed = (): number => Date.now() - startedAt

  interface Behavior {
    // Step finishes (status "completed") at this elapsed time.
    completeAtMs: number
    // Heartbeat POSTs fail while this returns true.
    heartbeatFails?: (elapsedMs: number) => boolean
    // Called for each /logs request; return text to send bytes.
    logs?: (stream: string, elapsedMs: number) => string | undefined
    onStatus?: (elapsedMs: number) => void
  }

  function route(behavior: Behavior): void {
    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return jsonResponse(200, { status: 'running' })
      }
      if (urlStr.includes('/heartbeat')) {
        if (behavior.heartbeatFails?.(elapsed())) {
          throw new Error('connect ETIMEDOUT')
        }
        return jsonResponse(200, { status: 'ok' })
      }
      if (urlStr.includes('/logs')) {
        const stream = /stream=(\w+)/.exec(urlStr)?.[1] ?? 'stdout'
        const text = behavior.logs?.(stream, elapsed())
        return text ? bytesResponse(text) : bytesResponse('')
      }
      if (urlStr.includes('/status')) {
        behavior.onStatus?.(elapsed())
        const done = elapsed() >= behavior.completeAtMs
        return jsonResponse(200, {
          status: done ? 'completed' : 'running',
          exit_code: done ? 0 : null
        })
      }
      throw new Error(`unexpected request: ${urlStr}`)
    })
  }

  // Run rpcPodStep without leaving an unhandled rejection while time advances.
  function startStep(): {
    outcome: () => { value?: number; error?: Error } | undefined
    promise: Promise<unknown>
  } {
    let settled: { value?: number; error?: Error } | undefined
    const promise = rpcPodStep(
      POD_IP,
      8765,
      '/__w/_temp/run.sh',
      'token',
      'pod',
      'job'
    ).then(
      value => {
        settled = { value }
      },
      error => {
        settled = { error }
      }
    )
    return { outcome: () => settled, promise }
  }

  beforeEach(() => {
    jest.clearAllMocks()
    jest.useFakeTimers()
    startedAt = Date.now()
    fetchMock = jest.fn()
    global.fetch = fetchMock
    stdoutWrite = jest
      .spyOn(process.stdout, 'write')
      .mockImplementation(() => true)
    jest.spyOn(process.stderr, 'write').mockImplementation(() => true)
    mockGetPodByName.mockResolvedValue({
      status: {
        phase: 'Failed',
        reason: 'Evicted',
        message: 'The node was low on resource: memory.',
        podIP: POD_IP
      }
    })
  })

  afterEach(() => {
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  it('a step that prints nothing for 25 minutes still exits 0', async () => {
    route({ completeAtMs: 25 * MINUTE_MS })
    const step = startStep()

    await jest.advanceTimersByTimeAsync(25 * MINUTE_MS + 5000)
    await step.promise

    expect(step.outcome()).toEqual({ value: 0 })
    // Heartbeats kept flowing the whole time (one every 3s).
    const heartbeats = fetchMock.mock.calls.filter(([u]) =>
      u.includes('/heartbeat')
    )
    expect(heartbeats.length).toBeGreaterThan(400)
    expect(core.error).not.toHaveBeenCalled()
  })

  it('a 90s heartbeat blip recovers and the step still exits 0', async () => {
    route({
      completeAtMs: 5 * MINUTE_MS,
      heartbeatFails: t => t < 90 * 1000
    })
    const step = startStep()

    await jest.advanceTimersByTimeAsync(5 * MINUTE_MS + 5000)
    await step.promise

    expect(step.outcome()).toEqual({ value: 0 })
    expect(core.error).not.toHaveBeenCalled()
  })

  it('fails with a pod diagnosis once the heartbeat has been silent past the grace', async () => {
    route({ completeAtMs: 60 * MINUTE_MS, heartbeatFails: () => true })
    const step = startStep()

    // Just inside the grace: still waiting.
    await jest.advanceTimersByTimeAsync(GRACE_MS - 1000)
    expect(step.outcome()).toBeUndefined()

    // Just past it: the next poll gives up and reports why.
    await jest.advanceTimersByTimeAsync(3000)
    await step.promise

    expect(RPC_HEARTBEAT_GRACE_SECONDS).toBe(120)
    expect(step.outcome()?.error?.message).toContain('Pod was evicted')
    expect(core.error).toHaveBeenCalledWith(
      expect.stringContaining('Pod was evicted')
    )
  })

  it('falls back to a generic message naming the grace when the diagnosis fails', async () => {
    mockGetPodByName.mockRejectedValue(new Error('api unavailable'))
    route({ completeAtMs: 60 * MINUTE_MS, heartbeatFails: () => true })
    const step = startStep()

    await jest.advanceTimersByTimeAsync(GRACE_MS + 3000)
    await step.promise

    expect(step.outcome()?.error?.message).toBe(
      `RPC heartbeat failed for ${RPC_HEARTBEAT_GRACE_SECONDS}s — infrastructure failure`
    )
  })

  describe('idle log poll', () => {
    it('backs off from the minimum to the maximum interval while quiet', async () => {
      const statusTimes: number[] = []
      route({
        completeAtMs: 12 * 1000,
        onStatus: t => statusTimes.push(t)
      })
      const step = startStep()

      await jest.advanceTimersByTimeAsync(15 * 1000)
      await step.promise

      const gaps = statusTimes.slice(1).map((t, i) => t - statusTimes[i])
      expect(gaps.slice(0, 5)).toEqual([
        LOG_POLL_MIN_INTERVAL_MS,
        400,
        800,
        LOG_POLL_MAX_INTERVAL_MS,
        LOG_POLL_MAX_INTERVAL_MS
      ])
      expect(Math.max(...gaps)).toBe(LOG_POLL_MAX_INTERVAL_MS)
    })

    it('resets to the minimum interval when bytes arrive', async () => {
      const statusTimes: number[] = []
      let sent = false
      let bytesAtStatusCount = -1
      route({
        completeAtMs: 15 * 1000,
        onStatus: t => statusTimes.push(t),
        logs: (stream, t) => {
          if (stream === 'stdout' && t >= 5000 && !sent) {
            sent = true
            // The status call for this poll comes right after the log fetch.
            bytesAtStatusCount = statusTimes.length
            return 'hello\n'
          }
          return undefined
        }
      })
      const step = startStep()

      await jest.advanceTimersByTimeAsync(20 * 1000)
      await step.promise

      expect(bytesAtStatusCount).toBeGreaterThan(0)
      const gaps = statusTimes.slice(1).map((t, i) => t - statusTimes[i])
      // gaps[n] is the wait after status call n. The poll that received bytes
      // waits the minimum, the next quiet poll waits the minimum again, and
      // only then does the backoff start doubling.
      expect(gaps[bytesAtStatusCount]).toBe(LOG_POLL_MIN_INTERVAL_MS)
      expect(gaps[bytesAtStatusCount + 1]).toBe(LOG_POLL_MIN_INTERVAL_MS)
      expect(gaps[bytesAtStatusCount + 2]).toBe(400)
      expect(stdoutWrite).toHaveBeenCalledWith(Buffer.from('hello\n'))
    })

    it('still drains the remaining output after the step completes', async () => {
      let drained = false
      route({
        completeAtMs: 3000,
        logs: (stream, t) => {
          // Output that shows up only after the step has finished.
          if (stream === 'stdout' && t >= 3000 && !drained) {
            drained = true
            return 'last words\n'
          }
          return undefined
        }
      })
      const step = startStep()

      await jest.advanceTimersByTimeAsync(10 * 1000)
      await step.promise

      expect(step.outcome()).toEqual({ value: 0 })
      expect(stdoutWrite).toHaveBeenCalledWith(Buffer.from('last words\n'))
    })
  })
})

// ---------------------------------------------------------------------------
// Module mocks — must be declared before importing the module under test
// ---------------------------------------------------------------------------

const MOCK_UUID = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'

const mockExecPodStepOutput = jest.fn()
const mockGetPodByName = jest.fn()

jest.mock('../src/k8s', () => ({
  execPodStepOutput: (...args) => mockExecPodStepOutput(...args),
  getPodByName: (...args) => mockGetPodByName(...args)
}))

jest.mock('@actions/core', () => ({
  debug: jest.fn(),
  info: jest.fn(),
  warning: jest.fn(),
  error: jest.fn()
}))

// Mock sleep to be instant in tests
jest.mock('../src/k8s/utils', () => ({
  sleep: jest.fn().mockResolvedValue(undefined)
}))

// Mock crypto.randomUUID — the property is non-configurable so jest.spyOn
// cannot redefine it. Use a module-level mock with passthrough for other methods.
jest.mock('crypto', () => {
  const actual = jest.requireActual('crypto')
  return {
    ...actual,
    randomUUID: jest.fn().mockReturnValue(MOCK_UUID)
  }
})

import { spawn, spawnSync } from 'child_process'
import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import * as core from '@actions/core'
import {
  buildKillStepGroupCommand,
  deployRpcServer,
  killRpcJob,
  RPC_HEARTBEAT_GRACE_SECONDS,
  RPC_PORT,
  RPC_SERVER_CONTAINER_PATH,
  RpcUnreachableError,
  rpcPodStep
} from '../src/k8s/rpc'

const GRACE_MS = RPC_HEARTBEAT_GRACE_SECONDS * 1000

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/** Await a promise that is expected to reject and return its error. */
async function rejectionOf(promise: Promise<unknown>): Promise<Error> {
  try {
    await promise
  } catch (err) {
    return err as Error
  }
  throw new Error('expected the promise to reject')
}

/** Build a minimal Response-like object that satisfies the fetch API. */
function fakeResponse(
  status: number,
  body?: unknown,
  opts?: { arrayBuffer?: ArrayBuffer }
): Response {
  const bodyStr = body !== undefined ? JSON.stringify(body) : ''
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: `${status}`,
    headers: new Headers({ 'content-type': 'application/json' }),
    text: jest.fn().mockResolvedValue(bodyStr),
    json: jest.fn().mockResolvedValue(body),
    arrayBuffer: jest
      .fn()
      .mockResolvedValue(
        opts?.arrayBuffer ?? new TextEncoder().encode(bodyStr).buffer
      ),
    body: null,
    bodyUsed: false,
    redirected: false,
    type: 'basic' as ResponseType,
    url: '',
    clone: jest.fn()
  } as unknown as Response
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('deployRpcServer', () => {
  type StepResult = { exitCode: number; stdout: string }

  const TOKEN = 'secret-token-123'
  const OOM_LINE = 'rpc-server: oom_score_adj -1000 applied'
  const LISTENING = `RPC server listening on [::]:${RPC_PORT}`

  let realFetch: typeof fetch
  let mockFetch: jest.Mock
  let tmpDir: string
  let imageBinary: string
  let savedEnv: NodeJS.ProcessEnv

  /**
   * Route mocks by command content.
   *   execPodStepOutput: server start (`env ... rpc-server`), pkill cleanup,
   *   log tail. global.fetch: the /health probe.
   */
  function setupDefaultMocks(overrides?: {
    start?: StepResult | Error | ((attempt: number) => StepResult)
    log?: StepResult
    health?: 'ok' | 'fail-once-ok' | 'always-fail' | 'network-error'
  }): void {
    const cfg = {
      start: {
        exitCode: 0,
        stdout: `${OOM_LINE}\n${LISTENING}`
      } as StepResult | Error | ((attempt: number) => StepResult),
      log: { exitCode: 0, stdout: '(no log)' },
      health: 'ok' as 'ok' | 'fail-once-ok' | 'always-fail' | 'network-error',
      ...overrides
    }

    let startCalls = 0
    mockExecPodStepOutput.mockImplementation(async (cmd: string[]) => {
      if (cmd[0] === 'env') {
        startCalls++
        if (cfg.start instanceof Error) throw cfg.start
        if (typeof cfg.start === 'function') return cfg.start(startCalls)
        return cfg.start
      }
      return cfg.log
    })

    let healthCalls = 0
    mockFetch.mockImplementation(async () => {
      healthCalls++
      switch (cfg.health) {
        case 'ok':
          return fakeResponse(200, { status: 'ok' })
        case 'fail-once-ok':
          return healthCalls === 1
            ? fakeResponse(500)
            : fakeResponse(200, { status: 'ok' })
        case 'always-fail':
          return fakeResponse(500)
        case 'network-error':
          throw new Error('fetch failed')
      }
    })
  }

  // Health polling waits on the wall clock; jump it forward on every probe so
  // a server that never becomes healthy times out instantly.
  function fastForwardClockOnEachProbe(): void {
    const startTime = Date.now()
    let offset = 0
    jest.spyOn(Date, 'now').mockImplementation(() => startTime + offset)
    mockFetch.mockImplementation(async () => {
      offset += 16000
      return fakeResponse(500)
    })
  }

  beforeEach(() => {
    jest.clearAllMocks()
    savedEnv = { ...process.env }

    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'rpc-deploy-'))
    imageBinary = path.join(tmpDir, 'image-rpc-server')
    fs.writeFileSync(imageBinary, 'fake-binary', { mode: 0o600 })
    fs.mkdirSync(path.join(tmpDir, '_temp'))
    process.env.RUNNER_TEMP = path.join(tmpDir, '_temp')
    process.env.ACTIONS_RUNNER_RPC_SERVER_PATH = imageBinary

    realFetch = global.fetch
    mockFetch = jest.fn()
    global.fetch = mockFetch as unknown as typeof fetch

    mockGetPodByName.mockResolvedValue({ status: { podIP: '10.0.0.1' } })
    setupDefaultMocks()
  })

  afterEach(() => {
    global.fetch = realFetch
    process.env = savedEnv
    fs.rmSync(tmpDir, { recursive: true, force: true })
    jest.restoreAllMocks()
  })

  function startCommands(): string[][] {
    return mockExecPodStepOutput.mock.calls
      .map(([cmd]) => cmd as string[])
      .filter(cmd => cmd[0] === 'env')
  }

  it('copies the binary from the image to RUNNER_TEMP, executable', async () => {
    await deployRpcServer('my-pod', 'my-container', TOKEN)

    const copied = path.join(process.env.RUNNER_TEMP as string, 'rpc-server')
    expect(fs.readFileSync(copied, 'utf8')).toBe('fake-binary')
    expect(fs.statSync(copied).mode & 0o777).toBe(0o755)
  })

  it('uses the default image path when ACTIONS_RUNNER_RPC_SERVER_PATH is unset', async () => {
    delete process.env.ACTIONS_RUNNER_RPC_SERVER_PATH
    await expect(
      deployRpcServer('my-pod', 'my-container', TOKEN)
    ).rejects.toThrow('/home/runner/k8s/rpc-server')
  })

  it('throws when the binary is missing from the image, without starting anything', async () => {
    process.env.ACTIONS_RUNNER_RPC_SERVER_PATH = path.join(tmpDir, 'absent')

    await expect(
      deployRpcServer('my-pod', 'my-container', TOKEN)
    ).rejects.toThrow('RPC server binary not found')
    expect(mockExecPodStepOutput).not.toHaveBeenCalled()
  })

  it('throws if the pod has no IP address', async () => {
    mockGetPodByName.mockResolvedValue({ status: {} })

    await expect(
      deployRpcServer('my-pod', 'my-container', TOKEN)
    ).rejects.toThrow('has no IP address')
  })

  it('starts the copied binary on the fixed port with --daemonize and the token', async () => {
    await deployRpcServer('my-pod', 'my-container', TOKEN)

    const [cmd] = startCommands()
    expect(cmd.slice(2)).toEqual([
      RPC_SERVER_CONTAINER_PATH,
      '--port',
      String(RPC_PORT),
      '--token',
      TOKEN,
      '--daemonize'
    ])
    expect(RPC_PORT).toBe(8765)
    expect(RPC_SERVER_CONTAINER_PATH).toBe('/__w/_temp/rpc-server')
  })

  it('passes the shared grace constant to the server as its watchdog timeout', async () => {
    await deployRpcServer('my-pod', 'my-container', TOKEN)

    const [cmd] = startCommands()
    expect(cmd[1]).toBe(
      `RPC_HEARTBEAT_TIMEOUT_SECS=${RPC_HEARTBEAT_GRACE_SECONDS}`
    )
    expect(RPC_HEARTBEAT_GRACE_SECONDS).toBe(120)
  })

  it('returns the pod IP and fixed port once /health answers', async () => {
    setupDefaultMocks({ health: 'fail-once-ok' })

    const result = await deployRpcServer('my-pod', 'my-container', TOKEN)

    expect(result).toEqual({ podIp: '10.0.0.1', port: RPC_PORT })
    expect(mockFetch.mock.calls[0][0]).toBe(
      `http://10.0.0.1:${RPC_PORT}/health`
    )
  })

  it('reports the OOM protection line from the server start output', async () => {
    await deployRpcServer('my-pod', 'my-container', TOKEN)

    expect(core.info).toHaveBeenCalledWith(OOM_LINE)
  })

  it('never logs the token or the start command', async () => {
    setupDefaultMocks({ start: { exitCode: 1, stdout: 'boom' } })
    fastForwardClockOnEachProbe()
    await deployRpcServer('my-pod', 'my-container', TOKEN).catch(() => {})
    setupDefaultMocks({ health: 'always-fail' })
    fastForwardClockOnEachProbe()
    await deployRpcServer('my-pod', 'my-container', TOKEN).catch(() => {})

    const logged = [
      ...(core.debug as jest.Mock).mock.calls,
      ...(core.info as jest.Mock).mock.calls,
      ...(core.warning as jest.Mock).mock.calls,
      ...(core.error as jest.Mock).mock.calls
    ]
      .map(args => String(args[0]))
      .join('\n')
    expect(logged).not.toContain(TOKEN)
    expect(logged).not.toContain('--daemonize')
  })

  it('retries the start when the output has no listening line', async () => {
    setupDefaultMocks({
      start: attempt =>
        attempt === 1
          ? { exitCode: 0, stdout: '' }
          : { exitCode: 0, stdout: LISTENING }
    })

    const result = await deployRpcServer('my-pod', 'my-container', TOKEN)

    expect(result.port).toBe(RPC_PORT)
    expect(startCommands()).toHaveLength(2)
  })

  it('cleans up a leftover server before a retry, without matching its own shell', async () => {
    setupDefaultMocks({
      start: attempt =>
        attempt === 1
          ? { exitCode: 1, stdout: 'address in use' }
          : { exitCode: 0, stdout: LISTENING }
    })

    await deployRpcServer('my-pod', 'my-container', TOKEN)

    const cleanup = mockExecPodStepOutput.mock.calls
      .map(([cmd]) => (cmd as string[]).join(' '))
      .find(cmd => cmd.includes('pkill'))
    expect(cleanup).toBeDefined()
    // The `[_]` keeps the pattern from matching the sh -c command line itself.
    expect(cleanup).toContain('[_]temp/rpc-server')
  })

  it('throws after three failed starts, with per-attempt diagnostics', async () => {
    setupDefaultMocks({ start: new Error('exec denied') })

    const err = await rejectionOf(
      deployRpcServer('my-pod', 'my-container', TOKEN)
    )

    expect(err.message).toContain('RPC server failed after 3 attempts')
    expect(err.message).toContain('attempt 1:')
    expect(err.message).toContain('attempt 3:')
    expect(err.message).toContain('exec denied')
    expect(startCommands()).toHaveLength(3)
  })

  it('throws when the health check never passes, including the server log', async () => {
    setupDefaultMocks({
      log: { exitCode: 0, stdout: 'bind: permission denied' },
      health: 'always-fail'
    })
    fastForwardClockOnEachProbe()

    const err = await rejectionOf(
      deployRpcServer('my-pod', 'my-container', TOKEN)
    )

    expect(err.message).toContain('RPC server failed after 3 attempts')
    expect(err.message).toContain('health check timed out')
    expect(err.message).toContain('bind: permission denied')
    expect(startCommands()).toHaveLength(3)
  })

  it('treats health check network errors as a failed health check', async () => {
    setupDefaultMocks({ health: 'network-error' })
    const startTime = Date.now()
    let offset = 0
    jest.spyOn(Date, 'now').mockImplementation(() => startTime + offset)
    mockFetch.mockImplementation(async () => {
      offset += 16000
      throw new Error('fetch failed')
    })

    await expect(
      deployRpcServer('my-pod', 'my-container', TOKEN)
    ).rejects.toThrow('RPC server failed after 3 attempts')
  })
})

describe('rpcPodStep', () => {
  const POD_IP = '10.0.0.1'
  const PORT = 8080
  const SCRIPT_PATH = '/__w/_temp/run.sh'
  const TOKEN = 'test-token-abc'
  const POD_NAME = 'test-pod'
  const CONTAINER_NAME = 'job'

  let fetchMock: jest.Mock
  let stdoutWriteSpy: jest.SpyInstance
  let stderrWriteSpy: jest.SpyInstance

  beforeEach(() => {
    jest.clearAllMocks()
    fetchMock = jest.fn()
    global.fetch = fetchMock

    // Default: getPodByName returns a pod in Unknown state (for diagnostics)
    mockGetPodByName.mockResolvedValue({
      status: { phase: 'Running', podIP: POD_IP }
    })

    // Suppress actual stdout/stderr writes during tests
    stdoutWriteSpy = jest
      .spyOn(process.stdout, 'write')
      .mockImplementation(() => true)
    stderrWriteSpy = jest
      .spyOn(process.stderr, 'write')
      .mockImplementation(() => true)
  })

  afterEach(() => {
    jest.restoreAllMocks()
  })

  /**
   * Helper: set up fetchMock to handle the standard /exec -> /logs -> /status
   * flow in a single pass. Returns the exit code provided.
   */
  function setupSimpleExecFlow(exitCode: number, status = 'completed'): void {
    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()

      // /exec POST
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return Promise.resolve(
          fakeResponse(200, { id: MOCK_UUID, status: 'running' })
        )
      }

      // /logs GET
      if (urlStr.includes('/logs')) {
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }

      // /status GET
      if (urlStr.includes('/status')) {
        return Promise.resolve(
          fakeResponse(200, { id: MOCK_UUID, status, exit_code: exitCode })
        )
      }

      // /heartbeat POST
      if (urlStr.includes('/heartbeat')) {
        return Promise.resolve(fakeResponse(200, { status: 'ok' }))
      }

      return Promise.reject(new Error(`Unexpected fetch: ${urlStr}`))
    })
  }

  it('should send POST /exec with correct body and auth token', async () => {
    setupSimpleExecFlow(0)

    await rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)

    // Find the /exec call
    const execCall = fetchMock.mock.calls.find(
      ([url, opts]) => url.includes('/exec') && opts?.method === 'POST'
    )
    expect(execCall).toBeDefined()

    const [url, opts] = execCall
    expect(url).toBe(`http://${POD_IP}:${PORT}/exec`)
    expect(opts.headers['X-Auth-Token']).toBe(TOKEN)
    expect(opts.headers['Content-Type']).toBe('application/json')

    const body = JSON.parse(opts.body)
    expect(body.id).toBe(MOCK_UUID)
    expect(body.path).toBe(SCRIPT_PATH)
  })

  it('should poll /status until completed and return exit code 0', async () => {
    setupSimpleExecFlow(0, 'completed')

    const exitCode = await rpcPodStep(
      POD_IP,
      PORT,
      SCRIPT_PATH,
      TOKEN,
      POD_NAME,
      CONTAINER_NAME
    )

    expect(exitCode).toBe(0)
  })

  it('should return exit code on failure (non-zero)', async () => {
    setupSimpleExecFlow(1, 'failed')

    const exitCode = await rpcPodStep(
      POD_IP,
      PORT,
      SCRIPT_PATH,
      TOKEN,
      POD_NAME,
      CONTAINER_NAME
    )

    expect(exitCode).toBe(1)
  })

  it('should return exit code 137 for killed process', async () => {
    setupSimpleExecFlow(137, 'failed')

    const exitCode = await rpcPodStep(
      POD_IP,
      PORT,
      SCRIPT_PATH,
      TOKEN,
      POD_NAME,
      CONTAINER_NAME
    )

    expect(exitCode).toBe(137)
  })

  it('should return -1 when exit_code is null', async () => {
    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }
      if (urlStr.includes('/logs')) {
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        return Promise.resolve(
          fakeResponse(200, {
            id: 'test',
            status: 'completed',
            exit_code: null
          })
        )
      }
      if (urlStr.includes('/heartbeat')) {
        return Promise.resolve(fakeResponse(200, { status: 'ok' }))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    const exitCode = await rpcPodStep(
      POD_IP,
      PORT,
      SCRIPT_PATH,
      TOKEN,
      POD_NAME,
      CONTAINER_NAME
    )

    expect(exitCode).toBe(-1)
  })

  it('should stream stdout via GET /logs?stream=stdout', async () => {
    const stdoutData = new TextEncoder().encode('hello stdout\n')
    let statusCallCount = 0

    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }
      if (urlStr.includes('/logs') && urlStr.includes('stream=stdout')) {
        // Return data on first call, empty on subsequent
        if (statusCallCount === 0) {
          return Promise.resolve(
            fakeResponse(200, null, {
              arrayBuffer: stdoutData.buffer.slice(
                stdoutData.byteOffset,
                stdoutData.byteOffset + stdoutData.byteLength
              )
            })
          )
        }
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/logs') && urlStr.includes('stream=stderr')) {
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        statusCallCount++
        if (statusCallCount >= 2) {
          return Promise.resolve(
            fakeResponse(200, { status: 'completed', exit_code: 0 })
          )
        }
        return Promise.resolve(
          fakeResponse(200, { status: 'running', exit_code: null })
        )
      }
      if (urlStr.includes('/heartbeat')) {
        return Promise.resolve(fakeResponse(200, { status: 'ok' }))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    await rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)

    // Verify stdout was fetched with correct query params
    const stdoutCalls = fetchMock.mock.calls.filter(
      ([url]) =>
        url.includes('/logs') &&
        url.includes('stream=stdout') &&
        url.includes('offset=')
    )
    expect(stdoutCalls.length).toBeGreaterThanOrEqual(1)

    // Verify process.stdout.write was called with the log data
    expect(stdoutWriteSpy).toHaveBeenCalled()
  })

  it('should stream stderr via GET /logs?stream=stderr', async () => {
    const stderrData = new TextEncoder().encode('error output\n')
    let statusCallCount = 0

    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }
      if (urlStr.includes('/logs') && urlStr.includes('stream=stdout')) {
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/logs') && urlStr.includes('stream=stderr')) {
        if (statusCallCount === 0) {
          return Promise.resolve(
            fakeResponse(200, null, {
              arrayBuffer: stderrData.buffer.slice(
                stderrData.byteOffset,
                stderrData.byteOffset + stderrData.byteLength
              )
            })
          )
        }
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        statusCallCount++
        if (statusCallCount >= 2) {
          return Promise.resolve(
            fakeResponse(200, { status: 'completed', exit_code: 0 })
          )
        }
        return Promise.resolve(
          fakeResponse(200, { status: 'running', exit_code: null })
        )
      }
      if (urlStr.includes('/heartbeat')) {
        return Promise.resolve(fakeResponse(200, { status: 'ok' }))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    await rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)

    const stderrCalls = fetchMock.mock.calls.filter(
      ([url]) => url.includes('/logs') && url.includes('stream=stderr')
    )
    expect(stderrCalls.length).toBeGreaterThanOrEqual(1)

    expect(stderrWriteSpy).toHaveBeenCalled()
  })

  it('should track offset correctly with large log responses', async () => {
    const chunk1 = new TextEncoder().encode('first chunk data\n')
    const chunk2 = new TextEncoder().encode('second chunk data\n')
    let stdoutCallIdx = 0
    let statusCallCount = 0

    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }
      if (urlStr.includes('/logs') && urlStr.includes('stream=stdout')) {
        stdoutCallIdx++
        if (stdoutCallIdx === 1) {
          // First call: offset=0, return chunk1
          expect(urlStr).toContain('offset=0')
          return Promise.resolve(
            fakeResponse(200, null, {
              arrayBuffer: chunk1.buffer.slice(
                chunk1.byteOffset,
                chunk1.byteOffset + chunk1.byteLength
              )
            })
          )
        } else if (stdoutCallIdx === 2) {
          // Second call: offset should be chunk1.byteLength
          expect(urlStr).toContain(`offset=${chunk1.byteLength}`)
          return Promise.resolve(
            fakeResponse(200, null, {
              arrayBuffer: chunk2.buffer.slice(
                chunk2.byteOffset,
                chunk2.byteOffset + chunk2.byteLength
              )
            })
          )
        }
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/logs') && urlStr.includes('stream=stderr')) {
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        statusCallCount++
        if (statusCallCount >= 3) {
          return Promise.resolve(
            fakeResponse(200, { status: 'completed', exit_code: 0 })
          )
        }
        return Promise.resolve(
          fakeResponse(200, { status: 'running', exit_code: null })
        )
      }
      if (urlStr.includes('/heartbeat')) {
        return Promise.resolve(fakeResponse(200, { status: 'ok' }))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    const exitCode = await rpcPodStep(
      POD_IP,
      PORT,
      SCRIPT_PATH,
      TOKEN,
      POD_NAME,
      CONTAINER_NAME
    )

    expect(exitCode).toBe(0)
    // stdout was written at least twice (chunk1 and chunk2)
    expect(stdoutWriteSpy.mock.calls.length).toBeGreaterThanOrEqual(2)
  })

  it('should handle empty log responses', async () => {
    setupSimpleExecFlow(0)

    const exitCode = await rpcPodStep(
      POD_IP,
      PORT,
      SCRIPT_PATH,
      TOKEN,
      POD_NAME,
      CONTAINER_NAME
    )

    expect(exitCode).toBe(0)
  })

  it('should retry /exec on network error up to 3 times then throw', async () => {
    const netErr = new Error('ECONNREFUSED')
    fetchMock
      .mockRejectedValueOnce(netErr)
      .mockRejectedValueOnce(netErr)
      .mockRejectedValueOnce(netErr)

    await expect(
      rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
    ).rejects.toThrow('RPC /exec network error after 3 attempts')
  })

  it('should succeed if /exec network error recovers on retry', async () => {
    const netErr = new Error('ECONNREFUSED')

    let execCallCount = 0
    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        // Fail first, succeed second
        execCallCount++
        if (execCallCount === 1) {
          return Promise.reject(netErr)
        }
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }
      if (urlStr.includes('/logs')) {
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        return Promise.resolve(
          fakeResponse(200, { status: 'completed', exit_code: 0 })
        )
      }
      if (urlStr.includes('/heartbeat')) {
        return Promise.resolve(fakeResponse(200, { status: 'ok' }))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    const exitCode = await rpcPodStep(
      POD_IP,
      PORT,
      SCRIPT_PATH,
      TOKEN,
      POD_NAME,
      CONTAINER_NAME
    )

    expect(exitCode).toBe(0)
  })

  it('should retry /exec on 5xx up to 3 times then throw', async () => {
    fetchMock
      .mockResolvedValueOnce(fakeResponse(500, 'internal error'))
      .mockResolvedValueOnce(fakeResponse(502, 'bad gateway'))
      .mockResolvedValueOnce(fakeResponse(503, 'service unavailable'))

    await expect(
      rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
    ).rejects.toThrow('RPC /exec failed after 3 attempts')
  })

  it('should succeed if /exec 5xx recovers on retry', async () => {
    let execCallCount = 0
    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        execCallCount++
        if (execCallCount === 1) {
          return Promise.resolve(fakeResponse(500, 'error'))
        }
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }
      if (urlStr.includes('/logs')) {
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        return Promise.resolve(
          fakeResponse(200, { status: 'completed', exit_code: 0 })
        )
      }
      if (urlStr.includes('/heartbeat')) {
        return Promise.resolve(fakeResponse(200, { status: 'ok' }))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    const exitCode = await rpcPodStep(
      POD_IP,
      PORT,
      SCRIPT_PATH,
      TOKEN,
      POD_NAME,
      CONTAINER_NAME
    )

    expect(exitCode).toBe(0)
  })

  it('should throw immediately on /exec 4xx without retry', async () => {
    fetchMock.mockResolvedValueOnce(fakeResponse(400, 'bad request'))

    await expect(
      rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
    ).rejects.toThrow('RPC /exec failed (400)')

    // Only one /exec call should have been made (no retries)
    const execCalls = fetchMock.mock.calls.filter(
      ([url, opts]) => url.includes('/exec') && opts?.method === 'POST'
    )
    expect(execCalls).toHaveLength(1)
  })

  it('should throw immediately on /exec 403', async () => {
    fetchMock.mockResolvedValueOnce(fakeResponse(403, 'forbidden'))

    await expect(
      rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
    ).rejects.toThrow('RPC /exec failed (403)')
  })

  it('should surface cancellation-state context on /exec 409 "A job is already running"', async () => {
    // 409 with the canonical body → hook fetches /status for diagnostic
    // context, then throws a cancellation-aware error.
    fetchMock
      .mockResolvedValueOnce(
        fakeResponse(409, { error: 'A job is already running' })
      )
      .mockResolvedValueOnce(
        fakeResponse(200, {
          id: 'prior-uuid',
          status: 'running',
          exit_code: null
        })
      )

    await expect(
      rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
    ).rejects.toThrow(
      /Step cannot start: workflow pod still has a prior step in flight[\s\S]*id=prior-uuid[\s\S]*status=running[\s\S]*cancels a step/
    )
  })

  it('should fall through to generic 409 message when body does not match', async () => {
    fetchMock.mockResolvedValueOnce(fakeResponse(409, 'some other 409 reason'))

    await expect(
      rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
    ).rejects.toThrow('RPC /exec failed (409)')
  })

  it('should send heartbeats during execution', async () => {
    // setInterval fires heartbeats every 3000ms in the source code.
    // We need the main polling loop to stay alive long enough for a heartbeat
    // to fire. We use fake timers and make sleep return a real timer-based
    // promise so that advanceTimersByTimeAsync progresses both setInterval
    // and sleep.
    jest.useFakeTimers()

    // eslint-disable-next-line @typescript-eslint/no-require-imports, @typescript-eslint/no-var-requires
    const { sleep: mockSleep } = require('../src/k8s/utils') as {
      sleep: jest.Mock
    }
    mockSleep.mockImplementation(
      async (ms: number) => new Promise(r => setTimeout(r, ms))
    )

    let statusCallCount = 0

    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }
      if (urlStr.includes('/logs')) {
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        statusCallCount++
        // Keep running for several polls so heartbeat interval fires
        if (statusCallCount >= 20) {
          return Promise.resolve(
            fakeResponse(200, { status: 'completed', exit_code: 0 })
          )
        }
        return Promise.resolve(
          fakeResponse(200, { status: 'running', exit_code: null })
        )
      }
      if (urlStr.includes('/heartbeat') && init?.method === 'POST') {
        return Promise.resolve(fakeResponse(200, { status: 'ok' }))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    const promise = rpcPodStep(
      POD_IP,
      PORT,
      SCRIPT_PATH,
      TOKEN,
      POD_NAME,
      CONTAINER_NAME
    )

    // Advance fake clock in small steps to let both sleep and setInterval fire.
    // The idle poll backs off 200, 400, 800, then 1000ms, so 20 idle polls take
    // about 18.4s; HEARTBEAT_INTERVAL_MS = 3000 fires several times within it.
    for (let i = 0; i < 100; i++) {
      await jest.advanceTimersByTimeAsync(200)
    }

    const exitCode = await promise

    expect(exitCode).toBe(0)

    // Check that heartbeat calls were made
    const heartbeatCalls = fetchMock.mock.calls.filter(
      ([url, opts]) => url.includes('/heartbeat') && opts?.method === 'POST'
    )
    expect(heartbeatCalls.length).toBeGreaterThanOrEqual(1)

    // Verify heartbeat uses correct auth header
    expect(heartbeatCalls[0][1].headers['X-Auth-Token']).toBe(TOKEN)

    // Restore sleep mock to instant for other tests
    mockSleep.mockResolvedValue(undefined)
    jest.useRealTimers()
  })

  it('should clear heartbeat interval after completion', async () => {
    jest.useFakeTimers()

    setupSimpleExecFlow(0)

    const clearIntervalSpy = jest.spyOn(global, 'clearInterval')

    const promise = rpcPodStep(
      POD_IP,
      PORT,
      SCRIPT_PATH,
      TOKEN,
      POD_NAME,
      CONTAINER_NAME
    )
    await jest.advanceTimersByTimeAsync(500)
    await promise

    expect(clearIntervalSpy).toHaveBeenCalled()

    jest.useRealTimers()
  })

  it('should throw on heartbeat timeout with OOM diagnostic', async () => {
    const realDateNow = Date.now
    const startTime = realDateNow()
    let timeOffset = 0

    jest.spyOn(Date, 'now').mockImplementation(() => startTime + timeOffset)

    // Pod shows OOMKilled container
    mockGetPodByName.mockResolvedValue({
      status: {
        phase: 'Failed',
        podIP: POD_IP,
        containerStatuses: [
          {
            name: CONTAINER_NAME,
            state: {
              terminated: { reason: 'OOMKilled', exitCode: 137 }
            }
          }
        ]
      }
    })

    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }
      if (urlStr.includes('/logs')) {
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        timeOffset += GRACE_MS + 1000
        return Promise.resolve(
          fakeResponse(200, { status: 'running', exit_code: null })
        )
      }
      if (urlStr.includes('/heartbeat')) {
        return Promise.reject(new Error('connection refused'))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    await expect(
      rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
    ).rejects.toThrow('OOMKilled')
  })

  it('should throw on heartbeat timeout with eviction diagnostic', async () => {
    const realDateNow = Date.now
    const startTime = realDateNow()
    let timeOffset = 0

    jest.spyOn(Date, 'now').mockImplementation(() => startTime + timeOffset)

    mockGetPodByName.mockResolvedValue({
      status: {
        phase: 'Failed',
        reason: 'Evicted',
        message: 'The node was low on resource: memory.',
        podIP: POD_IP
      }
    })

    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }
      if (urlStr.includes('/logs')) {
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        timeOffset += GRACE_MS + 1000
        return Promise.resolve(
          fakeResponse(200, { status: 'running', exit_code: null })
        )
      }
      if (urlStr.includes('/heartbeat')) {
        return Promise.reject(new Error('connection refused'))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    await expect(
      rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
    ).rejects.toThrow('Pod was evicted')
  })

  it('should throw on heartbeat timeout with node-failure diagnostic', async () => {
    const realDateNow = Date.now
    const startTime = realDateNow()
    let timeOffset = 0

    jest.spyOn(Date, 'now').mockImplementation(() => startTime + timeOffset)

    mockGetPodByName.mockResolvedValue({
      status: {
        phase: 'Unknown',
        message: 'Node lost contact',
        podIP: POD_IP
      }
    })

    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }
      if (urlStr.includes('/logs')) {
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        timeOffset += GRACE_MS + 1000
        return Promise.resolve(
          fakeResponse(200, { status: 'running', exit_code: null })
        )
      }
      if (urlStr.includes('/heartbeat')) {
        return Promise.reject(new Error('connection refused'))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    await expect(
      rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
    ).rejects.toThrow('possible node failure')
  })

  it('should fall back to generic message when diagnostics fail', async () => {
    const realDateNow = Date.now
    const startTime = realDateNow()
    let timeOffset = 0

    jest.spyOn(Date, 'now').mockImplementation(() => startTime + timeOffset)

    // getPodByName throws — K8s API unreachable
    mockGetPodByName.mockRejectedValue(new Error('API server unreachable'))

    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }
      if (urlStr.includes('/logs')) {
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        timeOffset += GRACE_MS + 1000
        return Promise.resolve(
          fakeResponse(200, { status: 'running', exit_code: null })
        )
      }
      if (urlStr.includes('/heartbeat')) {
        return Promise.reject(new Error('connection refused'))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    await expect(
      rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
    ).rejects.toThrow('RPC heartbeat failed for 120s')
  })

  it('should detect RPC process death when container is still running', async () => {
    const realDateNow = Date.now
    const startTime = realDateNow()
    let timeOffset = 0

    jest.spyOn(Date, 'now').mockImplementation(() => startTime + timeOffset)

    // Pod is still Running but RPC server died
    mockGetPodByName.mockResolvedValue({
      status: {
        phase: 'Running',
        podIP: POD_IP,
        containerStatuses: [
          {
            name: CONTAINER_NAME,
            state: { running: { startedAt: '2025-01-01T00:00:00Z' } }
          }
        ]
      }
    })

    // execPodStepOutput returns the server log
    mockExecPodStepOutput.mockResolvedValue({
      exitCode: 0,
      stdout: 'Killed'
    })

    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }
      if (urlStr.includes('/logs')) {
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        timeOffset += GRACE_MS + 1000
        return Promise.resolve(
          fakeResponse(200, { status: 'running', exit_code: null })
        )
      }
      if (urlStr.includes('/heartbeat')) {
        return Promise.reject(new Error('connection refused'))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    await expect(
      rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
    ).rejects.toThrow('RPC server process died')
  })

  describe('when the server dies mid-step', () => {
    // Heartbeats fail and the clock jumps past the grace on the first /status.
    function setupDeadServer(podStatus: unknown): void {
      const startTime = Date.now()
      let timeOffset = 0
      jest.spyOn(Date, 'now').mockImplementation(() => startTime + timeOffset)
      mockGetPodByName.mockResolvedValue({ status: podStatus })
      mockExecPodStepOutput.mockResolvedValue({ exitCode: 0, stdout: 'Killed' })
      fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
        const urlStr = url.toString()
        if (urlStr.includes('/exec') && init?.method === 'POST') {
          return fakeResponse(200, { status: 'running' })
        }
        if (urlStr.includes('/logs')) {
          return fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        }
        if (urlStr.includes('/status')) {
          timeOffset += GRACE_MS + 1000
          return fakeResponse(200, { status: 'running', exit_code: null })
        }
        throw new Error('connection refused')
      })
    }

    function killGroupCalls(): string[][] {
      return mockExecPodStepOutput.mock.calls
        .map(([cmd]) => cmd as string[])
        .filter(cmd => cmd.join(' ').includes('job.pgid'))
    }

    it('kills the step process group once, then fails with the diagnosis', async () => {
      setupDeadServer({
        phase: 'Running',
        podIP: POD_IP,
        containerStatuses: [
          {
            name: CONTAINER_NAME,
            state: { running: { startedAt: '2025-01-01T00:00:00Z' } }
          }
        ]
      })

      await expect(
        rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
      ).rejects.toThrow('RPC server process died')

      const calls = killGroupCalls()
      expect(calls).toHaveLength(1)
      expect(calls[0].slice(0, 2)).toEqual(['sh', '-c'])
      expect(calls[0][2]).toBe(buildKillStepGroupCommand())
      expect(calls[0][2]).toContain('/tmp/rpc-logs/job.pgid')
      // Targets the job container, with a short timeout.
      const call = mockExecPodStepOutput.mock.calls.find(([cmd]) =>
        (cmd as string[]).join(' ').includes('job.pgid')
      ) as unknown[]
      expect(call[1]).toBe(POD_NAME)
      expect(call[2]).toBe(CONTAINER_NAME)
      expect(call[3]).toBeLessThanOrEqual(15000)
    })

    it('still fails the step when the kill exec itself fails', async () => {
      setupDeadServer({
        phase: 'Running',
        podIP: POD_IP,
        containerStatuses: [
          { name: CONTAINER_NAME, state: { running: { startedAt: 'x' } } }
        ]
      })
      mockExecPodStepOutput.mockImplementation(async (cmd: string[]) => {
        if (cmd.join(' ').includes('job.pgid')) throw new Error('exec failed')
        return { exitCode: 0, stdout: 'Killed' }
      })

      await expect(
        rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
      ).rejects.toThrow('RPC server process died')
    })

    it('does not run the kill exec for other causes such as OOM', async () => {
      setupDeadServer({
        phase: 'Failed',
        podIP: POD_IP,
        containerStatuses: [
          {
            name: CONTAINER_NAME,
            state: { terminated: { reason: 'OOMKilled', exitCode: 137 } }
          }
        ]
      })

      await expect(
        rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
      ).rejects.toThrow('OOMKilled')
      expect(killGroupCalls()).toHaveLength(0)
    })
  })

  describe('/exec reachability', () => {
    const refused = (): Error =>
      Object.assign(new TypeError('fetch failed'), {
        cause: Object.assign(new Error('connect ECONNREFUSED'), {
          code: 'ECONNREFUSED'
        })
      })
    const timedOut = (): Error =>
      Object.assign(new TypeError('fetch failed'), {
        cause: Object.assign(new Error('connect timeout'), {
          code: 'UND_ERR_CONNECT_TIMEOUT'
        })
      })

    it('throws RpcUnreachableError when every attempt fails to connect', async () => {
      fetchMock
        .mockRejectedValueOnce(refused())
        .mockRejectedValueOnce(timedOut())
        .mockRejectedValueOnce(refused())

      const err = await rejectionOf(
        rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
      )

      expect(err).toBeInstanceOf(RpcUnreachableError)
      expect(err.message).toContain('RPC /exec network error after 3 attempts')
      expect(fetchMock).toHaveBeenCalledTimes(3)
    })

    it('treats an AggregateError of refused addresses as unreachable', async () => {
      const aggregate = Object.assign(new TypeError('fetch failed'), {
        cause: Object.assign(new Error('all addresses failed'), {
          errors: [{ code: 'ECONNREFUSED' }, { code: 'ENETUNREACH' }]
        })
      })
      fetchMock.mockRejectedValue(aggregate)

      await expect(
        rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
      ).rejects.toBeInstanceOf(RpcUnreachableError)
    })

    it('does not fall back when an error could have come after the request was sent', async () => {
      const reset = Object.assign(new TypeError('fetch failed'), {
        cause: Object.assign(new Error('read ECONNRESET'), {
          code: 'ECONNRESET'
        })
      })
      fetchMock
        .mockRejectedValueOnce(refused())
        .mockRejectedValueOnce(reset)
        .mockRejectedValueOnce(refused())

      const err = await rejectionOf(
        rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
      )

      expect(err).not.toBeInstanceOf(RpcUnreachableError)
      expect(err.message).toContain('RPC /exec network error')
    })

    it('does not fall back on a response timeout (the server may have the step)', async () => {
      const timeout = new DOMException(
        'The operation timed out',
        'TimeoutError'
      )
      fetchMock.mockRejectedValue(timeout)

      const err = await rejectionOf(
        rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
      )

      expect(err).not.toBeInstanceOf(RpcUnreachableError)
    })

    it('does not fall back once the server answered, even if later attempts cannot connect', async () => {
      fetchMock
        .mockResolvedValueOnce(fakeResponse(503, 'busy'))
        .mockRejectedValueOnce(refused())
        .mockRejectedValueOnce(refused())

      const err = await rejectionOf(
        rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
      )

      expect(err).not.toBeInstanceOf(RpcUnreachableError)
    })

    it('does not fall back after the server accepted the step and then the log poll fails', async () => {
      const startTime = Date.now()
      let timeOffset = 0
      jest.spyOn(Date, 'now').mockImplementation(() => startTime + timeOffset)
      fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
        const urlStr = url.toString()
        if (urlStr.includes('/exec') && init?.method === 'POST') {
          return fakeResponse(200, { status: 'running' })
        }
        // Everything after acceptance cannot connect.
        timeOffset += GRACE_MS + 1000
        throw refused()
      })

      const err = await rejectionOf(
        rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
      )

      expect(err).not.toBeInstanceOf(RpcUnreachableError)
      expect(
        fetchMock.mock.calls.filter(([u]) => u.includes('/exec'))
      ).toHaveLength(1)
    })
  })

  it('should handle /status fetch failures gracefully (retry)', async () => {
    let statusCallCount = 0

    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }
      if (urlStr.includes('/logs')) {
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        statusCallCount++
        if (statusCallCount === 1) {
          // First status call fails
          return Promise.reject(new Error('timeout'))
        }
        // Second call succeeds with completion
        return Promise.resolve(
          fakeResponse(200, { status: 'completed', exit_code: 0 })
        )
      }
      if (urlStr.includes('/heartbeat')) {
        return Promise.resolve(fakeResponse(200, { status: 'ok' }))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    const exitCode = await rpcPodStep(
      POD_IP,
      PORT,
      SCRIPT_PATH,
      TOKEN,
      POD_NAME,
      CONTAINER_NAME
    )

    expect(exitCode).toBe(0)
    expect(statusCallCount).toBe(2)
  })

  it('should handle /logs fetch failures gracefully (continue polling)', async () => {
    let logsCallCount = 0

    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }
      if (urlStr.includes('/logs')) {
        logsCallCount++
        if (logsCallCount <= 2) {
          // First two log fetches fail
          return Promise.reject(new Error('network error'))
        }
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        if (logsCallCount >= 3) {
          return Promise.resolve(
            fakeResponse(200, { status: 'completed', exit_code: 0 })
          )
        }
        return Promise.resolve(
          fakeResponse(200, { status: 'running', exit_code: null })
        )
      }
      if (urlStr.includes('/heartbeat')) {
        return Promise.resolve(fakeResponse(200, { status: 'ok' }))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    // Should complete despite log fetch failures
    const exitCode = await rpcPodStep(
      POD_IP,
      PORT,
      SCRIPT_PATH,
      TOKEN,
      POD_NAME,
      CONTAINER_NAME
    )

    expect(exitCode).toBe(0)
  })

  it('should do a final log flush after status becomes completed', async () => {
    const finalStdout = new TextEncoder().encode('final output\n')
    let statusCallCount = 0
    let postCompletionLogFetch = false

    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }
      if (urlStr.includes('/logs') && urlStr.includes('stream=stdout')) {
        if (statusCallCount >= 1 && !postCompletionLogFetch) {
          // This is the final flush — return remaining data
          postCompletionLogFetch = true
          return Promise.resolve(
            fakeResponse(200, null, {
              arrayBuffer: finalStdout.buffer.slice(
                finalStdout.byteOffset,
                finalStdout.byteOffset + finalStdout.byteLength
              )
            })
          )
        }
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/logs') && urlStr.includes('stream=stderr')) {
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        statusCallCount++
        return Promise.resolve(
          fakeResponse(200, { status: 'completed', exit_code: 0 })
        )
      }
      if (urlStr.includes('/heartbeat')) {
        return Promise.resolve(fakeResponse(200, { status: 'ok' }))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    const exitCode = await rpcPodStep(
      POD_IP,
      PORT,
      SCRIPT_PATH,
      TOKEN,
      POD_NAME,
      CONTAINER_NAME
    )

    expect(exitCode).toBe(0)

    // After status shows completed, there should be log fetches for final flush
    const allLogCalls = fetchMock.mock.calls.filter(([url]) =>
      url.includes('/logs')
    )
    // At minimum: during-poll logs + final flush = 4 calls (2 streams x 2 phases)
    expect(allLogCalls.length).toBeGreaterThanOrEqual(4)
  })

  it('should handle non-200 /logs response (return empty)', async () => {
    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }
      if (urlStr.includes('/logs')) {
        // Server returns 404 for logs
        return Promise.resolve(
          fakeResponse(404, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        return Promise.resolve(
          fakeResponse(200, { status: 'completed', exit_code: 0 })
        )
      }
      if (urlStr.includes('/heartbeat')) {
        return Promise.resolve(fakeResponse(200, { status: 'ok' }))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    const exitCode = await rpcPodStep(
      POD_IP,
      PORT,
      SCRIPT_PATH,
      TOKEN,
      POD_NAME,
      CONTAINER_NAME
    )

    expect(exitCode).toBe(0)
  })

  it('should pass abort signal with timeout to fetch calls', async () => {
    setupSimpleExecFlow(0)

    await rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)

    // Verify all fetch calls include a signal
    for (const [, opts] of fetchMock.mock.calls) {
      if (opts) {
        expect(opts.signal).toBeDefined()
      }
    }
  })

  it('should include exec error body text in thrown error message', async () => {
    fetchMock.mockResolvedValueOnce(
      fakeResponse(422, 'Unprocessable: missing required field "id"')
    )

    await expect(
      rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
    ).rejects.toThrow('RPC /exec failed (422)')
  })

  it('should handle /exec response where text() throws', async () => {
    const badResp = {
      ok: false,
      status: 400,
      statusText: '400',
      headers: new Headers(),
      text: jest.fn().mockRejectedValue(new Error('stream consumed')),
      json: jest.fn().mockRejectedValue(new Error('stream consumed')),
      arrayBuffer: jest.fn(),
      body: null,
      bodyUsed: true,
      redirected: false,
      type: 'basic' as ResponseType,
      url: '',
      clone: jest.fn()
    } as unknown as Response

    fetchMock.mockResolvedValueOnce(badResp)

    await expect(
      rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
    ).rejects.toThrow('RPC /exec failed (400)')
  })

  // -------------------------------------------------------------------
  // Scenario tests — long-running, cancellation, OOM, sudden pod death
  // -------------------------------------------------------------------

  it('should stream stdout and stderr over many poll cycles (long-running script)', async () => {
    // Simulates a process running for many poll iterations, producing
    // new stdout/stderr chunks periodically, with heartbeats succeeding.
    const totalPolls = 30
    let statusCallCount = 0
    let stdoutCallCount = 0
    let stderrCallCount = 0

    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }
      if (urlStr.includes('/logs') && urlStr.includes('stream=stdout')) {
        stdoutCallCount++
        // Produce a chunk every 5 polls
        if (stdoutCallCount % 5 === 0 && statusCallCount < totalPolls) {
          const chunk = new TextEncoder().encode(
            `stdout line ${stdoutCallCount}\n`
          )
          return Promise.resolve(
            fakeResponse(200, null, {
              arrayBuffer: chunk.buffer.slice(
                chunk.byteOffset,
                chunk.byteOffset + chunk.byteLength
              )
            })
          )
        }
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/logs') && urlStr.includes('stream=stderr')) {
        stderrCallCount++
        // Produce stderr every 7 polls
        if (stderrCallCount % 7 === 0 && statusCallCount < totalPolls) {
          const chunk = new TextEncoder().encode(
            `stderr warning ${stderrCallCount}\n`
          )
          return Promise.resolve(
            fakeResponse(200, null, {
              arrayBuffer: chunk.buffer.slice(
                chunk.byteOffset,
                chunk.byteOffset + chunk.byteLength
              )
            })
          )
        }
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        statusCallCount++
        if (statusCallCount >= totalPolls) {
          return Promise.resolve(
            fakeResponse(200, { status: 'completed', exit_code: 0 })
          )
        }
        return Promise.resolve(
          fakeResponse(200, { status: 'running', exit_code: null })
        )
      }
      if (urlStr.includes('/heartbeat')) {
        return Promise.resolve(fakeResponse(200, { status: 'ok' }))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    const exitCode = await rpcPodStep(
      POD_IP,
      PORT,
      SCRIPT_PATH,
      TOKEN,
      POD_NAME,
      CONTAINER_NAME
    )

    expect(exitCode).toBe(0)
    // Verify multiple stdout and stderr writes occurred
    expect(stdoutWriteSpy.mock.calls.length).toBeGreaterThanOrEqual(5)
    expect(stderrWriteSpy.mock.calls.length).toBeGreaterThanOrEqual(3)
  })

  it('should handle process killed mid-execution (cancelled job)', async () => {
    // Process runs for several polls producing logs, then is killed externally.
    // Status changes from running to failed with exit code 143 (SIGTERM).
    let statusCallCount = 0
    const killAfterPolls = 5

    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }
      if (urlStr.includes('/logs') && urlStr.includes('stream=stdout')) {
        if (statusCallCount < killAfterPolls) {
          const chunk = new TextEncoder().encode('running...\n')
          return Promise.resolve(
            fakeResponse(200, null, {
              arrayBuffer: chunk.buffer.slice(
                chunk.byteOffset,
                chunk.byteOffset + chunk.byteLength
              )
            })
          )
        }
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/logs') && urlStr.includes('stream=stderr')) {
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        statusCallCount++
        if (statusCallCount > killAfterPolls) {
          // Process was killed — SIGTERM = exit code 143 (128 + 15)
          return Promise.resolve(
            fakeResponse(200, { status: 'failed', exit_code: 143 })
          )
        }
        return Promise.resolve(
          fakeResponse(200, { status: 'running', exit_code: null })
        )
      }
      if (urlStr.includes('/heartbeat')) {
        return Promise.resolve(fakeResponse(200, { status: 'ok' }))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    const exitCode = await rpcPodStep(
      POD_IP,
      PORT,
      SCRIPT_PATH,
      TOKEN,
      POD_NAME,
      CONTAINER_NAME
    )

    expect(exitCode).toBe(143)
    // Some stdout was produced before the kill
    expect(stdoutWriteSpy).toHaveBeenCalled()
  })

  it('should handle OOM kill on the script (exit code 137 with log streaming)', async () => {
    // Script runs, producing logs, then is OOM killed. The RPC server
    // catches the exit and reports status: failed, exit_code: 137.
    let statusCallCount = 0
    const oomAfterPolls = 8

    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }
      if (urlStr.includes('/logs') && urlStr.includes('stream=stdout')) {
        if (statusCallCount <= oomAfterPolls) {
          const chunk = new TextEncoder().encode('allocating memory...\n')
          return Promise.resolve(
            fakeResponse(200, null, {
              arrayBuffer: chunk.buffer.slice(
                chunk.byteOffset,
                chunk.byteOffset + chunk.byteLength
              )
            })
          )
        }
        // After OOM, no more output
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/logs') && urlStr.includes('stream=stderr')) {
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        statusCallCount++
        if (statusCallCount > oomAfterPolls) {
          // SIGKILL from OOM killer: 128 + 9 = 137
          return Promise.resolve(
            fakeResponse(200, { status: 'failed', exit_code: 137 })
          )
        }
        return Promise.resolve(
          fakeResponse(200, { status: 'running', exit_code: null })
        )
      }
      if (urlStr.includes('/heartbeat')) {
        return Promise.resolve(fakeResponse(200, { status: 'ok' }))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    const exitCode = await rpcPodStep(
      POD_IP,
      PORT,
      SCRIPT_PATH,
      TOKEN,
      POD_NAME,
      CONTAINER_NAME
    )

    expect(exitCode).toBe(137)
    expect(stdoutWriteSpy).toHaveBeenCalled()
  })

  it('should handle OOM kill on workflow pod (all fetches fail, then diagnostic)', async () => {
    // Pod OOM: after some successful polls, ALL fetches start failing.
    // Heartbeat grace period elapses, then diagnostic reveals OOMKilled.
    const realDateNow = Date.now
    const startTime = realDateNow()
    let timeOffset = 0
    let statusCallCount = 0
    const oomAfterPolls = 3

    jest.spyOn(Date, 'now').mockImplementation(() => startTime + timeOffset)

    mockGetPodByName.mockResolvedValue({
      status: {
        phase: 'Failed',
        podIP: POD_IP,
        containerStatuses: [
          {
            name: CONTAINER_NAME,
            state: {
              terminated: { reason: 'OOMKilled', exitCode: 137 }
            }
          }
        ]
      }
    })

    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }

      // After OOM, all fetches fail (server is gone)
      if (statusCallCount >= oomAfterPolls) {
        if (urlStr.includes('/status')) {
          // Advance time to trigger heartbeat grace expiry
          timeOffset += 25000
          return Promise.reject(new Error('ECONNREFUSED'))
        }
        if (urlStr.includes('/heartbeat')) {
          return Promise.reject(new Error('ECONNREFUSED'))
        }
        if (urlStr.includes('/logs')) {
          return Promise.reject(new Error('ECONNREFUSED'))
        }
      }

      if (urlStr.includes('/logs')) {
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        statusCallCount++
        return Promise.resolve(
          fakeResponse(200, { status: 'running', exit_code: null })
        )
      }
      if (urlStr.includes('/heartbeat')) {
        return Promise.resolve(fakeResponse(200, { status: 'ok' }))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    await expect(
      rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
    ).rejects.toThrow('OOMKilled')
  })

  it('should handle workflow pod sudden death with generic termination', async () => {
    // Pod dies for a non-OOM reason (e.g., preemption, crash).
    // All fetches fail, heartbeat times out, diagnostic shows terminated
    // with reason: Error.
    const realDateNow = Date.now
    const startTime = realDateNow()
    let timeOffset = 0

    jest.spyOn(Date, 'now').mockImplementation(() => startTime + timeOffset)

    mockGetPodByName.mockResolvedValue({
      status: {
        phase: 'Failed',
        podIP: POD_IP,
        containerStatuses: [
          {
            name: CONTAINER_NAME,
            state: {
              terminated: {
                reason: 'Error',
                exitCode: 1,
                message: 'container exited unexpectedly'
              }
            }
          }
        ]
      }
    })

    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      const urlStr = url.toString()
      if (urlStr.includes('/exec') && init?.method === 'POST') {
        return Promise.resolve(fakeResponse(200, { status: 'running' }))
      }
      if (urlStr.includes('/logs')) {
        return Promise.resolve(
          fakeResponse(200, null, { arrayBuffer: new ArrayBuffer(0) })
        )
      }
      if (urlStr.includes('/status')) {
        timeOffset += GRACE_MS + 1000
        return Promise.resolve(
          fakeResponse(200, { status: 'running', exit_code: null })
        )
      }
      if (urlStr.includes('/heartbeat')) {
        return Promise.reject(new Error('connection refused'))
      }
      return Promise.reject(new Error(`Unexpected: ${urlStr}`))
    })

    await expect(
      rpcPodStep(POD_IP, PORT, SCRIPT_PATH, TOKEN, POD_NAME, CONTAINER_NAME)
    ).rejects.toThrow('container exited unexpectedly')
  })
})

// ---------------------------------------------------------------------------
// killRpcJob
// ---------------------------------------------------------------------------

describe('killRpcJob', () => {
  const POD_IP = '10.0.0.1'
  const PORT = 8080
  const TOKEN = 'test-token-abc'

  let fetchMock: jest.Mock

  beforeEach(() => {
    jest.clearAllMocks()
    fetchMock = jest.fn()
    global.fetch = fetchMock as any
  })

  it('POSTs /kill with the auth token', async () => {
    fetchMock.mockResolvedValueOnce(
      fakeResponse(200, { id: 'x', status: 'failed', exit_code: -1 })
    )

    await killRpcJob(POD_IP, PORT, TOKEN)

    expect(fetchMock).toHaveBeenCalledTimes(1)
    const [url, opts] = fetchMock.mock.calls[0]
    expect(url).toContain('/kill')
    expect(opts.method).toBe('POST')
    expect(opts.headers['X-Auth-Token']).toBe(TOKEN)
  })

  it('swallows network errors (best-effort)', async () => {
    fetchMock.mockRejectedValueOnce(new Error('connection refused'))

    // Must not throw — caller is about to exit and can't handle errors.
    await expect(killRpcJob(POD_IP, PORT, TOKEN)).resolves.toBeUndefined()
  })

  it('swallows non-2xx responses (best-effort)', async () => {
    fetchMock.mockResolvedValueOnce(fakeResponse(500, 'oops'))

    await expect(killRpcJob(POD_IP, PORT, TOKEN)).resolves.toBeUndefined()
  })
})

describe('buildKillStepGroupCommand', () => {
  // Run the real command under each shell the job image might provide and
  // check that a process group recorded in the pgid file is actually killed.
  const shells = ['sh', 'busybox sh'].filter(sh => {
    const bin = sh.split(' ')[0]
    return (
      spawnSync('sh', ['-c', `command -v ${bin}`], { stdio: 'ignore' })
        .status === 0
    )
  })

  function isAlive(pid: number): boolean {
    try {
      process.kill(pid, 0)
      return true
    } catch {
      return false
    }
  }

  it.each(shells)('kills the recorded process group with %s', async sh => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'rpc-pgid-'))
    const pgidFile = path.join(dir, 'job.pgid')
    // A step leader in its own session (detached does setsid, so pgid == pid),
    // with a child in the group.
    const leader = spawn('sh', ['-c', 'sleep 60 & wait'], {
      stdio: 'ignore',
      detached: true
    })
    leader.unref()
    try {
      const pid = leader.pid as number
      fs.writeFileSync(pgidFile, String(pid))
      await new Promise(r => setTimeout(r, 300))
      expect(isAlive(pid)).toBe(true)

      const [bin, ...args] = sh.split(' ')
      const result = spawnSync(
        bin,
        [...args, '-c', buildKillStepGroupCommand(pgidFile)],
        { timeout: 10000 }
      )

      expect(result.status).toBe(0)
      await new Promise(r => setTimeout(r, 300))
      expect(isAlive(pid)).toBe(false)
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })

  it.each(shells)(
    'does nothing when the pgid file is missing or invalid (%s)',
    sh => {
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'rpc-pgid-'))
      try {
        const [bin, ...args] = sh.split(' ')
        const missing = spawnSync(
          bin,
          [...args, '-c', buildKillStepGroupCommand(path.join(dir, 'none'))],
          { timeout: 10000 }
        )
        expect(missing.status).toBe(0)

        const bad = path.join(dir, 'bad.pgid')
        fs.writeFileSync(bad, '-1 oops')
        const invalid = spawnSync(
          bin,
          [...args, '-c', buildKillStepGroupCommand(bad)],
          { timeout: 10000 }
        )
        expect(invalid.status).toBe(0)
      } finally {
        fs.rmSync(dir, { recursive: true, force: true })
      }
    }
  )
})

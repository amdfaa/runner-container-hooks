// prepareJob's in-pod server step, with the cluster calls mocked out. The
// server is optional: when it cannot be started, prepareJob still succeeds and
// leaves the RPC state out of the response so script steps use the exec
// launcher.

const mockExecPodStepOutput = jest.fn()
const mockGetPodByName = jest.fn()

jest.mock('../src/k8s', () => ({
  containerPorts: jest.fn().mockReturnValue([]),
  createPod: jest.fn().mockResolvedValue({
    metadata: { name: 'job-pod' },
    spec: { containers: [] }
  }),
  prunePods: jest.fn().mockResolvedValue(undefined),
  waitForPodPhases: jest.fn().mockResolvedValue(undefined),
  isPodContainerAlpine: jest.fn().mockResolvedValue(false),
  getPrepareJobTimeoutSeconds: jest.fn().mockReturnValue(60),
  execPodStepOutput: (...args) => mockExecPodStepOutput(...args),
  getPodByName: (...args) => mockGetPodByName(...args)
}))

jest.mock('@actions/core', () => ({
  debug: jest.fn(),
  info: jest.fn(),
  warning: jest.fn(),
  error: jest.fn()
}))

jest.mock('../src/k8s/utils', () => ({
  ...jest.requireActual('../src/k8s/utils'),
  sleep: jest.fn().mockResolvedValue(undefined)
}))

import * as core from '@actions/core'
import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import { prepareJob } from '../src/hooks/prepare-job'
import { RPC_PORT } from '../src/k8s/rpc'

const UNAVAILABLE = 'in-pod server unavailable, using exec launcher'

describe('prepareJob in-pod server', () => {
  let tmpDir: string
  let responseFile: string
  let imageBinary: string
  let savedEnv: NodeJS.ProcessEnv

  const jobArgs = {
    container: { image: 'ubuntu:22.04' },
    services: []
  } as never

  function readState(): Record<string, unknown> {
    return JSON.parse(fs.readFileSync(responseFile, 'utf8')).state
  }

  function startCalls(): string[][] {
    return mockExecPodStepOutput.mock.calls
      .map(([cmd]) => cmd as string[])
      .filter(cmd => cmd[0] === 'env')
  }

  function loggedText(): string {
    return [
      ...(core.debug as jest.Mock).mock.calls,
      ...(core.info as jest.Mock).mock.calls,
      ...(core.warning as jest.Mock).mock.calls,
      ...(core.error as jest.Mock).mock.calls
    ]
      .map(args => String(args[0]))
      .join('\n')
  }

  beforeEach(() => {
    jest.clearAllMocks()
    savedEnv = { ...process.env }
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'prepare-job-rpc-'))
    responseFile = path.join(tmpDir, 'response.json')
    fs.writeFileSync(responseFile, '')
    fs.mkdirSync(path.join(tmpDir, '_temp'))
    imageBinary = path.join(tmpDir, 'image-rpc-server')
    fs.writeFileSync(imageBinary, 'fake-binary')
    process.env.RUNNER_TEMP = path.join(tmpDir, '_temp')
    process.env.ACTIONS_RUNNER_RPC_SERVER_PATH = imageBinary
    delete process.env.RUNNER_WORKSPACE
    delete process.env.ACTIONS_RUNNER_CONTAINER_HOOK_TEMPLATE

    mockGetPodByName.mockResolvedValue({ status: { podIP: '10.1.2.3' } })
    mockExecPodStepOutput.mockResolvedValue({
      exitCode: 0,
      stdout: `rpc-server: oom_score_adj -1000 applied\nRPC server listening on [::]:${RPC_PORT}`
    })
    global.fetch = jest.fn().mockResolvedValue({ ok: true }) as never
  })

  afterEach(() => {
    process.env = savedEnv
    fs.rmSync(tmpDir, { recursive: true, force: true })
    jest.restoreAllMocks()
  })

  it('records the pod IP, fixed port and token in state when the server is healthy', async () => {
    await prepareJob(jobArgs, responseFile)

    const state = readState()
    expect(state.jobPod).toBe('job-pod')
    expect(state.rpcPodIp).toBe('10.1.2.3')
    expect(state.rpcPort).toBe(RPC_PORT)
    expect(typeof state.rpcToken).toBe('string')
    expect((state.rpcToken as string).length).toBeGreaterThan(8)
    expect(core.warning).not.toHaveBeenCalledWith(UNAVAILABLE)
    expect(
      fs.statSync(path.join(process.env.RUNNER_TEMP as string, 'rpc-server'))
        .mode & 0o777
    ).toBe(0o755)
  })

  it('never logs the token', async () => {
    await prepareJob(jobArgs, responseFile)

    expect(loggedText()).not.toContain(readState().rpcToken as string)
  })

  it('falls back to the exec launcher when the binary is missing from the image', async () => {
    process.env.ACTIONS_RUNNER_RPC_SERVER_PATH = path.join(tmpDir, 'absent')

    await prepareJob(jobArgs, responseFile)

    expect(readState()).toEqual({ jobPod: 'job-pod' })
    expect(core.warning).toHaveBeenCalledWith(UNAVAILABLE)
    expect(startCalls()).toHaveLength(0)
  })

  it('falls back to the exec launcher when the health check never passes', async () => {
    // Each health probe jumps the clock past the 15s health window.
    const startTime = Date.now()
    let offset = 0
    jest.spyOn(Date, 'now').mockImplementation(() => startTime + offset)
    global.fetch = jest.fn().mockImplementation(async () => {
      offset += 16000
      return { ok: false, status: 500 }
    }) as never

    await prepareJob(jobArgs, responseFile)

    expect(readState()).toEqual({ jobPod: 'job-pod' })
    expect(core.warning).toHaveBeenCalledWith(UNAVAILABLE)
    // Three deploy attempts, then the fallback.
    expect(startCalls()).toHaveLength(3)
  })

  it('falls back when the server cannot be started', async () => {
    mockExecPodStepOutput.mockRejectedValue(new Error('exec denied'))

    await prepareJob(jobArgs, responseFile)

    expect(readState()).toEqual({ jobPod: 'job-pod' })
    expect(core.warning).toHaveBeenCalledWith(UNAVAILABLE)
  })

  it('still writes the rest of the response when falling back', async () => {
    process.env.ACTIONS_RUNNER_RPC_SERVER_PATH = path.join(tmpDir, 'absent')

    await prepareJob(jobArgs, responseFile)

    const response = JSON.parse(fs.readFileSync(responseFile, 'utf8'))
    expect(response.isAlpine).toBe(false)
    expect(response.context).toEqual({})
  })
})

// runScriptStep's choice between the in-pod server and the exec launcher, with
// the cluster and server calls mocked. Covers: the RPC path, the launcher path
// when prepare_job recorded no RPC state, the per-step fallback when the server
// cannot be reached at all, and never running a step twice.

const mockExecDetachedScriptStep = jest.fn()
const mockRpcPodStep = jest.fn()
const mockKillRpcJob = jest.fn()
const mockRegisterCleanup = jest.fn()
const mockUnregisterCleanup = jest.fn()

jest.mock('../src/k8s', () => ({
  execDetachedScriptStep: (...args) => mockExecDetachedScriptStep(...args)
}))

jest.mock('../src/k8s/rpc', () => ({
  ...jest.requireActual('../src/k8s/rpc'),
  rpcPodStep: (...args) => mockRpcPodStep(...args),
  killRpcJob: (...args) => mockKillRpcJob(...args)
}))

jest.mock('../src/safety/process-handlers', () => ({
  registerCleanup: (...args) => mockRegisterCleanup(...args),
  unregisterCleanup: (...args) => mockUnregisterCleanup(...args)
}))

jest.mock('@actions/core', () => ({
  debug: jest.fn(),
  info: jest.fn(),
  warning: jest.fn(),
  error: jest.fn()
}))

import * as core from '@actions/core'
import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import { runScriptStep } from '../src/hooks/run-script-step'
import { RpcUnreachableError } from '../src/k8s/rpc'

const RPC_STATE = {
  jobPod: 'job-pod',
  rpcPodIp: '10.1.2.3',
  rpcPort: 8765,
  rpcToken: 'secret-token'
}

describe('runScriptStep', () => {
  let tmpDir: string
  let savedEnv: NodeJS.ProcessEnv

  const args = {
    entryPoint: 'bash',
    entryPointArgs: ['-e', '/__w/_temp/user.sh'],
    workingDirectory: '/__w/repo/repo',
    prependPath: [],
    environmentVariables: {}
  } as never

  function tempFiles(): string[] {
    return fs.readdirSync(process.env.RUNNER_TEMP as string)
  }

  beforeEach(() => {
    jest.clearAllMocks()
    savedEnv = { ...process.env }
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'run-script-step-'))
    process.env.RUNNER_TEMP = tmpDir
    mockExecDetachedScriptStep.mockResolvedValue(undefined)
    mockRpcPodStep.mockResolvedValue(0)
    mockKillRpcJob.mockResolvedValue(undefined)
  })

  afterEach(() => {
    process.env = savedEnv
    fs.rmSync(tmpDir, { recursive: true, force: true })
  })

  describe('with in-pod server state', () => {
    it('runs the entry script through the server, not the launcher', async () => {
      await runScriptStep(args, RPC_STATE, null)

      expect(mockRpcPodStep).toHaveBeenCalledTimes(1)
      const [ip, port, scriptPath, token, pod, container] =
        mockRpcPodStep.mock.calls[0]
      expect([ip, port, token, pod, container]).toEqual([
        '10.1.2.3',
        8765,
        'secret-token',
        'job-pod',
        'job'
      ])
      expect(scriptPath).toMatch(/^\/__w\/_temp\/.+\.sh$/)
      expect(mockExecDetachedScriptStep).not.toHaveBeenCalled()
    })

    it('registers a cancel cleanup that kills the job, and removes it afterwards', async () => {
      await runScriptStep(args, RPC_STATE, null)

      expect(mockRegisterCleanup).toHaveBeenCalledTimes(1)
      const cleanup = mockRegisterCleanup.mock.calls[0][0]
      expect(mockUnregisterCleanup).toHaveBeenCalledWith(cleanup)

      await cleanup()
      expect(mockKillRpcJob).toHaveBeenCalledWith(
        '10.1.2.3',
        8765,
        'secret-token'
      )
    })

    it('fails with the usual step-failure message on a non-zero exit', async () => {
      mockRpcPodStep.mockResolvedValue(3)

      await expect(runScriptStep(args, RPC_STATE, null)).rejects.toThrow(
        /^failed to run script step: .*exit code 3/
      )
      expect(mockExecDetachedScriptStep).not.toHaveBeenCalled()
      expect(mockUnregisterCleanup).toHaveBeenCalled()
    })

    it('fails the step, without a second run, when the server accepted it and then failed', async () => {
      mockRpcPodStep.mockRejectedValue(
        new Error('RPC heartbeat failed for 120s — infrastructure failure')
      )

      await expect(runScriptStep(args, RPC_STATE, null)).rejects.toThrow(
        'failed to run script step: RPC heartbeat failed for 120s'
      )
      expect(mockExecDetachedScriptStep).not.toHaveBeenCalled()
      expect(mockUnregisterCleanup).toHaveBeenCalled()
    })

    it('does not fall back when /exec was refused with a 4xx', async () => {
      mockRpcPodStep.mockRejectedValue(
        new Error('RPC /exec failed (409): busy')
      )

      await expect(runScriptStep(args, RPC_STATE, null)).rejects.toThrow(
        'failed to run script step: RPC /exec failed (409)'
      )
      expect(mockExecDetachedScriptStep).not.toHaveBeenCalled()
    })

    it('runs this step through the launcher, once, when the server was never reached', async () => {
      mockRpcPodStep.mockRejectedValue(
        new RpcUnreachableError('RPC /exec network error after 3 attempts')
      )

      await runScriptStep(args, RPC_STATE, null)

      expect(mockRpcPodStep).toHaveBeenCalledTimes(1)
      expect(mockExecDetachedScriptStep).toHaveBeenCalledTimes(1)
      expect(mockExecDetachedScriptStep.mock.calls[0][0]).toMatch(
        /^\/__w\/_temp\/.+-launcher\.sh$/
      )
      expect(core.warning).toHaveBeenCalledWith(
        expect.stringContaining('in-pod server unreachable')
      )
      expect(mockUnregisterCleanup).toHaveBeenCalled()
    })

    it('reports a failing launcher after a per-step fallback as a step failure', async () => {
      mockRpcPodStep.mockRejectedValue(new RpcUnreachableError('unreachable'))
      mockExecDetachedScriptStep.mockRejectedValue(
        'command terminated with non-zero exit code: exit 1'
      )

      await expect(runScriptStep(args, RPC_STATE, null)).rejects.toThrow(
        'failed to run script step: command terminated with non-zero exit code'
      )
    })

    it('removes the entry script from RUNNER_TEMP on success and on failure', async () => {
      await runScriptStep(args, RPC_STATE, null)
      expect(tempFiles()).toEqual([])

      mockRpcPodStep.mockResolvedValue(1)
      await runScriptStep(args, RPC_STATE, null).catch(() => undefined)
      expect(tempFiles()).toEqual([])
    })

    it('removes both the entry script and the launcher after a per-step fallback', async () => {
      mockRpcPodStep.mockRejectedValue(new RpcUnreachableError('unreachable'))

      await runScriptStep(args, RPC_STATE, null)

      expect(tempFiles()).toEqual([])
    })
  })

  describe('without in-pod server state (launcher path)', () => {
    it.each([
      ['no RPC fields', { jobPod: 'job-pod' }],
      [
        'a partial set of RPC fields',
        { jobPod: 'job-pod', rpcPodIp: '10.1.2.3' }
      ]
    ])('uses the detached launcher with %s', async (_name, state) => {
      await runScriptStep(args, state, null)

      expect(mockRpcPodStep).not.toHaveBeenCalled()
      expect(mockRegisterCleanup).not.toHaveBeenCalled()
      expect(mockExecDetachedScriptStep).toHaveBeenCalledTimes(1)
      const [launcherPath, pod, container] =
        mockExecDetachedScriptStep.mock.calls[0]
      expect(launcherPath).toMatch(/^\/__w\/_temp\/.+-launcher\.sh$/)
      expect([pod, container]).toEqual(['job-pod', 'job'])
      expect(core.warning).not.toHaveBeenCalled()
    })

    it('keeps the launcher failure semantics', async () => {
      mockExecDetachedScriptStep.mockRejectedValue(
        'command terminated with non-zero exit code: exit 2'
      )

      await expect(
        runScriptStep(args, { jobPod: 'job-pod' }, null)
      ).rejects.toThrow('failed to run script step: command terminated')
    })

    it('removes the entry script and the launcher afterwards', async () => {
      await runScriptStep(args, { jobPod: 'job-pod' }, null)
      expect(tempFiles()).toEqual([])
    })
  })
})

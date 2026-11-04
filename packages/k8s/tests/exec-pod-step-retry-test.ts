// Unit test for execPodStep's retry behavior. Fully mocks
// @kubernetes/client-node so this needs no real cluster/kubeconfig and never
// touches the network: only the retry decision logic is exercised.
//
// Covers two delivery paths for a dial failure like "No agent available":
//   1. exec.exec() itself rejecting (pre-existing behavior)
//   2. the resp callback reporting a Failure status (new in this change) —
//      only treated as retryable when zero bytes were ever written to the
//      stdout/stderr streams passed to exec.exec()

const mockExecFn = jest.fn()

jest.mock('@actions/core', () => ({
  debug: jest.fn(),
  warning: jest.fn(),
  error: jest.fn(),
  info: jest.fn()
}))

jest.mock('@kubernetes/client-node', () => ({
  KubeConfig: jest.fn().mockImplementation(() => ({
    loadFromDefault: jest.fn(),
    makeApiClient: jest.fn().mockReturnValue({}),
    getContexts: jest.fn().mockReturnValue([])
  })),
  CoreV1Api: jest.fn(),
  BatchV1Api: jest.fn(),
  AuthorizationV1Api: jest.fn(),
  Exec: jest.fn().mockImplementation(() => ({ exec: mockExecFn }))
}))

import * as core from '@actions/core'
import { execPodStep } from '../src/k8s'

// This branch's execPodStepOnce has no .then() on exec.exec() — the resolved
// value (a websocket, in real usage) is never inspected — so the mock can
// resolve with anything.
const MOCK_WEBSOCKET = {}

// args passed to exec.exec(namespace, pod, container, command, stdout,
// stderr, stdin, tty, callback) — callback is the 9th (index 8) argument,
// stdout/stderr are indices 4 and 5.
function callbackArg(
  args: unknown[]
): (resp: { status: string; code?: number; message?: string }) => void {
  return args[8] as (resp: {
    status: string
    code?: number
    message?: string
  }) => void
}
function stdoutArg(args: unknown[]): NodeJS.WritableStream {
  return args[4] as NodeJS.WritableStream
}

describe('execPodStep retry on dial failures', () => {
  beforeEach(() => {
    process.env.ACTIONS_RUNNER_KUBERNETES_NAMESPACE = 'test-ns'
    jest.useFakeTimers()
    mockExecFn.mockReset()
  })

  afterEach(() => {
    jest.useRealTimers()
  })

  it('retries a pre-connection "No agent available" rejection and then succeeds', async () => {
    mockExecFn
      .mockImplementationOnce(() =>
        Promise.reject(
          new Error('No agent available: dial tcp 10.0.0.5:10250: i/o timeout')
        )
      )
      .mockImplementationOnce((...args) => {
        const callback = callbackArg(args)
        setTimeout(() => callback({ status: 'Success', code: 0 }), 0)
        return Promise.resolve(MOCK_WEBSOCKET)
      })

    const promise = execPodStep(['sh', '-c', 'true'], 'pod-1', 'runner')
    await jest.advanceTimersByTimeAsync(30000)

    await expect(promise).resolves.toBeUndefined()
    expect(mockExecFn).toHaveBeenCalledTimes(2)
  })

  it('retries a resp-callback "No agent available" failure when no output was produced', async () => {
    mockExecFn
      .mockImplementationOnce((...args) => {
        const callback = callbackArg(args)
        setTimeout(
          () =>
            callback({
              status: 'Failure',
              message: 'No agent available: dial tcp timeout'
            }),
          0
        )
        return Promise.resolve(MOCK_WEBSOCKET)
      })
      .mockImplementationOnce((...args) => {
        const callback = callbackArg(args)
        setTimeout(() => callback({ status: 'Success', code: 0 }), 0)
        return Promise.resolve(MOCK_WEBSOCKET)
      })

    const promise = execPodStep(['sh', '-c', 'true'], 'pod-1', 'runner')
    await jest.advanceTimersByTimeAsync(30000)

    await expect(promise).resolves.toBeUndefined()
    expect(mockExecFn).toHaveBeenCalledTimes(2)
    expect(core.warning).toHaveBeenCalledWith(
      expect.stringContaining('No agent available')
    )
  })

  it('does NOT retry a resp-callback "No agent available" failure once output was produced', async () => {
    mockExecFn.mockImplementation((...args) => {
      const out = stdoutArg(args)
      out.write('partial test output\n')
      const callback = callbackArg(args)
      setTimeout(
        () =>
          callback({
            status: 'Failure',
            message: 'No agent available: dial tcp timeout'
          }),
        0
      )
      return Promise.resolve(MOCK_WEBSOCKET)
    })

    const promise = execPodStep(['sh', '-c', 'true'], 'pod-1', 'runner')
    const assertion = expect(promise).rejects.toBe(
      'No agent available: dial tcp timeout'
    )
    await jest.advanceTimersByTimeAsync(1000)
    await assertion
    expect(mockExecFn).toHaveBeenCalledTimes(1)
  })

  it('does not retry a resp-callback failure with an unrelated message, even with no output', async () => {
    mockExecFn.mockImplementation((...args) => {
      const callback = callbackArg(args)
      setTimeout(
        () => callback({ status: 'Failure', message: 'command not found' }),
        0
      )
      return Promise.resolve(MOCK_WEBSOCKET)
    })

    const promise = execPodStep(['sh', '-c', 'true'], 'pod-1', 'runner')
    const assertion = expect(promise).rejects.toBe('command not found')
    await jest.advanceTimersByTimeAsync(1000)
    await assertion
    expect(mockExecFn).toHaveBeenCalledTimes(1)
  })

  it('never retries once the command has actually run, even if the failure message says "No agent available"', async () => {
    mockExecFn.mockImplementation((...args) => {
      const out = stdoutArg(args)
      out.write('test ran and failed\n')
      const callback = callbackArg(args)
      setTimeout(
        () =>
          callback({
            status: 'Failure',
            message: 'No agent available (coincidental — command already ran)'
          }),
        0
      )
      return Promise.resolve(MOCK_WEBSOCKET)
    })

    const promise = execPodStep(['sh', '-c', 'false'], 'pod-1', 'runner')
    const assertion = expect(promise).rejects.toBe(
      'No agent available (coincidental — command already ran)'
    )
    await jest.advanceTimersByTimeAsync(1000)
    await assertion
    expect(mockExecFn).toHaveBeenCalledTimes(1)
  })

  it('gives up after the max attempts and throws', async () => {
    mockExecFn.mockImplementation(() =>
      Promise.reject(new Error('No agent available: dial tcp timeout'))
    )

    const promise = execPodStep(['sh', '-c', 'true'], 'pod-1', 'runner')
    const assertion = expect(promise).rejects.toThrow(/No agent available/)
    await jest.advanceTimersByTimeAsync(120000)
    await assertion
    expect(mockExecFn).toHaveBeenCalledTimes(5)
  })
})

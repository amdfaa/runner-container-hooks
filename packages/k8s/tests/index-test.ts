// index.ts must install the SIGTERM/SIGINT handlers once, at startup. Cancel
// cleanup (killing the in-pod step) only fires if they are installed.

const mockCleanupJob = jest.fn().mockResolvedValue(undefined)
const mockGetInput = jest.fn()
const mockExit = jest
  .spyOn(process, 'exit')
  .mockImplementation((() => undefined) as never)

jest.mock('hooklib', () => ({
  ...jest.requireActual('hooklib'),
  getInputFromStdin: (...args) => mockGetInput(...args)
}))

jest.mock('../src/hooks', () => ({
  cleanupJob: (...args) => mockCleanupJob(...args),
  prepareJob: jest.fn(),
  runContainerStep: jest.fn(),
  runScriptStep: jest.fn()
}))

jest.mock('../src/k8s', () => ({
  isAuthPermissionsOK: jest.fn().mockResolvedValue(true),
  namespace: jest.fn().mockReturnValue('default'),
  requiredPermissions: []
}))

jest.mock('@actions/core', () => ({
  debug: jest.fn(),
  info: jest.fn(),
  warning: jest.fn(),
  error: jest.fn()
}))

import { installProcessHandlers } from '../src/safety/process-handlers'

describe('index startup', () => {
  it('installs the signal handlers exactly once', async () => {
    const before = {
      SIGTERM: process.listeners('SIGTERM'),
      SIGINT: process.listeners('SIGINT')
    }
    mockGetInput.mockResolvedValue({
      command: 'cleanup_job',
      args: {},
      state: {},
      responseFile: ''
    })

    // eslint-disable-next-line @typescript-eslint/no-require-imports
    require('../src/index')
    await new Promise(resolve => setImmediate(resolve))

    const added = (name: 'SIGTERM' | 'SIGINT'): NodeJS.SignalsListener[] =>
      process.listeners(name).filter(l => !before[name].includes(l))
    expect(added('SIGTERM')).toHaveLength(1)
    expect(added('SIGINT')).toHaveLength(1)
    // The hook then ran normally.
    expect(mockCleanupJob).toHaveBeenCalledTimes(1)
    expect(mockExit).toHaveBeenCalledWith(0)

    // Installing again is a no-op: the module guards against double install.
    installProcessHandlers()
    expect(added('SIGTERM')).toHaveLength(1)
    expect(added('SIGINT')).toHaveLength(1)

    added('SIGTERM').forEach(l => process.removeListener('SIGTERM', l))
    added('SIGINT').forEach(l => process.removeListener('SIGINT', l))
  })
})

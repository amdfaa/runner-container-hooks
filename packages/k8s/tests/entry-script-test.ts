// The entry script merges stderr into stdout with `exec 2>&1`, so a step's
// errors stay in order with its output on the single log stream. These tests
// write the real script and run it with a real shell; no cluster is needed.

import { spawnSync } from 'child_process'
import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import { writeEntryPointScript } from '../src/k8s/utils'

describe('writeEntryPointScript stream merging', () => {
  let tmpDir: string
  const savedTemp = process.env.RUNNER_TEMP

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'entry-script-'))
    process.env.RUNNER_TEMP = tmpDir
  })

  afterEach(() => {
    process.env.RUNNER_TEMP = savedTemp
    fs.rmSync(tmpDir, { recursive: true, force: true })
  })

  it('puts `exec 2>&1` right after the shebang, before PATH and cd', () => {
    const { runnerPath } = writeEntryPointScript(
      tmpDir,
      'sh',
      ['-c', 'true'],
      ['/prepend/bin'],
      { A: 'b' }
    )

    const lines = fs.readFileSync(runnerPath, 'utf8').split('\n')

    expect(lines[0]).toBe('#!/bin/sh -l')
    expect(lines[1]).toBe('exec 2>&1')
    expect(lines[2]).toBe('export PATH=/prepend/bin:$PATH')
    expect(lines[3]).toMatch(/^cd /)
  })

  it('keeps `exec 2>&1` second when there is no prepend path', () => {
    const { runnerPath } = writeEntryPointScript(tmpDir, 'sh', ['-c', 'true'])

    const lines = fs.readFileSync(runnerPath, 'utf8').split('\n')

    expect(lines.slice(0, 2)).toEqual(['#!/bin/sh -l', 'exec 2>&1'])
  })

  it('emits alternating stdout and stderr lines in order on stdout alone', () => {
    const { runnerPath } = writeEntryPointScript(tmpDir, 'sh', [
      '-c',
      `'echo out1; echo err1 >&2; echo out2; echo err2 >&2; echo out3'`
    ])

    // Run it the way the server does: `sh -e <script>`, stdout and stderr
    // going to separate pipes.
    const result = spawnSync('sh', ['-e', runnerPath], { encoding: 'utf8' })

    expect(result.status).toBe(0)
    expect(result.stdout).toBe('out1\nerr1\nout2\nerr2\nout3\n')
    expect(result.stderr).toBe('')
  })

  it('still reports the step exit code', () => {
    const { runnerPath } = writeEntryPointScript(tmpDir, 'sh', [
      '-c',
      `'echo before; echo boom >&2; exit 7'`
    ])

    const result = spawnSync('sh', ['-e', runnerPath], { encoding: 'utf8' })

    expect(result.status).toBe(7)
    expect(result.stdout).toBe('before\nboom\n')
  })
})

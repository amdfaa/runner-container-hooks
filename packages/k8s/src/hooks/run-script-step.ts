/* eslint-disable @typescript-eslint/no-unused-vars */
import * as fs from 'fs'
import * as core from '@actions/core'
import { RunScriptStepArgs } from 'hooklib'
import { execDetachedScriptStep } from '../k8s'
import { killRpcJob, RpcUnreachableError, rpcPodStep } from '../k8s/rpc'
import { registerCleanup, unregisterCleanup } from '../safety/process-handlers'
import {
  formatError,
  writeEntryPointScript,
  writeDetachedLauncherScript
} from '../k8s/utils'
import { JOB_CONTAINER_NAME } from './constants'

export async function runScriptStep(
  args: RunScriptStepArgs,
  state,
  responseFile
): Promise<void> {
  const { entryPoint, entryPointArgs, environmentVariables } = args
  const { containerPath, runnerPath } = writeEntryPointScript(
    args.workingDirectory,
    entryPoint,
    entryPointArgs,
    args.prependPath,
    environmentVariables
  )

  try {
    // prepare_job records rpcPodIp, rpcPort and rpcToken only when the in-pod
    // server came up healthy; otherwise this step uses the exec launcher.
    if (state.rpcPodIp && state.rpcPort && state.rpcToken) {
      try {
        await runViaRpc(containerPath, state)
        return
      } catch (err) {
        if (!(err instanceof RpcUnreachableError)) {
          throw err
        }
        // The server never saw this step, so running it through the launcher
        // cannot run it twice. Any other error means the server may have the
        // step and must fail it.
        core.warning(
          `in-pod server unreachable, running this step through the exec launcher: ${formatError(err)}`
        )
      }
    }
    await runViaLauncher(containerPath, state)
  } finally {
    fs.rmSync(runnerPath)
  }
}

async function runViaRpc(containerPath: string, state): Promise<void> {
  // On cancel (SIGTERM/SIGINT to the hook), tell the server to kill the step.
  // Without this the step outlives the cancelled hook and the next step's
  // /exec is refused because a job is still running.
  const killOnCancel = async (): Promise<void> => {
    await killRpcJob(state.rpcPodIp, state.rpcPort, state.rpcToken)
  }
  registerCleanup(killOnCancel)
  let exitCode: number
  try {
    exitCode = await rpcPodStep(
      state.rpcPodIp,
      state.rpcPort,
      containerPath,
      state.rpcToken,
      state.jobPod,
      JOB_CONTAINER_NAME
    )
  } catch (err) {
    if (err instanceof RpcUnreachableError) {
      throw err
    }
    const message = formatError(err)
    core.debug(`rpcPodStep failed: ${message}`)
    throw new Error(`failed to run script step: ${message}`)
  } finally {
    unregisterCleanup(killOnCancel)
  }
  if (exitCode !== 0) {
    throw new Error(
      `failed to run script step: command terminated with exit code ${exitCode}`
    )
  }
}

async function runViaLauncher(containerPath: string, state): Promise<void> {
  const launcher = writeDetachedLauncherScript(containerPath)
  try {
    await execDetachedScriptStep(
      launcher.containerPath,
      state.jobPod,
      JOB_CONTAINER_NAME
    )
  } catch (err) {
    const message = formatError(err)
    core.debug(`execDetachedScriptStep failed: ${message}`)
    throw new Error(`failed to run script step: ${message}`)
  } finally {
    fs.rmSync(launcher.runnerPath)
  }
}

import * as core from '@actions/core'
import * as k8s from '@kubernetes/client-node'
import * as stream from 'stream'
import type { ContainerInfo, Registry } from 'hooklib'
import {
  getJobPodName,
  getRunnerPodName,
  getSecretName,
  getStepPodName,
  getVolumeClaimName,
  RunnerInstanceLabel
} from '../hooks/constants'
import {
  PodPhase,
  formatError,
  mergePodSpecWithOptions,
  mergeObjectMeta,
  useKubeScheduler,
  fixArgs
} from './utils'

const kc = new k8s.KubeConfig()

kc.loadFromDefault()

const k8sApi = kc.makeApiClient(k8s.CoreV1Api)
const k8sBatchV1Api = kc.makeApiClient(k8s.BatchV1Api)
const k8sAuthorizationV1Api = kc.makeApiClient(k8s.AuthorizationV1Api)

const DEFAULT_WAIT_FOR_POD_TIME_SECONDS = 10 * 60 // 10 min
const DEFAULT_AUTH_CHECK_TIMEOUT_SECONDS = 30
const SSAR_REQUEST_TIMEOUT_SECONDS = 5

export const POD_VOLUME_NAME = 'work'

export const requiredPermissions = [
  {
    group: '',
    verbs: ['get', 'list', 'create', 'delete'],
    resource: 'pods',
    subresource: ''
  },
  {
    group: '',
    verbs: ['get', 'create'],
    resource: 'pods',
    subresource: 'exec'
  },
  {
    group: '',
    verbs: ['get', 'list', 'watch'],
    resource: 'pods',
    subresource: 'log'
  },
  {
    group: 'batch',
    verbs: ['get', 'list', 'create', 'delete'],
    resource: 'jobs',
    subresource: ''
  },
  {
    group: '',
    verbs: ['create', 'delete', 'get', 'list'],
    resource: 'secrets',
    subresource: ''
  }
]

export async function createPod(
  jobContainer?: k8s.V1Container,
  services?: k8s.V1Container[],
  registry?: Registry,
  extension?: k8s.V1PodTemplateSpec
): Promise<k8s.V1Pod> {
  const containers: k8s.V1Container[] = []
  if (jobContainer) {
    containers.push(jobContainer)
  }
  if (services?.length) {
    containers.push(...services)
  }

  const appPod = new k8s.V1Pod()

  appPod.apiVersion = 'v1'
  appPod.kind = 'Pod'

  appPod.metadata = new k8s.V1ObjectMeta()
  appPod.metadata.name = getJobPodName()

  const instanceLabel = new RunnerInstanceLabel()
  appPod.metadata.labels = {
    [instanceLabel.key]: instanceLabel.value
  }
  appPod.metadata.annotations = {}

  appPod.spec = new k8s.V1PodSpec()
  appPod.spec.containers = containers
  appPod.spec.restartPolicy = 'Never'

  const nodeName = await getCurrentNodeName()
  if (useKubeScheduler()) {
    appPod.spec.affinity = await getPodAffinity(nodeName)
  } else {
    appPod.spec.nodeName = nodeName
  }
  const claimName = getVolumeClaimName()
  appPod.spec.volumes = [
    {
      name: 'work',
      persistentVolumeClaim: { claimName }
    }
  ]

  if (registry) {
    const secret = await createDockerSecret(registry)
    if (!secret?.metadata?.name) {
      throw new Error(`created secret does not have secret.metadata.name`)
    }
    const secretReference = new k8s.V1LocalObjectReference()
    secretReference.name = secret.metadata.name
    appPod.spec.imagePullSecrets = [secretReference]
  }

  if (extension?.metadata) {
    mergeObjectMeta(appPod, extension.metadata)
  }

  if (extension?.spec) {
    mergePodSpecWithOptions(appPod.spec, extension.spec)
  }

  return await createPodWithRetry(appPod)
}

const CREATE_POD_MAX_BACKOFF_SECONDS = 60

// createNamespacedPod is a POST with a deterministic, caller-chosen pod
// name (getJobPodName()) — a connection error here doesn't tell us whether
// the request ever reached the server. If it did, the pod now exists and a
// naive retry would hit a 409 forever. Treat "already exists" as success
// (adopt the pod a prior attempt created) instead of failing or looping.
async function createPodWithRetry(appPod: k8s.V1Pod): Promise<k8s.V1Pod> {
  const backOffManager = new BackOffManager(CREATE_POD_MAX_BACKOFF_SECONDS)
  while (true) {
    try {
      return await k8sApi.createNamespacedPod({
        namespace: namespace(),
        body: appPod
      })
    } catch (error) {
      if (error instanceof k8s.ApiException && error.code === 409) {
        core.warning(
          `[createPod] pod ${appPod.metadata?.name} already exists ` +
            `(a prior attempt likely created it before its response was lost); ` +
            `using the existing pod instead of failing.`
        )
        return await k8sApi.readNamespacedPod({
          name: appPod.metadata?.name as string,
          namespace: namespace()
        })
      }
      core.warning(
        `[createPod] failed to create job pod, retrying: ${formatError(error)}`
      )
      await backOffManager.backOff()
    }
  }
}

export async function createJob(
  container: k8s.V1Container,
  extension?: k8s.V1PodTemplateSpec
): Promise<k8s.V1Job> {
  const runnerInstanceLabel = new RunnerInstanceLabel()

  const job = new k8s.V1Job()
  job.apiVersion = 'batch/v1'
  job.kind = 'Job'
  job.metadata = new k8s.V1ObjectMeta()
  job.metadata.name = getStepPodName()
  job.metadata.labels = { [runnerInstanceLabel.key]: runnerInstanceLabel.value }
  job.metadata.annotations = {}

  job.spec = new k8s.V1JobSpec()
  job.spec.ttlSecondsAfterFinished = 300
  job.spec.backoffLimit = 0
  job.spec.template = new k8s.V1PodTemplateSpec()

  job.spec.template.spec = new k8s.V1PodSpec()
  job.spec.template.metadata = new k8s.V1ObjectMeta()
  job.spec.template.metadata.labels = {}
  job.spec.template.metadata.annotations = {}
  job.spec.template.spec.containers = [container]
  job.spec.template.spec.restartPolicy = 'Never'

  const nodeName = await getCurrentNodeName()
  if (useKubeScheduler()) {
    job.spec.template.spec.affinity = await getPodAffinity(nodeName)
  } else {
    job.spec.template.spec.nodeName = nodeName
  }

  const claimName = getVolumeClaimName()
  job.spec.template.spec.volumes = [
    {
      name: 'work',
      persistentVolumeClaim: { claimName }
    }
  ]

  if (extension) {
    if (extension.metadata) {
      // apply metadata both to the job and the pod created by the job
      mergeObjectMeta(job, extension.metadata)
      mergeObjectMeta(job.spec.template, extension.metadata)
    }
    if (extension.spec) {
      mergePodSpecWithOptions(job.spec.template.spec, extension.spec)
    }
  }

  return await k8sBatchV1Api.createNamespacedJob({
    namespace: namespace(),
    body: job
  })
}

export async function getContainerJobPodName(jobName: string): Promise<string> {
  const selector = `job-name=${jobName}`
  const backOffManager = new BackOffManager(60)
  while (true) {
    const podList = await k8sApi.listNamespacedPod({
      namespace: namespace(),
      labelSelector: selector,
      limit: 1
    })

    if (!podList.items?.length) {
      await backOffManager.backOff()
      continue
    }

    if (!podList.items[0].metadata?.name) {
      throw new Error(
        `Failed to determine the name of the pod for job ${jobName}`
      )
    }
    return podList.items[0].metadata.name
  }
}

export async function deletePod(name: string): Promise<void> {
  await k8sApi.deleteNamespacedPod({
    name,
    namespace: namespace(),
    gracePeriodSeconds: 0
  })
}

// Thrown only when we can prove the exec never produced any container
// output — i.e. nothing ran yet, so retrying can't double-execute a
// non-idempotent command. Two call sites construct this:
//   - exec.exec() itself rejecting (the connection was never established)
//   - the resp callback reporting a Failure status while zero bytes were
//     ever written to stdout/stderr (the API server can report a dial
//     failure like "No agent available" *after* nominally accepting the
//     exec request, via this callback instead of a promise rejection —
//     but if nothing was ever printed, the command still never started)
class ExecConnectionError extends Error {
  // Bytes already streamed to stdout/stderr before this error, when known.
  // Only set by execLauncherOnce, so execDetachedScriptStep's retry can
  // resume its tail from the right byte offset instead of replaying output.
  bytesStreamed?: number
  // True when the stream merely went quiet. A healthy step can print nothing
  // for many minutes (test output buffered to a file). That is not a failed
  // connection, so it must not consume the retry budget.
  idle: boolean
  constructor(message?: string, bytesStreamed?: number, idle = false) {
    super(message)
    // This repo compiles to ES5 (see tsconfig.json), where extending a
    // built-in like Error breaks the prototype chain unless restored
    // manually — without this, `instanceof ExecConnectionError` is always
    // false and the retry above never fires.
    Object.setPrototypeOf(this, ExecConnectionError.prototype)
    this.bytesStreamed = bytesStreamed
    this.idle = idle
  }
}

const NO_AGENT_AVAILABLE_MAX_ATTEMPTS = 5

export async function execPodStep(
  command: string[],
  podName: string,
  containerName: string,
  stdin?: stream.Readable
): Promise<void> {
  const backOffManager = new BackOffManager()
  let attempt = 0
  while (true) {
    try {
      return await execPodStepOnce(command, podName, containerName, stdin)
    } catch (error) {
      attempt++
      if (
        !(error instanceof ExecConnectionError) ||
        !/no agent available/i.test(formatError(error)) ||
        attempt >= NO_AGENT_AVAILABLE_MAX_ATTEMPTS
      ) {
        throw error
      }
      core.warning(
        `[execPodStep] connection setup failed ("No agent available"), ` +
          `retrying (attempt ${attempt}/${NO_AGENT_AVAILABLE_MAX_ATTEMPTS}): ${formatError(error)}`
      )
      await backOffManager.backOff()
    }
  }
}

// Forwards to `target` unchanged, while recording whether any bytes ever
// passed through — the only reliable way to know if the exec'd command
// produced output, since exec.exec() takes the destination streams directly.
function trackedOutput(target: NodeJS.WritableStream): {
  stream: stream.PassThrough
  hasOutput: () => boolean
} {
  let received = false
  const passthrough = new stream.PassThrough()
  passthrough.on('data', () => {
    received = true
  })
  passthrough.pipe(target)
  return { stream: passthrough, hasOutput: () => received }
}

async function execPodStepOnce(
  command: string[],
  podName: string,
  containerName: string,
  stdin?: stream.Readable
): Promise<void> {
  const exec = new k8s.Exec(kc)
  command = fixArgs(command)
  const stdout = trackedOutput(process.stdout)
  const stderr = trackedOutput(process.stderr)
  const hasOutput = (): boolean => stdout.hasOutput() || stderr.hasOutput()
  // Exec returns a websocket. If websocket fails, we should reject the promise. Otherwise, websocket will call a callback. Since at that point, websocket is not failing, we can safely resolve or reject the promise.
  await new Promise(function (resolve, reject) {
    exec
      .exec(
        namespace(),
        podName,
        containerName,
        command,
        stdout.stream,
        stderr.stream,
        stdin ?? null,
        false /* tty */,
        resp => {
          // kube.exec returns an error if exit code is not 0, but we can't actually get the exit code
          if (resp.status === 'Success') {
            resolve(resp.code)
          } else {
            core.debug(
              JSON.stringify({
                message: resp?.message,
                details: resp?.details
              })
            )
            // No output yet means the command never started, so a dial
            // failure reported here is safe to retry the same way as a
            // connection-setup failure. Gate on the message too, not just
            // hasOutput() — anything else (e.g. a real "command not found"
            // with no output) keeps its original rejection value unchanged.
            if (
              !hasOutput() &&
              /no agent available/i.test(formatError(resp?.message))
            ) {
              reject(new ExecConnectionError(formatError(resp?.message)))
            } else {
              reject(resp?.message)
            }
          }
        }
      )
      // If exec.exec fails, explicitly reject the outer promise
      .catch(e => reject(new ExecConnectionError(formatError(e))))
  })
}

// Like trackedOutput(), but counts bytes instead of a boolean — used by
// execDetachedScriptStep to know what byte offset a reconnect's launcher
// invocation should resume tailing from.
function countedOutput(target: NodeJS.WritableStream): {
  stream: stream.PassThrough
  byteCount: () => number
} {
  let count = 0
  const passthrough = new stream.PassThrough()
  passthrough.on('data', chunk => {
    count += chunk.length
  })
  passthrough.pipe(target)
  return { stream: passthrough, byteCount: () => count }
}

// A connection that goes silent without the client-node library ever
// invoking the resp callback (no close/error status) would otherwise hang
// a detached-script step forever. Reconnect and keep tailing. Silence itself
// is not a failure: only a real connect or stream error counts toward
// DETACHED_SCRIPT_MAX_ATTEMPTS, and that count resets once a reconnect
// delivers new output.
const DETACHED_SCRIPT_IDLE_TIMEOUT_SECONDS = 120
const DETACHED_SCRIPT_MAX_ATTEMPTS = 10

async function execLauncherOnce(
  command: string[],
  podName: string,
  containerName: string
): Promise<void> {
  const exec = new k8s.Exec(kc)
  const fixedCommand = fixArgs(command)
  const stdout = countedOutput(process.stdout)
  const stderr = countedOutput(process.stderr)
  const totalBytes = (): number => stdout.byteCount() + stderr.byteCount()

  let idleTimer: ReturnType<typeof setTimeout> | undefined
  try {
    await new Promise<void>((resolve, reject) => {
      const resetIdleTimer = (): void => {
        clearTimeout(idleTimer)
        idleTimer = setTimeout(() => {
          reject(
            new ExecConnectionError(
              `no output for ${DETACHED_SCRIPT_IDLE_TIMEOUT_SECONDS}s, assuming dropped connection`,
              totalBytes(),
              true
            )
          )
        }, DETACHED_SCRIPT_IDLE_TIMEOUT_SECONDS * 1000)
      }
      resetIdleTimer()
      stdout.stream.on('data', resetIdleTimer)
      stderr.stream.on('data', resetIdleTimer)

      exec
        .exec(
          namespace(),
          podName,
          containerName,
          fixedCommand,
          stdout.stream,
          stderr.stream,
          null,
          false /* tty */,
          resp => {
            if (resp.status === 'Success') {
              resolve()
            } else if (/no agent available/i.test(formatError(resp?.message))) {
              reject(
                new ExecConnectionError(
                  formatError(resp?.message),
                  totalBytes()
                )
              )
            } else {
              reject(resp?.message)
            }
          }
        )
        .catch(e =>
          reject(new ExecConnectionError(formatError(e), totalBytes()))
        )
    })
  } finally {
    clearTimeout(idleTimer)
    // Each attempt pipes a fresh PassThrough into process.stdout/stderr;
    // without unpiping, a step that retries several times (up to
    // DETACHED_SCRIPT_MAX_ATTEMPTS) leaks listeners onto those long-lived,
    // process-wide streams for the rest of the hook process's life.
    stdout.stream.unpipe(process.stdout)
    stderr.stream.unpipe(process.stderr)
  }
}

// Runs a detached-launcher script (see writeDetachedLauncherScript) for a
// job's script step. Unlike execPodStep, a connection drop is retried
// *regardless* of whether output was already seen — re-invoking the launcher
// never replays the user's script, it only resumes watching a process that
// is already running detached from this exec session.
export async function execDetachedScriptStep(
  launcherContainerPath: string,
  podName: string,
  containerName: string
): Promise<void> {
  const backOffManager = new BackOffManager()
  let offset = 0
  let connectionFailures = 0
  while (true) {
    try {
      return await execLauncherOnce(
        ['sh', launcherContainerPath, String(offset)],
        podName,
        containerName
      )
    } catch (error) {
      if (!(error instanceof ExecConnectionError)) {
        throw error
      }
      const streamed =
        typeof error.bytesStreamed === 'number' ? error.bytesStreamed : 0
      offset += streamed
      // Output, or a stream that stayed up and merely went quiet, means this
      // reconnect reached the still-running step. The next real drop is a new
      // incident rather than one more strike.
      if (streamed > 0 || error.idle) {
        connectionFailures = 0
      }
      if (!error.idle) {
        connectionFailures++
        if (connectionFailures >= DETACHED_SCRIPT_MAX_ATTEMPTS) {
          throw error
        }
      }
      const where = `resuming from byte offset ${offset}`
      core.warning(
        error.idle
          ? `[execDetachedScriptStep] ${formatError(error)}; ${where}`
          : `[execDetachedScriptStep] connection dropped mid-stream, ${where} ` +
              `(attempt ${connectionFailures}/${DETACHED_SCRIPT_MAX_ATTEMPTS}): ${formatError(error)}`
      )
      await backOffManager.backOff()
    }
  }
}

export async function waitForJobToComplete(jobName: string): Promise<void> {
  const backOffManager = new BackOffManager()
  while (true) {
    try {
      if (await isJobSucceeded(jobName)) {
        return
      }
    } catch (error) {
      throw new Error(`job ${jobName} has failed: ${formatError(error)}`)
    }
    await backOffManager.backOff()
  }
}

export async function createDockerSecret(
  registry: Registry
): Promise<k8s.V1Secret> {
  const authContent = {
    auths: {
      [registry.serverUrl || 'https://index.docker.io/v1/']: {
        username: registry.username,
        password: registry.password,
        auth: Buffer.from(`${registry.username}:${registry.password}`).toString(
          'base64'
        )
      }
    }
  }

  const runnerInstanceLabel = new RunnerInstanceLabel()

  const secretName = getSecretName()
  const secret = new k8s.V1Secret()
  secret.immutable = true
  secret.apiVersion = 'v1'
  secret.metadata = new k8s.V1ObjectMeta()
  secret.metadata.name = secretName
  secret.metadata.namespace = namespace()
  secret.metadata.labels = {
    [runnerInstanceLabel.key]: runnerInstanceLabel.value
  }
  secret.type = 'kubernetes.io/dockerconfigjson'
  secret.kind = 'Secret'
  secret.data = {
    '.dockerconfigjson': Buffer.from(JSON.stringify(authContent)).toString(
      'base64'
    )
  }

  return await k8sApi.createNamespacedSecret({
    namespace: namespace(),
    body: secret
  })
}

export async function createSecretForEnvs(envs: {
  [key: string]: string
}): Promise<string> {
  const runnerInstanceLabel = new RunnerInstanceLabel()

  const secret = new k8s.V1Secret()
  const secretName = getSecretName()
  secret.immutable = true
  secret.apiVersion = 'v1'
  secret.metadata = new k8s.V1ObjectMeta()
  secret.metadata.name = secretName

  secret.metadata.labels = {
    [runnerInstanceLabel.key]: runnerInstanceLabel.value
  }
  secret.kind = 'Secret'
  secret.data = {}
  for (const [key, value] of Object.entries(envs)) {
    secret.data[key] = Buffer.from(value).toString('base64')
  }

  await k8sApi.createNamespacedSecret({
    namespace: namespace(),
    body: secret
  })
  return secretName
}

export async function deleteSecret(name: string): Promise<void> {
  await k8sApi.deleteNamespacedSecret({
    name,
    namespace: namespace()
  })
}

export async function pruneSecrets(): Promise<void> {
  const secretList = await k8sApi.listNamespacedSecret({
    namespace: namespace(),
    labelSelector: new RunnerInstanceLabel().toString()
  })
  if (!secretList.items.length) {
    return
  }

  await Promise.all(
    secretList.items.map(
      async secret =>
        secret.metadata?.name && (await deleteSecret(secret.metadata.name))
    )
  )
}

export async function waitForPodPhases(
  podName: string,
  awaitingPhases: Set<PodPhase>,
  backOffPhases: Set<PodPhase>,
  maxTimeSeconds = DEFAULT_WAIT_FOR_POD_TIME_SECONDS
): Promise<void> {
  const backOffManager = new BackOffManager(maxTimeSeconds)
  let phase: PodPhase = PodPhase.UNKNOWN
  try {
    while (true) {
      try {
        phase = await getPodPhase(podName)
      } catch (error) {
        // A transient failure fetching pod status (e.g. a brief API server
        // connectivity blip) isn't the same as the pod being unhealthy —
        // retry it with the same backoff budget instead of failing the
        // job on one bad network moment.
        core.warning(
          `[waitForPodPhases] failed to fetch pod status, retrying: ${formatError(error)}`
        )
        await backOffManager.backOff()
        continue
      }
      if (awaitingPhases.has(phase)) {
        return
      }

      if (!backOffPhases.has(phase)) {
        throw new Error(
          `Pod ${podName} is unhealthy with phase status ${phase}`
        )
      }
      await backOffManager.backOff()
    }
  } catch (error) {
    throw new Error(
      `Pod ${podName} is unhealthy with phase status ${phase}: ${formatError(error)}`
    )
  }
}

export function getPrepareJobTimeoutSeconds(): number {
  const envTimeoutSeconds =
    process.env['ACTIONS_RUNNER_PREPARE_JOB_TIMEOUT_SECONDS']

  if (!envTimeoutSeconds) {
    return DEFAULT_WAIT_FOR_POD_TIME_SECONDS
  }

  const timeoutSeconds = parseInt(envTimeoutSeconds, 10)
  if (!timeoutSeconds || timeoutSeconds <= 0) {
    core.warning(
      `Prepare job timeout is invalid ("${timeoutSeconds}"): use an int > 0`
    )
    return DEFAULT_WAIT_FOR_POD_TIME_SECONDS
  }

  return timeoutSeconds
}

async function getPodPhase(name: string): Promise<PodPhase> {
  const podPhaseLookup = new Set<string>([
    PodPhase.PENDING,
    PodPhase.RUNNING,
    PodPhase.SUCCEEDED,
    PodPhase.FAILED,
    PodPhase.UNKNOWN
  ])
  const pod = await k8sApi.readNamespacedPod({
    name,
    namespace: namespace()
  })

  if (!pod.status?.phase || !podPhaseLookup.has(pod.status.phase)) {
    return PodPhase.UNKNOWN
  }
  return pod.status?.phase as PodPhase
}

async function isJobSucceeded(name: string): Promise<boolean> {
  const job = await k8sBatchV1Api.readNamespacedJob({
    name,
    namespace: namespace()
  })
  if (job.status?.failed) {
    throw new Error(`job ${name} has failed`)
  }
  return !!job.status?.succeeded
}

export async function getPodLogs(
  podName: string,
  containerName: string
): Promise<void> {
  const log = new k8s.Log(kc)
  const logStream = new stream.PassThrough()
  logStream.on('data', chunk => {
    // use write rather than console.log to prevent double line feed
    process.stdout.write(chunk)
  })

  logStream.on('error', err => {
    process.stderr.write(err.message)
  })

  await log.log(namespace(), podName, containerName, logStream, {
    follow: true,
    pretty: false,
    timestamps: false
  })
  await new Promise(resolve => logStream.on('end', () => resolve(null)))
}

export async function prunePods(): Promise<void> {
  const podList = await k8sApi.listNamespacedPod({
    namespace: namespace(),
    labelSelector: new RunnerInstanceLabel().toString()
  })
  if (!podList.items.length) {
    return
  }

  await Promise.all(
    podList.items.map(
      async pod => pod.metadata?.name && (await deletePod(pod.metadata.name))
    )
  )
}

export async function getPodStatus(
  name: string
): Promise<k8s.V1PodStatus | undefined> {
  const backOffManager = new BackOffManager(30)
  while (true) {
    try {
      const pod = await k8sApi.readNamespacedPod({
        name,
        namespace: namespace()
      })
      return pod.status
    } catch (error) {
      core.warning(
        `[getPodStatus] failed to read pod status, retrying: ${formatError(error)}`
      )
      await backOffManager.backOff()
    }
  }
}

function withTimeout<T>(promise: Promise<T>, seconds: number): Promise<T> {
  return Promise.race([
    promise,
    new Promise<T>((_, reject) =>
      setTimeout(
        () => reject(new Error(`request timed out after ${seconds}s`)),
        seconds * 1000
      )
    )
  ])
}

export async function isAuthPermissionsOK(): Promise<boolean> {
  const backOffManager = new BackOffManager(DEFAULT_AUTH_CHECK_TIMEOUT_SECONDS)
  while (true) {
    try {
      const asyncs: Promise<k8s.V1SelfSubjectAccessReview>[] = []
      for (const resource of requiredPermissions) {
        for (const verb of resource.verbs) {
          const sar = new k8s.V1SelfSubjectAccessReview()
          sar.spec = new k8s.V1SelfSubjectAccessReviewSpec()
          sar.spec.resourceAttributes = new k8s.V1ResourceAttributes()
          sar.spec.resourceAttributes.verb = verb
          sar.spec.resourceAttributes.namespace = namespace()
          sar.spec.resourceAttributes.group = resource.group
          sar.spec.resourceAttributes.resource = resource.resource
          sar.spec.resourceAttributes.subresource = resource.subresource
          asyncs.push(
            withTimeout(
              k8sAuthorizationV1Api.createSelfSubjectAccessReview({
                body: sar
              }),
              SSAR_REQUEST_TIMEOUT_SECONDS
            )
          )
        }
      }
      const responses = await Promise.all(asyncs)
      return responses.every(resp => resp.status?.allowed)
    } catch (error) {
      core.warning(
        `isAuthPermissionsOK: SSAR check failed, retrying: ${formatError(error)}`
      )
      await backOffManager.backOff()
    }
  }
}

export async function isPodContainerAlpine(
  podName: string,
  containerName: string
): Promise<boolean> {
  let isAlpine = true
  try {
    await execPodStep(
      [
        'sh',
        '-c',
        `'[ $(cat /etc/*release* | grep -i -e "^ID=*alpine*" -c) != 0 ] || exit 1'`
      ],
      podName,
      containerName
    )
  } catch {
    isAlpine = false
  }

  return isAlpine
}

async function getCurrentNodeName(): Promise<string> {
  const resp = await k8sApi.readNamespacedPod({
    name: getRunnerPodName(),
    namespace: namespace()
  })

  const nodeName = resp.spec?.nodeName
  if (!nodeName) {
    throw new Error('Failed to determine node name')
  }
  return nodeName
}

async function getPodAffinity(nodeName: string): Promise<k8s.V1Affinity> {
  const affinity = new k8s.V1Affinity()
  affinity.nodeAffinity = new k8s.V1NodeAffinity()
  affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution =
    new k8s.V1NodeSelector()
  affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms =
    [
      {
        matchExpressions: [
          {
            key: 'kubernetes.io/hostname',
            operator: 'In',
            values: [nodeName]
          }
        ]
      }
    ]
  return affinity
}

export function namespace(): string {
  if (process.env['ACTIONS_RUNNER_KUBERNETES_NAMESPACE']) {
    return process.env['ACTIONS_RUNNER_KUBERNETES_NAMESPACE']
  }

  const context = kc.getContexts().find(ctx => ctx.namespace)
  if (!context?.namespace) {
    throw new Error(
      'Failed to determine namespace, falling back to `default`. Namespace should be set in context, or in env variable "ACTIONS_RUNNER_KUBERNETES_NAMESPACE"'
    )
  }
  return context.namespace
}

class BackOffManager {
  private backOffSeconds = 1
  totalTime = 0
  constructor(private throwAfterSeconds?: number) {
    if (!throwAfterSeconds || throwAfterSeconds < 0) {
      this.throwAfterSeconds = undefined
    }
  }

  async backOff(): Promise<void> {
    await new Promise(resolve =>
      setTimeout(resolve, this.backOffSeconds * 1000)
    )
    this.totalTime += this.backOffSeconds
    if (this.throwAfterSeconds && this.throwAfterSeconds < this.totalTime) {
      throw new Error('backoff timeout')
    }
    if (this.backOffSeconds < 20) {
      this.backOffSeconds *= 2
    }
    if (this.backOffSeconds > 20) {
      this.backOffSeconds = 20
    }
  }
}

export function containerPorts(
  container: ContainerInfo
): k8s.V1ContainerPort[] {
  const ports: k8s.V1ContainerPort[] = []
  if (!container.portMappings?.length) {
    return ports
  }
  for (const portDefinition of container.portMappings) {
    const portProtoSplit = portDefinition.split('/')
    if (portProtoSplit.length > 2) {
      throw new Error(`Unexpected port format: ${portDefinition}`)
    }

    const port = new k8s.V1ContainerPort()
    port.protocol =
      portProtoSplit.length === 2 ? portProtoSplit[1].toUpperCase() : 'TCP'

    const portSplit = portProtoSplit[0].split(':')
    if (portSplit.length > 2) {
      throw new Error('ports should have at most one ":" separator')
    }

    const parsePort = (p: string): number => {
      const num = Number(p)
      if (!Number.isInteger(num) || num < 1 || num > 65535) {
        throw new Error(`invalid container port: ${p}`)
      }
      return num
    }

    if (portSplit.length === 1) {
      port.containerPort = parsePort(portSplit[0])
    } else {
      port.hostPort = parsePort(portSplit[0])
      port.containerPort = parsePort(portSplit[1])
    }

    ports.push(port)
  }
  return ports
}

export async function getPodByName(name): Promise<k8s.V1Pod> {
  return await k8sApi.readNamespacedPod({
    name,
    namespace: namespace()
  })
}

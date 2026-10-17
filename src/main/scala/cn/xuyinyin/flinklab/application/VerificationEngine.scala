package cn.xuyinyin.flinklab.application

/** 结果验证器：只根据 Operator 返回的可观察证据判断操作是否完成，避免把“已提交”误报成“已成功”。 */
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.operator.observer.ResourceObservation
import cn.xuyinyin.flinklab.operator.watch.{FlinkStateSnapshotStatus, FlinkStatusSnapshot}

import java.time.Instant

/** 一次观察到的 Kubernetes 证据快照，包含判断成功所需的版本、状态和原始 JSON。 */
final case class Evidence(
    resource: ResourceRef,
    resourceVersion: Option[ResourceVersion],
    uid: Option[ResourceUid],
    generation: Option[Generation],
    observedGeneration: Option[Generation],
    reconciliation: ReconciliationState,
    jobState: JobState,
    snapshotState: Option[String],
    snapshotPath: Option[SnapshotPath],
    capturedAt: Instant,
    raw: String,
    savepointDirectory: Option[SnapshotPath] = None,
    snapshotError: Option[String] = None,
    snapshotFailures: Option[String] = None,
    actualProtection: Option[ActualProtection] = None
):
  /** 缩小证据字段后写入 operation 事件，避免把整份 CR 无限复制到审计记录。 */
  def audit(reason: String): VerificationEvidence =
    VerificationEvidence(resource, resourceVersion, uid, generation, observedGeneration, snapshotState, snapshotPath, reason)

/** 从观察结果提取可审计证据，不在这里猜测缺失字段的含义。 */
object Evidence:
  /** 将统一的 ResourceObservation 转成部署或快照对应的 Evidence。 */
  def fromObservation(namespace: Namespace, observation: ResourceObservation): Evidence =
    val resource = ResourceRef(namespace, observation.kind, DeploymentName.unsafe(observation.name), observation.uid.flatMap(ResourceUid.from(_).toOption))
    val rv = observation.resourceVersion.flatMap(ResourceVersion.from(_).toOption)
    val generation = observation.generation.flatMap(Generation.from(_).toOption)
    val observedGeneration = observation.observedGeneration.flatMap(Generation.from(_).toOption)
    observation.kind match
      case ResourceKind.StateSnapshot =>
        val snapshot = FlinkStateSnapshotStatus.fromJsonString(observation.payload).toOption
        Evidence(resource, rv, resource.uid, generation, observedGeneration, ReconciliationState.Unknown, JobState.Unknown, snapshot.flatMap(_.state), snapshot.flatMap(_.path.flatMap(SnapshotPath.from(_).toOption)), Instant.now(), observation.payload, snapshotError = snapshot.flatMap(_.error), snapshotFailures = snapshot.flatMap(_.failures))
      case _ =>
        val status = FlinkStatusSnapshot.fromJsonString(observation.payload).toOption
        val configuration = scala.util.Try(ujson.read(observation.payload)).toOption.flatMap(_.obj.get("spec").flatMap(_.objOpt)).flatMap(_.get("flinkConfiguration")).flatMap(_.objOpt).flatMap(_.get("state.savepoints.dir")).flatMap(_.strOpt).flatMap(SnapshotPath.from(_).toOption)
        Evidence(resource, rv, resource.uid, generation, observedGeneration, reconciliation(status.flatMap(_.reconciliationState), status.flatMap(_.error)), jobState(status.flatMap(_.jobState), status.flatMap(_.lifecycleState), observation.payload), None, savepointPath(status), Instant.now(), observation.payload, configuration, actualProtection = actualProtection(observation.payload, status))

  private def reconciliation(value: Option[String], error: Option[String]): ReconciliationState =
    if error.exists(_.trim.nonEmpty) then ReconciliationState.Error
    else value.map(_.toUpperCase) match
      case Some("DEPLOYED") | Some("READY") => ReconciliationState.Ready
      case Some("RECONCILING") | Some("UPGRADING") => ReconciliationState.Reconciling
      case Some("ERROR") | Some("FAILED") => ReconciliationState.Error
      case _ => ReconciliationState.Unknown

  private def jobState(value: Option[String], lifecycle: Option[String], raw: String): JobState = value.map(_.toUpperCase) match
    // Operator 的 savepoint 暂停会把 Job 标为 FINISHED；必须同时有生命周期和已协调声明，避免误认自然结束。
    case Some("FINISHED") if lifecycle.exists(_.equalsIgnoreCase("SUSPENDED")) &&
      reconciledJob(raw).flatMap(_.obj.get("state")).flatMap(_.strOpt).exists(_.equalsIgnoreCase("suspended")) => JobState.Suspended
    case Some("RUNNING") => JobState.Running
    case Some("SUSPENDED") => JobState.Suspended
    case Some("FINISHED") => JobState.Finished
    case Some("FAILED") => JobState.Failed
    case _ => JobState.Unknown

  private def reconciledJob(raw: String): Option[ujson.Value] =
    for
      parsed <- scala.util.Try(ujson.read(raw)).toOption
      statusObject <- parsed.obj.get("status").flatMap(_.objOpt)
      reconciliation <- statusObject.get("reconciliationStatus").flatMap(_.objOpt)
      reconciled <- reconciliation.get("lastReconciledSpec").flatMap(_.strOpt)
      spec <- scala.util.Try(ujson.read(reconciled)).toOption.flatMap(_.obj.get("spec").flatMap(_.objOpt))
      job <- spec.get("job").filter(_.objOpt.nonEmpty)
    yield job

  private def actualProtection(raw: String, status: Option[FlinkStatusSnapshot]): Option[ActualProtection] =
    val mode = reconciledJob(raw).flatMap(_.obj.get("upgradeMode")).flatMap(_.strOpt).map(_.trim.toLowerCase)
    mode.flatMap {
      case "last-state" | "laststate" => Some(ActualProtection.LastState)
      case "savepoint" => savepointPath(status).map(ActualProtection.Savepoint.apply)
      case "stateless" => Some(ActualProtection.EmptyState)
      case _ => None
    }

  private def savepointPath(status: Option[FlinkStatusSnapshot]): Option[SnapshotPath] =
    status.flatMap(value => value.savepoint.flatMap(_.upgradeSavepointPath).orElse(value.lastSavepointLocation)).flatMap(SnapshotPath.from(_).toOption)

final case class VerificationResult(evidence: Evidence, reason: String)

trait VerificationEngine:
  /** 根据请求意图、目标身份、提交 generation 和当前 Evidence 判定结果。 */
  def verify(
      operation: FlinkOperation,
      expected: ResourceRef,
      evidence: Evidence,
      expectedGeneration: Option[Generation] = None
  ): Either[ControlPlaneError, VerificationResult]

object DefaultVerificationEngine extends VerificationEngine:
  /** 严格策略要求对应的实际保护证据；READY 本身不足以代表成功。 */
  override def verify(
      operation: FlinkOperation,
      expected: ResourceRef,
      evidence: Evidence,
      expectedGeneration: Option[Generation]
  ): Either[ControlPlaneError, VerificationResult] =
    if evidence.resource.namespace != expected.namespace || evidence.resource.kind != expected.kind || evidence.resource.name != expected.name then
      Left(ControlPlaneError.VerificationFailed("observed resource identity does not match operation target"))
    else if expected.uid.nonEmpty && evidence.uid != expected.uid then
      Left(ControlPlaneError.VerificationFailed("observed resource UID does not match operation target"))
    else if expectedGeneration.nonEmpty && (evidence.generation != expectedGeneration || evidence.observedGeneration != expectedGeneration) then
      Left(ControlPlaneError.VerificationFailed("observed resource does not reconcile the submitted generation"))
    else if evidence.generation.nonEmpty && evidence.observedGeneration.exists(observed => evidence.generation.exists(_ != observed)) then
      Left(ControlPlaneError.VerificationFailed("observedGeneration does not match generation"))
    else
      operation match
        case FlinkOperation.Deploy(spec) if spec.requestsSavepointRestore =>
          restore(spec, evidence)
        case FlinkOperation.Upgrade(_, spec, _) if spec.requestsSavepointRestore =>
          restore(spec, evidence)
        case FlinkOperation.Suspend(_, policy) =>
          for
            _ <- if policy == SuspendPolicy.KeepState then protection(UpgradePolicy(StateProtection.Savepoint, FallbackPolicy.Forbidden), evidence) else Right(())
            result <- if evidence.jobState == JobState.Suspended && evidence.reconciliation == ReconciliationState.Ready then Right(VerificationResult(evidence, "job is suspended and reconciled"))
              else Left(ControlPlaneError.VerificationFailed(s"expected SUSPENDED with READY reconciliation, observed ${evidence.jobState}/${evidence.reconciliation}"))
          yield result
        case FlinkOperation.Resume(_) =>
          // FINISHED 可能是暂停前的旧作业，不能证明本次恢复已经运行；宁可继续观察或超时。
          if evidence.jobState == JobState.Running && evidence.reconciliation == ReconciliationState.Ready then
            Right(VerificationResult(evidence, "resumed job is running and reconciled"))
          else Left(ControlPlaneError.VerificationFailed(s"resume requires RUNNING with READY reconciliation, observed ${evidence.jobState}/${evidence.reconciliation}"))
        case FlinkOperation.Snapshot(_, _) =>
          if evidence.snapshotState.exists(_.equalsIgnoreCase("COMPLETED")) && evidence.snapshotPath.nonEmpty then Right(VerificationResult(evidence, "snapshot completed"))
          else
            val reason = evidence.snapshotError.orElse(evidence.snapshotFailures).getOrElse("snapshot has not completed with a result path")
            Left(ControlPlaneError.VerificationFailed(reason))
        case FlinkOperation.Upgrade(_, _, policy) =>
          protection(policy, evidence).flatMap(_ => ready(evidence, s"job is ${evidence.jobState} and reconciled"))
        case FlinkOperation.Restart(_, policy) =>
          protection(policy, evidence).flatMap(_ => ready(evidence, s"job is ${evidence.jobState} and reconciled"))
        case FlinkOperation.Delete(_, _) =>
          Left(ControlPlaneError.VerificationFailed("delete requires confirmation that the target resource is absent"))
        case _ =>
          ready(evidence, s"job is ${evidence.jobState} and reconciled")

  /** 恢复验证使用持久化的原始 path/nonce，重启后也不把普通 READY 当成恢复完成。 */
  private def restore(spec: FlinkDeploymentSpec, evidence: Evidence): Either[ControlPlaneError, VerificationResult] =
    val reconciled = for
      parsed <- scala.util.Try(ujson.read(evidence.raw)).toOption
      status <- parsed.obj.get("status").flatMap(_.objOpt)
      reconciliation <- status.get("reconciliationStatus").flatMap(_.objOpt)
      raw <- reconciliation.get("lastReconciledSpec").flatMap(_.strOpt)
      value <- scala.util.Try(ujson.read(raw)).toOption
      current <- value.obj.get("spec").flatMap(_.objOpt)
      job <- current.get("job").flatMap(_.objOpt)
    yield (current, job)
    val matches = reconciled.exists { (current, job) =>
      val nonce = job.get("savepointRedeployNonce").flatMap(value => scala.util.Try(BigDecimal(value.strOpt.getOrElse(value.num.toString)).bigDecimal.longValueExact()).toOption)
      val path = job.get("initialSavepointPath").flatMap(_.strOpt)
      val mode = job.get("upgradeMode").flatMap(_.strOpt)
      val fallback = current.get("flinkConfiguration").flatMap(_.objOpt).flatMap(_.get("kubernetes.operator.job.upgrade.last-state-fallback.enabled"))
      spec.job.stateProtection == StateProtection.Savepoint && spec.job.desiredState == DesiredJobState.Running &&
        spec.job.initialSavepointPath.exists(value => path.contains(value.snapshotPathValue)) &&
        spec.job.savepointRedeployNonce.nonEmpty && nonce == spec.job.savepointRedeployNonce && mode.contains("savepoint") &&
        fallback.exists(value => value.strOpt.contains("false") || value.boolOpt.contains(false))
    }
    if matches then ready(evidence, "explicit savepoint path and redeploy nonce are reconciled")
    else Left(ControlPlaneError.VerificationFailed("explicit restore path, redeploy nonce or forbidden fallback is not reconciled"))

  private def protection(policy: UpgradePolicy, evidence: Evidence): Either[ControlPlaneError, Unit] =
    evidence.actualProtection match
      case None => Left(ControlPlaneError.VerificationFailed("state protection evidence is unavailable"))
      case Some(actual) if matches(policy.protection, actual) => Right(())
      case Some(ActualProtection.LastState) if policy.protection == StateProtection.Savepoint && policy.fallback == FallbackPolicy.AllowLastState => Right(())
      case Some(actual) => Left(ControlPlaneError.VerificationFailed(s"requested ${policy.protection} but observed ${actual}"))

  private def matches(requested: StateProtection, actual: ActualProtection): Boolean =
    (requested, actual) match
      case (StateProtection.Stateless, ActualProtection.EmptyState) => true
      case (StateProtection.LastState, ActualProtection.LastState) => true
      case (StateProtection.Savepoint, ActualProtection.Savepoint(_)) => true
      case _ => false

  private def ready(evidence: Evidence, reason: String): Either[ControlPlaneError, VerificationResult] =
    if (evidence.jobState == JobState.Running || evidence.jobState == JobState.Finished) && evidence.reconciliation == ReconciliationState.Ready then
      Right(VerificationResult(evidence, reason))
    else Left(ControlPlaneError.VerificationFailed(s"expected RUNNING or FINISHED with READY reconciliation, observed ${evidence.jobState}/${evidence.reconciliation}"))

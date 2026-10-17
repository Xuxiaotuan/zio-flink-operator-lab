package cn.xuyinyin.flinklab.application

/** 操作策略校验：在真正写入 Kubernetes 之前检查目标状态、状态保护方式和回退策略。 */
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.ControlPlaneError.*
import zio.*

/** 在副作用发生前检查操作是否符合当前资源和状态保护策略。 */
trait PolicyEngine:
  def validate(operation: FlinkOperation, current: Option[ObservedJobState]): IO[ControlPlaneError, ValidatedOperation]

final class DefaultPolicyEngine extends PolicyEngine:
  /** 校验并规范化操作；返回的操作才允许提交给 Operator。 */
  override def validate(operation: FlinkOperation, current: Option[ObservedJobState]): IO[ControlPlaneError, ValidatedOperation] =
    operation match
      case operation @ FlinkOperation.Upgrade(target, spec, policy) =>
        for
          _ <- ZIO.fromEither(policy.validate)
          _ <- if spec.requestsSavepointRestore then validateRestore(spec) *>
            ZIO.fail(InvalidPolicy("explicit restore forbids fallback")).unless(policy.fallback == FallbackPolicy.Forbidden && policy.protection == StateProtection.Savepoint) else ZIO.unit
          observed <- ZIO.fromOption(current).orElseFail(InvalidPolicy("upgrade requires an observed target resource"))
          _ <- ZIO.fail(InvalidPolicy("upgrade target is not RUNNING")).unless(observed.jobState == JobState.Running)
          _ <- policy.protection match
            case StateProtection.Savepoint =>
              ZIO.fail(InvalidPolicy("savepoint storage is not configured")).unless(observed.savepointDirectory.nonEmpty)
            case StateProtection.LastState =>
              ZIO.fail(InvalidPolicy("checkpointing is not configured")).unless(spec.flinkConfiguration.keys.exists(_.contains("checkpointing.interval")))
            case _ => ZIO.unit
          _ <- ZIO.fail(InvalidPolicy("operation target does not match observed resource")).unless(observed.resource.namespace == target.namespace && observed.resource.kind == target.kind && observed.resource.name == target.name)
          normalizedJob = spec.job.copy(stateProtection = policy.protection, desiredState = DesiredJobState.Running)
          fallback = "kubernetes.operator.job.upgrade.last-state-fallback.enabled" -> (policy.fallback == FallbackPolicy.AllowLastState).toString
          normalized = spec.copy(job = normalizedJob, flinkConfiguration = spec.flinkConfiguration.updated(fallback._1, fallback._2))
        yield ValidatedOperation(operation.copy(spec = normalized), Some(observed))
      case operation @ FlinkOperation.Restart(target, policy) =>
        for
          _ <- ZIO.fromEither(policy.validate)
          observed <- ZIO.fromOption(current).orElseFail(InvalidPolicy("restart requires an observed target resource"))
          _ <- ZIO.fail(InvalidPolicy("restart target is not RUNNING")).unless(observed.jobState == JobState.Running)
          _ <- ZIO.fail(InvalidPolicy("savepoint storage is not configured")).when(policy.protection == StateProtection.Savepoint && observed.savepointDirectory.isEmpty)
          _ <- ZIO.fail(InvalidPolicy("restart target does not match observed resource")).unless(observed.resource.namespace == target.namespace && observed.resource.kind == target.kind && observed.resource.name == target.name)
        yield ValidatedOperation(operation, Some(observed))
      case FlinkOperation.Snapshot(_, _) =>
        ZIO.succeed(ValidatedOperation(operation, current))
      case FlinkOperation.Delete(target, policy) =>
        val boundTarget = current.flatMap(_.resource.uid).map(uid => target.copy(uid = Some(uid))).getOrElse(target)
        ZIO.succeed(ValidatedOperation(FlinkOperation.Delete(boundTarget, policy), current))
      case FlinkOperation.Deploy(spec) if spec.requestsSavepointRestore =>
        // 显式恢复不等于普通 upsert：原始 path/nonce 仍在持久化命令中，提交和恢复验证均据此判断。
        validateRestore(spec).as(ValidatedOperation(FlinkOperation.Deploy(spec.copy(flinkConfiguration = spec.flinkConfiguration.updated(
          "kubernetes.operator.job.upgrade.last-state-fallback.enabled", "false"))), current))
      case FlinkOperation.Deploy(_) =>
        ZIO.fail(InvalidPolicy("deployment already exists; use upgrade or an explicit savepoint restore")).when(current.nonEmpty)
          .as(ValidatedOperation(operation, current))
      case FlinkOperation.Suspend(_, _) | FlinkOperation.Resume(_) =>
        ZIO.succeed(ValidatedOperation(operation, current))

  /** 指定恢复必须同时声明路径、精确正 nonce、savepoint 模式和运行目标，不能静默降级为普通变更。 */
  private def validateRestore(spec: FlinkDeploymentSpec): IO[ControlPlaneError, Unit] =
    ZIO.fail(InvalidPolicy("explicit restore requires initialSavepointPath, a positive safe savepointRedeployNonce, savepoint mode and running state"))
      .unless(spec.job.initialSavepointPath.nonEmpty && spec.job.savepointRedeployNonce.exists(n => n > 0 && n <= 9007199254740991L) &&
        spec.job.stateProtection == StateProtection.Savepoint && spec.job.desiredState == DesiredJobState.Running).unit

object PolicyEngine:
  val live: ZLayer[Any, Nothing, PolicyEngine] = ZLayer.succeed(new DefaultPolicyEngine)

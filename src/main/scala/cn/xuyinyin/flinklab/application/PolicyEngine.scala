package cn.xuyinyin.flinklab.application

import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.ControlPlaneError.*
import zio.*

trait PolicyEngine:
  def validate(operation: FlinkOperation, current: Option[ObservedJobState]): IO[ControlPlaneError, ValidatedOperation]

final class DefaultPolicyEngine extends PolicyEngine:
  override def validate(operation: FlinkOperation, current: Option[ObservedJobState]): IO[ControlPlaneError, ValidatedOperation] =
    operation match
      case operation @ FlinkOperation.Upgrade(target, spec, policy) =>
        for
          _ <- ZIO.fromEither(policy.validate)
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
          _ <- ZIO.fail(InvalidPolicy("restart target does not match observed resource")).unless(observed.resource.namespace == target.namespace && observed.resource.kind == target.kind && observed.resource.name == target.name)
        yield ValidatedOperation(operation, Some(observed))
      case FlinkOperation.Snapshot(_, _) =>
        ZIO.succeed(ValidatedOperation(operation, current))
      case FlinkOperation.Delete(target, policy) =>
        val boundTarget = current.flatMap(_.resource.uid).map(uid => target.copy(uid = Some(uid))).getOrElse(target)
        ZIO.succeed(ValidatedOperation(FlinkOperation.Delete(boundTarget, policy), current))
      case FlinkOperation.Deploy(_) | FlinkOperation.Suspend(_, _) | FlinkOperation.Resume(_) =>
        ZIO.succeed(ValidatedOperation(operation, current))

object PolicyEngine:
  val live: ZLayer[Any, Nothing, PolicyEngine] = ZLayer.succeed(new DefaultPolicyEngine)

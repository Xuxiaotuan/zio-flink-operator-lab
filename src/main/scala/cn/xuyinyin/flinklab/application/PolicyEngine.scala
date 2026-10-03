package cn.xuyinyin.flinklab.application

import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.ControlPlaneError.*
import zio.*

trait PolicyEngine:
  def validate(operation: FlinkOperation, current: Option[ObservedJobState]): IO[ControlPlaneError, ValidatedOperation]

final class DefaultPolicyEngine extends PolicyEngine:
  override def validate(operation: FlinkOperation, current: Option[ObservedJobState]): IO[ControlPlaneError, ValidatedOperation] =
    operation match
      case FlinkOperation.Upgrade(target, _, policy) =>
        for
          _ <- ZIO.fromEither(policy.validate)
          observed <- ZIO.fromOption(current).orElseFail(InvalidPolicy("upgrade requires an observed target resource"))
          _ <- ZIO.fail(InvalidPolicy("upgrade target is not RUNNING")).unless(observed.jobState == JobState.Running)
          _ <- policy.protection match
            case StateProtection.Savepoint =>
              ZIO.fail(InvalidPolicy("savepoint storage is not configured")).unless(observed.savepointDirectory.nonEmpty)
            case _ => ZIO.unit
          _ <- ZIO.fail(InvalidPolicy("operation target does not match observed resource")).unless(observed.resource.namespace == target.namespace && observed.resource.kind == target.kind && observed.resource.name == target.name)
        yield ValidatedOperation(operation, Some(observed))
      case FlinkOperation.Snapshot(_, _) =>
        ZIO.succeed(ValidatedOperation(operation, current))
      case FlinkOperation.Deploy(_) | FlinkOperation.Suspend(_, _) | FlinkOperation.Resume(_) | FlinkOperation.Restart(_, _) | FlinkOperation.Delete(_, _) =>
        ZIO.succeed(ValidatedOperation(operation, current))

object PolicyEngine:
  val live: ZLayer[Any, Nothing, PolicyEngine] = ZLayer.succeed(new DefaultPolicyEngine)

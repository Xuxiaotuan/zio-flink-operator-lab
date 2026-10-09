package cn.xuyinyin.flinklab.application
/** 验证状态保护、savepoint、last-state 和回退策略的组合约束。 */

import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import zio.*
import zio.test.*

object PolicyEngineSpec extends ZIOSpecDefault:
  private val namespace = Namespace.unsafe("analytics")
  private val name = DeploymentName.unsafe("orders")
  private val target = ResourceRef(namespace, ResourceKind.Deployment, name)
  private val deploymentSpec = FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.WordCount", 1, StateProtection.Savepoint))

  private val running = ObservedJobState(
    target,
    JobState.Running,
    ReconciliationState.Ready,
    Generation.from(4).toOption,
    Generation.from(4).toOption,
    SnapshotPath.from("s3://flink-savepoints/orders/").toOption
  )

  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("policy engine")(
      test("requires storage evidence for a strict savepoint upgrade") {
        val missingStorage = running.copy(savepointDirectory = None)
        val operation = FlinkOperation.Upgrade(target, deploymentSpec, UpgradePolicy(StateProtection.Savepoint, FallbackPolicy.Forbidden))
        for
          result <- ZIO.serviceWithZIO[PolicyEngine](_.validate(operation, Some(missingStorage))).either
        yield assertTrue(result.left.exists(_.message.contains("savepoint storage")))
      },
      test("validates a running strict savepoint upgrade") {
        val operation = FlinkOperation.Upgrade(target, deploymentSpec, UpgradePolicy(StateProtection.Savepoint, FallbackPolicy.Forbidden))
        for
          result <- ZIO.serviceWithZIO[PolicyEngine](_.validate(operation, Some(running))).either
        yield assertTrue(result.isRight)
      }
    )
      .provide(PolicyEngine.live)

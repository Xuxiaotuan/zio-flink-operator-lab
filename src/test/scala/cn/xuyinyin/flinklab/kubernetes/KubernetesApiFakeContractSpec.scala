package cn.xuyinyin.flinklab.kubernetes
/** 验证业务调用经过 KubernetesApi 端口时使用正确的资源、路径和请求体。 */

import cn.xuyinyin.flinklab.cli.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.Namespace
import cn.xuyinyin.flinklab.operator.OperatorProgram
import cn.xuyinyin.flinklab.operator.watch.WatchEventType
import zio.*
import zio.test.*

object KubernetesApiFakeContractSpec extends ZIOSpecDefault:
  def spec =
    suite("kubernetes api fake contract")(
      test("apply sends the requested CR identity and JSON body") {
        for
          fake <- FakeKubernetesApi.make
          _ <- OperatorProgram.execute(
            Command.Apply(ResourceKind.Deployment, Map("name" -> "orders", "namespace" -> "analytics"), dryRun = true)
          ).provide(ZLayer.succeed(fake))
          records <- fake.records.get
        yield assertTrue(
          records.exists(_.startsWith("apply:analytics:flinkdeployment:orders:")),
          records.exists(_.contains("flink.apache.org/v1beta1"))
        )
      },
      test("savepoint sends an exact merge patch to the fake client") {
        for
          fake <- FakeKubernetesApi.make
          _ <- OperatorProgram.execute(
            Command.Savepoint(
              ResourceKind.Deployment,
              Map("name" -> "orders", "namespace" -> "analytics", "nonce" -> "42")
            )
          ).provide(ZLayer.succeed(fake))
          records <- fake.records.get
          patch = records.find(_.startsWith("patch:")).getOrElse("")
        yield assertTrue(
          patch.startsWith("patch:analytics:flinkdeployment:orders:"),
          patch.contains("savepointTriggerNonce"),
          !patch.contains("flinkConfiguration")
        )
      },
      test("watch returns events through the same client boundary") {
        for
          fake <- FakeKubernetesApi.make
          events <- fake.watch(Namespace.unsafe("analytics"), ResourceKind.Deployment, "orders").runCollect
        yield assertTrue(events.size == 1, events.head.eventType == WatchEventType.Modified)
      }
    )

package cn.xuyinyin.flinklab.domain
/** 验证 FlinkDeployment、FlinkStateSnapshot 和 opaque type 的资源渲染。 */

import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import zio.test.*

object FlinkResourcesSpec extends ZIOSpecDefault:
  def spec =
    suite("flink resources")(
      test("renders a FlinkDeployment v1beta1 resource") {
        val namespace = Namespace.unsafe("analytics")
        val name = DeploymentName.unsafe("orders")
        val jar = JobJarUri.unsafe("local:///opt/flink/examples/streaming/WordCount.jar")
        val json = ujson.read(FlinkResources.deployment(FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(jar, "example.WordCount", 2))))
        assertTrue(
          json("apiVersion").str == "flink.apache.org/v1beta1",
          json("kind").str == "FlinkDeployment",
          json("metadata")("name").str == "orders",
          json("spec")("job")("parallelism").num == 2
        )
      },
      test("validates opaque types at the boundary") {
        assertTrue(
          Namespace.from("").isLeft,
          DeploymentName.from("Orders").isLeft,
          JobJarUri.from("/tmp/job.jar").isLeft,
          JobJarUri.from("https://example.test/job.jar").isRight
        )
      },
      test("renders a FlinkStateSnapshot savepoint request") {
        val json = ujson.read(
          FlinkResources.render(
            FlinkStateSnapshotSpec(
              Namespace.unsafe("analytics"),
              DeploymentName.unsafe("orders-sp-1"),
              cn.xuyinyin.flinklab.domain.ResourceKind.Deployment,
              DeploymentName.unsafe("orders"),
              SnapshotType.Savepoint
            ).resource
          )
        )
        assertTrue(
          json("kind").str == "FlinkStateSnapshot",
          json("spec")("jobReference")("name").str == "orders",
          json("spec")("savepoint")("formatType").str == "CANONICAL"
        )
      }
    )

package cn.xuyinyin.flinklab.application

import cn.xuyinyin.flinklab.cli.{Command, ResourceKind}
import cn.xuyinyin.flinklab.domain.FlinkOperation
import zio.test.*

object OperationFactorySpec extends ZIOSpecDefault:
  def spec = suite("operation factory")(
    test("turns CLI apply into a typed deploy operation") {
      val command = Command.Apply(ResourceKind.Deployment, Map("name" -> "orders", "namespace" -> "analytics", "jar-uri" -> "local:///job.jar", "entry-class" -> "example.WordCount"), dryRun = false)
      val result = FlinkOperationFactory.fromCommand(command)
      assertTrue(result.exists(_.isInstanceOf[FlinkOperation.Deploy]))
    },
    test("turns an HTTP deployment document into the same typed operation") {
      val result = FlinkOperationFactory.fromDeploymentJson("analytics", """{"kind":"FlinkDeployment","metadata":{"name":"orders"},"spec":{"image":"flink:1.20.1","flinkVersion":"v1_20","job":{"jarURI":"local:///job.jar","entryClass":"example.WordCount","parallelism":2}}}""")
      assertTrue(result.exists(_.isInstanceOf[FlinkOperation.Deploy]))
    },
    test("rejects a misspelled state protection instead of defaulting to stateless") {
      val command = Command.Apply(ResourceKind.Deployment, Map("name" -> "orders", "upgrade-mode" -> "savpoint"), dryRun = false)
      assertTrue(FlinkOperationFactory.fromCommand(command).left.exists(_.contains("unknown state protection")))
    }
  )

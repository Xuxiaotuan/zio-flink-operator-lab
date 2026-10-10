package cn.xuyinyin.flinklab.application

/** 验证 HTTP JSON 能否转换为统一的 FlinkOperation，以及非法字段是否被拒绝。 */

import cn.xuyinyin.flinklab.domain.FlinkOperation
import zio.test.*

object OperationFactorySpec extends ZIOSpecDefault:
  def spec = suite("operation factory")(
    test("turns an HTTP deployment document into a typed operation") {
      val result = FlinkOperationFactory.fromDeploymentJson("analytics", """{"kind":"FlinkDeployment","metadata":{"name":"orders"},"spec":{"image":"flink:1.20.1","flinkVersion":"v1_20","job":{"jarURI":"local:///job.jar","entryClass":"example.WordCount","parallelism":2}}}""")
      assertTrue(result.exists(_.isInstanceOf[FlinkOperation.Deploy]))
    },
    test("preserves the savepoint redeploy nonce in a typed deployment") {
      val result = FlinkOperationFactory.fromDeploymentJson("analytics", """{"kind":"FlinkDeployment","metadata":{"name":"orders"},"spec":{"job":{"jarURI":"local:///job.jar","entryClass":"example.WordCount","parallelism":1,"initialSavepointPath":"s3://bucket/savepoint-1","savepointRedeployNonce":2}}}""")
      assertTrue(result match
        case Right(FlinkOperation.Deploy(spec)) => spec.job.initialSavepointPath.nonEmpty && spec.job.savepointRedeployNonce.contains(2L)
        case _ => false)
    },
    test("rejects invalid redeploy nonces without silently dropping or truncating them") {
      val invalid = List("0", "-1", "1.5", "null", "true", "\"oops\"", "9007199254740992")
      assertTrue(invalid.forall { nonce =>
        FlinkOperationFactory.fromDeploymentJson("analytics", s"""{"kind":"FlinkDeployment","metadata":{"name":"orders"},"spec":{"job":{"savepointRedeployNonce":$nonce}}}""").isLeft
      })
    },
    test("rejects a misspelled state protection instead of defaulting to stateless") {
      val result = FlinkOperationFactory.fromDeploymentJson("analytics", """{"kind":"FlinkDeployment","metadata":{"name":"orders"},"spec":{"job":{"upgradeMode":"savpoint"}}}""")
      assertTrue(result.left.exists(_.contains("unknown state protection")))
    },
    test("turns an HTTP snapshot document into a typed operation") {
      val result = FlinkOperationFactory.fromSnapshotJson("analytics", """{"targetKind":"deployment","targetName":"orders","snapshotName":"orders-savepoint","type":"savepoint"}""")
      assertTrue(result.exists(_.isInstanceOf[FlinkOperation.Snapshot]))
    }
  )

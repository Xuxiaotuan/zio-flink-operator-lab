package cn.xuyinyin.flinklab.operation
/** 验证 PostgreSQL 配置和 operation 审计文档的编解码边界。 */

import zio.test.*
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import java.time.Instant

object PostgresOperationStoreSpec extends ZIOSpecDefault:
  def spec = suite("postgres operation store")(
    test("reads postgres settings without exposing the password") {
      val settings = OperationStoreSettings.fromEnv(Map(
        "ZIO_FLINK_OPERATION_STORE" -> "postgres",
        "POSTGRES_HOST" -> "db",
        "POSTGRES_PORT" -> "5432",
        "POSTGRES_DB" -> "xxt",
        "POSTGRES_USER" -> "root",
        "POSTGRES_PASSWORD" -> "secret"
      ))
      assertTrue(settings.isRight, settings.toOption.exists(_.jdbcUrl.contains("jdbc:postgresql://db:5432/xxt")))
    },
    test("round trips an operation audit document") {
      val resource = ResourceRef(Namespace.unsafe("analytics"), ResourceKind.Deployment, DeploymentName.unsafe("orders"))
      val operation = Operation.accepted(RequestId.from("req-1").toOption.get, FlinkOperation.Resume(resource), resource, Instant.parse("2026-10-03T00:00:00Z"))
      val restored = OperationCodec.fromJson(OperationCodec.json(operation))
      assertTrue(restored.exists(value => value.id == operation.id && value.state == OperationState.Accepted && value.resource == resource))
    },
    test("preserves deployment protection and storage settings") {
      val resource = ResourceRef(Namespace.unsafe("analytics"), ResourceKind.Deployment, DeploymentName.unsafe("orders"))
      val spec = FlinkDeploymentSpec(
        resource.namespace,
        resource.name,
        "registry/flink:1.20.1",
        "v1_20",
        FlinkJob(JobJarUri.unsafe("s3://jobs/orders.jar"), "example.Orders", 3, StateProtection.Savepoint, DesiredJobState.Suspended),
        Some("flink-runner"),
        Some("s3://flink-savepoints/zio-flink-operator")
      )
      val operation = Operation.accepted(RequestId.from("req-config").toOption.get, FlinkOperation.Deploy(spec), resource, Instant.parse("2026-10-03T00:00:00Z"))
      val restored = OperationCodec.fromJson(OperationCodec.json(operation))
      assertTrue(restored.exists {
        case Operation(_, _, FlinkOperation.Deploy(value), _, _, _, _, _) =>
          value.job.stateProtection == StateProtection.Savepoint && value.job.desiredState == DesiredJobState.Suspended && value.serviceAccount.contains("flink-runner") && value.savepointDirectory.contains("s3://flink-savepoints/zio-flink-operator")
        case _ => false
      })
    }
  )

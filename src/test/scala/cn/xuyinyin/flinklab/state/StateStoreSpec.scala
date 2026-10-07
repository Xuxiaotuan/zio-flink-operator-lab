package cn.xuyinyin.flinklab.state
/** 验证 Kubernetes/PostgreSQL 状态后端配置和 resourceVersion 合并规则。 */

import zio.test.*

object StateStoreSpec extends ZIOSpecDefault:
  def spec = suite("state store configuration")(
    test("defaults to Kubernetes CR state") {
      val settings = StateStoreSettings.fromEnv(Map.empty)
      assertTrue(settings == Right(StateStoreSettings(StateBackend.Kubernetes, None, None, None, "zio_flink_operator_state")))
    },
    test("builds a PostgreSQL URL from deployment environment variables") {
      val settings = StateStoreSettings.fromEnv(
        Map(
          "ZIO_FLINK_STATE_BACKEND" -> "postgres",
          "POSTGRES_HOST" -> "100.82.226.63",
          "POSTGRES_PORT" -> "30660",
          "POSTGRES_DB" -> "xxt",
          "POSTGRES_USER" -> "root",
          "POSTGRES_PASSWORD" -> "secret"
        )
      )
      assertTrue(settings == Right(StateStoreSettings(
        StateBackend.Postgres,
        Some("jdbc:postgresql://100.82.226.63:30660/xxt"),
        Some("root"),
        Some("secret"),
        "zio_flink_operator_state"
      )))
    },
    test("rejects an incomplete PostgreSQL configuration") {
      val settings = StateStoreSettings.fromEnv(Map("ZIO_FLINK_STATE_BACKEND" -> "postgres"))
      assertTrue(settings.isLeft)
    },
    test("extracts a resource key and opaque resource version") {
      val record = StateRecord.fromJson(
        cn.xuyinyin.flinklab.domain.FlinkTypes.Namespace.unsafe("analytics"),
        cn.xuyinyin.flinklab.cli.ResourceKind.Deployment,
        """{"metadata":{"name":"orders","resourceVersion":"007"},"status":{"state":"RUNNING"}}"""
      )
      assertTrue(
        record.exists(_.key.name == "orders"),
        record.exists(_.resourceVersion.contains("007")),
        record.exists(_.payload.contains("RUNNING"))
      )
    }
  )

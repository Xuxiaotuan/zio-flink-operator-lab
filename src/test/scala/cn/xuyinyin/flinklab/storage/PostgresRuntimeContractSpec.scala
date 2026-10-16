package cn.xuyinyin.flinklab.storage

import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.metadata.MetadataStoreSettings
import cn.xuyinyin.flinklab.operation.*
import zio.*
import zio.test.*

/** 必须指向独立测试库；表名随机化，Scope 退出时只删除本测试的表和连接池。 */
object PostgresRuntimeContractSpec extends ZIOSpecDefault:
  def spec = suite("real PostgreSQL runtime contracts")(
    test("two stores share transitions and active scans exclude terminal history") {
      ZIO.scoped {
        val env = sys.env
        val jdbc = env("ZIO_FLINK_METADATA_TEST_JDBC_URL")
        val user = env.getOrElse("ZIO_FLINK_METADATA_TEST_USER", "postgres")
        val password = env.getOrElse("ZIO_FLINK_METADATA_TEST_PASSWORD", "test-only")
        val table = "zio_contract_" + java.util.UUID.randomUUID().toString.replace("-", "")
        val settings = OperationStoreSettings(jdbc, user, password, table)
        val resource = ResourceRef(Namespace.unsafe("analytics"), ResourceKind.Deployment, DeploymentName.unsafe("orders"))
        val operation = Operation.accepted(RequestId.generate(), FlinkOperation.Resume(resource), resource, java.time.Instant.now())
        for
          cleanup <- ZIO.acquireRelease(ZIO.attempt(new JdbcPool(jdbc, user, password)))(p => ZIO.attemptBlocking {
            p.withConnection { c =>
              val statement = c.createStatement()
              try statement.executeUpdate(s"DROP TABLE IF EXISTS $table")
              finally statement.close()
            }
            p.close()
          }.orDie)
          first <- ZIO.acquireRelease(ZIO.attempt(new PostgresOperationStore(settings)))(_.close)
          second <- ZIO.acquireRelease(ZIO.attempt(new PostgresOperationStore(settings)))(_.close)
          _ <- first.initialize
          _ <- first.create(operation)
          active <- second.listActive
          _ <- second.transition(operation.id, OperationEvent.ValidationStarted(java.time.Instant.now()))
          _ <- first.transition(operation.id, OperationEvent.Failed(java.time.Instant.now(), "contract terminal"))
          terminal <- second.get(operation.id)
          stillActive <- first.listActive
          limits <- ZIO.attemptBlocking(cleanup.withConnection { c =>
            val statement = c.createStatement()
            try
              val rows = statement.executeQuery("SHOW statement_timeout")
              try { rows.next(); rows.getString(1) }
              finally rows.close()
            finally statement.close()
          })
        yield assertTrue(active.map(_.id) == List(operation.id), terminal.exists(_.state == OperationState.Failed), stillActive.isEmpty, limits == "10s", !MetadataStoreSettings(jdbc, user, password).toString.contains(password))
      }
    } @@ TestAspect.ifEnv("ZIO_FLINK_METADATA_TEST_JDBC_URL")(_.nonEmpty)
  )

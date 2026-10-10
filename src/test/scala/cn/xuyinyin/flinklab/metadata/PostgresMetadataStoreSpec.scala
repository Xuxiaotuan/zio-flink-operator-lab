package cn.xuyinyin.flinklab.metadata

import zio.*
import zio.test.*

/** 只连接显式提供的测试数据库；不要指向生产元数据数据库。 */
object PostgresMetadataStoreSpec extends ZIOSpecDefault:
  def spec = suite("postgres metadata contract")(
    test("two store instances share idempotent registration, current schemas and lineage") {
      val settings = MetadataStoreSettings(
        sys.env("ZIO_FLINK_METADATA_TEST_JDBC_URL"),
        sys.env.getOrElse("ZIO_FLINK_METADATA_TEST_USER", "postgres"),
        sys.env.getOrElse("ZIO_FLINK_METADATA_TEST_PASSWORD", "test-only")
      )
      val first = new PostgresMetadataStore(settings)
      val second = new PostgresMetadataStore(settings)
      val id = "contract-" + java.util.UUID.randomUUID().toString
      val catalog = CatalogRegistration(id, "postgres", "jdbc:postgresql://db/test", "test", None)
      val at = "2026-10-10T00:00:00Z"
      val table = TableMetadata(id, "public", "orders", Nil, at)
      val snapshot = SchemaSnapshot(id, "public", "v1", List(table), at)
      val edge = LineageEdge(LineageNode.Dataset(id + ".source"), LineageNode.Dataset(id + ".target"), "INSERT", "SQL_STATIC", 0.7, at)
      for
        _ <- first.initialize
        _ <- first.createCatalog(catalog)
        repeated <- second.createCatalog(catalog).either
        conflict <- second.createCatalog(catalog.copy(database = "other")).exit
        orphan <- second.saveSnapshot(SchemaSnapshot(id + "-missing", "public", "v1", Nil, at)).exit
        _ <- first.saveSnapshot(snapshot)
        _ <- second.saveSnapshot(snapshot.copy(schema = "other", tables = List(table.copy(schema = "other"))))
        _ <- second.saveSnapshot(snapshot.copy(version = "v2", observedAt = "2026-10-11T00:00:00Z", tables = Nil))
        // 迟到的旧快照只能补历史，不能复活已删除的表。
        _ <- first.saveSnapshot(snapshot)
        current <- first.listTables(id, Some("public"))
        other <- first.listTables(id, Some("other"))
        history <- second.listSnapshots(id)
        _ <- first.saveLineage(edge)
        _ <- second.saveLineage(edge.copy(observedAt = "2026-10-11T00:00:00Z"))
        graph <- first.graph(edge.target.name)
      yield assertTrue(repeated == Right(catalog), conflict.isFailure, orphan.isFailure, current.isEmpty, other.size == 1, history.size == 3, graph.size == 1, graph.headOption.exists(_.observedAt == "2026-10-11T00:00:00Z"))
    } @@ TestAspect.ifEnv("ZIO_FLINK_METADATA_TEST_JDBC_URL")(_.nonEmpty)
  )

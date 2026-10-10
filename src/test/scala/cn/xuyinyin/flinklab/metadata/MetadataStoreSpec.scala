package cn.xuyinyin.flinklab.metadata

import zio.*
import zio.test.*

object MetadataStoreSpec extends ZIOSpecDefault:
  val at = "2026-10-10T00:00:00Z"
  val catalog = CatalogRegistration("warehouse", "postgres", "jdbc:postgresql://db/xxt", "xxt", Some("secret/pg"))
  val table = TableMetadata("warehouse", "public", "orders", List(TableColumn("id", "BIGINT", false)), at)
  def spec = suite("metadata store")(
    test("registration is idempotent but conflicting identities are rejected") {
      for
        store <- InMemoryMetadataStore.make
        _ <- store.createCatalog(catalog)
        same <- store.createCatalog(catalog)
        conflict <- store.createCatalog(catalog.copy(database = "other")).exit
      yield assertTrue(same == catalog, conflict.isFailure)
    },
    test("snapshot replaces only one schema and retains historical versions") {
      for
        store <- InMemoryMetadataStore.make
        _ <- store.createCatalog(catalog)
        snapshot = SchemaSnapshot("warehouse", "public", "v1", List(table), at)
        _ <- store.saveSnapshot(snapshot)
        _ <- store.saveSnapshot(snapshot)
        _ <- store.saveSnapshot(SchemaSnapshot("warehouse", "other", "v1", List(table.copy(schema = "other")), at))
        _ <- store.saveSnapshot(snapshot.copy(version = "v2", tables = Nil))
        current <- store.listTables("warehouse", Some("public"))
        other <- store.listTables("warehouse", Some("other"))
        history <- store.listSnapshots("warehouse")
        conflict <- store.saveSnapshot(snapshot.copy(catalogId = "missing")).exit
      yield assertTrue(current.isEmpty, other.size == 1, history.size == 3, conflict.isFailure)
    },
    test("rejects orphan snapshots and malformed column input") {
      for
        store <- InMemoryMetadataStore.make
        missing <- store.saveSnapshot(SchemaSnapshot("warehouse", "public", "v1", List(table), at)).exit
      yield assertTrue(missing.isFailure, MetadataJson.table(ujson.read(table.json.render().replace("BIGINT", ""))).isLeft)
    },
    test("rejects credentials embedded in endpoint and mismatched snapshot table scope") {
      val bad = catalog.copy(endpoint = "jdbc:postgresql://db/xxt?password=secret")
      val snapshot = SchemaSnapshot("warehouse", "public", "v1", List(table.copy(catalogId = "elsewhere")), at)
      assertTrue(bad.validate.isLeft, snapshot.validate.isLeft)
    },
    test("metadata store selection is explicit") {
      assertTrue(MetadataStore.configured(Map.empty).isRight, MetadataStore.configured(Map("ZIO_FLINK_METADATA_STORE" -> "memory")).isRight, MetadataStore.configured(Map("ZIO_FLINK_METADATA_STORE" -> "unknown")).isLeft)
    }
  )

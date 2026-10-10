package cn.xuyinyin.flinklab.metadata

import zio.test.*

object MetadataModelSpec extends ZIOSpecDefault:
  def spec = suite("metadata model")(
    test("validates identifiers and does not expose credentials") {
      val catalog = CatalogRegistration("warehouse", "postgres", "jdbc:postgresql://db/xxt", "xxt", Some("secret/pg"))
      val json = catalog.json.render()
      assertTrue(
        CatalogId.from("warehouse").isRight,
        CatalogId.from("bad/name").isLeft,
        json.contains("credentialRef"),
        !json.contains("password"),
        !json.contains("asd123456")
      )
    },
    test("renders schema columns and lineage evidence") {
      val table = TableMetadata("warehouse", "public", "orders", List(TableColumn("id", "BIGINT", false), TableColumn("status", "VARCHAR", true)), "2026-10-10T00:00:00Z")
      val edge = LineageEdge(LineageNode.Dataset("public.orders"), LineageNode.Dataset("public.order_summary"), "SELECT", "SQL_STATIC", 0.7, "2026-10-10T00:00:00Z", Some("job-1"))
      assertTrue(table.json("columns").arr.size == 2, edge.json("sourceType").str == "SQL_STATIC", edge.json("confidence").num == 0.7)
    }
  )

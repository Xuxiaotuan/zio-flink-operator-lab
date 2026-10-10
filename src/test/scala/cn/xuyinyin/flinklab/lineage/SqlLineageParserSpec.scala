package cn.xuyinyin.flinklab.lineage

import zio.test.*

object SqlLineageParserSpec extends ZIOSpecDefault:
  private val at = "2026-10-10T00:00:00Z"
  def spec = suite("sql lineage parser")(
    test("extracts insert select join with explicit static evidence") {
      val result = SqlLineageParser.extract("INSERT INTO analytics.summary SELECT o.id FROM raw.orders o JOIN raw.customers c ON o.id = c.id", at)
      assertTrue(result.toOption.exists(edges => edges.map(_.source.name).toSet == Set("raw.orders", "raw.customers") && edges.forall(_.target.name == "analytics.summary") && edges.forall(_.sourceType == "SQL_STATIC")))
    },
    test("supports quoted identifiers and CTAS without treating strings or comments as sources") {
      val result = SqlLineageParser.extract("CREATE TABLE `analytics`.`summary` AS SELECT 'FROM fake.table' AS label FROM `raw`.`orders` /* JOIN hidden */", at)
      assertTrue(result.toOption.exists(_.map(_.source.name) == List("raw.orders")))
    },
    test("no write statement has no derived data movement") {
      assertTrue(SqlLineageParser.extract("-- FROM fake.table\nSELECT 'FROM string'", at) == Right(Nil))
    },
    test("rejects ambiguous or unsupported syntax rather than partial lineage") {
      val inputs = List(
        "INSERT INTO sink SELECT * FROM a, b",
        "INSERT INTO sink SELECT * FROM (SELECT * FROM a) x",
        "WITH x AS (SELECT * FROM a) INSERT INTO sink SELECT * FROM x",
        "INSERT INTO sink SELECT * FROM a UNION SELECT * FROM b",
        "INSERT INTO sink SELECT * FROM TABLE(func())",
        "INSERT INTO sink SELECT * FROM a; INSERT INTO second SELECT * FROM b",
        "INSERT INTO sink SELECT * FROM a JOIN",
        "INSERT INTO sink SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b)",
        "INSERT INTO sink SELECT * FROM a /* unterminated",
        "INSERT INTO sink SELECT 'unterminated FROM a",
        "INSERT INTO sink VALUES (1)",
        "CREATE VIEW v AS SELECT * FROM a"
      )
      assertTrue(inputs.forall(SqlLineageParser.extract(_, at).isLeft))
    }
  )

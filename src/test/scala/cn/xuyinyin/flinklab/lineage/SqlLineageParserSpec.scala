package cn.xuyinyin.flinklab.lineage

import zio.test.*

object SqlLineageParserSpec extends ZIOSpecDefault:
  private val at = "2026-10-10T00:00:00Z"
  def spec = suite("sql lineage parser")(
    test("extracts insert select join with explicit static evidence") {
      val result = SqlLineageParser.extract("INSERT INTO analytics.summary SELECT o.id FROM `raw`.orders o JOIN `raw`.customers c ON o.id = c.id", at)
      assertTrue(result.toOption.exists(edges => edges.map(_.source.name).toSet == Set("raw.orders", "raw.customers") && edges.forall(_.target.name == "analytics.summary") && edges.forall(_.sourceType == "SQL_STATIC")))
    },
    test("supports quoted identifiers and CTAS without treating strings or comments as sources") {
      val result = SqlLineageParser.extract("CREATE TABLE `analytics`.`summary` AS SELECT 'FROM fake.table' AS label FROM `raw`.`orders` /* JOIN hidden */", at)
      assertTrue(result.toOption.exists(_.map(_.source.name) == List("raw.orders")))
    },
    test("no write statement has no derived data movement") {
      assertTrue(SqlLineageParser.extract("-- FROM fake.table\nSELECT 'FROM string'", at) == Right(Nil))
    },
    test("extracts every comma join source with and without aliases") {
      val inputs = List("INSERT INTO sink SELECT * FROM a, b", "INSERT INTO sink SELECT * FROM a a1, b b1 WHERE a1.id = b1.id")
      assertTrue(inputs.forall(sql => SqlLineageParser.extract(sql, at).toOption.exists(_.map(_.source.name).toSet == Set("a", "b"))))
    },
    test("extracts nested queries including expression subqueries") {
      val result = SqlLineageParser.extract("INSERT INTO sink SELECT (SELECT max(id) FROM c) FROM (SELECT * FROM a) x WHERE EXISTS (SELECT 1 FROM b WHERE b.id = x.id)", at)
      assertTrue(result.toOption.exists(_.map(_.source.name).toSet == Set("a", "b", "c")))
    },
    test("resolves CTE references to base tables and ignores unused CTEs") {
      val result = SqlLineageParser.extract("INSERT INTO sink WITH x AS (SELECT * FROM a), y AS (SELECT * FROM x JOIN b ON x.id = b.id), unused AS (SELECT * FROM ignored) SELECT * FROM y", at)
      assertTrue(result.toOption.exists(_.map(_.source.name).toSet == Set("a", "b")))
    },
    test("preserves quoted identifiers and permits a statement terminator") {
      val result = SqlLineageParser.extract("INSERT INTO `sink table` SELECT '; FROM fake' AS label FROM `raw space`.`order``items`;", at)
      assertTrue(result.toOption.exists(edges => edges.map(_.source.name) == List("raw space.order`items") && edges.forall(_.target.name == "sink table")))
    },
    test("rejects invalid SQL after a valid source rather than returning partial lineage") {
      val inputs = List("INSERT INTO sink SELECT * FROM a WHERE", "INSERT INTO sink SELECT FROM a", "INSERT INTO sink SELECT * FROM a JOIN b ON", "INSERT INTO sink SELECT * FROM a nonsense trailing")
      assertTrue(inputs.forall(SqlLineageParser.extract(_, at).isLeft))
    },
    test("supports Flink INSERT OVERWRITE columns and CTAS connector options") {
      val inputs = List(
        "INSERT OVERWRITE sink SELECT * FROM source_table",
        "INSERT INTO sink (id) SELECT id FROM source_table",
        "CREATE TABLE sink WITH ('connector' = 'blackhole') AS SELECT id FROM source_table"
      )
      assertTrue(inputs.forall(sql => SqlLineageParser.extract(sql, at).toOption.exists(edges => edges.map(_.source.name) == List("source_table") && edges.forall(_.target.name == "sink"))))
    },
    test("keeps nested CTE scopes separate and deduplicates repeated base tables") {
      val result = SqlLineageParser.extract("INSERT INTO sink WITH x AS (SELECT * FROM outer_source) SELECT * FROM x JOIN (WITH x AS (SELECT * FROM inner_source) SELECT * FROM x) nested ON TRUE JOIN outer_source repeated ON TRUE", at)
      assertTrue(result.toOption.exists(_.map(_.source.name) == List("outer_source", "inner_source")))
    },
    test("rejects unsupported relations nested behind otherwise valid sources") {
      val inputs = List(
        "INSERT INTO sink SELECT * FROM a JOIN TABLE(func()) f ON TRUE",
        "INSERT INTO sink SELECT * FROM a, UNNEST(a.items) AS u",
        "INSERT INTO sink SELECT * FROM a FOR SYSTEM_TIME AS OF TIMESTAMP '2026-10-10 00:00:00'",
        "INSERT INTO sink SELECT * FROM a WHERE id IN (SELECT id FROM b UNION SELECT id FROM c)",
        "INSERT INTO sink WITH x AS (SELECT * FROM x) SELECT * FROM x",
        "INSERT INTO sink WITH x AS (SELECT * FROM y), y AS (SELECT * FROM a) SELECT * FROM x",
        "INSERT INTO sink WITH x AS (SELECT * FROM a), x AS (SELECT * FROM b) SELECT * FROM x",
        "INSERT INTO sink WITH unused AS (SELECT * FROM TABLE(func())) SELECT * FROM a"
      )
      assertTrue(inputs.forall(SqlLineageParser.extract(_, at).isLeft))
    },
    test("enforces actual Flink reserved identifiers and refuses source-free writes") {
      val inputs = List("INSERT INTO sink SELECT * FROM raw.orders", "INSERT INTO sink SELECT 1", "", "  ", null)
      assertTrue(inputs.forall(SqlLineageParser.extract(_, at).isLeft))
    },
    test("rejects quoted identifier components containing dots instead of conflating qualification") {
      val inputs = List("INSERT INTO dst SELECT * FROM `a.b`", "INSERT INTO `a.b` SELECT * FROM src", "INSERT INTO dst WITH `a.b` AS (SELECT * FROM src) SELECT * FROM `a.b`")
      val qualified = SqlLineageParser.extract("INSERT INTO dst SELECT * FROM a.b", at)
      assertTrue(inputs.forall(SqlLineageParser.extract(_, at).isLeft), qualified.toOption.exists(_.map(_.source.name) == List("a.b")))
    },
    test("rejects ambiguous or unsupported syntax rather than partial lineage") {
      val inputs = List(
        "WITH x AS (SELECT * FROM a) INSERT INTO sink SELECT * FROM x",
        "INSERT INTO sink SELECT * FROM a UNION SELECT * FROM b",
        "INSERT INTO sink SELECT * FROM TABLE(func())",
        "INSERT INTO sink SELECT * FROM a; INSERT INTO second SELECT * FROM b",
        "INSERT INTO sink SELECT * FROM a JOIN",
        "INSERT INTO sink SELECT * FROM a /* unterminated",
        "INSERT INTO sink SELECT 'unterminated FROM a",
        "INSERT INTO sink VALUES (1)",
        "CREATE VIEW v AS SELECT * FROM a"
      )
      assertTrue(inputs.forall(SqlLineageParser.extract(_, at).isLeft))
    }
  )

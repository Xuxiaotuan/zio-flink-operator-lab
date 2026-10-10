package cn.xuyinyin.flinklab.metadata

import java.time.Instant
import scala.util.Try

/** 元数据身份与 Flink resourceName 分开；数据集始终保留 catalog/schema 上下文。 */
object CatalogId:
  def from(value: String): Either[String, String] =
    Either.cond(value != null && value.matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}"), value, "invalid catalog id")

object MetadataValidation:
  def text(value: String, field: String): Either[String, Unit] =
    Either.cond(value != null && value.trim.nonEmpty && value.length <= 512 && !value.exists(_.isControl), (), s"invalid $field")
  def instant(value: String): Either[String, Unit] = Try(Instant.parse(value)).toEither.left.map(_ => "invalid observedAt timestamp").map(_ => ())
  def all(values: List[Either[String, Unit]]): Either[String, Unit] = values.foldLeft[Either[String, Unit]](Right(()))((acc, v) => acc.flatMap(_ => v))

final case class CatalogRegistration(id: String, catalogType: String, endpoint: String, database: String, credentialRef: Option[String]):
  def validate: Either[String, Unit] =
    for
      _ <- CatalogId.from(id)
      _ <- MetadataValidation.all(List(MetadataValidation.text(catalogType, "type"), MetadataValidation.text(database, "database"), MetadataValidation.text(endpoint, "endpoint")))
      // 注册只保存不含 userinfo/query 的地址，阻止密码混进可查询的 endpoint。
      _ <- Either.cond(!endpoint.exists(c => "@?#;".contains(c)), (), "endpoint must not contain credentials, query parameters or fragments")
      _ <- credentialRef.map(MetadataValidation.text(_, "credentialRef")).getOrElse(Right(()))
    yield ()
  def json: ujson.Obj =
    val value = ujson.Obj("id" -> id, "type" -> catalogType, "endpoint" -> endpoint, "database" -> database)
    credentialRef.foreach(ref => value("credentialRef") = ref)
    value

final case class TableColumn(name: String, dataType: String, nullable: Boolean):
  def validate: Either[String, Unit] = MetadataValidation.all(List(MetadataValidation.text(name, "column name"), MetadataValidation.text(dataType, "column dataType")))
  def json: ujson.Obj = ujson.Obj("name" -> name, "dataType" -> dataType, "nullable" -> nullable)

final case class TableMetadata(catalogId: String, schema: String, name: String, columns: List[TableColumn], observedAt: String):
  def validate: Either[String, Unit] =
    for
      _ <- CatalogId.from(catalogId)
      _ <- MetadataValidation.all(List(MetadataValidation.text(schema, "schema"), MetadataValidation.text(name, "table name"), MetadataValidation.instant(observedAt)) ++ columns.map(_.validate))
      _ <- Either.cond(columns.size <= 2000 && columns.map(_.name).distinct.size == columns.size, (), "duplicate or too many columns")
    yield ()
  def json: ujson.Obj = ujson.Obj("catalogId" -> catalogId, "schema" -> schema, "name" -> name, "columns" -> ujson.Arr(columns.map(_.json)*), "observedAt" -> observedAt, "sourceType" -> "MANUAL")

final case class SchemaSnapshot(catalogId: String, schema: String, version: String, tables: List[TableMetadata], observedAt: String):
  def validate: Either[String, Unit] =
    for
      _ <- CatalogId.from(catalogId)
      _ <- MetadataValidation.all(List(MetadataValidation.text(schema, "schema"), MetadataValidation.text(version, "version"), MetadataValidation.instant(observedAt)) ++ tables.map(_.validate))
      _ <- Either.cond(tables.size <= 1000 && tables.map(_.name).distinct.size == tables.size, (), "duplicate or too many tables")
      _ <- Either.cond(tables.forall(t => t.catalogId == catalogId && t.schema == schema && t.observedAt == observedAt), (), "snapshot tables must match catalog, schema and observedAt")
    yield ()
  def json: ujson.Obj = ujson.Obj("catalogId" -> catalogId, "schema" -> schema, "version" -> version, "tables" -> ujson.Arr(tables.map(_.json)*), "observedAt" -> observedAt, "sourceType" -> "MANUAL")

sealed trait LineageNode:
  def name: String
  def kind: String
  def json: ujson.Obj = ujson.Obj("kind" -> kind, "name" -> name)
object LineageNode:
  final case class Dataset(name: String) extends LineageNode:
    val kind = "DATASET"

/** confidence 是静态规则标签，不是经过统计校准的运行概率。 */
final case class LineageEdge(source: LineageNode, target: LineageNode, operation: String, sourceType: String, confidence: Double, observedAt: String, jobId: Option[String] = None):
  def json: ujson.Obj =
    val value = ujson.Obj("source" -> source.json, "target" -> target.json, "operation" -> operation, "sourceType" -> sourceType, "confidence" -> confidence, "observedAt" -> observedAt)
    jobId.foreach(id => value("jobId") = id)
    value

/** 每次分析作为完整批次保存；重试以 digest 去重，不逐边写入留下半张图。 */
final case class LineageAnalysis(id: String, catalogId: String, schema: String, jobId: Option[String], edges: List[LineageEdge], observedAt: String):
  def json: ujson.Obj =
    val value = ujson.Obj("id" -> id, "catalogId" -> catalogId, "schema" -> schema, "edges" -> ujson.Arr(edges.map(_.json)*), "observedAt" -> observedAt, "sourceType" -> "SQL_STATIC", "evidenceStatus" -> (if edges.isEmpty then "UNKNOWN" else "STATIC_INFERENCE"))
    jobId.foreach(id => value("jobId") = id)
    value

object MetadataJson:
  private def read[A](f: => A): Either[String, A] = Try(f).toEither.left.map(_ => "invalid metadata JSON shape or field type")
  private def optional(v: ujson.Value, key: String): Option[String] = v.obj.get(key).map(_.str)
  def catalog(value: ujson.Value): Either[String, CatalogRegistration] =
    for
      _ <- Either.cond(value.objOpt.exists(_.keySet.subsetOf(Set("id", "type", "endpoint", "database", "credentialRef"))), (), "unknown catalog fields; credentials must use credentialRef")
      result <- read(CatalogRegistration(value("id").str, value("type").str, value("endpoint").str, value("database").str, optional(value, "credentialRef")))
      _ <- result.validate
    yield result
  def table(value: ujson.Value): Either[String, TableMetadata] =
    for
      result <- read(TableMetadata(value("catalogId").str, value("schema").str, value("name").str, value("columns").arr.toList.map(c => TableColumn(c("name").str, c("dataType").str, c("nullable").bool)), value("observedAt").str))
      _ <- result.validate
    yield result
  def snapshot(value: ujson.Value): Either[String, SchemaSnapshot] =
    for
      parts <- read((value("catalogId").str, value("schema").str, value("version").str, value("observedAt").str, value("tables").arr.toList))
      tables <- parts._5.foldLeft[Either[String, List[TableMetadata]]](Right(Nil))((acc, v) => for xs <- acc; t <- table(v) yield xs :+ t)
      result = SchemaSnapshot(parts._1, parts._2, parts._3, tables, parts._4)
      _ <- result.validate
    yield result
  def analysis(value: ujson.Value): Either[String, LineageAnalysis] = read {
    val edges = value("edges").arr.toList.map(e => LineageEdge(LineageNode.Dataset(e("source")("name").str), LineageNode.Dataset(e("target")("name").str), e("operation").str, e("sourceType").str, e("confidence").num, e("observedAt").str, optional(e, "jobId")))
    LineageAnalysis(value("id").str, value("catalogId").str, value("schema").str, optional(value, "jobId"), edges, value("observedAt").str)
  }

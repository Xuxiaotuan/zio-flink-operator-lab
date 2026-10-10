package cn.xuyinyin.flinklab.metadata

import cn.xuyinyin.flinklab.domain.ControlPlaneError
import zio.*
import java.sql.{Connection, DriverManager}

final case class MetadataStoreSettings(jdbcUrl: String, username: String, password: String):
  def withConnection[A](f: Connection => A): A =
    val connection = DriverManager.getConnection(jdbcUrl, username, password)
    try f(connection) finally connection.close()
object MetadataStoreSettings:
  def fromEnv(env: Map[String, String]): Either[String, MetadataStoreSettings] =
    val url = env.get("ZIO_FLINK_METADATA_JDBC_URL").orElse(env.get("ZIO_FLINK_POSTGRES_JDBC_URL")).orElse {
      for h <- env.get("POSTGRES_HOST"); p <- env.get("POSTGRES_PORT"); d <- env.get("POSTGRES_DB") yield s"jdbc:postgresql://$h:$p/$d"
    }
    for
      jdbc <- url.toRight("ZIO_FLINK_METADATA_JDBC_URL or POSTGRES_HOST is required")
      user <- env.get("ZIO_FLINK_METADATA_USER").orElse(env.get("POSTGRES_USER")).toRight("metadata postgres user is required")
      password <- env.get("ZIO_FLINK_METADATA_PASSWORD").orElse(env.get("POSTGRES_PASSWORD")).toRight("metadata postgres password is required")
    yield MetadataStoreSettings(jdbc, user, password)

/** PostgreSQL 元数据适配器；快照和血缘均用参数化 SQL 写入，JSON 只作为版本化载荷。 */
final class PostgresMetadataStore(settings: MetadataStoreSettings) extends MetadataStore:
  override def initialize: IO[Throwable, Unit] = ZIO.attemptBlocking(settings.withConnection { c =>
    val s = c.createStatement()
    try
      s.executeUpdate("CREATE TABLE IF NOT EXISTS zio_flink_catalogs (catalog_id TEXT PRIMARY KEY, catalog_json TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP)")
      s.executeUpdate("CREATE TABLE IF NOT EXISTS zio_flink_schema_snapshots (catalog_id TEXT NOT NULL, schema_name TEXT NOT NULL, version TEXT NOT NULL, snapshot_json TEXT NOT NULL, observed_at TIMESTAMPTZ NOT NULL, PRIMARY KEY (catalog_id, schema_name, version))")
      s.executeUpdate("CREATE TABLE IF NOT EXISTS zio_flink_lineage_edges (edge_key TEXT PRIMARY KEY, root_name TEXT NOT NULL, edge_json TEXT NOT NULL, observed_at TIMESTAMPTZ NOT NULL)")
    finally s.close()
  })

  override def listCatalogs = queryJson("SELECT catalog_json FROM zio_flink_catalogs ORDER BY catalog_id", "catalog") { MetadataJson.catalog }
  override def createCatalog(catalog: CatalogRegistration) = ZIO.fromEither(catalog.validate.left.map(ControlPlaneError.StoreFailure.apply)).flatMap { _ =>
    ZIO.attemptBlocking(settings.withConnection { c =>
      val s = c.prepareStatement("INSERT INTO zio_flink_catalogs(catalog_id,catalog_json) VALUES (?,?) ON CONFLICT (catalog_id) DO NOTHING")
      try
        s.setString(1, catalog.id)
        s.setString(2, catalog.json.render())
        s.executeUpdate()
        // 并发重复注册只读取既有身份，不覆盖另一副本已登记的配置。
        val read = c.prepareStatement("SELECT catalog_json FROM zio_flink_catalogs WHERE catalog_id = ?")
        try
          read.setString(1, catalog.id)
          val rows = read.executeQuery()
          try
            if !rows.next() || MetadataJson.catalog(ujson.read(rows.getString(1))) != Right(catalog) then
              throw IllegalArgumentException(s"catalog already exists: ${catalog.id}")
          finally rows.close()
        finally read.close()
        catalog
      finally s.close()
    }).mapError {
      case e: java.sql.SQLException if e.getSQLState == "23505" => ControlPlaneError.StoreFailure(s"catalog already exists: ${catalog.id}")
      case e => ControlPlaneError.StoreFailure(Option(e.getMessage).getOrElse(e.toString))
    }
  }
  override def listTables(catalogId: String, schema: Option[String]) = listSnapshots(catalogId).map(MetadataStore.currentTables(_, schema))
  override def getTable(catalogId: String, schema: String, name: String) = listTables(catalogId, Some(schema)).map(_.find(_.name == name))
  override def listSnapshots(catalogId: String) = queryJson(s"SELECT snapshot_json FROM zio_flink_schema_snapshots WHERE catalog_id = ? ORDER BY observed_at", "snapshot", List(catalogId)) { MetadataJson.snapshot }
  override def saveSnapshot(snapshot: SchemaSnapshot) = ZIO.fromEither(snapshot.validate.left.map(ControlPlaneError.StoreFailure.apply)).flatMap { _ =>
    ZIO.attemptBlocking(settings.withConnection { c =>
      val exists = c.prepareStatement("SELECT 1 FROM zio_flink_catalogs WHERE catalog_id = ?")
      try
        exists.setString(1, snapshot.catalogId)
        val rows = exists.executeQuery()
        try
          if !rows.next() then throw IllegalArgumentException(s"catalog not found: ${snapshot.catalogId}")
        finally rows.close()
      finally exists.close()
      val s = c.prepareStatement("INSERT INTO zio_flink_schema_snapshots(catalog_id,schema_name,version,snapshot_json,observed_at) VALUES (?,?,?,?,?::timestamptz) ON CONFLICT (catalog_id,schema_name,version) DO UPDATE SET snapshot_json=EXCLUDED.snapshot_json, observed_at=EXCLUDED.observed_at")
      try
        s.setString(1, snapshot.catalogId)
        s.setString(2, snapshot.schema)
        s.setString(3, snapshot.version)
        s.setString(4, snapshot.json.render())
        s.setString(5, snapshot.observedAt)
        s.executeUpdate()
        ()
      finally s.close()
    }).mapError(e => ControlPlaneError.StoreFailure(Option(e.getMessage).getOrElse(e.toString)))
  }
  override def saveLineage(edge: LineageEdge) = ZIO.attemptBlocking(settings.withConnection { c =>
    val key = s"${edge.source.name}|${edge.target.name}|${edge.operation}|${edge.sourceType}|${edge.jobId.getOrElse("")}"
    val s = c.prepareStatement("INSERT INTO zio_flink_lineage_edges(edge_key,root_name,edge_json,observed_at) VALUES (?,?,?,?::timestamptz) ON CONFLICT (edge_key) DO UPDATE SET edge_json=EXCLUDED.edge_json, observed_at=EXCLUDED.observed_at WHERE EXCLUDED.observed_at >= zio_flink_lineage_edges.observed_at")
    try
      s.setString(1, key)
      s.setString(2, edge.source.name)
      s.setString(3, edge.json.render())
      s.setString(4, edge.observedAt)
      s.executeUpdate()
      ()
    finally s.close()
  }).mapError(e => ControlPlaneError.StoreFailure(Option(e.getMessage).getOrElse(e.toString)))
  override def graph(root: String) = queryJson("SELECT edge_json FROM zio_flink_lineage_edges ORDER BY observed_at", "edge") { value =>
    val source = value("source")("name").str
    val target = value("target")("name").str
    Right(LineageEdge(LineageNode.Dataset(source), LineageNode.Dataset(target), value("operation").str, value("sourceType").str, value("confidence").num, value("observedAt").str, value.obj.get("jobId").flatMap(_.strOpt)))
  }.map(_.filter(edge => edge.source.name == root || edge.target.name == root))
  private def queryJson[A](sql: String, label: String, params: List[String] = Nil)(parse: ujson.Value => Either[String, A]): IO[ControlPlaneError, List[A]] = ZIO.attemptBlocking(settings.withConnection { c =>
    val ps = c.prepareStatement(sql); try
      params.zipWithIndex.foreach { case (value, index) => ps.setString(index + 1, value) }
      val rs = ps.executeQuery(); try
        val result = List.newBuilder[A]; while rs.next() do parse(ujson.read(rs.getString(1))) match { case Right(v) => result += v; case Left(e) => throw new IllegalArgumentException(s"invalid $label payload: $e") }; result.result()
      finally rs.close()
    finally ps.close()
  }).mapError(e => ControlPlaneError.StoreFailure(Option(e.getMessage).getOrElse(e.toString)))

object MetadataStoreLive:
  /** 生产默认 PostgreSQL；只有显式设置 memory 才启用单进程开发存储。 */
  val layer: ZLayer[Any, Throwable, MetadataStore] = ZLayer.fromZIO {
    ZIO.fromEither(MetadataStore.configured(sys.env).left.map(IllegalArgumentException(_))).flatMap {
      case "memory" => InMemoryMetadataStore.make.map(identity[MetadataStore])
      case _ => ZIO.fromEither(MetadataStoreSettings.fromEnv(sys.env).left.map(IllegalArgumentException(_))).map(new PostgresMetadataStore(_))
    }
  }

package cn.xuyinyin.flinklab.state

/** 状态后端：定义 Kubernetes CR 和 PostgreSQL 两种状态读取/审计存储，并维护有限生命周期历史。 */
import cn.xuyinyin.flinklab.domain.ResourceKind
import cn.xuyinyin.flinklab.domain.FlinkTypes.Namespace
import cn.xuyinyin.flinklab.kubernetes.KubernetesApi
import zio.*

import java.sql.{Connection, DriverManager, ResultSet}
import java.time.Instant

/** 状态观测的事实来源；Kubernetes 是默认模式，Postgres 是外置审计模式。 */
enum StateBackend:
  case Kubernetes, Postgres

object StateBackend:
  def fromString(value: String): Either[String, StateBackend] =
    value.trim.toLowerCase match
      case "kubernetes" | "k8s" => Right(Kubernetes)
      case "postgres" | "postgresql" | "pg" => Right(Postgres)
      case other => Left(s"unsupported ZIO_FLINK_STATE_BACKEND: $other (use kubernetes or postgres)")

final case class StateStoreSettings(
    backend: StateBackend,
    jdbcUrl: Option[String],
    username: Option[String],
    password: Option[String],
    table: String
):
  def validate: Either[String, Unit] =
    backend match
      case StateBackend.Kubernetes => Right(())
      case StateBackend.Postgres =>
        for
          _ <- jdbcUrl.toRight("ZIO_FLINK_POSTGRES_JDBC_URL or POSTGRES_HOST is required for postgres state backend")
          _ <- username.toRight("ZIO_FLINK_POSTGRES_USER or POSTGRES_USER is required for postgres state backend")
          _ <- password.toRight("ZIO_FLINK_POSTGRES_PASSWORD or POSTGRES_PASSWORD is required for postgres state backend")
          _ <- Either.cond(table.matches("[a-zA-Z_][a-zA-Z0-9_]*"), (), s"invalid state table name: $table")
        yield ()

object StateStoreSettings:
  def fromEnv(env: Map[String, String]): Either[String, StateStoreSettings] =
    for
      backend <- StateBackend.fromString(env.getOrElse("ZIO_FLINK_STATE_BACKEND", "kubernetes"))
      settings = StateStoreSettings(
        backend = backend,
        jdbcUrl = env.get("ZIO_FLINK_POSTGRES_JDBC_URL").filter(_.nonEmpty).orElse(jdbcUrlFromParts(env)),
        username = env.get("ZIO_FLINK_POSTGRES_USER").filter(_.nonEmpty).orElse(env.get("POSTGRES_USER").filter(_.nonEmpty)),
        password = env.get("ZIO_FLINK_POSTGRES_PASSWORD").filter(_.nonEmpty).orElse(env.get("POSTGRES_PASSWORD").filter(_.nonEmpty)),
        table = env.getOrElse("ZIO_FLINK_STATE_TABLE", "zio_flink_operator_state")
      )
      _ <- settings.validate
    yield settings

  private def jdbcUrlFromParts(env: Map[String, String]): Option[String] =
    for
      host <- env.get("POSTGRES_HOST").filter(_.nonEmpty)
      port <- env.get("POSTGRES_PORT").filter(_.nonEmpty)
      database <- env.get("POSTGRES_DB").filter(_.nonEmpty)
    yield s"jdbc:postgresql://$host:$port/$database"

final case class StateKey(namespace: Namespace, kind: ResourceKind, name: String)

final case class StateRecord(
    key: StateKey,
    resourceVersion: Option[String],
    payload: String,
    observedAt: Instant
):
  def json: ujson.Obj =
    ujson.Obj(
      "namespace" -> key.namespace.namespaceValue,
      "kind" -> key.kind.apiResource,
      "name" -> key.name,
      "resourceVersion" -> resourceVersion.getOrElse(""),
      "observedAt" -> observedAt.toString,
      "payload" -> ujson.read(payload)
    )

/** 资源最新状态加有限生命周期历史；历史只记录状态变化和删除。 */
final case class StateJournal(
    key: StateKey,
    resourceVersion: Option[String],
    payload: String,
    observedAt: Instant,
    history: List[ujson.Obj],
    deleted: Boolean = false
):
  def record: StateRecord = StateRecord(key, resourceVersion, payload, observedAt)

  def json: ujson.Obj =
    ujson.Obj(
      "namespace" -> key.namespace.namespaceValue,
      "kind" -> key.kind.apiResource,
      "name" -> key.name,
      "resourceVersion" -> resourceVersion.getOrElse(""),
      "observedAt" -> observedAt.toString,
      "deleted" -> deleted,
      "payload" -> ujson.read(payload),
      "history" -> ujson.Arr(history*)
    )

object StateJournal:
  val MaxHistory = 100

  def fromRecord(record: StateRecord): StateJournal =
    merge(None, record)

  /** 相同 status 不重复扩张历史，避免轮询把数据库写成无限增长日志。 */
  def merge(previous: Option[StateJournal], record: StateRecord): StateJournal =
    val nextStatus = statusPayload(record.payload)
    val changed = previous.forall(previousJournal => statusPayload(previousJournal.payload) != nextStatus)
    val event = ujson.Obj(
      "event" -> (if previous.isEmpty then "OBSERVED" else if changed then "STATUS_CHANGED" else "OBSERVED"),
      "resourceVersion" -> record.resourceVersion.getOrElse(""),
      "observedAt" -> record.observedAt.toString,
      "status" -> ujson.read(nextStatus)
    )
    val history = previous match
      case Some(value) if !changed => value.history
      case Some(value) => (value.history :+ event).takeRight(MaxHistory)
      case None => List(event)
    StateJournal(record.key, record.resourceVersion, record.payload, record.observedAt, history, deleted = false)

  def markDeleted(value: StateJournal): StateJournal =
    if value.deleted then value
    else
      val event = ujson.Obj("event" -> "DELETED", "observedAt" -> Instant.now().toString)
      value.copy(history = (value.history :+ event).takeRight(MaxHistory), deleted = true)

  def fromJson(value: ujson.Value): StateJournal =
    val obj = value.obj
    val key = StateKey(
      Namespace.unsafe(obj("namespace").str),
      ResourceKind.fromApiResource(obj("kind").str).getOrElse(throw IllegalArgumentException("unknown state journal resource kind")),
      obj("name").str
    )
    StateJournal(
      key,
      Option(obj("resourceVersion").str).filter(_.nonEmpty),
      obj("payload").render(),
      Instant.parse(obj("observedAt").str),
      obj("history").arr.toList.map(_.obj),
      obj("deleted").bool
    )

  private def statusPayload(payload: String): String =
    scala.util.Try(ujson.read(payload)).toOption.flatMap(_.obj.get("status")).map(_.render()).getOrElse("{}")

object StateRecord:
  def fromJson(namespace: Namespace, kind: ResourceKind, raw: String): Either[String, StateRecord] =
    scala.util.Try(ujson.read(raw)).toEither.left.map(_.toString).flatMap { value =>
      val name = value.obj
        .get("metadata")
        .flatMap(_.objOpt)
        .flatMap(_.get("name"))
        .flatMap(_.strOpt)
        .filter(_.nonEmpty)
        .toRight("state resource metadata.name is required")
      name.map { resourceName =>
        StateRecord(
          StateKey(namespace, kind, resourceName),
          value.obj.get("metadata").flatMap(_.objOpt).flatMap(_.get("resourceVersion")).flatMap(_.strOpt),
          value.render(),
          Instant.now()
        )
      }
    }

  def fromCollection(namespace: Namespace, kind: ResourceKind, raw: String): Either[String, List[StateRecord]] =
    scala.util.Try(ujson.read(raw)).toEither.left.map(_.toString).flatMap { value =>
      value.obj.get("items").flatMap(_.arrOpt).toRight("Kubernetes list response must contain items").flatMap { items =>
        items.toList.foldLeft[Either[String, List[StateRecord]]](Right(Nil)) { (records, item) =>
          for
            current <- records
            record <- fromJson(namespace, kind, item.render())
          yield current :+ record
        }
      }
    }

/** 状态存储端口；Kubernetes 实现直接读 CR，Postgres 实现保存观测投影。 */
trait StateStore:
  def backend: StateBackend
  def initialize: IO[Throwable, Unit] = ZIO.unit
  def putJournal(journal: StateJournal): IO[Throwable, Unit]
  def getJournal(key: StateKey): IO[Throwable, Option[StateJournal]]
  def listJournals(namespace: Namespace): IO[Throwable, List[StateJournal]]
  def put(record: StateRecord): IO[Throwable, Unit] = putJournal(StateJournal.fromRecord(record))
  def get(key: StateKey): IO[Throwable, Option[StateRecord]] = getJournal(key).map(_.map(_.record))
  def list(namespace: Namespace): IO[Throwable, List[StateRecord]] = listJournals(namespace).map(_.map(_.record))
  def delete(key: StateKey): IO[Throwable, Unit]

/** Kubernetes CRs are the source of truth in this mode; no second cache is introduced. */
/** Kubernetes 模式不复制缓存，CR 本身就是事实源。 */
final class KubernetesStateStore(api: KubernetesApi) extends StateStore:
  override val backend: StateBackend = StateBackend.Kubernetes

  override def putJournal(journal: StateJournal): UIO[Unit] = ZIO.unit

  override def getJournal(key: StateKey): IO[Throwable, Option[StateJournal]] =
    api.get(key.namespace, key.kind, key.name).flatMap { raw =>
      ZIO.fromEither(StateRecord.fromJson(key.namespace, key.kind, raw).left.map(IllegalArgumentException(_))).map(record => Some(StateJournal.fromRecord(record)))
    }

  override def listJournals(namespace: Namespace): IO[Throwable, List[StateJournal]] =
    ZIO.foreach(List(ResourceKind.Deployment, ResourceKind.SessionJob, ResourceKind.StateSnapshot)) { kind =>
      api.list(namespace, kind).flatMap(raw =>
        ZIO.fromEither(StateRecord.fromCollection(namespace, kind, raw).left.map(IllegalArgumentException(_)))
      ).map(_.map(StateJournal.fromRecord))
    }.map(_.flatten)

  override def delete(key: StateKey): UIO[Unit] = ZIO.unit

/** PostgreSQL 模式保存最新观测、resourceVersion 和有限生命周期历史。 */
final class PostgresStateStore(settings: StateStoreSettings) extends StateStore:
  override val backend: StateBackend = StateBackend.Postgres

  override def initialize: IO[Throwable, Unit] =
    ZIO.attemptBlocking {
      withConnection { connection =>
        val statement = connection.createStatement()
        try statement.executeUpdate(
          s"""CREATE TABLE IF NOT EXISTS ${settings.table} (
              |  namespace TEXT NOT NULL,
              |  resource_kind TEXT NOT NULL,
              |  resource_name TEXT NOT NULL,
              |  resource_version TEXT,
              |  payload TEXT NOT NULL,
              |  observed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
              |  lifecycle_json TEXT NOT NULL DEFAULT '[]',
              |  deleted BOOLEAN NOT NULL DEFAULT FALSE,
              |  PRIMARY KEY (namespace, resource_kind, resource_name)
              |)""".stripMargin
        )
        finally statement.close()
        val alter = connection.createStatement()
        try
          alter.executeUpdate(s"ALTER TABLE ${settings.table} ADD COLUMN IF NOT EXISTS lifecycle_json TEXT NOT NULL DEFAULT '[]'")
          alter.executeUpdate(s"ALTER TABLE ${settings.table} ADD COLUMN IF NOT EXISTS deleted BOOLEAN NOT NULL DEFAULT FALSE")
        finally alter.close()
      }
    }

  /** 使用 resourceVersion 条件 upsert，防止旧观察覆盖新观察。 */
  override def putJournal(journal: StateJournal): IO[Throwable, Unit] =
    ZIO.attemptBlocking {
      withConnection { connection =>
        // 只有 resourceVersion 不旧于数据库值时才覆盖，避免多副本轮询乱序回写。
        val sql =
          s"""INSERT INTO ${settings.table}
             |  (namespace, resource_kind, resource_name, resource_version, payload, observed_at, lifecycle_json, deleted)
             |VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
             |ON CONFLICT (namespace, resource_kind, resource_name) DO UPDATE SET
             |  resource_version = EXCLUDED.resource_version,
             |  payload = EXCLUDED.payload,
             |  observed_at = CURRENT_TIMESTAMP,
             |  lifecycle_json = EXCLUDED.lifecycle_json,
             |  deleted = EXCLUDED.deleted
             |WHERE ${settings.table}.resource_version IS NULL
             |   OR EXCLUDED.resource_version IS NULL
             |   OR EXCLUDED.resource_version = ${settings.table}.resource_version
             |   OR (EXCLUDED.resource_version ~ '^[0-9]+$$'
             |       AND ${settings.table}.resource_version ~ '^[0-9]+$$'
             |       AND EXCLUDED.resource_version::bigint >= ${settings.table}.resource_version::bigint)""".stripMargin
        val statement = connection.prepareStatement(sql)
        try
          statement.setString(1, journal.key.namespace.namespaceValue)
          statement.setString(2, journal.key.kind.apiResource)
          statement.setString(3, journal.key.name)
          journal.resourceVersion.fold(statement.setObject(4, null))(statement.setString(4, _))
          statement.setString(5, journal.payload)
          statement.setString(6, ujson.Arr(journal.history*).render())
          statement.setBoolean(7, journal.deleted)
          statement.executeUpdate()
          ()
        finally statement.close()
      }
    }

  override def getJournal(key: StateKey): IO[Throwable, Option[StateJournal]] =
    ZIO.attemptBlocking {
      withConnection { connection =>
        val statement = connection.prepareStatement(
          s"SELECT resource_version, payload, observed_at, lifecycle_json, deleted FROM ${settings.table} WHERE namespace = ? AND resource_kind = ? AND resource_name = ?"
        )
        try
          statement.setString(1, key.namespace.namespaceValue)
          statement.setString(2, key.kind.apiResource)
          statement.setString(3, key.name)
          val result = statement.executeQuery()
          try if result.next() then Some(readJournal(key, result)) else None
          finally result.close()
        finally statement.close()
      }
    }

  override def listJournals(namespace: Namespace): IO[Throwable, List[StateJournal]] =
    ZIO.attemptBlocking {
      withConnection { connection =>
        val statement = connection.prepareStatement(
          s"SELECT resource_kind, resource_name, resource_version, payload, observed_at, lifecycle_json, deleted FROM ${settings.table} WHERE namespace = ? ORDER BY resource_kind, resource_name"
        )
        try
          statement.setString(1, namespace.namespaceValue)
          val result = statement.executeQuery()
          try
            val records = List.newBuilder[StateJournal]
            while result.next() do
              ResourceKind.fromApiResource(result.getString("resource_kind")).foreach { kind =>
                records += readJournal(StateKey(namespace, kind, result.getString("resource_name")), result)
              }
            records.result()
          finally result.close()
        finally statement.close()
      }
    }

  override def delete(key: StateKey): IO[Throwable, Unit] =
    ZIO.attemptBlocking {
      withConnection { connection =>
        val statement = connection.prepareStatement(
          s"DELETE FROM ${settings.table} WHERE namespace = ? AND resource_kind = ? AND resource_name = ?"
        )
        try
          statement.setString(1, key.namespace.namespaceValue)
          statement.setString(2, key.kind.apiResource)
          statement.setString(3, key.name)
          statement.executeUpdate()
          ()
        finally statement.close()
      }
    }

  private def readJournal(key: StateKey, result: ResultSet): StateJournal =
    val observedAt = Option(result.getObject("observed_at", classOf[java.time.OffsetDateTime]))
      .map(_.toInstant)
      .getOrElse(Instant.EPOCH)
    StateJournal(key, Option(result.getString("resource_version")), result.getString("payload"), observedAt,
      ujson.read(result.getString("lifecycle_json")).arr.toList.map(_.obj), result.getBoolean("deleted"))

  private def withConnection[A](f: Connection => A): A =
    val connection = DriverManager.getConnection(settings.jdbcUrl.get, settings.username.get, settings.password.get)
    try f(connection)
    finally connection.close()

object StateStore:
  val live: ZLayer[KubernetesApi, Throwable, StateStore] =
    ZLayer.fromZIO {
      for
        api <- ZIO.service[KubernetesApi]
        settings <- ZIO.fromEither(StateStoreSettings.fromEnv(sys.env).left.map(IllegalArgumentException(_)))
      yield settings.backend match
        case StateBackend.Kubernetes => new KubernetesStateStore(api)
        case StateBackend.Postgres => new PostgresStateStore(settings)
    }

  def kubernetes(api: KubernetesApi): StateStore = new KubernetesStateStore(api)

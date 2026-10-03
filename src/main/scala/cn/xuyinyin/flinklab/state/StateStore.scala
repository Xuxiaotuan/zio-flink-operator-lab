package cn.xuyinyin.flinklab.state

import cn.xuyinyin.flinklab.cli.ResourceKind
import cn.xuyinyin.flinklab.domain.FlinkTypes.Namespace
import cn.xuyinyin.flinklab.kubernetes.KubernetesApi
import zio.*

import java.sql.{Connection, DriverManager, ResultSet}
import java.time.Instant

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

trait StateStore:
  def backend: StateBackend
  def initialize: IO[Throwable, Unit] = ZIO.unit
  def put(record: StateRecord): IO[Throwable, Unit]
  def get(key: StateKey): IO[Throwable, Option[StateRecord]]
  def list(namespace: Namespace): IO[Throwable, List[StateRecord]]
  def delete(key: StateKey): IO[Throwable, Unit]

/** Kubernetes CRs are the source of truth in this mode; no second cache is introduced. */
final class KubernetesStateStore(api: KubernetesApi) extends StateStore:
  override val backend: StateBackend = StateBackend.Kubernetes

  override def put(record: StateRecord): UIO[Unit] = ZIO.unit

  override def get(key: StateKey): IO[Throwable, Option[StateRecord]] =
    api.get(key.namespace, key.kind, key.name).flatMap { raw =>
      ZIO.fromEither(StateRecord.fromJson(key.namespace, key.kind, raw).left.map(IllegalArgumentException(_))).map(Some(_))
    }

  override def list(namespace: Namespace): IO[Throwable, List[StateRecord]] =
    ZIO.foreach(List(ResourceKind.Deployment, ResourceKind.SessionJob, ResourceKind.StateSnapshot)) { kind =>
      api.list(namespace, kind).flatMap(raw =>
        ZIO.fromEither(StateRecord.fromCollection(namespace, kind, raw).left.map(IllegalArgumentException(_)))
      )
    }.map(_.flatten)

  override def delete(key: StateKey): UIO[Unit] = ZIO.unit

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
              |  PRIMARY KEY (namespace, resource_kind, resource_name)
              |)""".stripMargin
        )
        finally statement.close()
      }
    }

  override def put(record: StateRecord): IO[Throwable, Unit] =
    ZIO.attemptBlocking {
      withConnection { connection =>
        val sql =
          s"""INSERT INTO ${settings.table}
             |  (namespace, resource_kind, resource_name, resource_version, payload, observed_at)
             |VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
             |ON CONFLICT (namespace, resource_kind, resource_name) DO UPDATE SET
             |  resource_version = EXCLUDED.resource_version,
             |  payload = EXCLUDED.payload,
             |  observed_at = CURRENT_TIMESTAMP
             |WHERE ${settings.table}.resource_version IS NULL
             |   OR EXCLUDED.resource_version IS NULL
             |   OR EXCLUDED.resource_version = ${settings.table}.resource_version
             |   OR (EXCLUDED.resource_version ~ '^[0-9]+$$'
             |       AND ${settings.table}.resource_version ~ '^[0-9]+$$'
             |       AND EXCLUDED.resource_version::bigint >= ${settings.table}.resource_version::bigint)""".stripMargin
        val statement = connection.prepareStatement(sql)
        try
          statement.setString(1, record.key.namespace.namespaceValue)
          statement.setString(2, record.key.kind.apiResource)
          statement.setString(3, record.key.name)
          record.resourceVersion.fold(statement.setObject(4, null))(statement.setString(4, _))
          statement.setString(5, record.payload)
          statement.executeUpdate()
          ()
        finally statement.close()
      }
    }

  override def get(key: StateKey): IO[Throwable, Option[StateRecord]] =
    ZIO.attemptBlocking {
      withConnection { connection =>
        val statement = connection.prepareStatement(
          s"SELECT resource_version, payload, observed_at FROM ${settings.table} WHERE namespace = ? AND resource_kind = ? AND resource_name = ?"
        )
        try
          statement.setString(1, key.namespace.namespaceValue)
          statement.setString(2, key.kind.apiResource)
          statement.setString(3, key.name)
          val result = statement.executeQuery()
          try if result.next() then Some(readRecord(key, result)) else None
          finally result.close()
        finally statement.close()
      }
    }

  override def list(namespace: Namespace): IO[Throwable, List[StateRecord]] =
    ZIO.attemptBlocking {
      withConnection { connection =>
        val statement = connection.prepareStatement(
          s"SELECT resource_kind, resource_name, resource_version, payload, observed_at FROM ${settings.table} WHERE namespace = ? ORDER BY resource_kind, resource_name"
        )
        try
          statement.setString(1, namespace.namespaceValue)
          val result = statement.executeQuery()
          try
            val records = List.newBuilder[StateRecord]
            while result.next() do
              ResourceKind.fromApiResource(result.getString("resource_kind")).foreach { kind =>
                records += readRecord(StateKey(namespace, kind, result.getString("resource_name")), result)
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

  private def readRecord(key: StateKey, result: ResultSet): StateRecord =
    val observedAt = Option(result.getObject("observed_at", classOf[java.time.OffsetDateTime]))
      .map(_.toInstant)
      .getOrElse(Instant.EPOCH)
    StateRecord(key, Option(result.getString("resource_version")), result.getString("payload"), observedAt)

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

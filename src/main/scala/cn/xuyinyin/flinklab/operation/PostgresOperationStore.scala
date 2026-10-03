package cn.xuyinyin.flinklab.operation

import cn.xuyinyin.flinklab.domain.*
import zio.*

import java.sql.{Connection, DriverManager}

final case class OperationStoreSettings(
    jdbcUrl: String,
    username: String,
    password: String,
    table: String = "zio_flink_operations"
):
  def validate: Either[String, Unit] =
    Either.cond(table.matches("[a-zA-Z_][a-zA-Z0-9_]*"), (), s"invalid operation table name: $table")

object OperationStoreSettings:
  def fromEnv(env: Map[String, String]): Either[String, OperationStoreSettings] =
    val jdbc = env.get("ZIO_FLINK_POSTGRES_JDBC_URL").filter(_.nonEmpty).orElse {
      for
        host <- env.get("POSTGRES_HOST").filter(_.nonEmpty)
        port <- env.get("POSTGRES_PORT").filter(_.nonEmpty)
        database <- env.get("POSTGRES_DB").filter(_.nonEmpty)
      yield s"jdbc:postgresql://$host:$port/$database"
    }
    for
      jdbcUrl <- jdbc.toRight("ZIO_FLINK_POSTGRES_JDBC_URL or POSTGRES_HOST is required")
      username <- env.get("ZIO_FLINK_POSTGRES_USER").filter(_.nonEmpty).orElse(env.get("POSTGRES_USER").filter(_.nonEmpty)).toRight("ZIO_FLINK_POSTGRES_USER or POSTGRES_USER is required")
      password <- env.get("ZIO_FLINK_POSTGRES_PASSWORD").filter(_.nonEmpty).orElse(env.get("POSTGRES_PASSWORD").filter(_.nonEmpty)).toRight("ZIO_FLINK_POSTGRES_PASSWORD or POSTGRES_PASSWORD is required")
      settings = OperationStoreSettings(jdbcUrl, username, password, env.getOrElse("ZIO_FLINK_OPERATION_TABLE", "zio_flink_operations"))
      _ <- settings.validate
    yield settings

final class PostgresOperationStore(settings: OperationStoreSettings) extends OperationStore:
  override def initialize: IO[Throwable, Unit] =
    ZIO.attemptBlocking {
      withConnection { connection =>
        val statement = connection.createStatement()
        try
          statement.executeUpdate(s"CREATE TABLE IF NOT EXISTS ${settings.table} (operation_id TEXT PRIMARY KEY, operation_json TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP)")
          statement.executeUpdate(s"ALTER TABLE ${settings.table} ADD COLUMN IF NOT EXISTS request_id TEXT")
          statement.executeUpdate(s"UPDATE ${settings.table} SET request_id = operation_id WHERE request_id IS NULL")
          statement.executeUpdate(s"ALTER TABLE ${settings.table} ALTER COLUMN request_id SET NOT NULL")
          statement.executeUpdate(s"CREATE UNIQUE INDEX IF NOT EXISTS ${settings.table}_request_id_uq ON ${settings.table} (request_id)")
        finally statement.close()
      }
    }

  override def create(operation: Operation): IO[ControlPlaneError, Unit] =
    ZIO.attemptBlocking {
      withConnection { connection =>
        val statement = connection.prepareStatement(s"INSERT INTO ${settings.table} (operation_id, request_id, operation_json) VALUES (?, ?, ?)")
        try
          statement.setString(1, operation.id.operationIdValue)
          statement.setString(2, operation.requestId.requestIdValue)
          statement.setString(3, OperationCodec.json(operation))
          statement.executeUpdate()
          ()
        finally statement.close()
      }
    }.mapError {
      case error: java.sql.SQLException if error.getSQLState == "23505" => ControlPlaneError.OperationAlreadyExists(operation.id)
      case error => ControlPlaneError.StoreFailure(Option(error.getMessage).getOrElse(error.toString))
    }

  override def transition(id: OperationId, event: OperationEvent): IO[ControlPlaneError, Operation] =
    ZIO.attemptBlocking {
      withConnection { connection =>
        connection.setAutoCommit(false)
        try
          val select = connection.prepareStatement(s"SELECT operation_json FROM ${settings.table} WHERE operation_id = ? FOR UPDATE")
          val current = try
            select.setString(1, id.operationIdValue)
            val result = select.executeQuery()
            try if result.next() then Some(result.getString(1)) else None finally result.close()
          finally select.close()
          val operation = current.toRight(ControlPlaneError.OperationNotFound(id)).flatMap(raw => OperationCodec.fromJson(raw).left.map(ControlPlaneError.StoreFailure.apply)).flatMap(_.advance(event))
          val next = operation.fold(error => throw StoreTransitionException(error), identity)
          val update = connection.prepareStatement(s"UPDATE ${settings.table} SET operation_json = ?, updated_at = CURRENT_TIMESTAMP WHERE operation_id = ?")
          try
            update.setString(1, OperationCodec.json(next))
            update.setString(2, id.operationIdValue)
            update.executeUpdate()
          finally update.close()
          connection.commit()
          next
        catch
          case error: Throwable => connection.rollback(); throw error
        finally connection.setAutoCommit(true)
      }
    }.mapError {
      case StoreTransitionException(error) => error
      case error => ControlPlaneError.StoreFailure(Option(error.getMessage).getOrElse(error.toString))
    }

  override def get(id: OperationId): IO[ControlPlaneError, Option[Operation]] =
    ZIO.attemptBlocking {
      withConnection { connection =>
        val statement = connection.prepareStatement(s"SELECT operation_json FROM ${settings.table} WHERE operation_id = ?")
        try
          statement.setString(1, id.operationIdValue)
          val result = statement.executeQuery()
          try if result.next() then OperationCodec.fromJson(result.getString(1)).map(Some(_)).left.map(ControlPlaneError.StoreFailure.apply) else Right(None) finally result.close()
        finally statement.close()
      }
    }.mapError(error => ControlPlaneError.StoreFailure(Option(error.getMessage).getOrElse(error.toString))).flatMap(ZIO.fromEither)

  override def list: IO[ControlPlaneError, List[Operation]] =
    ZIO.attemptBlocking {
      withConnection { connection =>
        val statement = connection.createStatement()
        try
          val result = statement.executeQuery(s"SELECT operation_json FROM ${settings.table} ORDER BY updated_at, operation_id")
          try
            val builder = List.newBuilder[Operation]
            while result.next() do OperationCodec.fromJson(result.getString(1)) match
              case Right(operation) => builder += operation
              case Left(error) => throw StoreTransitionException(ControlPlaneError.StoreFailure(error))
            Right(builder.result())
          finally result.close()
        finally statement.close()
      }
    }.mapError {
      case StoreTransitionException(error) => error
      case error => ControlPlaneError.StoreFailure(Option(error.getMessage).getOrElse(error.toString))
    }.flatMap(ZIO.fromEither)

  override def findByRequestId(requestId: RequestId): IO[ControlPlaneError, Option[Operation]] =
    ZIO.attemptBlocking {
      withConnection { connection =>
        val statement = connection.prepareStatement(s"SELECT operation_json FROM ${settings.table} WHERE request_id = ?")
        try
          statement.setString(1, requestId.requestIdValue)
          val result = statement.executeQuery()
          try if result.next() then OperationCodec.fromJson(result.getString(1)).map(Some(_)).left.map(ControlPlaneError.StoreFailure.apply) else Right(None) finally result.close()
        finally statement.close()
      }
    }.mapError(error => ControlPlaneError.StoreFailure(Option(error.getMessage).getOrElse(error.toString))).flatMap(ZIO.fromEither)

  private def withConnection[A](f: Connection => A): A =
    val connection = DriverManager.getConnection(settings.jdbcUrl, settings.username, settings.password)
    try f(connection)
    finally connection.close()

private final case class StoreTransitionException(error: ControlPlaneError) extends RuntimeException(error.message)

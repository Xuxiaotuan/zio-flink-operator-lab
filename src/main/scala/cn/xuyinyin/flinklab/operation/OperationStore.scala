package cn.xuyinyin.flinklab.operation

import cn.xuyinyin.flinklab.domain.*
import zio.*

trait OperationStore:
  def initialize: IO[Throwable, Unit] = ZIO.unit
  def create(operation: Operation): IO[ControlPlaneError, Unit]
  def transition(id: OperationId, event: OperationEvent): IO[ControlPlaneError, Operation]
  def get(id: OperationId): IO[ControlPlaneError, Option[Operation]]
  def list: IO[ControlPlaneError, List[Operation]]
  def findByRequestId(requestId: RequestId): IO[ControlPlaneError, Option[Operation]] =
    list.map(_.find(_.requestId == requestId))

final class InMemoryOperationStore private (ref: Ref[Map[OperationId, Operation]]) extends OperationStore:
  override def create(operation: Operation): IO[ControlPlaneError, Unit] =
    ref.modify { current =>
      if current.contains(operation.id) then
        (Left(ControlPlaneError.OperationAlreadyExists(operation.id)), current)
      else
        (Right(()), current.updated(operation.id, operation))
    }.flatMap(result => ZIO.fromEither(result))

  override def transition(id: OperationId, event: OperationEvent): IO[ControlPlaneError, Operation] =
    ref.modify { current =>
      current.get(id) match
        case None => (Left(ControlPlaneError.OperationNotFound(id)), current)
        case Some(operation) =>
          operation.advance(event) match
            case Left(error)    => (Left(error), current)
            case Right(next)    => (Right(next), current.updated(id, next))
    }.flatMap(result => ZIO.fromEither(result))

  override def get(id: OperationId): IO[ControlPlaneError, Option[Operation]] = ref.get.map(_.get(id))

  override def list: IO[ControlPlaneError, List[Operation]] = ref.get.map(_.values.toList.sortBy(_.createdAt))

object InMemoryOperationStore:
  def make: UIO[InMemoryOperationStore] = Ref.make(Map.empty[OperationId, Operation]).map(new InMemoryOperationStore(_))
  val layer: ZLayer[Any, Nothing, OperationStore] =
    ZLayer.fromZIO(make)

object OperationStore:
  val live: ZLayer[Any, Throwable, OperationStore] =
    ZLayer.fromZIO {
      sys.env.get("ZIO_FLINK_OPERATION_STORE").map(_.trim.toLowerCase) match
        case Some("postgres") | Some("postgresql") =>
          ZIO.fromEither(OperationStoreSettings.fromEnv(sys.env).left.map(IllegalArgumentException(_))).map(new PostgresOperationStore(_))
        case _ => InMemoryOperationStore.make.map(identity[OperationStore])
    }

final case class AcceptedOperation(
    operationId: OperationId,
    requestId: RequestId,
    acceptedAt: java.time.Instant
)

trait FlinkControlPlane:
  def accept(requestId: RequestId, operation: FlinkOperation): IO[ControlPlaneError, AcceptedOperation]
  def get(id: OperationId): IO[ControlPlaneError, Option[Operation]]

final class DefaultFlinkControlPlane(store: OperationStore) extends FlinkControlPlane:
  override def accept(requestId: RequestId, operation: FlinkOperation): IO[ControlPlaneError, AcceptedOperation] =
    val now = java.time.Instant.now()
    val resource = operation match
      case FlinkOperation.Deploy(spec)         => ResourceRef(spec.namespace, cn.xuyinyin.flinklab.cli.ResourceKind.Deployment, spec.name)
      case FlinkOperation.Upgrade(target, _, _) => target
      case FlinkOperation.Suspend(target, _)    => target
      case FlinkOperation.Resume(target)        => target
      case FlinkOperation.Restart(target, _)    => target
      case FlinkOperation.Snapshot(target, _)   => target
      case FlinkOperation.Delete(target, _)     => target
    store.findByRequestId(requestId).flatMap {
      case Some(existing) => ZIO.succeed(AcceptedOperation(existing.id, existing.requestId, existing.createdAt))
      case None =>
        val accepted = Operation.accepted(requestId, operation, resource, now)
        store.create(accepted).as(AcceptedOperation(accepted.id, requestId, now)).catchSome {
          case ControlPlaneError.OperationAlreadyExists(_) =>
            store.findByRequestId(requestId).flatMap {
              case Some(existing) => ZIO.succeed(AcceptedOperation(existing.id, existing.requestId, existing.createdAt))
              case None => ZIO.fail(ControlPlaneError.OperationAlreadyExists(accepted.id))
            }
        }
    }

  override def get(id: OperationId): IO[ControlPlaneError, Option[Operation]] = store.get(id)

object FlinkControlPlane:
  val live: ZLayer[OperationStore, Nothing, FlinkControlPlane] =
    ZLayer.fromFunction(new DefaultFlinkControlPlane(_))

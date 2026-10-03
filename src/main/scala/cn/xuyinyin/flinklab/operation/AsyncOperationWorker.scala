package cn.xuyinyin.flinklab.operation

import cn.xuyinyin.flinklab.application.{DefaultVerificationEngine, Evidence, VerificationEngine}
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.kubernetes.KubernetesApi
import cn.xuyinyin.flinklab.operator.observer.ResourceObserver
import cn.xuyinyin.flinklab.operator.savepoint.SavepointPatch
import zio.*

import java.time.Instant

trait AsyncOperationWorker:
  def submit(requestId: RequestId, operation: FlinkOperation): IO[ControlPlaneError, AcceptedOperation]
  def process(id: OperationId): IO[ControlPlaneError, Operation]

/** A process-level mutex protects reconcile side effects; the durable store protects transitions across replicas. */
final class OperationMutex(private val semaphore: Semaphore):
  def withPermit[A](effect: => IO[ControlPlaneError, A]): IO[ControlPlaneError, A] = semaphore.withPermit(effect)

object OperationMutex:
  val make: UIO[OperationMutex] = Semaphore.make(1).map(new OperationMutex(_))

final class DefaultAsyncOperationWorker(
    api: KubernetesApi,
    observer: ResourceObserver,
    verifier: VerificationEngine,
    store: OperationStore,
    mutex: OperationMutex
) extends AsyncOperationWorker:
  private val controlPlane = new DefaultFlinkControlPlane(store)

  override def submit(requestId: RequestId, operation: FlinkOperation): IO[ControlPlaneError, AcceptedOperation] =
    for
      accepted <- controlPlane.accept(requestId, operation)
      _ <- process(accepted.operationId).forkDaemon
    yield accepted

  override def process(id: OperationId): IO[ControlPlaneError, Operation] = mutex.withPermit {
    for
      current <- getRequired(id)
      _ <- transition(id, OperationEvent.ValidationStarted(Instant.now()))
      _ <- transition(id, OperationEvent.ValidationPassed(Instant.now()))
      response <- submitToKubernetes(current.command).mapError(error => ControlPlaneError.VerificationFailed(error.getMessage)).tapError(error => fail(id, error.message))
      _ <- transition(id, OperationEvent.Submitted(Instant.now(), metadata(response)._1, metadata(response)._2))
      _ <- transition(id, OperationEvent.WaitingForObservation(Instant.now()))
      completed <- current.command match
        case FlinkOperation.Delete(_, _) =>
          for
            _ <- transition(id, OperationEvent.VerificationStarted(Instant.now()))
            result <- transition(id, OperationEvent.VerificationSucceeded(Instant.now()))
          yield result
        case _ =>
          val target = current.resource
          for
            observation <- observer.observe(target.namespace, target.kind, Some(target.name.nameValue)).take(1).runHead.flatMap(value => ZIO.fromOption(value).orElseFail(new IllegalStateException("resource observer completed without an observation"))).mapError(error => ControlPlaneError.VerificationFailed(error.getMessage))
            evidence = Evidence.fromObservation(target.namespace, observation)
            _ <- transition(id, OperationEvent.Observed(Instant.now(), evidence.observedGeneration))
            _ <- transition(id, OperationEvent.VerificationStarted(Instant.now()))
            _ <- ZIO.fromEither(verifier.verify(current.command, target, evidence)).mapError(identity)
            result <- transition(id, OperationEvent.VerificationSucceeded(Instant.now()))
          yield result
    yield completed
  }.catchAll { error =>
    fail(id, error.message) *> ZIO.fail(error)
  }

  private def getRequired(id: OperationId): IO[ControlPlaneError, Operation] =
    store.get(id).flatMap(ZIO.fromOption(_).orElseFail(ControlPlaneError.OperationNotFound(id)))

  private def transition(id: OperationId, event: OperationEvent): IO[ControlPlaneError, Operation] = store.transition(id, event)

  private def fail(id: OperationId, reason: String): IO[ControlPlaneError, Unit] =
    store.get(id).flatMap {
      case Some(operation) if Set(OperationState.Completed, OperationState.Failed, OperationState.TimedOut, OperationState.Superseded, OperationState.Uncertain).contains(operation.state) => ZIO.unit
      case Some(_) => store.transition(id, OperationEvent.Failed(Instant.now(), reason)).unit
      case None => ZIO.unit
    }

  private def submitToKubernetes(operation: FlinkOperation): IO[Throwable, String] = operation match
    case FlinkOperation.Deploy(spec) => api.apply(spec.namespace, spec.resource.render(), dryRun = false)
    case FlinkOperation.Upgrade(target, spec, _) => api.apply(target.namespace, spec.resource.render(), dryRun = false)
    case FlinkOperation.Suspend(target, _) => api.patch(target.namespace, target.kind, target.name.nameValue, """{"spec":{"job":{"state":"suspended"}}}""")
    case FlinkOperation.Resume(target) => api.patch(target.namespace, target.kind, target.name.nameValue, """{"spec":{"job":{"state":"running"}}}""")
    case FlinkOperation.Restart(target, _) => api.patch(target.namespace, target.kind, target.name.nameValue, """{"spec":{"restartNonce":""" + java.lang.System.currentTimeMillis() + """}}""")
    case FlinkOperation.Snapshot(target, policy) =>
      val patch = policy.snapshotType match
        case SnapshotType.Savepoint => SavepointPatch.trigger(java.lang.System.currentTimeMillis())
        case SnapshotType.Checkpoint => """{"spec":{"job":{"checkpointTriggerNonce":""" + java.lang.System.currentTimeMillis() + """}}}"""
      api.patch(target.namespace, target.kind, target.name.nameValue, patch)
    case FlinkOperation.Delete(target, _) => api.delete(target.namespace, target.kind, target.name.nameValue)

  private def metadata(response: String): (Option[Generation], Option[ResourceVersion]) =
    scala.util.Try(ujson.read(response)).toOption.flatMap(_.obj.get("metadata").flatMap(_.objOpt)).map { metadata =>
      val generation = metadata.get("generation").flatMap(value => value.numOpt.map(_.toLong).orElse(value.strOpt.flatMap(_.toLongOption))).flatMap(Generation.from(_).toOption)
      val resourceVersion = metadata.get("resourceVersion").flatMap(_.strOpt).flatMap(ResourceVersion.from(_).toOption)
      generation -> resourceVersion
    }.getOrElse(None -> None)

object AsyncOperationWorker:
  val live: ZLayer[KubernetesApi & OperationStore, Nothing, AsyncOperationWorker] =
    ZLayer.fromZIO {
      for
        api <- ZIO.service[KubernetesApi]
        store <- ZIO.service[OperationStore]
        mutex <- OperationMutex.make
      yield new DefaultAsyncOperationWorker(api, cn.xuyinyin.flinklab.operator.observer.ResourceObserver.live(api), DefaultVerificationEngine, store, mutex)
    }

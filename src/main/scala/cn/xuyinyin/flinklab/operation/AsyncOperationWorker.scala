package cn.xuyinyin.flinklab.operation

import cn.xuyinyin.flinklab.application.{DefaultPolicyEngine, DefaultVerificationEngine, Evidence, PolicyEngine, VerificationEngine}
import cn.xuyinyin.flinklab.cli.ResourceKind
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.kubernetes.KubernetesApi
import cn.xuyinyin.flinklab.operator.observer.ResourceObserver
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
    mutex: OperationMutex,
    policy: PolicyEngine = new DefaultPolicyEngine,
    coordinator: ResourceCoordinator = InMemoryResourceCoordinator
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
      completed <- coordinator.withLock(current.resource, id)(runOperation(id, current))
    yield completed
  }.catchAll { error =>
    terminalize(id, error) *> ZIO.fail(error)
  }

  private def runOperation(id: OperationId, current: Operation): IO[ControlPlaneError, Operation] =
    for
      _ <- transition(id, OperationEvent.ValidationStarted(Instant.now()))
      observed <- currentObservation(current)
      validated <- policy.validate(current.command, observed)
      _ <- transition(id, OperationEvent.ValidationPassed(Instant.now()))
      response <- submitToKubernetes(id, validated.operation).mapError(submissionError)
      _ <- transition(id, OperationEvent.Submitted(Instant.now(), metadata(response)._1, metadata(response)._2))
      _ <- transition(id, OperationEvent.WaitingForObservation(Instant.now()))
      completed <- current.command match
        case FlinkOperation.Delete(_, _) =>
          for
            _ <- transition(id, OperationEvent.VerificationStarted(Instant.now()))
            _ <- awaitDeletion(current.resource)
            result <- transition(id, OperationEvent.VerificationSucceeded(Instant.now()))
          yield result
        case _ =>
          val target = observationTarget(id, current)
          for
            _ <- transition(id, OperationEvent.VerificationStarted(Instant.now()))
            evidence <- awaitVerification(current.command, target, metadata(response)._1)
            _ <- transition(id, OperationEvent.Observed(Instant.now(), evidence.observedGeneration))
            result <- transition(id, OperationEvent.VerificationSucceeded(Instant.now()))
          yield result
    yield completed

  private def getRequired(id: OperationId): IO[ControlPlaneError, Operation] =
    store.get(id).flatMap(ZIO.fromOption(_).orElseFail(ControlPlaneError.OperationNotFound(id)))

  private def currentObservation(operation: Operation): IO[ControlPlaneError, Option[ObservedJobState]] =
    operation.command match
      case FlinkOperation.Deploy(_) | FlinkOperation.Snapshot(_, _) => ZIO.succeed(None)
      case _ =>
        observer.observe(operation.resource.namespace, operation.resource.kind, Some(operation.resource.name.nameValue)).take(1).runHead
          .map(_.map(observation => observedState(operation.resource, Evidence.fromObservation(operation.resource.namespace, observation))))
          .mapError(error => ControlPlaneError.VerificationFailed(Option(error.getMessage).getOrElse(error.toString)))

  private def observationTarget(id: OperationId, operation: Operation): ResourceRef =
    operation.command match
      case FlinkOperation.Snapshot(target, policy) =>
        ResourceRef(target.namespace, ResourceKind.StateSnapshot, policy.snapshotName.getOrElse(snapshotName(target.name, id)))
      case _ => operation.resource

  private def snapshotName(target: DeploymentName, id: OperationId): DeploymentName =
    val suffix = s"-snapshot-${id.operationIdValue.take(8)}"
    val base = target.nameValue.take((63 - suffix.length).max(1))
    DeploymentName.unsafe((base + suffix).take(63))

  private def observedState(resource: ResourceRef, evidence: Evidence): ObservedJobState =
    ObservedJobState(resource, evidence.jobState, evidence.reconciliation, evidence.generation, evidence.observedGeneration, evidence.snapshotPath)

  private def awaitVerification(operation: FlinkOperation, target: ResourceRef, expectedGeneration: Option[Generation]): IO[ControlPlaneError, Evidence] =
    observer.observe(target.namespace, target.kind, Some(target.name.nameValue))
      .map(observation => Evidence.fromObservation(target.namespace, observation))
      .map { evidence =>
        verifier.verify(operation, target, evidence, expectedGeneration) match
          case Right(result) => Some(Right(result.evidence))
          case Left(error) if evidence.jobState == JobState.Failed || evidence.reconciliation == ReconciliationState.Error => Some(Left(error))
          case Left(_) => None
      }
      .collectSome
      .runHead
      .mapError(error => ControlPlaneError.VerificationFailed(Option(error.getMessage).getOrElse(error.toString)))
      .flatMap {
        case Some(Right(evidence)) => ZIO.succeed(evidence)
        case Some(Left(error)) => ZIO.fail(error)
        case None => ZIO.fail(ControlPlaneError.VerificationFailed("verification stream completed before the submitted generation was observed"))
      }
      .timeoutFail(ControlPlaneError.VerificationTimedOut("verification timed out while waiting for the submitted generation"))(30.seconds)

  private def awaitDeletion(target: ResourceRef): IO[ControlPlaneError, Unit] =
    observer.observe(target.namespace, target.kind, Some(target.name.nameValue))
      .filter(_.eventType == cn.xuyinyin.flinklab.operator.watch.WatchEventType.Deleted)
      .runHead
      .mapError(error => ControlPlaneError.VerificationFailed(Option(error.getMessage).getOrElse(error.toString)))
      .flatMap(value => ZIO.when(value.isEmpty)(ZIO.fail(ControlPlaneError.VerificationFailed("delete observation stream completed before the target was deleted"))))
      .timeoutFail(ControlPlaneError.VerificationTimedOut("delete verification timed out"))(30.seconds)
      .unit

  private def transition(id: OperationId, event: OperationEvent): IO[ControlPlaneError, Operation] = store.transition(id, event)

  private def terminalize(id: OperationId, error: ControlPlaneError): IO[ControlPlaneError, Unit] =
    val event = error match
      case ControlPlaneError.VerificationTimedOut(reason) => OperationEvent.TimedOut(Instant.now(), reason)
      case ControlPlaneError.UncertainFailure(reason) => OperationEvent.Uncertain(Instant.now(), reason)
      case _ => OperationEvent.Failed(Instant.now(), error.message)
    store.get(id).flatMap {
      case Some(operation) if Set(OperationState.Completed, OperationState.Failed, OperationState.TimedOut, OperationState.Superseded, OperationState.Uncertain).contains(operation.state) => ZIO.unit
      case Some(_) => store.transition(id, event).unit
      case None => ZIO.unit
    }

  private def submissionError(error: Throwable): ControlPlaneError =
    error match
      case _: java.io.IOException => ControlPlaneError.UncertainFailure(Option(error.getMessage).getOrElse(error.toString))
      case _: cn.xuyinyin.flinklab.kubernetes.KubernetesApiError => ControlPlaneError.UncertainFailure(Option(error.getMessage).getOrElse(error.toString))
      case _ => ControlPlaneError.VerificationFailed(Option(error.getMessage).getOrElse(error.toString))

  private def submitToKubernetes(id: OperationId, operation: FlinkOperation): IO[Throwable, String] = operation match
    case FlinkOperation.Deploy(spec) => api.apply(spec.namespace, spec.resource.render(), dryRun = false)
    case FlinkOperation.Upgrade(target, spec, _) => api.apply(target.namespace, spec.resource.render(), dryRun = false)
    case FlinkOperation.Suspend(target, _) => api.patch(target.namespace, target.kind, target.name.nameValue, """{"spec":{"job":{"state":"suspended"}}}""")
    case FlinkOperation.Resume(target) => api.patch(target.namespace, target.kind, target.name.nameValue, """{"spec":{"job":{"state":"running"}}}""")
    case FlinkOperation.Restart(target, _) => api.patch(target.namespace, target.kind, target.name.nameValue, """{"spec":{"restartNonce":""" + java.lang.System.currentTimeMillis() + """}}""")
    case FlinkOperation.Snapshot(target, policy) =>
      val spec = FlinkStateSnapshotSpec(target.namespace, policy.snapshotName.getOrElse(snapshotName(target.name, id)), target.kind, target.name, policy.snapshotType)
      api.apply(target.namespace, spec.resource.render(), dryRun = false)
    case FlinkOperation.Delete(target, _) => api.delete(target.namespace, target.kind, target.name.nameValue)

  private def metadata(response: String): (Option[Generation], Option[ResourceVersion]) =
    scala.util.Try(ujson.read(response)).toOption.flatMap(_.obj.get("metadata").flatMap(_.objOpt)).map { metadata =>
      val generation = metadata.get("generation").flatMap(value => value.numOpt.map(_.toLong).orElse(value.strOpt.flatMap(_.toLongOption))).flatMap(Generation.from(_).toOption)
      val resourceVersion = metadata.get("resourceVersion").flatMap(_.strOpt).flatMap(ResourceVersion.from(_).toOption)
      generation -> resourceVersion
    }.getOrElse(None -> None)

object AsyncOperationWorker:
  val live: ZLayer[KubernetesApi & OperationStore & PolicyEngine & ResourceCoordinator, Nothing, AsyncOperationWorker] =
    ZLayer.fromZIO {
      for
        api <- ZIO.service[KubernetesApi]
        store <- ZIO.service[OperationStore]
        policy <- ZIO.service[PolicyEngine]
        coordinator <- ZIO.service[ResourceCoordinator]
        mutex <- OperationMutex.make
      yield new DefaultAsyncOperationWorker(api, cn.xuyinyin.flinklab.operator.observer.ResourceObserver.live(api), DefaultVerificationEngine, store, mutex, policy, coordinator)
    }

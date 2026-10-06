package cn.xuyinyin.flinklab.domain

import cn.xuyinyin.flinklab.cli.ResourceKind
import cn.xuyinyin.flinklab.domain.FlinkTypes.*

import java.time.Instant
import java.util.UUID

/** Typed identifiers keep retry, audit and resource identity distinct. */
opaque type RequestId = String
object RequestId:
  def from(value: String): Either[String, RequestId] =
    if value.trim.nonEmpty then Right(value.trim) else Left("request id must not be empty")
  def generate(): RequestId = UUID.randomUUID().toString
extension (value: RequestId)
  def requestIdValue: String = value

opaque type OperationId = String
object OperationId:
  def from(value: String): Either[String, OperationId] =
    if value.trim.nonEmpty then Right(value.trim) else Left("operation id must not be empty")
  def generate(): OperationId = UUID.randomUUID().toString
  def forRequest(requestId: RequestId): OperationId = UUID.nameUUIDFromBytes(requestId.requestIdValue.getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString
extension (value: OperationId)
  def operationIdValue: String = value

opaque type ResourceUid = String
object ResourceUid:
  def from(value: String): Either[String, ResourceUid] =
    if value.trim.nonEmpty then Right(value.trim) else Left("resource uid must not be empty")
extension (value: ResourceUid)
  def resourceUidValue: String = value

opaque type ResourceVersion = String
object ResourceVersion:
  def from(value: String): Either[String, ResourceVersion] =
    if value.trim.nonEmpty then Right(value.trim) else Left("resource version must not be empty")
extension (value: ResourceVersion)
  def resourceVersionValue: String = value

opaque type Generation = Long
object Generation:
  def from(value: Long): Either[String, Generation] =
    if value > 0 then Right(value) else Left("generation must be positive")
  def unsafe(value: Long): Generation = value
extension (value: Generation)
  def generationValue: Long = value

opaque type SnapshotPath = String
object SnapshotPath:
  def from(value: String): Either[String, SnapshotPath] =
    if value.trim.nonEmpty then Right(value.trim) else Left("snapshot path must not be empty")
extension (value: SnapshotPath)
  def snapshotPathValue: String = value

enum StateProtection:
  case Stateless, LastState, Savepoint

object StateProtection:
  extension (value: StateProtection)
    def operatorValue: String = value match
      case StateProtection.Stateless => "stateless"
      case StateProtection.LastState  => "last-state"
      case StateProtection.Savepoint  => "savepoint"

  def parse(value: String): Either[String, StateProtection] =
    value.trim.toLowerCase match
      case "stateless"  => Right(Stateless)
      case "last-state" => Right(LastState)
      case "savepoint"  => Right(Savepoint)
      case other => Left(s"unknown state protection: $other (use stateless, last-state or savepoint)")

enum DesiredJobState:
  case Running, Suspended

object DesiredJobState:
  def parse(value: String): Either[String, DesiredJobState] = value.trim.toLowerCase match
    case "running" => Right(Running)
    case "suspended" => Right(Suspended)
    case other => Left(s"unknown desired job state: $other (use running or suspended)")

  extension (value: DesiredJobState)
    def operatorValue: String = value match
      case DesiredJobState.Running   => "running"
      case DesiredJobState.Suspended => "suspended"

enum FallbackPolicy:
  case Forbidden, AllowLastState

enum JobState:
  case Running, Suspended, Finished, Failed, Unknown

enum ReconciliationState:
  case BeforeFirstDeployment, Deploying, Ready, Reconciling, Error, Unknown

final case class UpgradePolicy(
    protection: StateProtection,
    fallback: FallbackPolicy
):
  def validate: Either[ControlPlaneError, Unit] =
    (protection, fallback) match
      case (StateProtection.Stateless, FallbackPolicy.AllowLastState) =>
        Left(ControlPlaneError.InvalidPolicy("stateless operations cannot enable last-state fallback"))
      case _ => Right(())

final case class ResourceRef(
    namespace: Namespace,
    kind: ResourceKind,
    name: DeploymentName,
    uid: Option[ResourceUid] = None
):
  def json: ujson.Obj =
    val value = ujson.Obj(
      "namespace" -> namespace.namespaceValue,
      "kind" -> kind.apiResource,
      "name" -> name.nameValue
    )
    uid.foreach(current => value("uid") = current.resourceUidValue)
    value

final case class ObservedJobState(
    resource: ResourceRef,
    jobState: JobState,
    reconciliation: ReconciliationState,
    generation: Option[Generation],
    observedGeneration: Option[Generation],
    savepointDirectory: Option[SnapshotPath],
    lastSavepointPath: Option[SnapshotPath] = None
)

enum DeletePolicy:
  case Graceful, Force

enum SuspendPolicy:
  case KeepState, DiscardState

final case class SnapshotPolicy(snapshotType: SnapshotType, snapshotName: Option[DeploymentName] = None)

enum FlinkOperation:
  case Deploy(spec: FlinkDeploymentSpec)
  case Upgrade(target: ResourceRef, spec: FlinkDeploymentSpec, policy: UpgradePolicy)
  case Suspend(target: ResourceRef, policy: SuspendPolicy)
  case Resume(target: ResourceRef)
  case Restart(target: ResourceRef, policy: UpgradePolicy)
  case Snapshot(target: ResourceRef, policy: SnapshotPolicy)
  case Delete(target: ResourceRef, policy: DeletePolicy)

enum OperationState:
  case Accepted
  case Validating
  case Submitted
  case WaitingForObservation
  case Reconciling
  case Verifying
  case Completed
  case Failed
  case Superseded
  case TimedOut
  case Uncertain

enum ActualProtection:
  case EmptyState
  case LastState
  case Savepoint(path: SnapshotPath)

final case class SnapshotRef(
    resource: ResourceRef,
    state: String,
    path: Option[SnapshotPath]
):
  def json: ujson.Obj =
    val value = ujson.Obj("resource" -> resource.json, "state" -> state)
    path.foreach(current => value("path") = current.snapshotPathValue)
    value

final case class VerificationEvidence(
    resource: ResourceRef,
    resourceVersion: Option[ResourceVersion],
    uid: Option[ResourceUid],
    generation: Option[Generation],
    observedGeneration: Option[Generation],
    snapshotState: Option[String],
    snapshotPath: Option[SnapshotPath],
    reason: String
):
  def json: ujson.Obj =
    val value = ujson.Obj(
      "resource" -> resource.json,
      "reason" -> reason
    )
    resourceVersion.foreach(current => value("resourceVersion") = current.resourceVersionValue)
    uid.foreach(current => value("uid") = current.resourceUidValue)
    generation.foreach(current => value("generation") = current.generationValue)
    observedGeneration.foreach(current => value("observedGeneration") = current.generationValue)
    snapshotState.foreach(current => value("snapshotState") = current)
    snapshotPath.foreach(current => value("snapshotPath") = current.snapshotPathValue)
    value

enum OperationEvent:
  case Accepted(at: Instant)
  case ValidationStarted(at: Instant)
  case ValidationPassed(at: Instant)
  case Submitted(at: Instant, generation: Option[Generation], resourceVersion: Option[ResourceVersion], uid: Option[ResourceUid] = None)
  case WaitingForObservation(at: Instant)
  case Observed(at: Instant, observedGeneration: Option[Generation])
  case ReconciliationStarted(at: Instant)
  case SnapshotStarted(at: Instant, snapshot: SnapshotRef)
  case SnapshotCompleted(at: Instant, snapshot: SnapshotRef)
  case FallbackDetected(at: Instant, requested: StateProtection, actual: ActualProtection)
  case VerificationStarted(at: Instant)
  case VerificationSucceeded(at: Instant, evidence: Option[VerificationEvidence] = None)
  case Failed(at: Instant, reason: String)
  case TimedOut(at: Instant, reason: String)
  case Superseded(at: Instant, replacedBy: OperationId)
  case Uncertain(at: Instant, reason: String)

object OperationEvent:
  def at(event: OperationEvent): Instant = event match
    case Accepted(value)                       => value
    case ValidationStarted(value)              => value
    case ValidationPassed(value)               => value
    case Submitted(value, _, _, _)             => value
    case WaitingForObservation(value)          => value
    case Observed(value, _)                    => value
    case ReconciliationStarted(value)          => value
    case SnapshotStarted(value, _)             => value
    case SnapshotCompleted(value, _)           => value
    case FallbackDetected(value, _, _)         => value
    case VerificationStarted(value)            => value
    case VerificationSucceeded(value, _)       => value
    case Failed(value, _)                      => value
    case TimedOut(value, _)                    => value
    case Superseded(value, _)                  => value
    case Uncertain(value, _)                   => value

  def json(event: OperationEvent): ujson.Obj = event match
    case Accepted(at) => ujson.Obj("type" -> "ACCEPTED", "at" -> at.toString)
    case ValidationStarted(at) => ujson.Obj("type" -> "VALIDATION_STARTED", "at" -> at.toString)
    case ValidationPassed(at) => ujson.Obj("type" -> "VALIDATION_PASSED", "at" -> at.toString)
    case Submitted(at, generation, resourceVersion, uid) =>
      val value = ujson.Obj(
        "type" -> "SUBMITTED",
        "at" -> at.toString,
        "generation" -> generation.map(_.generationValue).getOrElse(0L),
        "resourceVersion" -> resourceVersion.map(_.resourceVersionValue).getOrElse("")
      )
      uid.foreach(current => value("uid") = current.resourceUidValue)
      value
    case WaitingForObservation(at) => ujson.Obj("type" -> "WAITING_FOR_OBSERVATION", "at" -> at.toString)
    case Observed(at, observedGeneration) =>
      ujson.Obj("type" -> "OBSERVED", "at" -> at.toString, "observedGeneration" -> observedGeneration.map(_.generationValue).getOrElse(0L))
    case ReconciliationStarted(at) => ujson.Obj("type" -> "RECONCILIATION_STARTED", "at" -> at.toString)
    case SnapshotStarted(at, snapshot) => ujson.Obj("type" -> "SNAPSHOT_STARTED", "at" -> at.toString, "snapshot" -> snapshot.json)
    case SnapshotCompleted(at, snapshot) => ujson.Obj("type" -> "SNAPSHOT_COMPLETED", "at" -> at.toString, "snapshot" -> snapshot.json)
    case FallbackDetected(at, requested, actual) =>
      ujson.Obj("type" -> "FALLBACK_DETECTED", "at" -> at.toString, "requestedProtection" -> requested.toString, "actualProtection" -> actual.toString)
    case VerificationStarted(at) => ujson.Obj("type" -> "VERIFICATION_STARTED", "at" -> at.toString)
    case VerificationSucceeded(at, evidence) =>
      val value = ujson.Obj("type" -> "VERIFICATION_SUCCEEDED", "at" -> at.toString)
      evidence.foreach(current => value("evidence") = current.json)
      value
    case Failed(at, reason) => ujson.Obj("type" -> "FAILED", "at" -> at.toString, "reason" -> reason)
    case TimedOut(at, reason) => ujson.Obj("type" -> "TIMED_OUT", "at" -> at.toString, "reason" -> reason)
    case Superseded(at, replacedBy) => ujson.Obj("type" -> "SUPERSEDED", "at" -> at.toString, "replacedBy" -> replacedBy.operationIdValue)
    case Uncertain(at, reason) => ujson.Obj("type" -> "UNCERTAIN", "at" -> at.toString, "reason" -> reason)

sealed trait ControlPlaneError extends Product with Serializable:
  def message: String

object ControlPlaneError:
  final case class InvalidPolicy(reason: String) extends ControlPlaneError:
    def message: String = reason
  final case class InvalidTransition(from: OperationState, event: OperationEvent) extends ControlPlaneError:
    def message: String = s"operation cannot apply ${event.getClass.getSimpleName} from $from"
  final case class OperationAlreadyExists(id: OperationId) extends ControlPlaneError:
    def message: String = s"operation already exists: ${id.operationIdValue}"
  final case class OperationNotFound(id: OperationId) extends ControlPlaneError:
    def message: String = s"operation not found: ${id.operationIdValue}"
  final case class UnsupportedOperation(operation: FlinkOperation) extends ControlPlaneError:
    def message: String = s"operation is not supported by this control-plane phase: ${operation.getClass.getSimpleName}"
  final case class VerificationFailed(reason: String) extends ControlPlaneError:
    def message: String = reason
  final case class VerificationTimedOut(reason: String) extends ControlPlaneError:
    def message: String = reason
  final case class UncertainFailure(reason: String) extends ControlPlaneError:
    def message: String = reason
  final case class ResourceBusy(resource: ResourceRef) extends ControlPlaneError:
    def message: String = s"resource is already being mutated: ${resource.namespace.namespaceValue}/${resource.kind.apiResource}/${resource.name.nameValue}"
  final case class StoreFailure(reason: String) extends ControlPlaneError:
    def message: String = reason

final case class ValidatedOperation(
    operation: FlinkOperation,
    observed: Option[ObservedJobState]
)

final case class Operation(
    id: OperationId,
    requestId: RequestId,
    command: FlinkOperation,
    resource: ResourceRef,
    state: OperationState,
    events: List[OperationEvent],
    createdAt: Instant,
    updatedAt: Instant
):
  def json: ujson.Obj =
    ujson.Obj(
      "operationId" -> id.operationIdValue,
      "requestId" -> requestId.requestIdValue,
      "operationType" -> command.getClass.getSimpleName.stripSuffix("$"),
      "resource" -> resource.json,
      "state" -> state.toString.toUpperCase,
      "createdAt" -> createdAt.toString,
      "updatedAt" -> updatedAt.toString,
      "events" -> ujson.Arr(events.map(OperationEvent.json)*),
    )

  def advance(event: OperationEvent): Either[ControlPlaneError, Operation] =
    OperationStateMachine.next(state, event).map(nextState => copy(state = nextState, events = events :+ event, updatedAt = OperationEvent.at(event)))

object Operation:
  def accepted(requestId: RequestId, command: FlinkOperation, resource: ResourceRef, at: Instant): Operation =
    val id = OperationId.forRequest(requestId)
    Operation(id, requestId, command, resource, OperationState.Accepted, List(OperationEvent.Accepted(at)), at, at)

object OperationStateMachine:
  def next(state: OperationState, event: OperationEvent): Either[ControlPlaneError, OperationState] =
    val next = event match
      case OperationEvent.Accepted(_)                  => None
      case OperationEvent.ValidationStarted(_)         => state match
        case OperationState.Accepted => Some(OperationState.Validating)
        case _ => None
      case OperationEvent.ValidationPassed(_)          => state match
        case OperationState.Validating => Some(OperationState.Validating)
        case _ => None
      case OperationEvent.Submitted(_, _, _, _)        => state match
        case OperationState.Validating => Some(OperationState.Submitted)
        case _ => None
      case OperationEvent.WaitingForObservation(_)     => state match
        case OperationState.Submitted => Some(OperationState.WaitingForObservation)
        case _ => None
      case OperationEvent.Observed(_, _)               => state match
        case OperationState.WaitingForObservation => Some(OperationState.Reconciling)
        case OperationState.Reconciling            => Some(OperationState.Reconciling)
        case OperationState.Verifying              => Some(OperationState.Verifying)
        case _ => None
      case OperationEvent.ReconciliationStarted(_)     => state match
        case OperationState.Submitted | OperationState.WaitingForObservation => Some(OperationState.Reconciling)
        case _ => None
      case OperationEvent.SnapshotStarted(_, _)        => state match
        case OperationState.Reconciling => Some(OperationState.Verifying)
        case _ => None
      case OperationEvent.SnapshotCompleted(_, _)      => state match
        case OperationState.Verifying => Some(OperationState.Verifying)
        case _ => None
      case OperationEvent.FallbackDetected(_, _, _)    => state match
        case OperationState.Verifying => Some(OperationState.Verifying)
        case _ => None
      case OperationEvent.VerificationStarted(_)       => state match
        case OperationState.WaitingForObservation | OperationState.Reconciling | OperationState.Verifying => Some(OperationState.Verifying)
        case _ => None
      case OperationEvent.VerificationSucceeded(_, _)  => state match
        case OperationState.Verifying => Some(OperationState.Completed)
        case _ => None
      case OperationEvent.Failed(_, _)                 => terminalTransition(state, OperationState.Failed)
      case OperationEvent.TimedOut(_, _)               => terminalTransition(state, OperationState.TimedOut)
      case OperationEvent.Superseded(_, _)             => terminalTransition(state, OperationState.Superseded)
      case OperationEvent.Uncertain(_, _)              => terminalTransition(state, OperationState.Uncertain)
    next.toRight(ControlPlaneError.InvalidTransition(state, event))

  private def terminalTransition(state: OperationState, terminal: OperationState): Option[OperationState] =
    state match
      case OperationState.Completed | OperationState.Failed | OperationState.Superseded | OperationState.TimedOut | OperationState.Uncertain => None
      case _ => Some(terminal)

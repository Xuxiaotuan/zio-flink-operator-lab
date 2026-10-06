package cn.xuyinyin.flinklab.application

import cn.xuyinyin.flinklab.cli.ResourceKind
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.operator.observer.ResourceObservation
import cn.xuyinyin.flinklab.operator.watch.{FlinkStateSnapshotStatus, FlinkStatusSnapshot}

import java.time.Instant

final case class Evidence(
    resource: ResourceRef,
    resourceVersion: Option[ResourceVersion],
    uid: Option[ResourceUid],
    generation: Option[Generation],
    observedGeneration: Option[Generation],
    reconciliation: ReconciliationState,
    jobState: JobState,
    snapshotState: Option[String],
    snapshotPath: Option[SnapshotPath],
    capturedAt: Instant,
    raw: String
)

object Evidence:
  def fromObservation(namespace: Namespace, observation: ResourceObservation): Evidence =
    val resource = ResourceRef(namespace, observation.kind, DeploymentName.unsafe(observation.name), observation.uid.flatMap(ResourceUid.from(_).toOption))
    val rv = observation.resourceVersion.flatMap(ResourceVersion.from(_).toOption)
    val generation = observation.generation.flatMap(Generation.from(_).toOption)
    val observedGeneration = observation.observedGeneration.flatMap(Generation.from(_).toOption)
    observation.kind match
      case ResourceKind.StateSnapshot =>
        val snapshot = FlinkStateSnapshotStatus.fromJsonString(observation.payload).toOption
        Evidence(resource, rv, resource.uid, generation, observedGeneration, ReconciliationState.Unknown, JobState.Unknown, snapshot.flatMap(_.state), snapshot.flatMap(_.path.flatMap(SnapshotPath.from(_).toOption)), Instant.now(), observation.payload)
      case _ =>
        val status = FlinkStatusSnapshot.fromJsonString(observation.payload).toOption
        Evidence(resource, rv, resource.uid, generation, observedGeneration, reconciliation(status.flatMap(_.reconciliationState), status.flatMap(_.error)), jobState(status.flatMap(_.jobState)), None, status.flatMap(_.lastSavepointLocation.flatMap(SnapshotPath.from(_).toOption)), Instant.now(), observation.payload)

  private def reconciliation(value: Option[String], error: Option[String]): ReconciliationState =
    if error.exists(_.trim.nonEmpty) then ReconciliationState.Error
    else value.map(_.toUpperCase) match
      case Some("DEPLOYED") | Some("READY") => ReconciliationState.Ready
      case Some("RECONCILING") | Some("UPGRADING") => ReconciliationState.Reconciling
      case Some("ERROR") | Some("FAILED") => ReconciliationState.Error
      case _ => ReconciliationState.Unknown

  private def jobState(value: Option[String]): JobState = value.map(_.toUpperCase) match
    case Some("RUNNING") => JobState.Running
    case Some("SUSPENDED") => JobState.Suspended
    case Some("FINISHED") => JobState.Finished
    case Some("FAILED") => JobState.Failed
    case _ => JobState.Unknown

final case class VerificationResult(evidence: Evidence, reason: String)

trait VerificationEngine:
  def verify(
      operation: FlinkOperation,
      expected: ResourceRef,
      evidence: Evidence,
      expectedGeneration: Option[Generation] = None
  ): Either[ControlPlaneError, VerificationResult]

object DefaultVerificationEngine extends VerificationEngine:
  override def verify(
      operation: FlinkOperation,
      expected: ResourceRef,
      evidence: Evidence,
      expectedGeneration: Option[Generation]
  ): Either[ControlPlaneError, VerificationResult] =
    if evidence.resource.namespace != expected.namespace || evidence.resource.kind != expected.kind || evidence.resource.name != expected.name then
      Left(ControlPlaneError.VerificationFailed("observed resource identity does not match operation target"))
    else if expected.uid.nonEmpty && evidence.uid != expected.uid then
      Left(ControlPlaneError.VerificationFailed("observed resource UID does not match operation target"))
    else if expectedGeneration.nonEmpty && (evidence.generation != expectedGeneration || evidence.observedGeneration != expectedGeneration) then
      Left(ControlPlaneError.VerificationFailed("observed resource does not reconcile the submitted generation"))
    else if evidence.generation.nonEmpty && evidence.observedGeneration.exists(observed => evidence.generation.exists(_ != observed)) then
      Left(ControlPlaneError.VerificationFailed("observedGeneration does not match generation"))
    else
      operation match
        case FlinkOperation.Suspend(_, _) =>
          if evidence.jobState == JobState.Suspended then Right(VerificationResult(evidence, "job is suspended"))
          else Left(ControlPlaneError.VerificationFailed(s"expected SUSPENDED, observed ${evidence.jobState}"))
        case FlinkOperation.Snapshot(_, _) =>
          if evidence.snapshotState.exists(_.equalsIgnoreCase("COMPLETED")) then Right(VerificationResult(evidence, "snapshot completed"))
          else Left(ControlPlaneError.VerificationFailed("snapshot has not completed"))
        case FlinkOperation.Delete(_, _) => Right(VerificationResult(evidence, "delete acknowledged"))
        case _ =>
          if (evidence.jobState == JobState.Running || evidence.jobState == JobState.Finished) && evidence.reconciliation == ReconciliationState.Ready then
            Right(VerificationResult(evidence, s"job is ${evidence.jobState} and reconciled"))
          else Left(ControlPlaneError.VerificationFailed(s"expected RUNNING or FINISHED with READY reconciliation, observed ${evidence.jobState}/${evidence.reconciliation}"))

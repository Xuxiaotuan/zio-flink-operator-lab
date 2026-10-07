package cn.xuyinyin.flinklab.operator.watch

/** Kubernetes watch 状态模型：解析 FlinkDeployment/FlinkSessionJob/FlinkStateSnapshot 的状态和事件。 */
import ujson.*

import scala.util.Try

/** Kubernetes watch 的五种事件类型。 */
enum WatchEventType:
  case Added, Modified, Deleted, Bookmark, Error

object WatchEventType:
  def fromString(value: String): Either[String, WatchEventType] =
    value.toUpperCase match
      case "ADDED"    => Right(Added)
      case "MODIFIED" => Right(Modified)
      case "DELETED"  => Right(Deleted)
      case "BOOKMARK" => Right(Bookmark)
      case "ERROR"    => Right(Error)
      case other      => Left(s"unknown Kubernetes watch event type: $other")

final case class FlinkCondition(
    conditionType: Option[String],
    status: Option[String],
    reason: Option[String],
    message: Option[String],
    lastTransitionTime: Option[String],
    observedGeneration: Option[String]
):
  def json: Obj =
    val result = Obj()
    JsonFields.put(result, "type", conditionType)
    JsonFields.put(result, "status", status)
    JsonFields.put(result, "reason", reason)
    JsonFields.put(result, "message", message)
    JsonFields.put(result, "lastTransitionTime", lastTransitionTime)
    JsonFields.put(result, "observedGeneration", observedGeneration)
    result

final case class CheckpointAttempt(
    formatType: Option[String],
    timestamp: Option[String],
    triggerNonce: Option[String],
    triggerType: Option[String]
):
  def json: Obj =
    val result = Obj()
    JsonFields.put(result, "formatType", formatType)
    JsonFields.put(result, "timeStamp", timestamp)
    JsonFields.put(result, "triggerNonce", triggerNonce)
    JsonFields.put(result, "triggerType", triggerType)
    result

final case class CheckpointStatus(
    formatType: Option[String],
    lastCheckpoint: Option[CheckpointAttempt],
    lastPeriodicTimestamp: Option[String],
    triggerId: Option[String],
    triggerTimestamp: Option[String],
    triggerType: Option[String]
):
  def json: Obj =
    val result = Obj()
    JsonFields.put(result, "formatType", formatType)
    lastCheckpoint.foreach(value => result("lastCheckpoint") = value.json)
    JsonFields.put(result, "lastPeriodicCheckpointTimestamp", lastPeriodicTimestamp)
    JsonFields.put(result, "triggerId", triggerId)
    JsonFields.put(result, "triggerTimestamp", triggerTimestamp)
    JsonFields.put(result, "triggerType", triggerType)
    result

final case class SavepointAttempt(
    formatType: Option[String],
    location: Option[String],
    timestamp: Option[String],
    triggerNonce: Option[String],
    triggerType: Option[String]
):
  def json: Obj =
    val result = Obj()
    JsonFields.put(result, "formatType", formatType)
    JsonFields.put(result, "location", location)
    JsonFields.put(result, "timeStamp", timestamp)
    JsonFields.put(result, "triggerNonce", triggerNonce)
    JsonFields.put(result, "triggerType", triggerType)
    result

final case class SavepointStatus(
    formatType: Option[String],
    lastPeriodicTimestamp: Option[String],
    lastSavepoint: Option[SavepointAttempt],
    history: Vector[SavepointAttempt],
    triggerId: Option[String],
    triggerTimestamp: Option[String],
    triggerType: Option[String],
    upgradeSavepointPath: Option[String]
):
  def json: Obj =
    val result = Obj()
    JsonFields.put(result, "formatType", formatType)
    JsonFields.put(result, "lastPeriodicSavepointTimestamp", lastPeriodicTimestamp)
    lastSavepoint.foreach(value => result("lastSavepoint") = value.json)
    result("savepointHistory") = Arr(history.map(_.json)*)
    JsonFields.put(result, "triggerId", triggerId)
    JsonFields.put(result, "triggerTimestamp", triggerTimestamp)
    JsonFields.put(result, "triggerType", triggerType)
    JsonFields.put(result, "upgradeSavepointPath", upgradeSavepointPath)
    result

/** Normalized status of a FlinkDeployment or FlinkSessionJob CR. */
/** 对 FlinkDeployment/FlinkSessionJob status 的稳定投影。 */
final case class FlinkStatusSnapshot(
    name: String,
    resourceVersion: Option[String],
    lifecycleState: Option[String],
    jobState: Option[String],
    deploymentStatus: Option[String],
    message: Option[String],
    lastSavepointLocation: Option[String] = None,
    kind: Option[String] = None,
    error: Option[String] = None,
    jobId: Option[String] = None,
    jobName: Option[String] = None,
    startTime: Option[String] = None,
    updateTime: Option[String] = None,
    reconciliationState: Option[String] = None,
    observedGeneration: Option[String] = None,
    taskManagerReplicas: Option[String] = None,
    checkpoint: Option[CheckpointStatus] = None,
    savepoint: Option[SavepointStatus] = None,
    conditions: Vector[FlinkCondition] = Vector.empty
):
  def json: Obj =
    val result = Obj("name" -> name)
    JsonFields.put(result, "kind", kind)
    JsonFields.put(result, "resourceVersion", resourceVersion)
    JsonFields.put(result, "lifecycleState", lifecycleState)
    JsonFields.put(result, "jobState", jobState)
    JsonFields.put(result, "jobManagerDeploymentStatus", deploymentStatus)
    JsonFields.put(result, "message", message)
    JsonFields.put(result, "error", error)
    JsonFields.put(result, "jobId", jobId)
    JsonFields.put(result, "jobName", jobName)
    JsonFields.put(result, "startTime", startTime)
    JsonFields.put(result, "updateTime", updateTime)
    JsonFields.put(result, "reconciliationState", reconciliationState)
    JsonFields.put(result, "observedGeneration", observedGeneration)
    JsonFields.put(result, "taskManagerReplicas", taskManagerReplicas)
    checkpoint.foreach(value => result("checkpoint") = value.json)
    savepoint.foreach(value => result("savepoint") = value.json)
    result("conditions") = Arr(conditions.map(_.json)*)
    result

object FlinkStatusSnapshot:
  /** 入口保留解析错误，调用方可以把坏事件记为验证失败。 */
  def fromJsonString(json: String): Either[String, FlinkStatusSnapshot] =
    Try(ujson.read(json)).toEither.left.map(error => Option(error.getMessage).getOrElse(error.toString)).flatMap(fromJson)

  def fromJson(value: Value): Either[String, FlinkStatusSnapshot] =
    Try {
      val metadata = value.obj.get("metadata").flatMap(_.objOpt).getOrElse(Map.empty[String, Value])
      val status = value.obj.get("status").flatMap(_.objOpt).getOrElse(Map.empty[String, Value])
      val jobStatus = objectValue(status, "jobStatus")
      val checkpointInfo = jobStatus.flatMap(objectValue(_, "checkpointInfo")).map(parseCheckpoint)
      val savepointInfo = jobStatus.flatMap(objectValue(_, "savepointInfo")).map(parseSavepoint).map { info =>
        info.copy(upgradeSavepointPath = jobStatus.flatMap(stringValue(_, "upgradeSavepointPath")))
      }
      val conditions = arrayValue(status, "conditions").map(parseCondition).toVector
      val message = conditions.flatMap(_.message).find(_.nonEmpty)
      val name = stringValue(metadata, "name").filter(_.nonEmpty).toRight("watch object metadata.name is required")
      name.map { resolvedName =>
        FlinkStatusSnapshot(
          name = resolvedName,
          resourceVersion = stringValue(metadata, "resourceVersion"),
          lifecycleState = stringValue(status, "lifecycleState"),
          jobState = jobStatus.flatMap(stringValue(_, "state")),
          deploymentStatus = stringValue(status, "jobManagerDeploymentStatus"),
          message = message,
          lastSavepointLocation = savepointInfo.flatMap(_.lastSavepoint.flatMap(_.location)),
          kind = stringValue(value.obj, "kind"),
          error = stringValue(status, "error"),
          jobId = jobStatus.flatMap(stringValue(_, "jobId")),
          jobName = jobStatus.flatMap(stringValue(_, "jobName")),
          startTime = jobStatus.flatMap(stringValue(_, "startTime")),
          updateTime = jobStatus.flatMap(stringValue(_, "updateTime")),
          reconciliationState = objectValue(status, "reconciliationStatus").flatMap(stringValue(_, "state")),
          observedGeneration = stringValue(status, "observedGeneration"),
          taskManagerReplicas = objectValue(status, "taskManager").flatMap(stringValue(_, "replicas")),
          checkpoint = checkpointInfo,
          savepoint = savepointInfo,
          conditions = conditions
        )
      }.fold(error => throw IllegalArgumentException(error), identity)
    }.toEither.left.map(error => Option(error.getMessage).getOrElse(error.toString))

  private def parseCheckpoint(value: collection.Map[String, Value]): CheckpointStatus =
    CheckpointStatus(
      stringValue(value, "formatType"),
      objectValue(value, "lastCheckpoint").map(parseCheckpointAttempt),
      stringValue(value, "lastPeriodicCheckpointTimestamp"),
      stringValue(value, "triggerId"),
      stringValue(value, "triggerTimestamp"),
      stringValue(value, "triggerType")
    )

  private def parseCheckpointAttempt(value: collection.Map[String, Value]): CheckpointAttempt =
    CheckpointAttempt(stringValue(value, "formatType"), stringValue(value, "timeStamp"), stringValue(value, "triggerNonce"), stringValue(value, "triggerType"))

  private def parseSavepoint(value: collection.Map[String, Value]): SavepointStatus =
    SavepointStatus(
      stringValue(value, "formatType"),
      stringValue(value, "lastPeriodicSavepointTimestamp"),
      objectValue(value, "lastSavepoint").map(parseSavepointAttempt),
      arrayValue(value, "savepointHistory").map(item => parseSavepointAttempt(item.obj)).toVector,
      stringValue(value, "triggerId"),
      stringValue(value, "triggerTimestamp"),
      stringValue(value, "triggerType"),
      None
    )

  private def parseSavepointAttempt(value: collection.Map[String, Value]): SavepointAttempt =
    SavepointAttempt(stringValue(value, "formatType"), stringValue(value, "location"), stringValue(value, "timeStamp"), stringValue(value, "triggerNonce"), stringValue(value, "triggerType"))

  private def parseCondition(value: Value): FlinkCondition =
    val obj: collection.Map[String, Value] = value.obj
    FlinkCondition(stringValue(obj, "type"), stringValue(obj, "status"), stringValue(obj, "reason"), stringValue(obj, "message"), stringValue(obj, "lastTransitionTime"), stringValue(obj, "observedGeneration"))

  private def objectValue(value: collection.Map[String, Value], key: String): Option[collection.Map[String, Value]] = value.get(key).flatMap(_.objOpt)
  private def arrayValue(value: collection.Map[String, Value], key: String): List[Value] = value.get(key).flatMap(_.arrOpt).map(_.toList).getOrElse(Nil)
  private def stringValue(value: collection.Map[String, Value], key: String): Option[String] = value.get(key).flatMap { item => item.strOpt.orElse(item.numOpt.map(_.toString)) }.filter(_.nonEmpty)

object JsonFields:
  def put(target: Obj, key: String, value: Option[String]): Unit = value.foreach(item => target(key) = item)

/** 对 FlinkStateSnapshot status 的稳定投影。 */
final case class FlinkStateSnapshotStatus(
    name: String,
    resourceVersion: Option[String],
    jobReferenceKind: Option[String],
    jobReferenceName: Option[String],
    state: Option[String],
    path: Option[String],
    error: Option[String],
    failures: Option[String],
    resultTimestamp: Option[String],
    triggerId: Option[String],
    triggerTimestamp: Option[String]
):
  def json: Obj =
    val result = Obj("name" -> name)
    JsonFields.put(result, "resourceVersion", resourceVersion)
    JsonFields.put(result, "jobReferenceKind", jobReferenceKind)
    JsonFields.put(result, "jobReferenceName", jobReferenceName)
    JsonFields.put(result, "state", state)
    JsonFields.put(result, "path", path)
    JsonFields.put(result, "error", error)
    JsonFields.put(result, "failures", failures)
    JsonFields.put(result, "resultTimestamp", resultTimestamp)
    JsonFields.put(result, "triggerId", triggerId)
    JsonFields.put(result, "triggerTimestamp", triggerTimestamp)
    result

object FlinkStateSnapshotStatus:
  def fromJsonString(json: String): Either[String, FlinkStateSnapshotStatus] =
    Try(ujson.read(json)).toEither.left.map(error => Option(error.getMessage).getOrElse(error.toString)).flatMap(fromJson)

  def fromJson(value: Value): Either[String, FlinkStateSnapshotStatus] =
    Try {
      val metadata = value.obj.get("metadata").flatMap(_.objOpt).getOrElse(Map.empty[String, Value])
      val spec = value.obj.get("spec").flatMap(_.objOpt).getOrElse(Map.empty[String, Value])
      val status = value.obj.get("status").flatMap(_.objOpt).getOrElse(Map.empty[String, Value])
      val reference = spec.get("jobReference").flatMap(_.objOpt)
      val name = stringValue(metadata, "name").filter(_.nonEmpty).toRight("snapshot metadata.name is required")
      name.map { resolved =>
        FlinkStateSnapshotStatus(
          resolved,
          stringValue(metadata, "resourceVersion"),
          reference.flatMap(stringValue(_, "kind")),
          reference.flatMap(stringValue(_, "name")),
          stringValue(status, "state"),
          stringValue(status, "path"),
          stringValue(status, "error"),
          stringValue(status, "failures"),
          stringValue(status, "resultTimestamp"),
          stringValue(status, "triggerId"),
          stringValue(status, "triggerTimestamp")
        )
      }.fold(error => throw IllegalArgumentException(error), identity)
    }.toEither.left.map(error => Option(error.getMessage).getOrElse(error.toString))

  private def stringValue(value: collection.Map[String, Value], key: String): Option[String] = value.get(key).flatMap { item => item.strOpt.orElse(item.numOpt.map(_.toString)) }.filter(_.nonEmpty)

/** 已校验的 watch 事件；非 bookmark/error 事件都必须能解析成对应状态。 */
final case class WatchEvent(eventType: WatchEventType, resource: Value):
  def snapshot: Either[String, FlinkStatusSnapshot] = FlinkStatusSnapshot.fromJson(resource)
  def stateSnapshot: Either[String, FlinkStateSnapshotStatus] = FlinkStateSnapshotStatus.fromJson(resource)

object WatchEvent:
  def fromJson(value: Value): Either[String, WatchEvent] =
    Try {
      for
        eventType <- WatchEventType.fromString(value("type").str)
        resource = value("object")
        event = WatchEvent(eventType, resource)
        _ <- eventType match
          case WatchEventType.Bookmark | WatchEventType.Error => Right(())
          case _ if resource.obj.get("kind").flatMap(_.strOpt).contains("FlinkStateSnapshot") => event.stateSnapshot.map(_ => ())
          case _ => event.snapshot.map(_ => ())
      yield event
    }.toEither.left.map(error => Option(error.getMessage).getOrElse(error.toString)).flatten

/** 按事件到达顺序维护当前每个资源的最新状态。 */
object StatusReducer:
  def merge(current: Map[String, FlinkStatusSnapshot], next: FlinkStatusSnapshot): Map[String, FlinkStatusSnapshot] =
    current.updated(next.name, next)

  def mergeEvent(current: Map[String, FlinkStatusSnapshot], event: WatchEvent): Either[String, Map[String, FlinkStatusSnapshot]] =
    event.eventType match
      case WatchEventType.Bookmark | WatchEventType.Error => Right(current)
      case WatchEventType.Added | WatchEventType.Modified => event.snapshot.map(snapshot => merge(current, snapshot))
      case WatchEventType.Deleted => event.snapshot.map(deleted => current - deleted.name)

/** 快照资源专用 reducer，删除事件会移除对应资源。 */
object StateSnapshotReducer:
  def merge(current: Map[String, FlinkStateSnapshotStatus], next: FlinkStateSnapshotStatus): Map[String, FlinkStateSnapshotStatus] =
    current.updated(next.name, next)

  def mergeEvent(current: Map[String, FlinkStateSnapshotStatus], event: WatchEvent): Either[String, Map[String, FlinkStateSnapshotStatus]] =
    event.eventType match
      case WatchEventType.Bookmark | WatchEventType.Error => Right(current)
      case WatchEventType.Added | WatchEventType.Modified => event.stateSnapshot.map(snapshot => merge(current, snapshot))
      case WatchEventType.Deleted => event.stateSnapshot.map(deleted => current - deleted.name)

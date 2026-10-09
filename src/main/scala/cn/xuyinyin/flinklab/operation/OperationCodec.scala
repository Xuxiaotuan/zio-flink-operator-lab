package cn.xuyinyin.flinklab.operation

/** Operation 编解码器：在领域事件和 FlinkOperation CR 的 JSON 之间保持可审计的双向转换。 */
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*

import java.time.Instant

object OperationCodec:
  /** 把操作和事件历史编码成 FlinkOperation CR 内部的 operation JSON。 */
  def json(operation: Operation): String =
    val value = operation.json
    value("command") = commandJson(operation.command)
    value.render()

  /** 从持久化 JSON 恢复操作；任何结构错误都以 Left 返回。 */
  def fromJson(raw: String): Either[String, Operation] =
    try
      val value = ujson.read(raw)
      for
        id <- OperationId.from(value("operationId").str)
        requestId <- RequestId.from(value("requestId").str)
        resource <- resourceRef(value("resource"))
        command <- command(value("command"))
        state <- OperationState.values.find(_.toString.equalsIgnoreCase(value("state").str)).toRight(s"unknown operation state: ${value("state").str}")
        createdAt <- parseInstant(value("createdAt").str)
        updatedAt <- parseInstant(value("updatedAt").str)
        events <- value("events").arr.toList.foldLeft[Either[String, List[OperationEvent]]](Right(Nil)) { (acc, item) => for current <- acc; event <- eventValue(item) yield current :+ event }
      yield Operation(id, requestId, command, resource, state, events, createdAt, updatedAt)
    catch case error: Throwable => Left(Option(error.getMessage).getOrElse(error.toString))

  private def commandJson(command: FlinkOperation): ujson.Obj = command match
    case FlinkOperation.Deploy(spec) => ujson.Obj("type" -> "DEPLOY", "spec" -> spec.resource)
    case FlinkOperation.Upgrade(target, spec, policy) => ujson.Obj("type" -> "UPGRADE", "target" -> target.json, "spec" -> spec.resource, "protection" -> policy.protection.toString, "fallback" -> policy.fallback.toString)
    case FlinkOperation.Suspend(target, _) => ujson.Obj("type" -> "SUSPEND", "target" -> target.json)
    case FlinkOperation.Resume(target) => ujson.Obj("type" -> "RESUME", "target" -> target.json)
    case FlinkOperation.Restart(target, policy) => ujson.Obj("type" -> "RESTART", "target" -> target.json, "protection" -> policy.protection.toString, "fallback" -> policy.fallback.toString)
    case FlinkOperation.Snapshot(target, policy) =>
      val value = ujson.Obj("type" -> "SNAPSHOT", "target" -> target.json, "snapshotType" -> policy.snapshotType.toString)
      policy.snapshotName.foreach(name => value("snapshotName") = name.nameValue)
      value
    case FlinkOperation.Delete(target, _) => ujson.Obj("type" -> "DELETE", "target" -> target.json)

  private def command(value: ujson.Value): Either[String, FlinkOperation] =
    val obj = value.obj
    string(obj, "type").toRight("command.type is required").flatMap {
      case "DEPLOY" => decodeSpec(obj("spec")).map(FlinkOperation.Deploy.apply)
      case "UPGRADE" => for target <- resourceRef(obj("target")); spec <- decodeSpec(obj("spec")); policy <- policyValue(obj) yield FlinkOperation.Upgrade(target, spec, policy)
      case "SUSPEND" => resourceRef(obj("target")).map(target => FlinkOperation.Suspend(target, SuspendPolicy.KeepState))
      case "RESUME" => resourceRef(obj("target")).map(FlinkOperation.Resume.apply)
      case "RESTART" => for target <- resourceRef(obj("target")); policy <- policyValue(obj) yield FlinkOperation.Restart(target, policy)
      case "SNAPSHOT" => for
        target <- resourceRef(obj("target"))
        snapshotType <- string(obj, "snapshotType").toRight("snapshotType is required").flatMap(SnapshotType.parse)
        snapshotName <- string(obj, "snapshotName").map(value => DeploymentName.from(value).map(Some(_))).getOrElse(Right(None))
      yield FlinkOperation.Snapshot(target, SnapshotPolicy(snapshotType, snapshotName))
      case "DELETE" => resourceRef(obj("target")).map(target => FlinkOperation.Delete(target, DeletePolicy.Graceful))
      case other => Left(s"unsupported operation type: $other")
    }

  private def decodeSpec(value: ujson.Value): Either[String, FlinkDeploymentSpec] =
    val metadata: collection.Map[String, ujson.Value] = value.obj.get("metadata").flatMap(_.objOpt).getOrElse(Map.empty[String, ujson.Value])
    val spec: collection.Map[String, ujson.Value] = value.obj.get("spec").flatMap(_.objOpt).getOrElse(Map.empty[String, ujson.Value])
    val job: collection.Map[String, ujson.Value] = spec.get("job").flatMap(_.objOpt).getOrElse(Map.empty[String, ujson.Value])
    for
      namespace <- string(metadata, "namespace").toRight("spec namespace is required").flatMap(Namespace.from)
      name <- string(metadata, "name").toRight("spec name is required").flatMap(DeploymentName.from)
      jar <- string(job, "jarURI").toRight("job jarURI is required").flatMap(JobJarUri.from)
      parallelism <- number(job, "parallelism").toRight("job parallelism is required")
      protection <- string(job, "upgradeMode").map(StateProtection.parse).getOrElse(Right(StateProtection.Stateless))
      desiredState <- string(job, "state").map(_.toLowerCase match
        case "running" => Right(DesiredJobState.Running)
        case "suspended" => Right(DesiredJobState.Suspended)
        case other => Left(s"unknown desired job state: $other")
      ).getOrElse(Right(DesiredJobState.Running))
      args <- job.get("args").map(_.arrOpt.toRight("job args must be an array").flatMap(_.toList.foldLeft[Either[String, List[String]]](Right(Nil)) { (acc, item) => for current <- acc; value <- item.strOpt.toRight("job args must contain strings") yield current :+ value })).getOrElse(Right(Nil))
      initialSavepointPath <- string(job, "initialSavepointPath").map(SnapshotPath.from).map(_.map(Some(_))).getOrElse(Right(None))
      allowNonRestoredState = string(job, "allowNonRestoredState").flatMap(_.toBooleanOption)
      configuration: Map[String, String] = spec.get("flinkConfiguration").flatMap(_.objOpt).map(_.toSeq.flatMap { case (key, item) => item.strOpt.orElse(item.numOpt.map(_.toString)).orElse(item.boolOpt.map(_.toString)).map(value => key -> value) }.toMap).getOrElse(Map.empty)
      jobManagerResources = processResources(spec, "jobManager")
      taskManagerResources = processResources(spec, "taskManager")
      podTemplate = spec.get("podTemplate").flatMap(_.objOpt.map(entries => ujson.Obj.from(entries)))
    yield FlinkDeploymentSpec(
      namespace,
      name,
      string(spec, "image").getOrElse("flink:1.20.1"),
      string(spec, "flinkVersion").getOrElse("v1_20"),
      FlinkJob(jar, string(job, "entryClass").getOrElse("org.apache.flink.streaming.examples.wordcount.WordCount"), parallelism, protection, desiredState, args, initialSavepointPath, allowNonRestoredState),
      string(spec, "serviceAccount"),
      configuration.get("state.savepoints.dir"),
      configuration,
      jobManagerResources,
      taskManagerResources,
      podTemplate
    )

  private def resourceRef(value: ujson.Value): Either[String, ResourceRef] =
    val obj = value.obj
    for
      namespace <- string(obj, "namespace").toRight("resource namespace is required").flatMap(Namespace.from)
      kind <- string(obj, "kind").toRight("resource kind is required").flatMap(value => ResourceKind.fromApiResource(value).orElse(ResourceKind.parse(value).toOption).toRight(s"unsupported resource kind: $value"))
      name <- string(obj, "name").toRight("resource name is required").flatMap(DeploymentName.from)
      uid = string(obj, "uid").flatMap(ResourceUid.from(_).toOption)
    yield ResourceRef(namespace, kind, name, uid)

  private def policyValue(value: collection.Map[String, ujson.Value]): Either[String, UpgradePolicy] =
    for
      protection <- string(value, "protection").toRight("protection is required").flatMap(StateProtection.parse)
      fallback = string(value, "fallback").flatMap(value => scala.util.Try(FallbackPolicy.valueOf(value)).toOption).getOrElse(FallbackPolicy.Forbidden)
    yield UpgradePolicy(protection, fallback)

  // 事件解码必须覆盖所有状态机事件，否则重启恢复会丢失生命周期语义。
  private def eventValue(value: ujson.Value): Either[String, OperationEvent] =
    val obj = value.obj
    for at <- string(obj, "at").toRight("event.at is required").flatMap(parseInstant); eventType <- string(obj, "type").toRight("event.type is required") yield eventType match
      case "ACCEPTED" => OperationEvent.Accepted(at)
      case "VALIDATION_STARTED" => OperationEvent.ValidationStarted(at)
      case "VALIDATION_PASSED" => OperationEvent.ValidationPassed(at)
      case "SUBMITTED" => OperationEvent.Submitted(at, number(obj, "generation").flatMap(Generation.from(_).toOption), string(obj, "resourceVersion").flatMap(ResourceVersion.from(_).toOption), string(obj, "uid").flatMap(ResourceUid.from(_).toOption))
      case "WAITING_FOR_OBSERVATION" => OperationEvent.WaitingForObservation(at)
      case "OBSERVED" => OperationEvent.Observed(at, number(obj, "observedGeneration").flatMap(Generation.from(_).toOption))
      case "RECONCILIATION_STARTED" => OperationEvent.ReconciliationStarted(at)
      case "SNAPSHOT_STARTED" => OperationEvent.SnapshotStarted(at, snapshotValue(obj))
      case "SNAPSHOT_COMPLETED" => OperationEvent.SnapshotCompleted(at, snapshotValue(obj))
      case "FALLBACK_DETECTED" =>
        val requested = string(obj, "requestedProtection").toRight("requestedProtection is required").flatMap(StateProtection.parse).fold(error => throw IllegalArgumentException(error), identity)
        val actual = string(obj, "actualProtection").toRight("actualProtection is required").flatMap(actualProtection).fold(error => throw IllegalArgumentException(error), identity)
        OperationEvent.FallbackDetected(at, requested, actual)
      case "VERIFICATION_STARTED" => OperationEvent.VerificationStarted(at)
      case "VERIFICATION_SUCCEEDED" => OperationEvent.VerificationSucceeded(at, obj.get("evidence").flatMap(value => verificationEvidence(value).toOption))
      case "FAILED" => OperationEvent.Failed(at, string(obj, "reason").getOrElse("operation failed"))
      case "TIMED_OUT" => OperationEvent.TimedOut(at, string(obj, "reason").getOrElse("operation timed out"))
      case "SUPERSEDED" => OperationEvent.Superseded(at, OperationId.from(string(obj, "replacedBy").getOrElse("")).fold(error => throw IllegalArgumentException(error), identity))
      case "UNCERTAIN" => OperationEvent.Uncertain(at, string(obj, "reason").getOrElse("operation uncertain"))
      case _ => OperationEvent.Uncertain(at, s"restored event $eventType")

  private def snapshotValue(value: collection.Map[String, ujson.Value]): SnapshotRef =
    val snapshot = value.get("snapshot").map(_.obj).getOrElse(Map.empty[String, ujson.Value])
    val resource = resourceRef(snapshot.getOrElse("resource", throw IllegalArgumentException("snapshot.resource is required"))).fold(error => throw IllegalArgumentException(error), identity)
    SnapshotRef(resource, string(snapshot, "state").getOrElse("UNKNOWN"), string(snapshot, "path").flatMap(SnapshotPath.from(_).toOption))

  private def verificationEvidence(value: ujson.Value): Either[String, VerificationEvidence] =
    val obj = value.obj
    for
      resource <- resourceRef(obj.getOrElse("resource", ujson.Obj()))
      reason <- string(obj, "reason").toRight("verification evidence reason is required")
    yield VerificationEvidence(
      resource,
      string(obj, "resourceVersion").flatMap(ResourceVersion.from(_).toOption),
      string(obj, "uid").flatMap(ResourceUid.from(_).toOption),
      number(obj, "generation").flatMap(Generation.from(_).toOption),
      number(obj, "observedGeneration").flatMap(Generation.from(_).toOption),
      string(obj, "snapshotState"),
      string(obj, "snapshotPath").flatMap(SnapshotPath.from(_).toOption),
      reason
    )

  private def actualProtection(value: String): Either[String, ActualProtection] =
    if value == "EmptyState" then Right(ActualProtection.EmptyState)
    else if value == "LastState" then Right(ActualProtection.LastState)
    else if value.startsWith("Savepoint(") && value.endsWith(")") then SnapshotPath.from(value.stripPrefix("Savepoint(").stripSuffix(")")).map(ActualProtection.Savepoint.apply)
    else Left(s"unknown actual protection: $value")

  private def processResources(spec: collection.Map[String, ujson.Value], key: String): FlinkProcessResources =
    spec.get(key).flatMap(_.objOpt).flatMap(_.get("resource")).flatMap(_.objOpt).map { resource =>
      FlinkProcessResources(
        resource.get("cpu").flatMap(value => value.numOpt.orElse(value.strOpt.flatMap(_.toDoubleOption))).getOrElse(1),
        resource.get("memory").flatMap(_.strOpt).getOrElse("1024m")
      )
    }.getOrElse(FlinkProcessResources())

  private def parseInstant(value: String): Either[String, Instant] = scala.util.Try(Instant.parse(value)).toEither.left.map(_.getMessage)
  private def string(value: collection.Map[String, ujson.Value], key: String): Option[String] = value.get(key).flatMap(item => item.strOpt.orElse(item.numOpt.map(_.toString))).filter(_.nonEmpty)
  private def number(value: collection.Map[String, ujson.Value], key: String): Option[Int] = value.get(key).flatMap(item => item.numOpt.map(_.toInt).orElse(item.strOpt.flatMap(_.toIntOption))).filter(_ > 0)

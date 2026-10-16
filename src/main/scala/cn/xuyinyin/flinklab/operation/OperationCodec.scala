package cn.xuyinyin.flinklab.operation

/** Operation 编解码器：在领域事件和 FlinkOperation CR 的 JSON 之间保持可审计的双向转换。 */
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*

import java.time.Instant
import scala.util.control.NonFatal

object OperationCodec:
  /** 把操作和事件历史编码成 FlinkOperation CR 内部的 operation JSON。 */
  def json(operation: Operation): String =
    val value = operation.json
    value("schemaVersion") = 1
    value("command") = commandJson(operation.command)
    value.render()

  /** 从持久化 JSON 恢复操作；任何结构错误都以 Left 返回。 */
  def fromJson(raw: String): Either[String, Operation] =
    try
      val value = ujson.read(raw)
      for
        // 无版本记录是旧编码；新版本必须显式携带策略，不能猜测未来格式。
        version <- value.obj.get("schemaVersion") match
          case None => Right(0)
          case Some(ujson.Num(1)) => Right(1)
          case Some(other) => Left(s"unsupported operation schemaVersion: $other")
        id <- OperationId.from(value("operationId").str)
        requestId <- RequestId.from(value("requestId").str)
        resource <- resourceRef(value("resource"))
        command <- command(value("command"), version)
        state <- OperationState.values.find(_.toString.equalsIgnoreCase(value("state").str)).toRight(s"unknown operation state: ${value("state").str}")
        createdAt <- parseInstant(value("createdAt").str)
        updatedAt <- parseInstant(value("updatedAt").str)
        events <- value("events").arr.toList.foldLeft[Either[String, List[OperationEvent]]](Right(Nil)) { (acc, item) => for current <- acc; event <- eventValue(item) yield current :+ event }
        operation <- replay(Operation(id, requestId, command, resource, state, events, createdAt, updatedAt))
      yield operation
    catch case NonFatal(error) => Left(Option(error.getMessage).getOrElse(error.toString))

  /** 事件历史是恢复依据；独立保存的状态和时间必须能由同一历史推导出来。 */
  private def replay(stored: Operation): Either[String, Operation] =
    stored.events match
      case OperationEvent.Accepted(at) :: rest if at == stored.createdAt =>
        val seed = stored.copy(state = OperationState.Accepted, events = List(OperationEvent.Accepted(at)), updatedAt = at)
        rest.foldLeft[Either[String, Operation]](Right(seed)) { (current, event) =>
          current.flatMap(_.advance(event).left.map(_.message))
        }.flatMap { restored =>
          if restored.state == stored.state && restored.updatedAt == stored.updatedAt then Right(restored)
          else Left("stored operation state/updatedAt does not match event history")
        }
      case _ => Left("operation history must start with ACCEPTED at createdAt")

  private def commandJson(command: FlinkOperation): ujson.Obj = command match
    case FlinkOperation.Deploy(spec) => ujson.Obj("type" -> "DEPLOY", "spec" -> spec.resource)
    case FlinkOperation.Upgrade(target, spec, policy) => ujson.Obj("type" -> "UPGRADE", "target" -> target.json, "spec" -> spec.resource, "protection" -> policy.protection.toString, "fallback" -> policy.fallback.toString)
    case FlinkOperation.Suspend(target, policy) => ujson.Obj("type" -> "SUSPEND", "target" -> target.json, "policy" -> policy.toString)
    case FlinkOperation.Resume(target) => ujson.Obj("type" -> "RESUME", "target" -> target.json)
    case FlinkOperation.Restart(target, policy) => ujson.Obj("type" -> "RESTART", "target" -> target.json, "protection" -> policy.protection.toString, "fallback" -> policy.fallback.toString)
    case FlinkOperation.Snapshot(target, policy) =>
      val value = ujson.Obj("type" -> "SNAPSHOT", "target" -> target.json, "snapshotType" -> policy.snapshotType.toString)
      policy.snapshotName.foreach(name => value("snapshotName") = name.nameValue)
      value
    case FlinkOperation.Delete(target, policy) => ujson.Obj("type" -> "DELETE", "target" -> target.json, "policy" -> policy.toString)

  private def command(value: ujson.Value, version: Int): Either[String, FlinkOperation] =
    val obj = value.obj
    string(obj, "type").toRight("command.type is required").flatMap {
      case "DEPLOY" => decodeSpec(obj("spec")).map(FlinkOperation.Deploy.apply)
      case "UPGRADE" => for target <- resourceRef(obj("target")); spec <- decodeSpec(obj("spec")); policy <- policyValue(obj) yield FlinkOperation.Upgrade(target, spec, policy)
      case "SUSPEND" => for
        target <- resourceRef(obj("target"))
        policy <- enumPolicy(obj, "policy", SuspendPolicy.values, if version == 0 then Some(SuspendPolicy.KeepState) else None)
      yield FlinkOperation.Suspend(target, policy)
      case "RESUME" => resourceRef(obj("target")).map(FlinkOperation.Resume.apply)
      case "RESTART" => for target <- resourceRef(obj("target")); policy <- policyValue(obj) yield FlinkOperation.Restart(target, policy)
      case "SNAPSHOT" => for
        target <- resourceRef(obj("target"))
        snapshotType <- string(obj, "snapshotType").toRight("snapshotType is required").flatMap(SnapshotType.parse)
        snapshotName <- string(obj, "snapshotName").map(value => DeploymentName.from(value).map(Some(_))).getOrElse(Right(None))
      yield FlinkOperation.Snapshot(target, SnapshotPolicy(snapshotType, snapshotName))
      case "DELETE" => for
        target <- resourceRef(obj("target"))
        policy <- enumPolicy(obj, "policy", DeletePolicy.values, if version == 0 then Some(DeletePolicy.Graceful) else None)
      yield FlinkOperation.Delete(target, policy)
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
      allowNonRestoredState = job.get("allowNonRestoredState").map {
        case ujson.Bool(value) => value
        case ujson.Str("true") => true
        case ujson.Str("false") => false
        case _ => throw IllegalArgumentException("allowNonRestoredState must be a boolean")
      }
      savepointRedeployNonce = numberLong(job, "savepointRedeployNonce")
      configuration: Map[String, String] = spec.get("flinkConfiguration").map(_.obj.toSeq.map { case (key, item) =>
        key -> item.strOpt.orElse(item.numOpt.map(_.toString)).orElse(item.boolOpt.map(_.toString)).getOrElse(throw IllegalArgumentException(s"invalid flinkConfiguration.$key"))
      }.toMap).getOrElse(Map.empty)
      jobManagerResources = processResources(spec, "jobManager")
      taskManagerResources = processResources(spec, "taskManager")
      podTemplate = spec.get("podTemplate").map(value => ujson.Obj.from(value.obj))
    yield FlinkDeploymentSpec(
      namespace,
      name,
      string(spec, "image").getOrElse("flink:1.20.1"),
      string(spec, "flinkVersion").getOrElse("v1_20"),
      FlinkJob(jar, string(job, "entryClass").getOrElse("org.apache.flink.streaming.examples.wordcount.WordCount"), parallelism, protection, desiredState, args, initialSavepointPath, allowNonRestoredState, savepointRedeployNonce),
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
      uid <- optional(obj, "uid")(ResourceUid.from)
    yield ResourceRef(namespace, kind, name, uid)

  private def policyValue(value: collection.Map[String, ujson.Value]): Either[String, UpgradePolicy] =
    for
      protection <- string(value, "protection").toRight("protection is required").flatMap(StateProtection.parse)
      fallback <- enumPolicy(value, "fallback", FallbackPolicy.values, None)
    yield UpgradePolicy(protection, fallback)

  // 事件解码必须覆盖所有状态机事件，否则重启恢复会丢失生命周期语义。
  private def eventValue(value: ujson.Value): Either[String, OperationEvent] =
    val obj = value.obj
    for at <- string(obj, "at").toRight("event.at is required").flatMap(parseInstant); eventType <- string(obj, "type").toRight("event.type is required") yield eventType match
      case "ACCEPTED" => OperationEvent.Accepted(at)
      case "VALIDATION_STARTED" => OperationEvent.ValidationStarted(at)
      case "VALIDATION_PASSED" => OperationEvent.ValidationPassed(at)
      case "SUBMITTED" => OperationEvent.Submitted(at, eventGeneration(obj, "generation"), eventResourceVersion(obj), checkedOptional(obj, "uid")(ResourceUid.from))
      case "WAITING_FOR_OBSERVATION" => OperationEvent.WaitingForObservation(at)
      case "OBSERVED" => OperationEvent.Observed(at, eventGeneration(obj, "observedGeneration"))
      case "RECONCILIATION_STARTED" => OperationEvent.ReconciliationStarted(at)
      case "SNAPSHOT_STARTED" => OperationEvent.SnapshotStarted(at, snapshotValue(obj))
      case "SNAPSHOT_COMPLETED" => OperationEvent.SnapshotCompleted(at, snapshotValue(obj))
      case "FALLBACK_DETECTED" =>
        val requested = string(obj, "requestedProtection").toRight("requestedProtection is required").flatMap(StateProtection.parse).fold(error => throw IllegalArgumentException(error), identity)
        val actual = string(obj, "actualProtection").toRight("actualProtection is required").flatMap(actualProtection).fold(error => throw IllegalArgumentException(error), identity)
        OperationEvent.FallbackDetected(at, requested, actual)
      case "VERIFICATION_STARTED" => OperationEvent.VerificationStarted(at)
      case "VERIFICATION_SUCCEEDED" => OperationEvent.VerificationSucceeded(at, obj.get("evidence").map(value => verificationEvidence(value).fold(error => throw IllegalArgumentException(error), identity)))
      case "FAILED" => OperationEvent.Failed(at, string(obj, "reason").getOrElse("operation failed"))
      case "TIMED_OUT" => OperationEvent.TimedOut(at, string(obj, "reason").getOrElse("operation timed out"))
      case "SUPERSEDED" => OperationEvent.Superseded(at, OperationId.from(string(obj, "replacedBy").getOrElse("")).fold(error => throw IllegalArgumentException(error), identity))
      case "UNCERTAIN" => OperationEvent.Uncertain(at, string(obj, "reason").getOrElse("operation uncertain"))
      case other => throw IllegalArgumentException(s"unknown operation event: $other")

  private def snapshotValue(value: collection.Map[String, ujson.Value]): SnapshotRef =
    val snapshot = value.get("snapshot").map(_.obj).getOrElse(Map.empty[String, ujson.Value])
    val resource = resourceRef(snapshot.getOrElse("resource", throw IllegalArgumentException("snapshot.resource is required"))).fold(error => throw IllegalArgumentException(error), identity)
    SnapshotRef(resource, string(snapshot, "state").getOrElse("UNKNOWN"), checkedOptional(snapshot, "path")(SnapshotPath.from))

  private def verificationEvidence(value: ujson.Value): Either[String, VerificationEvidence] =
    val obj = value.obj
    for
      resource <- resourceRef(obj.getOrElse("resource", ujson.Obj()))
      reason <- string(obj, "reason").toRight("verification evidence reason is required")
      resourceVersion <- optional(obj, "resourceVersion")(ResourceVersion.from)
      uid <- optional(obj, "uid")(ResourceUid.from)
      generation = evidenceGeneration(obj, "generation")
      observedGeneration = evidenceGeneration(obj, "observedGeneration")
      snapshotPath <- optional(obj, "snapshotPath")(SnapshotPath.from)
    yield VerificationEvidence(resource, resourceVersion, uid, generation, observedGeneration, string(obj, "snapshotState"), snapshotPath, reason)

  private def actualProtection(value: String): Either[String, ActualProtection] =
    if value == "EmptyState" then Right(ActualProtection.EmptyState)
    else if value == "LastState" then Right(ActualProtection.LastState)
    else if value.startsWith("Savepoint(") && value.endsWith(")") then SnapshotPath.from(value.stripPrefix("Savepoint(").stripSuffix(")")).map(ActualProtection.Savepoint.apply)
    else Left(s"unknown actual protection: $value")

  private def processResources(spec: collection.Map[String, ujson.Value], key: String): FlinkProcessResources =
    spec.get(key).flatMap(_.obj.get("resource")).map { value =>
      val resource = value.obj
      val cpu = resource.get("cpu").map(item => item.numOpt.orElse(item.strOpt.flatMap(_.toDoubleOption)).filter(v => v.isFinite && v > 0).getOrElse(throw IllegalArgumentException(s"invalid $key cpu"))).getOrElse(1.0)
      FlinkProcessResources(cpu, string(resource, "memory").getOrElse("1024m"))
    }.getOrElse(FlinkProcessResources())

  private def parseInstant(value: String): Either[String, Instant] = scala.util.Try(Instant.parse(value)).toEither.left.map(_.getMessage)

  // 缺失与损坏不同：只有字段缺失才能用默认值，显式的 null、空白或错误类型必须拒绝。
  private def string(value: collection.Map[String, ujson.Value], key: String): Option[String] =
    value.get(key).map(item => item.strOpt.filter(_.trim.nonEmpty).getOrElse(throw IllegalArgumentException(s"$key must be a non-empty string")))

  private def optional[A](value: collection.Map[String, ujson.Value], key: String)(parse: String => Either[String, A]): Either[String, Option[A]] =
    string(value, key).map(parse(_).map(Some(_))).getOrElse(Right(None))

  private def checkedOptional[A](value: collection.Map[String, ujson.Value], key: String)(parse: String => Either[String, A]): Option[A] =
    optional(value, key)(parse).fold(error => throw IllegalArgumentException(error), identity)

  private def enumPolicy[A](value: collection.Map[String, ujson.Value], key: String, values: Array[A], legacyDefault: Option[A]): Either[String, A] =
    string(value, key) match
      case Some(raw) => values.find(_.toString == raw).toRight(s"unknown $key: $raw")
      case None => legacyDefault.toRight(s"$key is required")

  /** Long 曾被 ujson 编成字符串；兼容整数的两种表示，禁止小数截断和溢出。 */
  private def integral(value: ujson.Value, key: String): Long =
    val raw = value.strOpt.orElse(value.numOpt.map(_.toString)).getOrElse(throw IllegalArgumentException(s"$key must be an integer"))
    scala.util.Try(BigDecimal(raw).bigDecimal.longValueExact()).getOrElse(throw IllegalArgumentException(s"$key must be an exact Long integer"))

  private def numberLong(value: collection.Map[String, ujson.Value], key: String): Option[Long] =
    value.get(key).map { item =>
      val parsed = integral(item, key)
      if parsed <= 0 then throw IllegalArgumentException(s"$key must be positive")
      parsed
    }

  private def number(value: collection.Map[String, ujson.Value], key: String): Option[Int] =
    numberLong(value, key).map { parsed =>
      if parsed > Int.MaxValue then throw IllegalArgumentException(s"$key exceeds Int range")
      parsed.toInt
    }

  private def evidenceGeneration(value: collection.Map[String, ujson.Value], key: String): Option[Generation] =
    numberLong(value, key).map(Generation.unsafe)

  // 旧 Submitted/Observed 使用 0 和空 resourceVersion 表示未返回身份；仅这两个事件保留该约定。
  private def eventGeneration(value: collection.Map[String, ujson.Value], key: String): Option[Generation] =
    value.get(key).flatMap { item =>
      val parsed = integral(item, key)
      if parsed == 0 then None else Some(Generation.from(parsed).fold(error => throw IllegalArgumentException(error), identity))
    }

  private def eventResourceVersion(value: collection.Map[String, ujson.Value]): Option[ResourceVersion] =
    value.get("resourceVersion") match
      case Some(ujson.Str("")) | None => None
      case _ => checkedOptional(value, "resourceVersion")(ResourceVersion.from)

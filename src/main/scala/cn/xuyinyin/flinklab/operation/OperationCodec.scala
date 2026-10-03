package cn.xuyinyin.flinklab.operation

import cn.xuyinyin.flinklab.cli.ResourceKind
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*

import java.time.Instant

object OperationCodec:
  def json(operation: Operation): String =
    val value = operation.json
    value("command") = commandJson(operation.command)
    value.render()

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
    yield FlinkDeploymentSpec(namespace, name, string(spec, "image").getOrElse("flink:1.20.1"), string(spec, "flinkVersion").getOrElse("v1_20"), FlinkJob(jar, string(job, "entryClass").getOrElse("org.apache.flink.streaming.examples.wordcount.WordCount"), parallelism))

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

  private def eventValue(value: ujson.Value): Either[String, OperationEvent] =
    val obj = value.obj
    for at <- string(obj, "at").toRight("event.at is required").flatMap(parseInstant); eventType <- string(obj, "type").toRight("event.type is required") yield eventType match
      case "ACCEPTED" => OperationEvent.Accepted(at)
      case "VALIDATION_STARTED" => OperationEvent.ValidationStarted(at)
      case "VALIDATION_PASSED" => OperationEvent.ValidationPassed(at)
      case "SUBMITTED" => OperationEvent.Submitted(at, number(obj, "generation").flatMap(Generation.from(_).toOption), string(obj, "resourceVersion").flatMap(ResourceVersion.from(_).toOption))
      case "WAITING_FOR_OBSERVATION" => OperationEvent.WaitingForObservation(at)
      case "OBSERVED" => OperationEvent.Observed(at, number(obj, "observedGeneration").flatMap(Generation.from(_).toOption))
      case "RECONCILIATION_STARTED" => OperationEvent.ReconciliationStarted(at)
      case "VERIFICATION_STARTED" => OperationEvent.VerificationStarted(at)
      case "VERIFICATION_SUCCEEDED" => OperationEvent.VerificationSucceeded(at)
      case "FAILED" => OperationEvent.Failed(at, string(obj, "reason").getOrElse("operation failed"))
      case "TIMED_OUT" => OperationEvent.TimedOut(at, string(obj, "reason").getOrElse("operation timed out"))
      case "UNCERTAIN" => OperationEvent.Uncertain(at, string(obj, "reason").getOrElse("operation uncertain"))
      case _ => OperationEvent.Uncertain(at, s"restored event $eventType")

  private def parseInstant(value: String): Either[String, Instant] = scala.util.Try(Instant.parse(value)).toEither.left.map(_.getMessage)
  private def string(value: collection.Map[String, ujson.Value], key: String): Option[String] = value.get(key).flatMap(item => item.strOpt.orElse(item.numOpt.map(_.toString))).filter(_.nonEmpty)
  private def number(value: collection.Map[String, ujson.Value], key: String): Option[Int] = value.get(key).flatMap(item => item.numOpt.map(_.toInt).orElse(item.strOpt.flatMap(_.toIntOption))).filter(_ > 0)

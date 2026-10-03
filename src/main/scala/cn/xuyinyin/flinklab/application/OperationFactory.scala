package cn.xuyinyin.flinklab.application

import cn.xuyinyin.flinklab.cli.{Command, ResourceKind}
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*

/** Converts every external command into the same typed domain operation. */
object FlinkOperationFactory:
  def fromCommand(command: Command): Either[String, FlinkOperation] = command match
    case Command.Apply(kind, options, _) =>
      kind match
        case ResourceKind.Deployment => deploymentFromOptions(options).map(FlinkOperation.Deploy.apply)
        case ResourceKind.StateSnapshot => snapshotFromOptions(options).map { case (target, policy) => FlinkOperation.Snapshot(target, policy) }
        case ResourceKind.SessionJob => Left("session-job operations are not yet represented by the typed control-plane command")
    case _ => Left("only apply commands produce a FlinkOperation")

  def fromDeploymentJson(namespace: String, raw: String): Either[String, FlinkOperation] =
    for
      value <- read(raw)
      kind <- string(value.obj, "kind").toRight("deployment kind is required")
      _ <- Either.cond(kind == "FlinkDeployment", (), s"unsupported operation kind: $kind")
      metadata <- objectValue(value, "metadata")
      name <- string(metadata, "name").toRight("deployment metadata.name is required")
      spec <- objectValue(value, "spec")
      job = spec.get("job").flatMap(_.objOpt).getOrElse(Map.empty[String, ujson.Value])
      resolvedNamespace <- Namespace.from(namespace)
      resolvedName <- DeploymentName.from(name)
      image = string(spec, "image").getOrElse("flink:1.20.1")
      flinkVersion = string(spec, "flinkVersion").getOrElse("v1_20")
      jar <- JobJarUri.from(string(job, "jarURI").getOrElse("local:///opt/flink/examples/streaming/WordCount.jar"))
      entryClass = string(job, "entryClass").getOrElse("org.apache.flink.streaming.examples.wordcount.WordCount")
      parallelism = integer(job, "parallelism").getOrElse(1)
      serviceAccount = string(spec, "serviceAccount")
      savepointDirectory = objectValue(spec, "flinkConfiguration").toOption.flatMap(string(_, "state.savepoints.dir"))
    yield FlinkOperation.Deploy(FlinkDeploymentSpec(resolvedNamespace, resolvedName, image, flinkVersion, FlinkJob(jar, entryClass, parallelism), serviceAccount, savepointDirectory))

  def fromSnapshotJson(namespace: String, raw: String): Either[String, FlinkOperation] =
    for
      value <- read(raw)
      targetKind <- string(value.obj, "targetKind").toRight("targetKind is required").flatMap(parseKind)
      targetName <- string(value.obj, "targetName").toRight("targetName is required").flatMap(DeploymentName.from)
      snapshotType <- string(value.obj, "type").toRight("type is required").flatMap(SnapshotType.parse)
      snapshotName <- value.obj.get("snapshotName").flatMap(_.strOpt).filter(_.nonEmpty).map(value => DeploymentName.from(value).map(Some(_))).getOrElse(Right(None))
      resolvedNamespace <- Namespace.from(namespace)
    yield FlinkOperation.Snapshot(ResourceRef(resolvedNamespace, targetKind, targetName), SnapshotPolicy(snapshotType, snapshotName))

  private def deploymentFromOptions(options: Map[String, String]): Either[String, FlinkDeploymentSpec] =
    for
      namespace <- Namespace.from(options.getOrElse("namespace", sys.env.getOrElse("FLINK_NAMESPACE", "default")))
      name <- DeploymentName.from(options.getOrElse("name", "word-count"))
      jar <- JobJarUri.from(options.getOrElse("jar-uri", sys.env.getOrElse("FLINK_JOB_JAR_URI", "local:///opt/flink/examples/streaming/WordCount.jar")))
      parallelism = options.get("parallelism").orElse(sys.env.get("FLINK_PARALLELISM")).flatMap(_.toIntOption).filter(_ > 0).getOrElse(1)
      protection <- StateProtection.parse(options.getOrElse("upgrade-mode", "stateless"))
    yield FlinkDeploymentSpec(
      namespace,
      name,
      options.getOrElse("image", sys.env.getOrElse("FLINK_IMAGE", "flink:1.20.1")),
      options.getOrElse("flink-version", sys.env.getOrElse("FLINK_VERSION", "v1_20")),
      FlinkJob(jar, options.getOrElse("entry-class", sys.env.getOrElse("FLINK_ENTRY_CLASS", "org.apache.flink.streaming.examples.wordcount.WordCount")), parallelism, protection),
      options.get("service-account").orElse(sys.env.get("FLINK_SERVICE_ACCOUNT")),
      options.get("target-directory").orElse(sys.env.get("FLINK_SAVEPOINT_DIRECTORY"))
    )

  private def snapshotFromOptions(options: Map[String, String]): Either[String, (ResourceRef, SnapshotPolicy)] =
    for
      namespace <- Namespace.from(options.getOrElse("namespace", sys.env.getOrElse("FLINK_NAMESPACE", "default")))
      kind <- options.get("target-kind").toRight("target-kind is required").flatMap(parseKind)
      name <- DeploymentName.from(options.getOrElse("target-name", "word-count"))
      snapshotType <- SnapshotType.parse(options.getOrElse("snapshot-type", "savepoint"))
      snapshotName <- options.get("name").map(DeploymentName.from).map(_.map(Some(_))).getOrElse(Right(None))
    yield (ResourceRef(namespace, kind, name), SnapshotPolicy(snapshotType, snapshotName))

  private def parseKind(value: String): Either[String, ResourceKind] =
    value match
      case "FlinkDeployment" | "deployment" => Right(ResourceKind.Deployment)
      case "FlinkSessionJob" | "session-job" => Right(ResourceKind.SessionJob)
      case other => Left(s"unsupported target kind: $other")

  private def read(raw: String): Either[String, ujson.Value] = scala.util.Try(ujson.read(raw)).toEither.left.map(_.getMessage)
  private def objectValue(value: ujson.Value, key: String): Either[String, collection.Map[String, ujson.Value]] = value.obj.get(key).flatMap(_.objOpt).toRight(s"$key object is required")
  private def string(value: collection.Map[String, ujson.Value], key: String): Option[String] = value.get(key).flatMap(item => item.strOpt.orElse(item.numOpt.map(_.toString))).filter(_.nonEmpty)
  private def integer(value: collection.Map[String, ujson.Value], key: String): Option[Int] = value.get(key).flatMap(item => item.numOpt.map(_.toInt).orElse(item.strOpt.flatMap(_.toIntOption))).filter(_ > 0)

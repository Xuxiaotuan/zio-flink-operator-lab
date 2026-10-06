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
        case ResourceKind.Operation | ResourceKind.OperationLock => Left("operation resources are managed by the control plane")
    case Command.Upgrade(kind, options) if kind == ResourceKind.Deployment =>
      for
        spec <- deploymentFromOptions(options)
        target <- targetFromOptions(options)
        policy <- upgradePolicyFromOptions(options)
      yield FlinkOperation.Upgrade(target, spec, policy)
    case Command.Resume(kind, options) if kind == ResourceKind.Deployment => targetFromOptions(options).map(FlinkOperation.Resume.apply)
    case Command.Restart(kind, options) if kind == ResourceKind.Deployment =>
      for target <- targetFromOptions(options); policy <- upgradePolicyFromOptions(options) yield FlinkOperation.Restart(target, policy)
    case _ => Left("only apply commands produce a FlinkOperation")

  def fromDeploymentJson(namespace: String, raw: String): Either[String, FlinkOperation] =
    for
      value <- read(raw)
      kind <- string(value.obj, "kind").toRight("deployment kind is required")
      _ <- Either.cond(kind == "FlinkDeployment", (), s"unsupported operation kind: $kind")
      metadata <- objectValue(value, "metadata")
      name <- string(metadata, "name").toRight("deployment metadata.name is required")
      spec <- objectValue(value, "spec")
      _ <- ensureKeys(value.obj, Set("apiVersion", "kind", "metadata", "spec"), "deployment")
      _ <- ensureKeys(metadata, Set("name", "namespace"), "deployment.metadata")
      _ <- ensureKeys(spec, Set("image", "imagePullPolicy", "flinkVersion", "jobManager", "taskManager", "job", "serviceAccount", "flinkConfiguration", "podTemplate"), "deployment.spec")
      job = spec.get("job").flatMap(_.objOpt).getOrElse(Map.empty[String, ujson.Value])
      _ <- ensureKeys(job, Set("jarURI", "entryClass", "parallelism", "upgradeMode", "state", "args", "initialSavepointPath", "allowNonRestoredState"), "deployment.spec.job")
      resolvedNamespace <- Namespace.from(namespace)
      resolvedName <- DeploymentName.from(name)
      image = string(spec, "image").getOrElse("flink:1.20.1")
      flinkVersion = string(spec, "flinkVersion").getOrElse("v1_20")
      jar <- JobJarUri.from(string(job, "jarURI").getOrElse("local:///opt/flink/examples/streaming/WordCount.jar"))
      entryClass = string(job, "entryClass").getOrElse("org.apache.flink.streaming.examples.wordcount.WordCount")
      parallelism = integer(job, "parallelism").getOrElse(1)
      protection <- string(job, "upgradeMode").map(StateProtection.parse).getOrElse(Right(StateProtection.Stateless))
      desiredState <- string(job, "state").map(DesiredJobState.parse).getOrElse(Right(DesiredJobState.Running))
      args <- stringArray(job, "args")
      initialSavepointPath <- string(job, "initialSavepointPath").map(SnapshotPath.from).map(_.map(Some(_))).getOrElse(Right(None))
      allowNonRestoredState = valueBoolean(job, "allowNonRestoredState")
      jobManagerResources <- processResources(spec, "jobManager")
      taskManagerResources <- processResources(spec, "taskManager")
      imagePullPolicy <- string(spec, "imagePullPolicy").map(_.toLowerCase).filter(_ != "ifnotpresent").map(value => Left(s"unsupported imagePullPolicy: $value")).getOrElse(Right(()))
      serviceAccount = string(spec, "serviceAccount")
      flinkConfiguration <- configuration(spec)
      podTemplate <- spec.get("podTemplate").map(value => value.objOpt.map(entries => ujson.Obj.from(entries)).toRight("deployment.spec.podTemplate must be an object").map(Some(_))).getOrElse(Right(None))
    yield FlinkOperation.Deploy(FlinkDeploymentSpec(resolvedNamespace, resolvedName, image, flinkVersion, FlinkJob(jar, entryClass, parallelism, protection, desiredState, args, initialSavepointPath, allowNonRestoredState), serviceAccount, flinkConfiguration.get("state.savepoints.dir"), flinkConfiguration, jobManagerResources, taskManagerResources, podTemplate))

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

  private def targetFromOptions(options: Map[String, String]): Either[String, ResourceRef] =
    for
      namespace <- Namespace.from(options.getOrElse("namespace", sys.env.getOrElse("FLINK_NAMESPACE", "default")))
      name <- DeploymentName.from(options.getOrElse("name", "word-count"))
    yield ResourceRef(namespace, ResourceKind.Deployment, name)

  private def upgradePolicyFromOptions(options: Map[String, String]): Either[String, UpgradePolicy] =
    for
      protection <- StateProtection.parse(options.getOrElse("upgrade-mode", "stateless"))
      fallback <- options.get("fallback").map(parseFallback).getOrElse(Right(FallbackPolicy.Forbidden))
      policy = UpgradePolicy(protection, fallback)
      _ <- policy.validate.left.map(_.message)
    yield policy

  private def parseFallback(value: String): Either[String, FallbackPolicy] =
    value.trim.toLowerCase match
      case "forbidden" => Right(FallbackPolicy.Forbidden)
      case "allow-last-state" | "allowlaststate" => Right(FallbackPolicy.AllowLastState)
      case other => Left(s"unknown fallback policy: $other (use forbidden or allow-last-state)")

  private def parseKind(value: String): Either[String, ResourceKind] =
    value match
      case "FlinkDeployment" | "deployment" => Right(ResourceKind.Deployment)
      case "FlinkSessionJob" | "session-job" => Right(ResourceKind.SessionJob)
      case other => Left(s"unsupported target kind: $other")

  private def read(raw: String): Either[String, ujson.Value] = scala.util.Try(ujson.read(raw)).toEither.left.map(_.getMessage)
  private def objectValue(value: ujson.Value, key: String): Either[String, collection.Map[String, ujson.Value]] = value.obj.get(key).flatMap(_.objOpt).toRight(s"$key object is required")
  private def string(value: collection.Map[String, ujson.Value], key: String): Option[String] = value.get(key).flatMap(item => item.strOpt.orElse(item.numOpt.map(_.toString))).filter(_.nonEmpty)
  private def integer(value: collection.Map[String, ujson.Value], key: String): Option[Int] = value.get(key).flatMap(item => item.numOpt.map(_.toInt).orElse(item.strOpt.flatMap(_.toIntOption))).filter(_ > 0)
  private def valueBoolean(value: collection.Map[String, ujson.Value], key: String): Option[Boolean] = value.get(key).flatMap(_.boolOpt)
  private def stringArray(value: collection.Map[String, ujson.Value], key: String): Either[String, List[String]] =
    value.get(key) match
      case None => Right(Nil)
      case Some(item) => item.arrOpt.toRight(s"$key must be an array").flatMap(_.toList.foldLeft[Either[String, List[String]]](Right(Nil)) { (acc, next) => for current <- acc; text <- next.strOpt.toRight(s"$key entries must be strings") yield current :+ text })
  private def configuration(spec: collection.Map[String, ujson.Value]): Either[String, Map[String, String]] =
    spec.get("flinkConfiguration") match
      case None => Right(Map.empty)
      case Some(value) => value.objOpt.toRight("deployment.spec.flinkConfiguration must be an object").flatMap { config =>
        Either.cond(config.forall((_, value) => value.strOpt.nonEmpty || value.numOpt.nonEmpty || value.boolOpt.nonEmpty), config.view.mapValues(value => value.strOpt.orElse(value.numOpt.map(_.toString)).orElse(value.boolOpt.map(_.toString)).get).toMap, "deployment.spec.flinkConfiguration values must be scalar")
      }
  private def processResources(spec: collection.Map[String, ujson.Value], key: String): Either[String, FlinkProcessResources] =
    spec.get(key) match
      case None => Right(FlinkProcessResources())
      case Some(value) =>
        value.objOpt.toRight(s"deployment.spec.$key must be an object").flatMap { process =>
          for
            _ <- ensureKeys(process, Set("resource"), s"deployment.spec.$key")
            result <- process.get("resource") match
              case None => Right(FlinkProcessResources())
              case Some(resource) =>
                for
                  values <- resource.objOpt.toRight(s"deployment.spec.$key.resource must be an object")
                  _ <- ensureKeys(values, Set("cpu", "memory"), s"deployment.spec.$key.resource")
                yield FlinkProcessResources(doubleValue(values, "cpu").getOrElse(1), string(values, "memory").getOrElse("1024m"))
          yield result
        }
  private def doubleValue(value: collection.Map[String, ujson.Value], key: String): Option[Double] = value.get(key).flatMap(item => item.numOpt.orElse(item.strOpt.flatMap(_.toDoubleOption))).filter(_ > 0)
  private def ensureKeys(value: collection.Map[String, ujson.Value], allowed: Set[String], scope: String): Either[String, Unit] =
    value.keys.find(!allowed.contains(_)).toLeft(()).left.map(key => s"unsupported field in $scope: $key")

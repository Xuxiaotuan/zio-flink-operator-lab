package cn.xuyinyin.flinklab.application

/** 应用层 HTTP 工厂：把 JSON 请求转换成统一的 FlinkOperation。 */
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*

/** Converts every external command into the same typed domain operation. */
object FlinkOperationFactory:
  /** 解析 HTTP 提交的 FlinkDeployment，并拒绝未声明的字段。 */
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
      _ <- ensureKeys(job, Set("jarURI", "entryClass", "parallelism", "upgradeMode", "state", "args", "initialSavepointPath", "allowNonRestoredState", "savepointRedeployNonce"), "deployment.spec.job")
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
      savepointRedeployNonce <- job.get("savepointRedeployNonce") match
        case None => Right(None)
        case Some(value) =>
          // JSON 数值只接受精确整数，不能把小数截断成另一次恢复的 nonce。
          value.numOpt.filter(n => n > 0 && n <= 9007199254740991d && n == math.floor(n))
            .map(n => Some(n.toLong)).toRight("savepointRedeployNonce must be a positive safe JSON integer")
      jobManagerResources <- processResources(spec, "jobManager")
      taskManagerResources <- processResources(spec, "taskManager")
      imagePullPolicy <- string(spec, "imagePullPolicy").map(_.toLowerCase).filter(_ != "ifnotpresent").map(value => Left(s"unsupported imagePullPolicy: $value")).getOrElse(Right(()))
      serviceAccount = string(spec, "serviceAccount")
      flinkConfiguration <- configuration(spec)
      podTemplate <- spec.get("podTemplate").map(value => value.objOpt.map(entries => ujson.Obj.from(entries)).toRight("deployment.spec.podTemplate must be an object").map(Some(_))).getOrElse(Right(None))
    yield FlinkOperation.Deploy(FlinkDeploymentSpec(resolvedNamespace, resolvedName, image, flinkVersion, FlinkJob(jar, entryClass, parallelism, protection, desiredState, args, initialSavepointPath, allowNonRestoredState, savepointRedeployNonce), serviceAccount, flinkConfiguration.get("state.savepoints.dir"), flinkConfiguration, jobManagerResources, taskManagerResources, podTemplate))

  /** 解析快照请求；快照同样先变成 FlinkOperation 再进入 worker。 */
  def fromSnapshotJson(namespace: String, raw: String): Either[String, FlinkOperation] =
    for
      value <- read(raw)
      targetKind <- string(value.obj, "targetKind").toRight("targetKind is required").flatMap(parseKind)
      targetName <- string(value.obj, "targetName").toRight("targetName is required").flatMap(DeploymentName.from)
      snapshotType <- string(value.obj, "type").toRight("type is required").flatMap(SnapshotType.parse)
      snapshotName <- value.obj.get("snapshotName").flatMap(_.strOpt).filter(_.nonEmpty).map(value => DeploymentName.from(value).map(Some(_))).getOrElse(Right(None))
      resolvedNamespace <- Namespace.from(namespace)
    yield FlinkOperation.Snapshot(ResourceRef(resolvedNamespace, targetKind, targetName), SnapshotPolicy(snapshotType, snapshotName))

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

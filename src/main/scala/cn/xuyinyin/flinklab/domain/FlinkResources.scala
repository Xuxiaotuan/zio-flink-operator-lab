package cn.xuyinyin.flinklab.domain

import cn.xuyinyin.flinklab.cli.ResourceKind
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import ujson.*

final case class FlinkJob(
    jarUri: JobJarUri,
    entryClass: String,
    parallelism: Int,
    stateProtection: StateProtection = StateProtection.Stateless,
    desiredState: DesiredJobState = DesiredJobState.Running,
    args: List[String] = Nil,
    initialSavepointPath: Option[SnapshotPath] = None,
    allowNonRestoredState: Option[Boolean] = None
):
  def json: Obj =
    val value = Obj(
      "jarURI" -> jarUri.uriValue,
      "entryClass" -> entryClass,
      "parallelism" -> parallelism,
      "upgradeMode" -> stateProtection.operatorValue,
      "state" -> desiredState.operatorValue
    )
    if args.nonEmpty then value("args") = Arr(args.map(Str.apply)*)
    initialSavepointPath.foreach(path => value("initialSavepointPath") = path.snapshotPathValue)
    allowNonRestoredState.foreach(flag => value("allowNonRestoredState") = Bool(flag))
    value

final case class FlinkProcessResources(cpu: Double = 1, memory: String = "1024m"):
  def json: Obj = Obj("resource" -> Obj("cpu" -> cpu, "memory" -> memory))

final case class FlinkDeploymentSpec(
    namespace: Namespace,
    name: DeploymentName,
    image: String,
    flinkVersion: String,
    job: FlinkJob,
    serviceAccount: Option[String] = None,
    savepointDirectory: Option[String] = None,
    flinkConfiguration: Map[String, String] = Map.empty,
    jobManagerResources: FlinkProcessResources = FlinkProcessResources(),
    taskManagerResources: FlinkProcessResources = FlinkProcessResources(),
    podTemplate: Option[ujson.Obj] = None
):
  def json: Obj =
    val spec = Obj(
      "image" -> image,
      "imagePullPolicy" -> "IfNotPresent",
      "flinkVersion" -> flinkVersion,
      "jobManager" -> jobManagerResources.json,
      "taskManager" -> taskManagerResources.json,
      "job" -> job.json
    )
    serviceAccount.foreach(value => spec("serviceAccount") = value)
    val configuration = flinkConfiguration ++ savepointDirectory.map("state.savepoints.dir" -> _)
    if configuration.nonEmpty then spec("flinkConfiguration") = Obj.from(configuration.toSeq.sortBy(_._1).map((key, value) => key -> Str(value)))
    podTemplate.foreach(value => spec("podTemplate") = value)
    spec

  def resource: Obj =
    Obj(
      "apiVersion" -> "flink.apache.org/v1beta1",
      "kind" -> "FlinkDeployment",
      "metadata" -> Obj("name" -> name.nameValue, "namespace" -> namespace.namespaceValue),
      "spec" -> json
    )

final case class FlinkSessionJobSpec(
    namespace: Namespace,
    name: DeploymentName,
    deploymentName: DeploymentName,
    job: FlinkJob
):
  def resource: Obj =
    Obj(
      "apiVersion" -> "flink.apache.org/v1beta1",
      "kind" -> "FlinkSessionJob",
      "metadata" -> Obj("name" -> name.nameValue, "namespace" -> namespace.namespaceValue),
      "spec" -> Obj("deploymentName" -> deploymentName.nameValue, "job" -> job.json)
    )

enum SnapshotType:
  case Savepoint, Checkpoint

object SnapshotType:
  def parse(value: String): Either[String, SnapshotType] =
    value.toLowerCase match
      case "savepoint" => Right(Savepoint)
      case "checkpoint" => Right(Checkpoint)
      case other => Left(s"unknown snapshot type: $other (use savepoint or checkpoint)")

final case class FlinkStateSnapshotSpec(
    namespace: Namespace,
    name: DeploymentName,
    targetKind: ResourceKind,
    targetName: DeploymentName,
    snapshotType: SnapshotType
):
  def resource: Obj =
    val jobReference = Obj(
      "kind" -> (targetKind match
        case ResourceKind.Deployment => "FlinkDeployment"
        case ResourceKind.SessionJob => "FlinkSessionJob"
        case ResourceKind.StateSnapshot | ResourceKind.Operation | ResourceKind.OperationLock => throw IllegalArgumentException("state snapshot target must be a Flink job resource")),
      "name" -> targetName.nameValue
    )
    val spec = Obj("jobReference" -> jobReference)
    snapshotType match
      case SnapshotType.Savepoint => spec("savepoint") = Obj("formatType" -> "CANONICAL")
      case SnapshotType.Checkpoint => spec("checkpoint") = Obj()
    Obj(
      "apiVersion" -> "flink.apache.org/v1beta1",
      "kind" -> "FlinkStateSnapshot",
      "metadata" -> Obj("name" -> name.nameValue, "namespace" -> namespace.namespaceValue),
      "spec" -> spec
    )

object FlinkResources:
  def render(resource: Obj): String = resource.render(indent = 2)
  def deployment(config: FlinkDeploymentSpec): String = render(config.resource)
  def sessionJob(config: FlinkSessionJobSpec): String = render(config.resource)

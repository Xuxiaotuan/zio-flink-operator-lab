package cn.xuyinyin.flinklab.operator

/** CLI 执行器：渲染资源、提交 CR、发送 savepoint 请求、读取状态或启动 watch。 */
import cn.xuyinyin.flinklab.cli.{Command, ResourceKind}
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.kubernetes.KubernetesApi
import cn.xuyinyin.flinklab.application.FlinkOperationFactory
import cn.xuyinyin.flinklab.operation.AsyncOperationWorker
import cn.xuyinyin.flinklab.operator.savepoint.SavepointPatch
import cn.xuyinyin.flinklab.operator.watch.{FlinkStateSnapshotStatus, FlinkStatusSnapshot, RetryPolicy, Retrying, WatchStream}
import zio.*

object OperatorProgram:

  /** 执行只读/直接 CLI 命令；需要生命周期跟踪的变更转交 worker。 */
  def execute(command: Command): ZIO[KubernetesApi, Throwable, Unit] =
    command match
      case Command.Help => Console.printLine(help)
      case Command.Render(kind, options) => Console.printLine(resource(kind, options).json)
      case Command.Watch(kind, options) =>
        watchNamespace(options).flatMap { namespace =>
          ZIO.serviceWithZIO[KubernetesApi] { client =>
            val events = options.get("name") match
              case Some(name) => client.watch(namespace, kind, name)
              case None => client.watchFrom(namespace, kind, None, None)
            val resilient = Retrying.resilient(events, RetryPolicy(maxRetries = 5, initialDelay = 1.second))
            if kind == ResourceKind.StateSnapshot then
              WatchStream.latestSnapshots(resilient).runForeach(state => Console.printLine(renderSnapshots(state)))
            else
              WatchStream.latestStatuses(resilient).runForeach(state => Console.printLine(renderStatuses(state)))
          }
        }
      case Command.Savepoint(kind, options) =>
        patchSavepoint(kind, options, suspended = false)
      case Command.SuspendSavepoint(kind, options) =>
        patchSavepoint(kind, options, suspended = true)
      case Command.Apply(kind, options, dryRun) =>
        val config = resource(kind, options)
        ZIO.serviceWithZIO[KubernetesApi](_.apply(config.namespace, config.json, dryRun)).flatMap(output => Console.printLine(output.trim))
      case Command.Status(kind, options) =>
        resourceIdentity(kind, options).flatMap { case (namespace, name) =>
          for
            output <- ZIO.serviceWithZIO[KubernetesApi](_.get(namespace, kind, name))
            normalized <- if kind == ResourceKind.StateSnapshot then
              ZIO.fromEither(FlinkStateSnapshotStatus.fromJsonString(output).left.map(IllegalArgumentException(_))).map(_.json.render(indent = 2))
            else
              ZIO.fromEither(FlinkStatusSnapshot.fromJsonString(output).left.map(IllegalArgumentException(_))).map(_.json.render(indent = 2))
            _ <- Console.printLine(normalized)
          yield ()
        }
      case Command.Delete(kind, options) =>
        resourceIdentity(kind, options).flatMap { case (namespace, name) =>
          for
            output <- ZIO.serviceWithZIO[KubernetesApi](_.delete(namespace, kind, name))
            _ <- Console.printLine(output.trim)
          yield ()
        }
      case Command.Upgrade(_, _) | Command.Resume(_, _) | Command.Restart(_, _) =>
        ZIO.fail(IllegalArgumentException("typed mutating commands require the AsyncOperationWorker"))

  private final case class ResourceToApply(namespace: Namespace, json: String)

  private def resource(kind: ResourceKind, options: Map[String, String]): ResourceToApply =
    val namespace = Namespace.from(options.getOrElse("namespace", sys.env.getOrElse("FLINK_NAMESPACE", "default")))
      .fold(message => throw IllegalArgumentException(message), value => value)
    val name = DeploymentName.from(options.getOrElse("name", defaultName(kind)))
      .fold(message => throw IllegalArgumentException(message), value => value)
    val image = options.getOrElse("image", sys.env.getOrElse("FLINK_IMAGE", "flink:1.20.1"))
    val flinkVersion = options.getOrElse("flink-version", sys.env.getOrElse("FLINK_VERSION", "v1_20"))
    val jarUri = JobJarUri.from(options.getOrElse("jar-uri", sys.env.getOrElse("FLINK_JOB_JAR_URI", "local:///opt/flink/examples/streaming/WordCount.jar")))
      .fold(message => throw IllegalArgumentException(message), value => value)
    val entryClass = options.getOrElse("entry-class", sys.env.getOrElse("FLINK_ENTRY_CLASS", "org.apache.flink.streaming.examples.wordcount.WordCount"))
    val parallelism = options.get("parallelism").orElse(sys.env.get("FLINK_PARALLELISM")).map(_.toInt).getOrElse(1)
    val job = FlinkJob(jarUri, entryClass, parallelism)
    val serviceAccount = options.get("service-account").orElse(sys.env.get("FLINK_SERVICE_ACCOUNT"))
    val savepointDirectory = options.get("target-directory").orElse(sys.env.get("FLINK_SAVEPOINT_DIRECTORY"))

    kind match
      case ResourceKind.Deployment =>
        ResourceToApply(namespace, FlinkResources.deployment(FlinkDeploymentSpec(namespace, name, image, flinkVersion, job, serviceAccount, savepointDirectory)))
      case ResourceKind.SessionJob =>
        val deployment = DeploymentName.from(options.getOrElse("deployment", "word-count"))
          .fold(message => throw IllegalArgumentException(message), value => value)
        ResourceToApply(namespace, FlinkResources.sessionJob(FlinkSessionJobSpec(namespace, name, deployment, job)))
      case ResourceKind.StateSnapshot =>
        val targetKind = ResourceKind.parse(options.getOrElse("target-kind", "deployment")) match
          case Right(value) if value != ResourceKind.StateSnapshot => value
          case Right(_) => throw IllegalArgumentException("state snapshot target-kind must be deployment or session-job")
          case Left(message) => throw IllegalArgumentException(message)
        val targetName = DeploymentName.from(options.getOrElse("target-name", "word-count"))
          .fold(message => throw IllegalArgumentException(message), identity)
        val snapshotType = SnapshotType.parse(options.getOrElse("snapshot-type", "savepoint"))
          .fold(message => throw IllegalArgumentException(message), identity)
        ResourceToApply(namespace, FlinkResources.render(FlinkStateSnapshotSpec(namespace, name, targetKind, targetName, snapshotType).resource))
      case ResourceKind.Operation | ResourceKind.OperationLock =>
        throw IllegalArgumentException("operation resources are managed by the control plane")

  private def resourceIdentity(kind: ResourceKind, options: Map[String, String]): IO[Throwable, (Namespace, String)] =
    ZIO.attempt {
      val namespace = Namespace.from(options.getOrElse("namespace", sys.env.getOrElse("FLINK_NAMESPACE", "default")))
        .fold(message => throw IllegalArgumentException(message), value => value)
      val name = DeploymentName.from(options.getOrElse("name", defaultName(kind)))
        .fold(message => throw IllegalArgumentException(message), _.nameValue)
      (namespace, name)
    }

  private def watchNamespace(options: Map[String, String]): IO[Throwable, Namespace] =
    ZIO.fromEither(
      Namespace.from(options.getOrElse("namespace", sys.env.getOrElse("FLINK_NAMESPACE", "default")))
        .left.map(IllegalArgumentException(_))
    )

  private def patchSavepoint(
      kind: ResourceKind,
      options: Map[String, String],
      suspended: Boolean
  ): ZIO[KubernetesApi, Throwable, Unit] =
    resourceIdentity(kind, options).flatMap { case (namespace, name) =>
      for
        _ <- ZIO.fail(IllegalArgumentException("configure --target-directory on initial apply deployment, not during savepoint"))
          .when(options.contains("target-directory"))
        patch <- if suspended then ZIO.succeed(SavepointPatch.suspend)
          else ZIO.fromEither(options.get("nonce").toRight("--nonce is required").flatMap(value =>
            scala.util.Try(value.toLong).toEither.left.map(_ => s"invalid savepoint nonce: $value")
          )).mapError(message => IllegalArgumentException(message)).map(SavepointPatch.trigger)
        output <- ZIO.serviceWithZIO[KubernetesApi](_.patch(namespace, kind, name, patch))
        _ <- Console.printLine(output.trim)
      yield ()
    }

  private def renderStatuses(states: Map[String, FlinkStatusSnapshot]): String =
    ujson.Obj.from(states.toSeq.sortBy(_._1).map { case (name, snapshot) => name -> snapshot.json }).render(indent = 2)

  private def renderSnapshots(states: Map[String, FlinkStateSnapshotStatus]): String =
    ujson.Obj.from(states.toSeq.sortBy(_._1).map { case (name, snapshot) => name -> snapshot.json }).render(indent = 2)

  private def acceptedJson(accepted: cn.xuyinyin.flinklab.operation.AcceptedOperation): String =
    ujson.Obj("operationId" -> accepted.operationId.operationIdValue, "requestId" -> accepted.requestId.requestIdValue, "state" -> "ACCEPTED", "acceptedAt" -> accepted.acceptedAt.toString).render()

  /** 统一把 CLI 变更命令送入 typed operation worker。 */
  def executeWithWorker(command: Command): ZIO[KubernetesApi & AsyncOperationWorker, Throwable, Unit] = command match
    case Command.Apply(_, _, false) | Command.Upgrade(_, _) | Command.Resume(_, _) | Command.Restart(_, _) =>
      FlinkOperationFactory.fromCommand(command) match
        case Left(message) => ZIO.fail(IllegalArgumentException(message))
        case Right(operation) =>
          for
            accepted <- ZIO.environmentWithZIO[KubernetesApi & AsyncOperationWorker](environment => environment.get[AsyncOperationWorker].submit(RequestId.generate(), operation).mapError(error => IllegalArgumentException(error.message)))
            _ <- Console.printLine(acceptedJson(accepted))
          yield ()
    case _ => execute(command)

  private def defaultName(kind: ResourceKind): String =
    kind match
      case ResourceKind.Deployment => "word-count"
      case ResourceKind.SessionJob => "word-count-job"
      case ResourceKind.StateSnapshot => "snapshot-1"
      case ResourceKind.Operation | ResourceKind.OperationLock => throw IllegalArgumentException("operation resources are managed by the control plane")

  private val help =
    """ZIO + Flink Kubernetes Operator Lab

Commands:
  serve
  render <deployment|session-job|state-snapshot> [options]
  apply  <deployment|session-job|state-snapshot> [options] [--dry-run]
  upgrade deployment --name VALUE [options]
  resume deployment --name VALUE
  restart deployment --name VALUE [--upgrade-mode VALUE] [--fallback VALUE]
  watch <deployment|session-job|state-snapshot> [options]
  savepoint <deployment|session-job> --nonce VALUE [options] (legacy)
  suspend-savepoint <deployment|session-job> [options] (legacy)
  status <deployment|session-job|state-snapshot> [options]
  delete <deployment|session-job|state-snapshot> [options]

Options:
  --name VALUE
  --namespace VALUE
  --image VALUE
  --flink-version VALUE
  --jar-uri VALUE
  --entry-class VALUE
  --parallelism VALUE
  --target-directory VALUE (initial render/apply deployment only)
  --service-account VALUE
  --deployment VALUE   (session-job only)
  --target-kind deployment|session-job (state-snapshot only)
  --target-name VALUE (state-snapshot only)
  --snapshot-type savepoint|checkpoint (state-snapshot only)
  --fallback forbidden|allow-last-state
"""

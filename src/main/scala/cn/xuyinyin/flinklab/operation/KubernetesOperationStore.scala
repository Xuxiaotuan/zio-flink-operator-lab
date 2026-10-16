package cn.xuyinyin.flinklab.operation

/** Kubernetes OperationStore：把 operation 序列化到 FlinkOperation CR，并用 resourceVersion 做跨副本条件更新。 */
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.kubernetes.{KubernetesApi, KubernetesApiError}
import zio.*

/** Durable operation store backed by a FlinkOperation custom resource in Kubernetes. */
final class KubernetesOperationStore(api: KubernetesApi) extends OperationStore:
  private val namespace = Namespace.from(sys.env.getOrElse("FLINK_NAMESPACE", "default")).fold(_ => Namespace.unsafe("default"), identity)

  /** 创建 CR；409 映射为幂等冲突，由上层决定是否复用已有 operation。 */
  override def create(operation: Operation): IO[ControlPlaneError, Unit] =
    if operation.resource.namespace != namespace then
      ZIO.fail(ControlPlaneError.InvalidPolicy(s"operation namespace must match configured namespace ${namespace.namespaceValue}"))
    else
      api.create(namespace, KubernetesOperationResource.render(operation))
        .mapError(error => mapError(operation.id, error))
        .unit

  /** 读取当前 CR、按领域状态机推进，再带 resourceVersion patch 回 API Server。 */
  override def transition(id: OperationId, event: OperationEvent): IO[ControlPlaneError, Operation] =
    def attempt(remaining: Int): IO[ControlPlaneError, Operation] =
      for
        raw <- api.get(namespace, ResourceKind.Operation, id.operationIdValue).mapError {
          case KubernetesApiError(404, _) => ControlPlaneError.OperationNotFound(id)
          case error => transitionError(error)
        }
        current <- ZIO.fromEither(KubernetesOperationResource.parse(raw)).mapError(ControlPlaneError.StoreFailure.apply)
        // 上一次写入响应丢失后重试相同事件，不能重复追加审计记录。
        next <- if current.events.contains(event) then ZIO.succeed(current) else
          for
            advanced <- ZIO.fromEither(current.advance(event))
            response <- api.patch(advanced.resource.namespace, ResourceKind.Operation, id.operationIdValue,
              KubernetesOperationResource.render(advanced, KubernetesOperationResource.resourceVersion(raw))).either
            result <- response match
              case Right(_) => ZIO.succeed(advanced)
              // 每次冲突重新读 CR、重新校验状态机；绝不重发带旧 resourceVersion 的文档。
              case Left(KubernetesApiError(409, _)) if remaining > 0 => ZIO.yieldNow *> attempt(remaining - 1)
              case Left(error) => ZIO.fail(transitionError(error))
          yield result
      yield next
    attempt(3)

  override def get(id: OperationId): IO[ControlPlaneError, Option[Operation]] =
    api.get(namespace, ResourceKind.Operation, id.operationIdValue).flatMap { raw =>
      ZIO.fromEither(KubernetesOperationResource.parse(raw).map(Some(_)))
    }.mapError {
      case KubernetesApiError(404, _) => ControlPlaneError.OperationNotFound(id)
      case error: Throwable => ControlPlaneError.StoreFailure(Option(error.getMessage).getOrElse(error.toString))
      case error: String => ControlPlaneError.StoreFailure(error)
    }.catchSome { case ControlPlaneError.OperationNotFound(_) => ZIO.succeed(None) }

  override def list: IO[ControlPlaneError, List[Operation]] = listSelected(None)

  override def listActive: IO[ControlPlaneError, List[Operation]] =
    for
      indexed <- listSelected(Some("xxt.io/operation-active=true"))
      // 旧 CR 没有活动状态标签；兼容查询仍在本地过滤终态。
      legacy <- listSelected(Some("!xxt.io/operation-active"))
    yield (indexed ++ legacy).filter(KubernetesOperationResource.isActive).distinctBy(_.id)

  private def listSelected(labelSelector: Option[String]): IO[ControlPlaneError, List[Operation]] =
    api.list(namespace, ResourceKind.Operation, labelSelector)
      .flatMap(raw => ZIO.fromEither(KubernetesOperationResource.parseList(raw)))
      .mapError {
        case error: Throwable => ControlPlaneError.StoreFailure(Option(error.getMessage).getOrElse(error.toString))
        case error: String => ControlPlaneError.StoreFailure(error)
      }

  override def findByRequestId(requestId: RequestId): IO[ControlPlaneError, Option[Operation]] =
    get(OperationId.forRequest(requestId)).map(_.filter(_.requestId == requestId))

  private def mapError(id: OperationId, error: Throwable): ControlPlaneError =
    error match
      case KubernetesApiError(409, _) => ControlPlaneError.OperationAlreadyExists(id)
      case _ => ControlPlaneError.StoreFailure(Option(error.getMessage).getOrElse(error.toString))

  private def transitionError(error: Throwable): ControlPlaneError =
    ControlPlaneError.StoreFailure(Option(error.getMessage).getOrElse(error.toString))

object KubernetesOperationResource:
  private[operation] def isActive(operation: Operation): Boolean =
    !Set(OperationState.Completed, OperationState.Failed, OperationState.Superseded, OperationState.TimedOut, OperationState.Uncertain).contains(operation.state)

  /** 将原始 requestId 保留在 operation 中，标签使用稳定哈希以满足 Kubernetes 长度和字符限制。 */
  def render(operation: Operation, resourceVersion: Option[String] = None): String =
    val metadata = ujson.Obj(
      "name" -> operation.id.operationIdValue,
      "namespace" -> operation.resource.namespace.namespaceValue,
      "labels" -> ujson.Obj(
        "xxt.io/request-id" -> OperationId.forRequest(operation.requestId).operationIdValue,
        "xxt.io/operation-active" -> isActive(operation).toString
      )
    )
    resourceVersion.foreach(value => metadata("resourceVersion") = value)
    ujson.Obj(
      "apiVersion" -> "flink.apache.org/v1beta1",
      "kind" -> "FlinkOperation",
      "metadata" -> metadata,
      "spec" -> ujson.Obj("operation" -> ujson.read(OperationCodec.json(operation)))
    ).render()

  def resourceVersion(raw: String): Option[String] =
    scala.util.Try(ujson.read(raw)).toOption.flatMap(_.obj.get("metadata").flatMap(_.objOpt).flatMap(_.get("resourceVersion")).flatMap(_.strOpt))

  def parse(raw: String): Either[String, Operation] =
    try
      val value = ujson.read(raw)
      val operation = value.obj.get("spec").flatMap(_.objOpt).flatMap(_.get("operation"))
        .orElse(value.obj.get("status").flatMap(_.objOpt).flatMap(_.get("operation")))
        .toRight("FlinkOperation spec.operation is required")
      operation.flatMap(value => OperationCodec.fromJson(value.render()))
    catch
      case error: Throwable => Left(Option(error.getMessage).getOrElse(error.toString))

  def parseList(raw: String): Either[String, List[Operation]] =
    try
      val value = ujson.read(raw)
      value.obj.get("items").flatMap(_.arrOpt).map(_.toList).getOrElse(Nil).foldLeft[Either[String, List[Operation]]](Right(Nil)) { (acc, item) =>
        for
          operations <- acc
          operation <- parse(item.render())
        yield operations :+ operation
      }
    catch
      case error: Throwable => Left(Option(error.getMessage).getOrElse(error.toString))

package cn.xuyinyin.flinklab.operation

/** Kubernetes OperationStore：把 operation 序列化到 FlinkOperation CR，并用 resourceVersion 做跨副本条件更新。 */
import cn.xuyinyin.flinklab.cli.ResourceKind
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.kubernetes.{KubernetesApi, KubernetesApiError}
import zio.*

/** Durable operation store backed by a FlinkOperation custom resource in Kubernetes. */
final class KubernetesOperationStore(api: KubernetesApi) extends OperationStore:
  /** 创建 CR；409 映射为幂等冲突，由上层决定是否复用已有 operation。 */
  override def create(operation: Operation): IO[ControlPlaneError, Unit] =
    api.create(operation.resource.namespace, KubernetesOperationResource.render(operation))
      .mapError(error => mapError(operation.id, error))
      .unit

  /** 读取当前 CR、按领域状态机推进，再带 resourceVersion patch 回 API Server。 */
  override def transition(id: OperationId, event: OperationEvent): IO[ControlPlaneError, Operation] =
    for
      namespace <- ZIO.succeed(Namespace.from(sys.env.getOrElse("FLINK_NAMESPACE", "default")).fold(_ => Namespace.unsafe("default"), identity))
      raw <- api.get(namespace, ResourceKind.Operation, id.operationIdValue).mapError {
        case KubernetesApiError(404, _) => ControlPlaneError.OperationNotFound(id)
        case error => ControlPlaneError.StoreFailure(Option(error.getMessage).getOrElse(error.toString))
      }
      current <- ZIO.fromEither(KubernetesOperationResource.parse(raw).map(Some(_))).mapError(error => ControlPlaneError.StoreFailure(error)).flatMap(ZIO.fromOption(_).orElseFail(ControlPlaneError.OperationNotFound(id)))
      next <- ZIO.fromEither(current.advance(event))
      patch = KubernetesOperationResource.render(next, KubernetesOperationResource.resourceVersion(raw))
      _ <- api.patch(next.resource.namespace, ResourceKind.Operation, id.operationIdValue, patch).mapError(error => transitionError(id, error))
    yield next

  override def get(id: OperationId): IO[ControlPlaneError, Option[Operation]] =
    val namespace = Namespace.from(sys.env.getOrElse("FLINK_NAMESPACE", "default")).fold(_ => Namespace.unsafe("default"), identity)
    api.get(namespace, ResourceKind.Operation, id.operationIdValue).flatMap { raw =>
      ZIO.fromEither(KubernetesOperationResource.parse(raw).map(Some(_)))
    }.mapError {
      case KubernetesApiError(404, _) => ControlPlaneError.OperationNotFound(id)
      case error: Throwable => ControlPlaneError.StoreFailure(Option(error.getMessage).getOrElse(error.toString))
      case error: String => ControlPlaneError.StoreFailure(error)
    }.catchSome { case ControlPlaneError.OperationNotFound(_) => ZIO.succeed(None) }

  override def list: IO[ControlPlaneError, List[Operation]] =
    val namespace = Namespace.from(sys.env.getOrElse("FLINK_NAMESPACE", "default")).fold(_ => Namespace.unsafe("default"), identity)
    api.list(namespace, ResourceKind.Operation)
      .flatMap(raw => ZIO.fromEither(KubernetesOperationResource.parseList(raw)))
      .mapError {
        case error: Throwable => ControlPlaneError.StoreFailure(Option(error.getMessage).getOrElse(error.toString))
        case error: String => ControlPlaneError.StoreFailure(error)
      }

  override def findByRequestId(requestId: RequestId): IO[ControlPlaneError, Option[Operation]] =
    list.map(_.find(_.requestId == requestId))

  private def mapError(id: OperationId, error: Throwable): ControlPlaneError =
    error match
      case KubernetesApiError(409, _) => ControlPlaneError.OperationAlreadyExists(id)
      case _ => ControlPlaneError.StoreFailure(Option(error.getMessage).getOrElse(error.toString))

  private def transitionError(id: OperationId, error: Throwable): ControlPlaneError =
    ControlPlaneError.StoreFailure(Option(error.getMessage).getOrElse(error.toString))

object KubernetesOperationResource:
  /** 将 Operation 放入 FlinkOperation.spec.operation，保留 requestId 标签用于查询。 */
  def render(operation: Operation, resourceVersion: Option[String] = None): String =
    val metadata = ujson.Obj(
      "name" -> operation.id.operationIdValue,
      "namespace" -> operation.resource.namespace.namespaceValue,
      "labels" -> ujson.Obj("xxt.io/request-id" -> operation.requestId.requestIdValue)
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

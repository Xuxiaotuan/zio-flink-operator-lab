package cn.xuyinyin.flinklab.kubernetes

/** Kubernetes API 端口和生产适配器：所有 Flink CR 的读写、watch、重试都从这里进入 Kubernetes API Server。 */
import cn.xuyinyin.flinklab.domain.ResourceKind
import cn.xuyinyin.flinklab.domain.FlinkTypes.Namespace
import cn.xuyinyin.flinklab.domain.{ResourceUid, resourceUidValue}
import cn.xuyinyin.flinklab.operator.watch.{WatchEvent, WatchEventType}
import com.google.gson.reflect.TypeToken
import io.kubernetes.client.openapi.{ApiClient, JSON, Pair}
import io.kubernetes.client.openapi.apis.CustomObjectsApi
import io.kubernetes.client.util.{Config, Watch}
import okhttp3.{Call, MediaType, RequestBody}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import zio.*
import zio.stream.*

/** Kubernetes 副作用端口；业务层只依赖这个抽象，不直接依赖 Java Client。 */
trait KubernetesApi:
  /** 用 server-side apply 创建或更新一个 CR；dryRun 只返回 API Server 预览。 */
  def apply(namespace: Namespace, resource: String, dryRun: Boolean): IO[Throwable, String]
  def create(namespace: Namespace, resource: String): IO[Throwable, String] =
    ZIO.fail(UnsupportedOperationException("create is not implemented by this KubernetesApi"))
  def list(namespace: Namespace, kind: ResourceKind): IO[Throwable, String] =
    ZIO.fail(UnsupportedOperationException("list is not implemented by this KubernetesApi"))
  def list(namespace: Namespace, kind: ResourceKind, labelSelector: Option[String]): IO[Throwable, String] =
    list(namespace, kind)
  def get(namespace: Namespace, kind: ResourceKind, name: String): IO[Throwable, String]
  def delete(namespace: Namespace, kind: ResourceKind, name: String): IO[Throwable, String]
  def delete(namespace: Namespace, kind: ResourceKind, name: String, uid: Option[ResourceUid]): IO[Throwable, String] = delete(namespace, kind, name)
  /** 带 UID/resourceVersion 前置条件删除，避免接管过期锁时误删续租后的对象。 */
  def delete(namespace: Namespace, kind: ResourceKind, name: String, uid: Option[ResourceUid], resourceVersion: Option[String]): IO[Throwable, String] = delete(namespace, kind, name, uid)
  def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String): IO[Throwable, String]
  def watch(namespace: Namespace, kind: ResourceKind, name: String): ZStream[Any, Throwable, WatchEvent] =
    watchFrom(namespace, kind, Some(name), None)
  /** 从指定 resourceVersion 开始 watch；resourceVersion 始终作为不透明字符串传递。 */
  def watchFrom(namespace: Namespace, kind: ResourceKind, name: Option[String], resourceVersion: Option[String]): ZStream[Any, Throwable, WatchEvent] =
    watch(namespace, kind, name.getOrElse(""))

final case class KubernetesApiSettings(
    group: String = "flink.apache.org",
    version: String = "v1beta1",
    fieldManager: String = "zio-flink-operator-lab",
    maxRetries: Int = 3
)

object KubernetesApiSettings:
  def fromEnv(env: Map[String, String]): KubernetesApiSettings =
    KubernetesApiSettings(
      group = env.getOrElse("FLINK_OPERATOR_GROUP", "flink.apache.org"),
      version = env.getOrElse("FLINK_OPERATOR_VERSION", "v1beta1"),
      fieldManager = env.getOrElse("FLINK_OPERATOR_FIELD_MANAGER", "zio-flink-operator-lab"),
      maxRetries = env.get("FLINK_OPERATOR_MAX_RETRIES").flatMap(_.toIntOption).getOrElse(3)
    )

/** Direct Kubernetes API adapter. It submits Flink CRs through the API server. */
final class KubernetesApiLive(settings: KubernetesApiSettings, suppliedClient: => ApiClient) extends KubernetesApi:
  private lazy val apiClient = suppliedClient
  // 重试由下面的 HTTP 方法/状态码规则决定，禁止 OkHttp 在未知写入结果后暗中重放。
  private lazy val requestClient = apiClient.getHttpClient.newBuilder().retryOnConnectionFailure(false).build()

  override def apply(namespace: Namespace, resource: String, dryRun: Boolean): IO[Throwable, String] =
    for
      value <- ZIO.attempt(ujson.read(resource))
      kind <- ZIO.fromEither(
        value.obj.get("kind").flatMap(_.strOpt).toRight(IllegalArgumentException("Flink resource kind is required"))
      )
      name <- ZIO.fromEither(
        value.obj
          .get("metadata")
          .flatMap(_.objOpt)
          .flatMap(_.get("name"))
          .flatMap(_.strOpt)
          .filter(_.nonEmpty)
          .toRight(IllegalArgumentException("Flink resource metadata.name is required"))
      )
      result <- request(
        method = "PATCH",
        path = resourcePath(namespace, apiResource(kind), name),
        query = Map("fieldManager" -> settings.fieldManager) ++ dryRunQuery(dryRun),
        body = Some(resource),
        contentType = "application/apply-patch+yaml"
      )
    yield result

  override def create(namespace: Namespace, resource: String): IO[Throwable, String] =
    for
      value <- ZIO.attempt(ujson.read(resource))
      kind <- ZIO.fromEither(value.obj.get("kind").flatMap(_.strOpt).toRight(IllegalArgumentException("Flink resource kind is required")))
      result <- request("POST", collectionPath(namespace, apiResource(kind)), query = Map("fieldManager" -> settings.fieldManager), body = Some(resource), contentType = "application/json")
    yield result

  override def get(namespace: Namespace, kind: ResourceKind, name: String): IO[Throwable, String] =
    request("GET", resourcePath(namespace, apiResource(kind), name))

  override def list(namespace: Namespace, kind: ResourceKind): IO[Throwable, String] =
    list(namespace, kind, None)

  override def list(namespace: Namespace, kind: ResourceKind, labelSelector: Option[String]): IO[Throwable, String] =
    request("GET", collectionPath(namespace, apiResource(kind)), query = labelSelector.map("labelSelector" -> _).toMap)

  override def delete(namespace: Namespace, kind: ResourceKind, name: String): IO[Throwable, String] =
    request("DELETE", resourcePath(namespace, apiResource(kind), name))

  override def delete(namespace: Namespace, kind: ResourceKind, name: String, uid: Option[ResourceUid]): IO[Throwable, String] = delete(namespace, kind, name, uid, None)

  override def delete(namespace: Namespace, kind: ResourceKind, name: String, uid: Option[ResourceUid], resourceVersion: Option[String]): IO[Throwable, String] =
    request(
      method = "DELETE",
      path = resourcePath(namespace, apiResource(kind), name),
      body = if uid.nonEmpty || resourceVersion.nonEmpty then Some(ujson.Obj(
        "preconditions" -> ujson.Obj.from(Seq(
          uid.map(value => "uid" -> ujson.Str(value.resourceUidValue)),
          resourceVersion.map(value => "resourceVersion" -> ujson.Str(value))
        ).flatten.toMap)
      ).render()) else None
    )

  override def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String): IO[Throwable, String] =
    request(
      method = "PATCH",
      path = resourcePath(namespace, apiResource(kind), name),
      query = Map("fieldManager" -> settings.fieldManager),
      body = Some(patch),
      contentType = "application/merge-patch+json"
    )

  /** 先登记 Call 的关闭责任，再进行可取消的建连；响应头迟迟不返回时也能退出 Scope。 */
  override def watchFrom(namespace: Namespace, kind: ResourceKind, name: Option[String], resourceVersion: Option[String]): ZStream[Any, Throwable, WatchEvent] =
    ZStream.unwrapScoped {
      ZIO.acquireRelease(ZIO.attempt {
        val customObjects = new CustomObjectsApi(apiClient)
        val builder = customObjects.listNamespacedCustomObject(settings.group, settings.version, namespace.namespaceValue, apiResource(kind))
        name.foreach(value => builder.fieldSelector(s"metadata.name=$value"))
        resourceVersion.foreach(value => builder.resourceVersion(value))
        val call = builder.allowWatchBookmarks(true).watch(true).buildCall(null)
        new WatchConnection(call)
      })(connection => ZIO.attemptBlocking(connection.close()).ignore).flatMap { connection =>
        ZIO.attemptBlockingCancelable(connection.open())(ZIO.attemptBlocking(connection.close()).ignore)
          .map(watch => ZStream.repeatZIOOption(nextEvent(watch, connection)))
      }
    }

  /** 处理“取消先到、Watch 后返回”的竞态；迟到的响应也必须关闭，不能遗漏在 Scope 外。 */
  private final class WatchConnection(call: Call):
    private val closed = new AtomicBoolean(false)
    private val current = new AtomicReference[Watch[Object]]()
    def open(): Watch[Object] =
      val responseType = new TypeToken[Watch.Response[Object]]() {}.getType
      val watch = Watch.createWatch[Object](apiClient, call, responseType)
      current.set(watch)
      if closed.get() then
        watch.close()
        throw new java.io.IOException("watch was closed during connection establishment")
      watch
    def close(): Unit =
      closed.set(true)
      // 必须先 cancel 解开 socket 阻塞，再关闭响应体；close 允许与迟到的 open 重复调用。
      call.cancel()
      Option(current.get()).foreach(_.close())

  private def nextEvent(watch: Watch[Object], connection: WatchConnection): ZIO[Any, Option[Throwable], WatchEvent] =
    ZIO.attemptBlockingCancelable {
      if watch.hasNext then Some(watch.next()) else None
    }(ZIO.attemptBlocking(connection.close()).ignore).mapError(Some(_)).flatMap {
      case None => ZIO.fail(None)
      case Some(response) =>
        val objectJson =
          if response.`object` != null then JSON.serialize(response.`object`)
          else if response.status != null then JSON.serialize(response.status)
          else "{}"
        ZIO.fromEither(WatchEvent.fromJson(ujson.Obj("type" -> response.`type`, "object" -> ujson.read(objectJson))))
          .mapError(message => Some(IllegalArgumentException(message)))
          .flatMap {
            case event if event.eventType == WatchEventType.Error => ZIO.fail(Some(watchError(event)))
            case event => ZIO.succeed(event)
          }
    }

  private def errorMessage(event: WatchEvent): String =
    event.resource.obj.get("message").map(_.str).filter(_.nonEmpty).getOrElse("Kubernetes watch returned an error event")

  private def watchError(event: WatchEvent): Throwable =
    val status = event.resource.obj.get("code").flatMap(_.numOpt).map(_.toInt)
    status.map(code => KubernetesApiError(code, errorMessage(event))).getOrElse(IllegalArgumentException(errorMessage(event)))

  private def apiResource(kind: ResourceKind): String =
    kind match
      case ResourceKind.Deployment => "flinkdeployments"
      case ResourceKind.SessionJob => "flinksessionjobs"
      case ResourceKind.StateSnapshot => "flinkstatesnapshots"
      case ResourceKind.Operation => "flinkoperations"
      case ResourceKind.OperationLock => "flinkoperationlocks"

  private def apiResource(kind: String): String =
    kind match
      case "FlinkDeployment" => "flinkdeployments"
      case "FlinkSessionJob" => "flinksessionjobs"
      case "FlinkStateSnapshot" => "flinkstatesnapshots"
      case "FlinkOperation" => "flinkoperations"
      case "FlinkOperationLock" => "flinkoperationlocks"
      case other => throw IllegalArgumentException(s"unsupported Flink custom resource kind: $other")

  private def resourcePath(namespace: Namespace, resource: String, name: String): String =
    "/apis/" + settings.group + "/" + settings.version + "/namespaces/" +
      namespace.namespaceValue + "/" + resource + "/" + name

  private def collectionPath(namespace: Namespace, resource: String): String =
    "/apis/" + settings.group + "/" + settings.version + "/namespaces/" + namespace.namespaceValue + "/" + resource

  private def dryRunQuery(dryRun: Boolean): Map[String, String] =
    if dryRun then Map("dryRun" -> "All") else Map.empty

  /** 统一处理 HTTP 请求、响应关闭和仅对可重试错误的指数退避。 */
  private def request(
      method: String,
      path: String,
      query: Map[String, String] = Map.empty,
      body: Option[String] = None,
      contentType: String = "application/json"
  ): IO[Throwable, String] =
    val effect = ZIO.attempt {
      val queryParams = new java.util.ArrayList[Pair]()
      query.foreach { case (key, value) => queryParams.add(new Pair(key, value)) }
      val headers = new java.util.HashMap[String, String]()
      headers.put("Accept", "application/json")
      headers.put("Content-Type", contentType)
      val requestWithoutBody = apiClient.buildRequest(
        null,
        path,
        method,
        queryParams,
        new java.util.ArrayList[Pair](),
        null,
        headers,
        new java.util.HashMap[String, String](),
        new java.util.HashMap[String, Object](),
        if apiClient.getAuthentications.containsKey("BearerToken") then Array("BearerToken") else Array.empty[String],
        null
      )
      val request = body match
        case Some(value) =>
          requestWithoutBody.newBuilder().method(method, RequestBody.create(value, MediaType.parse(contentType))).build()
        case None => requestWithoutBody
      requestClient.newCall(request)
    }.flatMap { call =>
      // 每次尝试独立创建 Call；租约丢失、请求超时或服务关闭都要传递到实际 socket。
      ZIO.attemptBlockingCancelable {
        val response = call.execute()
        try
          val responseBody = Option(response.body()).map(_.string()).getOrElse("")
          // 读取响应体也可能阻塞；正常、异常、取消路径都由 finally 关闭响应。
          if response.isSuccessful then responseBody
          else throw KubernetesApiError(response.code(), responseBody)
        finally response.close()
      }(ZIO.succeed(call.cancel()))
    }
    // 只有读取和 dry-run 能安全重放未知结果；真实变更仅重试明确拒绝执行的 429。
    val safeToReplay = method == "GET" || method == "HEAD" || query.get("dryRun").contains("All")
    effect.retry((Schedule.exponential(50.millis) && Schedule.recurs(settings.maxRetries)).whileInput(error => isRetryable(error, safeToReplay)))

  private def isRetryable(error: Throwable, safeToReplay: Boolean): Boolean =
    error match
      case KubernetesApiError(status, _) => status == 429 || (safeToReplay && status >= 500)
      case _: java.io.IOException           => safeToReplay
      case _                                => false

object KubernetesApiLive:
  def apply(settings: KubernetesApiSettings): KubernetesApiLive =
    new KubernetesApiLive(settings, Config.defaultClient())

object KubernetesApi:
  val live: ZLayer[Any, Nothing, KubernetesApi] =
    ZLayer.succeed(KubernetesApiLive(KubernetesApiSettings.fromEnv(sys.env)))

final case class KubernetesApiError(status: Int, body: String)
    extends RuntimeException(s"Kubernetes API request failed with HTTP $status: $body")

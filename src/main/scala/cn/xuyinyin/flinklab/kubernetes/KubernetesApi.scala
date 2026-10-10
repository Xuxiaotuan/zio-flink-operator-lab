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
import okhttp3.{MediaType, RequestBody}
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
      result <- request("POST", collectionPath(namespace, apiResource(kind)), body = Some(resource), contentType = "application/json")
    yield result

  override def get(namespace: Namespace, kind: ResourceKind, name: String): IO[Throwable, String] =
    request("GET", resourcePath(namespace, apiResource(kind), name))

  override def list(namespace: Namespace, kind: ResourceKind): IO[Throwable, String] =
    request("GET", collectionPath(namespace, apiResource(kind)))

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

  /** 将 Java Watch 转成受 Scope 管理的 ZStream，流结束时关闭底层连接。 */
  override def watchFrom(namespace: Namespace, kind: ResourceKind, name: Option[String], resourceVersion: Option[String]): ZStream[Any, Throwable, WatchEvent] =
    ZStream.unwrapScoped {
      ZIO.acquireRelease(
        ZIO.attemptBlocking {
          val client = apiClient
          val customObjects = new CustomObjectsApi(client)
          val builder = customObjects
            .listNamespacedCustomObject(
              settings.group,
              settings.version,
              namespace.namespaceValue,
              apiResource(kind)
            )
          name.foreach(value => builder.fieldSelector(s"metadata.name=$value"))
          resourceVersion.foreach(value => builder.resourceVersion(value))
          // allowWatchBookmarks 让服务能收到游标进展，但 bookmark 不会进入业务状态。
          val call = builder.allowWatchBookmarks(true).watch(true).buildCall(null)
          val responseType = new TypeToken[Watch.Response[Object]]() {}.getType
          (client, Watch.createWatch[Object](client, call, responseType))
        }
      ) { case (_, watch) => ZIO.attemptBlocking(watch.close()).ignore }
        .map { case (_, watch) =>
          ZStream.repeatZIOOption(nextEvent(watch))
        }
    }

  private def nextEvent(watch: Watch[Object]): ZIO[Any, Option[Throwable], WatchEvent] =
    ZIO.attemptBlocking {
      if watch.hasNext then Some(watch.next()) else None
    }.mapError(Some(_)).flatMap {
      case None => ZIO.fail(None)
      case Some(response) =>
        val objectJson =
          if response.`object` != null then JSON.serialize(response.`object`)
          else if response.status != null then JSON.serialize(response.status)
          else "{}"
        ZIO.fromEither(WatchEvent.fromJson(ujson.Obj("type" -> response.`type`, "object" -> ujson.read(objectJson))))
          .mapError(message => Some(IllegalArgumentException(message)))
          .flatMap {
            case event if event.eventType == WatchEventType.Error =>
              ZIO.fail(Some(watchError(event)))
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
    val effect = ZIO.attemptBlocking {
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
      val response = apiClient.getHttpClient.newCall(request).execute()
      try
        val responseBody = Option(response.body()).map(_.string()).getOrElse("")
        // 非 2xx 统一转成 KubernetesApiError，下面的 retry 只按状态码分类。
        if response.isSuccessful then responseBody
        else throw KubernetesApiError(response.code(), responseBody)
      finally response.close()
    }
    // 429/5xx/网络中断可重试，4xx 权限或参数错误必须立即返回。
    effect.retry((Schedule.exponential(50.millis) && Schedule.recurs(settings.maxRetries)).whileInput(isRetryable))

  private def isRetryable(error: Throwable): Boolean =
    error match
      case KubernetesApiError(status, _) => status == 429 || status >= 500
      case _: java.io.IOException           => true
      case _                                => false

object KubernetesApiLive:
  def apply(settings: KubernetesApiSettings): KubernetesApiLive =
    new KubernetesApiLive(settings, Config.defaultClient())

object KubernetesApi:
  val live: ZLayer[Any, Nothing, KubernetesApi] =
    ZLayer.succeed(KubernetesApiLive(KubernetesApiSettings.fromEnv(sys.env)))

final case class KubernetesApiError(status: Int, body: String)
    extends RuntimeException(s"Kubernetes API request failed with HTTP $status: $body")

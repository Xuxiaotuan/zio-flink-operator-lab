package cn.xuyinyin.flinklab.server

/** HTTP 控制面：将外部请求路由为查询或类型化 FlinkOperation，变更请求统一交给 AsyncOperationWorker。 */
import cn.xuyinyin.flinklab.cli.ResourceKind
import cn.xuyinyin.flinklab.domain.FlinkTypes.Namespace
import cn.xuyinyin.flinklab.kubernetes.KubernetesApi
import cn.xuyinyin.flinklab.operator.savepoint.SavepointPatch
import cn.xuyinyin.flinklab.operator.watch.{FlinkStateSnapshotStatus, FlinkStatusSnapshot}
import cn.xuyinyin.flinklab.state.{KubernetesStateStore, StateJournal, StateKey, StateRecord, StateStore}
import cn.xuyinyin.flinklab.application.FlinkOperationFactory
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.operation.{AcceptedOperation, AsyncOperationWorker, OperationStore}
import zio.*

/** HTTP 请求的最小内部表示，便于路由层脱离具体 Web Server。 */
final case class ApiRequest(
    method: String,
    path: String,
    query: Map[String, String] = Map.empty,
    body: String = ""
)

/** HTTP 响应模型，测试和真实 HttpServer 共用。 */
final case class ApiResponse(status: Int, body: String, contentType: String = "application/json")

final case class KubernetesHttpSettings(
    defaultNamespace: String = "default",
    defaultSavepointDirectory: Option[String] = None
)

object KubernetesHttpSettings:
  def fromEnv(env: Map[String, String]): KubernetesHttpSettings =
    KubernetesHttpSettings(
      defaultNamespace = env.getOrElse("FLINK_NAMESPACE", "default"),
      defaultSavepointDirectory = env.get("FLINK_SAVEPOINT_DIRECTORY").filter(_.nonEmpty)
    )

/** Stateless HTTP facade. Every mutating operation still goes through KubernetesApi. */
object KubernetesHttpApi:
  /** 无 worker 的查询/直接 CR 路由，变更 worker 路由通过 handleWith 重载进入。 */
  def handle(request: ApiRequest, settings: KubernetesHttpSettings = KubernetesHttpSettings()): ZIO[KubernetesApi & StateStore, Nothing, ApiResponse] =
    route(request, settings).catchAll(error => ZIO.succeed(errorResponse(error)))

  def handleWith(api: KubernetesApi, request: ApiRequest, settings: KubernetesHttpSettings): UIO[ApiResponse] =
    handleWith(api, new KubernetesStateStore(api), request, settings)

  def handleWith(api: KubernetesApi, store: StateStore, request: ApiRequest, settings: KubernetesHttpSettings): UIO[ApiResponse] =
    handle(request, settings).provide(ZLayer.make[KubernetesApi & StateStore](ZLayer.succeed(api), ZLayer.succeed(store)))

  def handleWith(api: KubernetesApi, store: StateStore, worker: AsyncOperationWorker, request: ApiRequest, settings: KubernetesHttpSettings): UIO[ApiResponse] =
    routeWithWorker(request, settings, worker, None).provide(ZLayer.make[KubernetesApi & StateStore](ZLayer.succeed(api), ZLayer.succeed(store))).catchAll(error => ZIO.succeed(errorResponse(error)))

  /** 生产入口：同时注入状态存储、operation store 和异步 worker。 */
  def handleWith(api: KubernetesApi, store: StateStore, operationStore: OperationStore, worker: AsyncOperationWorker, request: ApiRequest, settings: KubernetesHttpSettings): UIO[ApiResponse] =
    routeWithWorker(request, settings, worker, Some(operationStore)).provide(ZLayer.make[KubernetesApi & StateStore](ZLayer.succeed(api), ZLayer.succeed(store))).catchAll(error => ZIO.succeed(errorResponse(error)))

  private def routeWithWorker(request: ApiRequest, settings: KubernetesHttpSettings, worker: AsyncOperationWorker, operationStore: Option[OperationStore]): ZIO[KubernetesApi & StateStore, Throwable, ApiResponse] =
    request.query.get("dryRun").map(_.trim.toLowerCase) match
      case Some("true") =>
        request.path.split('/').toList.filter(_.nonEmpty) match
          case "v1" :: "deployments" :: Nil => applyDeployment(request, settings)
          case "v1" :: "snapshots" :: Nil => createSnapshot(request, settings)
          case _ => ZIO.fail(IllegalArgumentException("dryRun is only supported for deployment and snapshot apply requests"))
      case Some("false") | None => routeWithWorkerMutation(request, settings, worker, operationStore)
      case Some(value) => ZIO.fail(IllegalArgumentException(s"dryRun must be true or false, got: $value"))

  // 所有 HTTP 变更在这里先解析成 FlinkOperation，再交给同一个 worker。
  private def routeWithWorkerMutation(request: ApiRequest, settings: KubernetesHttpSettings, worker: AsyncOperationWorker, operationStore: Option[OperationStore]): ZIO[KubernetesApi & StateStore, Throwable, ApiResponse] =
    (request.method.toUpperCase, request.path.split('/').toList.filter(_.nonEmpty)) match
      case ("POST", "v1" :: "deployments" :: Nil) =>
        // 创建/更新部署统一生成 FlinkOperation，不直接 apply CR。
        for
          resource <- parseJson(request.body)
          namespace <- namespaceFor(resource, request.query, settings)
          body = prepareResource(resource, namespace, settings.defaultSavepointDirectory)
          operation <- ZIO.fromEither(FlinkOperationFactory.fromDeploymentJson(namespace.namespaceValue, body).left.map(IllegalArgumentException(_)))
          accepted <- worker.submit(requestId(request), operation).mapError(error => IllegalArgumentException(error.message))
        yield acceptedResponse(accepted, 202)
      case ("POST", "v1" :: "snapshots" :: Nil) =>
        // 快照也使用同一套 requestId、锁和验证协议。
        for
          body <- parseJson(request.body)
          namespace <- namespaceFrom(request.query, settings)
          operation <- ZIO.fromEither(FlinkOperationFactory.fromSnapshotJson(namespace.namespaceValue, body.render()).left.map(IllegalArgumentException(_)))
          accepted <- worker.submit(requestId(request), operation).mapError(error => IllegalArgumentException(error.message))
        yield acceptedResponse(accepted, 202)
      case ("POST", "v1" :: "deployments" :: name :: "savepoint" :: Nil) =>
        for
          namespace <- namespaceFrom(request.query, settings)
          deploymentName <- deploymentName(name)
          operation = FlinkOperation.Snapshot(ResourceRef(namespace, ResourceKind.Deployment, cn.xuyinyin.flinklab.domain.FlinkTypes.DeploymentName.unsafe(deploymentName)), SnapshotPolicy(cn.xuyinyin.flinklab.domain.SnapshotType.Savepoint))
          accepted <- worker.submit(requestId(request), operation).mapError(error => IllegalArgumentException(error.message))
        yield acceptedResponse(accepted, 202)
      case ("POST", "v1" :: "deployments" :: name :: "suspend-savepoint" :: Nil) =>
        for
          namespace <- namespaceFrom(request.query, settings)
          deploymentName <- deploymentName(name)
          operation = FlinkOperation.Suspend(ResourceRef(namespace, ResourceKind.Deployment, cn.xuyinyin.flinklab.domain.FlinkTypes.DeploymentName.unsafe(deploymentName)), SuspendPolicy.KeepState)
          accepted <- worker.submit(requestId(request), operation).mapError(error => IllegalArgumentException(error.message))
        yield acceptedResponse(accepted, 202)
      case ("POST", "v1" :: "deployments" :: name :: "upgrade" :: Nil) =>
        for
          body <- parseJson(request.body)
          namespace <- namespaceFor(body, request.query, settings)
          deploymentName <- deploymentName(name)
          specOperation <- ZIO.fromEither(FlinkOperationFactory.fromDeploymentJson(namespace.namespaceValue, prepareResource(body, namespace, settings.defaultSavepointDirectory)).left.map(IllegalArgumentException(_)))
          spec <- specOperation match
            case FlinkOperation.Deploy(value) if value.name.nameValue == deploymentName => ZIO.succeed(value)
            case _ => ZIO.fail(IllegalArgumentException("upgrade body must describe the deployment named in the URL"))
          policy <- policyFrom(request.query, spec.job.stateProtection)
          _ <- ZIO.fail(IllegalArgumentException("upgradeMode query must match job.upgradeMode")).unless(request.query.get("upgradeMode").orElse(request.query.get("upgrade-mode")).isEmpty || policy.protection == spec.job.stateProtection)
          operation = FlinkOperation.Upgrade(ResourceRef(namespace, ResourceKind.Deployment, cn.xuyinyin.flinklab.domain.FlinkTypes.DeploymentName.unsafe(deploymentName)), spec, policy)
          accepted <- worker.submit(requestId(request), operation).mapError(error => IllegalArgumentException(error.message))
        yield acceptedResponse(accepted, 202)
      case ("POST", "v1" :: "deployments" :: name :: "resume" :: Nil) =>
        for
          namespace <- namespaceFrom(request.query, settings)
          deploymentName <- deploymentName(name)
          operation = FlinkOperation.Resume(ResourceRef(namespace, ResourceKind.Deployment, cn.xuyinyin.flinklab.domain.FlinkTypes.DeploymentName.unsafe(deploymentName)))
          accepted <- worker.submit(requestId(request), operation).mapError(error => IllegalArgumentException(error.message))
        yield acceptedResponse(accepted, 202)
      case ("POST", "v1" :: "deployments" :: name :: "restart" :: Nil) =>
        for
          namespace <- namespaceFrom(request.query, settings)
          deploymentName <- deploymentName(name)
          policy <- policyFrom(request.query, StateProtection.Stateless)
          operation = FlinkOperation.Restart(ResourceRef(namespace, ResourceKind.Deployment, cn.xuyinyin.flinklab.domain.FlinkTypes.DeploymentName.unsafe(deploymentName)), policy)
          accepted <- worker.submit(requestId(request), operation).mapError(error => IllegalArgumentException(error.message))
        yield acceptedResponse(accepted, 202)
      case ("DELETE", "v1" :: "deployments" :: name :: Nil) =>
        for
          namespace <- namespaceFrom(request.query, settings)
          deploymentName <- deploymentName(name)
          operation = FlinkOperation.Delete(ResourceRef(namespace, ResourceKind.Deployment, cn.xuyinyin.flinklab.domain.FlinkTypes.DeploymentName.unsafe(deploymentName)), DeletePolicy.Graceful)
          accepted <- worker.submit(requestId(request), operation).mapError(error => IllegalArgumentException(error.message))
        yield acceptedResponse(accepted, 202)
      case ("GET", "v1" :: "operations" :: id :: Nil) =>
        // operation 查询只读持久化 store，不触发 worker 或 Kubernetes 副作用。
        operationStore match
          case None => ZIO.succeed(ApiResponse(503, errorJson("operation store is not configured")))
          case Some(store) =>
            for
              operationId <- ZIO.fromEither(cn.xuyinyin.flinklab.domain.OperationId.from(id).left.map(IllegalArgumentException(_)))
              operation <- store.get(operationId).mapError(error => IllegalArgumentException(error.message))
            yield operation.map(value => ok(value.json.render())).getOrElse(ApiResponse(404, errorJson("operation not found")))
      case _ => route(request, settings)

  // 没有显式 requestId 时生成一次性幂等键；客户端重试应主动复用 requestId。
  private def requestId(request: ApiRequest): RequestId = RequestId.from(request.query.getOrElse("requestId", java.util.UUID.randomUUID().toString)).fold(_ => RequestId.generate(), identity)

  private def acceptedResponse(accepted: AcceptedOperation, status: Int): ApiResponse =
    ApiResponse(status, ujson.Obj("operationId" -> accepted.operationId.operationIdValue, "requestId" -> accepted.requestId.requestIdValue, "state" -> "ACCEPTED", "acceptedAt" -> accepted.acceptedAt.toString).render())

  /** 查询、dry-run 和兼容的直接 CR 路由。真正变更请求由上面的 worker 路由优先处理。 */
  private def route(request: ApiRequest, settings: KubernetesHttpSettings): ZIO[KubernetesApi & StateStore, Throwable, ApiResponse] =
    (request.method.toUpperCase, request.path.split('/').toList.filter(_.nonEmpty)) match
      case ("GET", "healthz" :: Nil) | ("GET", "readyz" :: Nil) =>
        ZIO.succeed(ok("{\"status\":\"ok\"}"))
      case ("POST", "v1" :: "deployments" :: Nil) =>
        applyDeployment(request, settings)
      case ("GET", "v1" :: "deployments" :: Nil) =>
        listResources(request, settings, ResourceKind.Deployment, FlinkStatusSnapshot.fromJsonString, _.json)
      case ("GET", "v1" :: "deployments" :: name :: Nil) =>
        withDeployment(request, settings, name)((api, namespace, kind, resourceName) => api.get(namespace, kind, resourceName))
      case ("GET", "v1" :: "deployments" :: name :: "status" :: Nil) =>
        deploymentStatus(request, settings, name)
      case ("DELETE", "v1" :: "deployments" :: name :: Nil) =>
        deleteResource(request, settings, name, ResourceKind.Deployment)
      case ("POST", "v1" :: "deployments" :: name :: "savepoint" :: Nil) =>
        savepoint(request, settings, name, suspended = false)
      case ("POST", "v1" :: "deployments" :: name :: "suspend-savepoint" :: Nil) =>
        savepoint(request, settings, name, suspended = true)
      case ("POST", "v1" :: "snapshots" :: Nil) =>
        createSnapshot(request, settings)
      case ("GET", "v1" :: "snapshots" :: Nil) =>
        listResources(request, settings, ResourceKind.StateSnapshot, FlinkStateSnapshotStatus.fromJsonString, _.json)
      case ("GET", "v1" :: "snapshots" :: name :: Nil) =>
        snapshotStatus(request, settings, name)
      case ("DELETE", "v1" :: "snapshots" :: name :: Nil) =>
        deleteResource(request, settings, name, ResourceKind.StateSnapshot)
      case ("GET", "v1" :: "state" :: Nil) =>
        listState(request, settings)
      case ("GET", "v1" :: "state" :: kind :: name :: Nil) =>
        getState(request, settings, kind, name)
      case _ => ZIO.succeed(ApiResponse(404, errorJson("route not found")))

  private def applyDeployment(request: ApiRequest, settings: KubernetesHttpSettings): ZIO[KubernetesApi & StateStore, Throwable, ApiResponse] =
    for
      resource <- parseJson(request.body)
      namespace <- namespaceFor(resource, request.query, settings)
      dryRun = request.query.get("dryRun").exists(_.equalsIgnoreCase("true"))
      body = prepareResource(resource, namespace, settings.defaultSavepointDirectory)
      output <- ZIO.serviceWithZIO[KubernetesApi](_.apply(namespace, body, dryRun))
    yield ok(output)

  private def listResources[A](
      request: ApiRequest,
      settings: KubernetesHttpSettings,
      kind: ResourceKind,
      parse: String => Either[String, A],
      render: A => ujson.Value
  ): ZIO[KubernetesApi & StateStore, Throwable, ApiResponse] =
    for
      namespace <- namespaceFrom(request.query, settings)
      raw <- ZIO.serviceWithZIO[KubernetesApi](_.list(namespace, kind))
      collection <- parseCollection(raw, parse, render)
      records <- ZIO.fromEither(StateRecord.fromCollection(namespace, kind, raw).left.map(IllegalArgumentException(_)))
      _ <- ZIO.serviceWithZIO[StateStore](store => ZIO.foreachDiscard(records)(record => persist(store, record)))
    yield ok(collection.render())

  private def parseCollection[A](raw: String, parse: String => Either[String, A], render: A => ujson.Value): IO[Throwable, ujson.Obj] =
    ZIO.fromEither {
      scala.util.Try(ujson.read(raw)).toEither.left.map(error => error.toString).flatMap { value =>
        val items = value.obj.get("items").flatMap(_.arrOpt).toRight("Kubernetes list response must contain items")
        items.flatMap { values =>
          values.toList.foldLeft[Either[String, Vector[ujson.Value]]](Right(Vector.empty)) { (acc, item) =>
            for
              current <- acc
              parsed <- parse(item.render())
            yield current :+ render(parsed)
          }.map { values =>
            val result = ujson.Obj("items" -> ujson.Arr(values*))
            value.obj.get("metadata").flatMap(_.objOpt).flatMap(_.get("resourceVersion")).flatMap(_.strOpt).foreach(rv => result("resourceVersion") = rv)
            result
          }
        }
      }.left.map(IllegalArgumentException(_))
    }

  private def deploymentStatus(request: ApiRequest, settings: KubernetesHttpSettings, name: String): ZIO[KubernetesApi & StateStore, Throwable, ApiResponse] =
    for
      namespace <- namespaceFrom(request.query, settings)
      deploymentName <- deploymentName(name)
      raw <- ZIO.serviceWithZIO[KubernetesApi](_.get(namespace, ResourceKind.Deployment, deploymentName))
      record <- ZIO.fromEither(StateRecord.fromJson(namespace, ResourceKind.Deployment, raw).left.map(IllegalArgumentException(_)))
      _ <- ZIO.serviceWithZIO[StateStore](persist(_, record))
      status <- ZIO.fromEither(FlinkStatusSnapshot.fromJsonString(raw).left.map(IllegalArgumentException(_)))
    yield ok(status.json.render())

  private def snapshotStatus(request: ApiRequest, settings: KubernetesHttpSettings, name: String): ZIO[KubernetesApi & StateStore, Throwable, ApiResponse] =
    for
      namespace <- namespaceFrom(request.query, settings)
      snapshotName <- deploymentName(name)
      raw <- ZIO.serviceWithZIO[KubernetesApi](_.get(namespace, ResourceKind.StateSnapshot, snapshotName))
      record <- ZIO.fromEither(StateRecord.fromJson(namespace, ResourceKind.StateSnapshot, raw).left.map(IllegalArgumentException(_)))
      _ <- ZIO.serviceWithZIO[StateStore](persist(_, record))
      status <- ZIO.fromEither(FlinkStateSnapshotStatus.fromJsonString(raw).left.map(IllegalArgumentException(_)))
    yield ok(status.json.render())

  private def createSnapshot(request: ApiRequest, settings: KubernetesHttpSettings): ZIO[KubernetesApi & StateStore, Throwable, ApiResponse] =
    for
      body <- parseJson(request.body)
      namespace <- namespaceFrom(request.query, settings)
      targetKind <- targetKind(body)
      targetName <- fieldName(body, "targetName")
      snapshotName <- fieldName(body, "snapshotName")
      snapshotType <- fieldName(body, "type").flatMap(value => ZIO.fromEither(SnapshotType.parse(value).left.map(IllegalArgumentException(_))))
      resource <- ZIO.fromEither {
        for
          validTarget <- cn.xuyinyin.flinklab.domain.FlinkTypes.DeploymentName.from(targetName)
          validSnapshot <- cn.xuyinyin.flinklab.domain.FlinkTypes.DeploymentName.from(snapshotName)
        yield FlinkResources.render(FlinkStateSnapshotSpec(namespace, validSnapshot, targetKind, validTarget, snapshotType).resource)
      }.mapError(IllegalArgumentException(_))
      dryRun = request.query.get("dryRun").exists(_.equalsIgnoreCase("true"))
      output <- ZIO.serviceWithZIO[KubernetesApi](_.apply(namespace, resource, dryRun))
    yield ok(output)

  private def withDeployment(
      request: ApiRequest,
      settings: KubernetesHttpSettings,
      name: String
  )(operation: (KubernetesApi, Namespace, ResourceKind, String) => IO[Throwable, String]): ZIO[KubernetesApi & StateStore, Throwable, ApiResponse] =
    for
      namespace <- namespaceFrom(request.query, settings)
      deploymentName <- ZIO.fromEither(
        cn.xuyinyin.flinklab.domain.FlinkTypes.DeploymentName.from(name)
          .map(_.nameValue)
          .left.map(IllegalArgumentException(_))
      )
      output <- ZIO.serviceWithZIO[KubernetesApi](api => operation(api, namespace, ResourceKind.Deployment, deploymentName))
    yield ok(output)

  private def deleteResource(
      request: ApiRequest,
      settings: KubernetesHttpSettings,
      name: String,
      kind: ResourceKind
  ): ZIO[KubernetesApi & StateStore, Throwable, ApiResponse] =
    for
      namespace <- namespaceFrom(request.query, settings)
      resourceName <- deploymentName(name)
      output <- ZIO.serviceWithZIO[KubernetesApi](_.delete(namespace, kind, resourceName))
      _ <- ZIO.serviceWithZIO[StateStore] { store =>
        val key = StateKey(namespace, kind, resourceName)
        store.getJournal(key).flatMap {
          case Some(journal) => store.putJournal(StateJournal.markDeleted(journal))
          case None => store.delete(key)
        }
      }
    yield ok(output)

  private def listState(request: ApiRequest, settings: KubernetesHttpSettings): ZIO[KubernetesApi & StateStore, Throwable, ApiResponse] =
    for
      namespace <- namespaceFrom(request.query, settings)
      store <- ZIO.service[StateStore]
      journals <- store.listJournals(namespace)
      result = ujson.Obj(
        "backend" -> store.backend.toString.toLowerCase,
        "namespace" -> namespace.namespaceValue,
        "items" -> ujson.Arr(journals.sortBy(journal => (journal.key.kind.apiResource, journal.key.name)).map(_.json)*)
      )
    yield ok(result.render())

  private def getState(
      request: ApiRequest,
      settings: KubernetesHttpSettings,
      kind: String,
      name: String
  ): ZIO[KubernetesApi & StateStore, Throwable, ApiResponse] =
    for
      namespace <- namespaceFrom(request.query, settings)
      parsedKind <- ZIO.fromEither(ResourceKind.fromApiResource(kind).orElse(ResourceKind.parse(kind).toOption).toRight(IllegalArgumentException(s"unsupported state kind: $kind")))
      resourceName <- deploymentName(name)
      store <- ZIO.service[StateStore]
      journal <- store.getJournal(StateKey(namespace, parsedKind, resourceName))
      response <- journal match
        case Some(value) => ZIO.succeed(ok(ujson.Obj("backend" -> store.backend.toString.toLowerCase, "item" -> value.json).render()))
        case None => ZIO.succeed(ApiResponse(404, errorJson("state record not found")))
    yield response

  /** 兼容旧 savepoint 接口；新变更路径仍通过 worker 创建 Snapshot operation。 */
  private def savepoint(
      request: ApiRequest,
      settings: KubernetesHttpSettings,
      name: String,
      suspended: Boolean
  ): ZIO[KubernetesApi & StateStore, Throwable, ApiResponse] =
    for
      namespace <- namespaceFrom(request.query, settings)
      deploymentName <- ZIO.fromEither(
        cn.xuyinyin.flinklab.domain.FlinkTypes.DeploymentName.from(name)
          .map(_.nameValue)
          .left.map(IllegalArgumentException(_))
      )
      patch <- if suspended then ZIO.succeed(SavepointPatch.suspend)
      else
        for
          value <- parseJson(request.body)
          nonce <- nonceFrom(value)
        yield SavepointPatch.trigger(nonce)
      output <- ZIO.serviceWithZIO[KubernetesApi](_.patch(namespace, ResourceKind.Deployment, deploymentName, patch))
    yield ok(output)

  private def parseJson(body: String): IO[Throwable, ujson.Value] =
    if body.trim.isEmpty then ZIO.fail(IllegalArgumentException("request body is required"))
    else ZIO.attempt(ujson.read(body))

  private def namespaceFor(resource: ujson.Value, query: Map[String, String], settings: KubernetesHttpSettings): IO[Throwable, Namespace] =
    val fromBody =
      resource.obj.get("metadata").flatMap(_.objOpt).flatMap(_.get("namespace")).flatMap(_.strOpt)
    namespaceFrom(fromBody.map("namespace" -> _).toMap ++ query, settings)

  private def namespaceFrom(query: Map[String, String], settings: KubernetesHttpSettings): IO[Throwable, Namespace] =
    ZIO.fromEither(
      Namespace.from(query.getOrElse("namespace", settings.defaultNamespace))
        .left.map(IllegalArgumentException(_))
    )

  private def deploymentName(value: String): IO[Throwable, String] =
    ZIO.fromEither(
      cn.xuyinyin.flinklab.domain.FlinkTypes.DeploymentName.from(value).map(_.nameValue).left.map(IllegalArgumentException(_))
    )

  private def targetKind(value: ujson.Value): IO[Throwable, ResourceKind] =
    ZIO.fromEither {
      value.obj.get("targetKind").flatMap(_.strOpt).toRight(IllegalArgumentException("request body must contain targetKind"))
        .flatMap {
          case "FlinkDeployment" | "deployment" => Right(ResourceKind.Deployment)
          case "FlinkSessionJob" | "session-job" => Right(ResourceKind.SessionJob)
          case other => Left(IllegalArgumentException(s"unsupported targetKind: $other"))
        }
    }

  private def fieldName(value: ujson.Value, field: String): IO[Throwable, String] =
    ZIO.fromEither(value.obj.get(field).flatMap(_.strOpt).filter(_.nonEmpty).toRight(IllegalArgumentException(s"request body must contain $field")))

  private def nonceFrom(value: ujson.Value): IO[Throwable, Long] =
    val nonceValue = value.obj.get("nonce").flatMap(_.strOpt).orElse(
      value.obj.get("nonce").flatMap(_.numOpt).flatMap { number =>
        val asLong = number.toLong
        Option.when(number == asLong.toDouble)(asLong.toString)
      }
    )

    ZIO.fromEither(
      nonceValue.toRight(IllegalArgumentException("request body must contain nonce as a string or safe integer"))
        .flatMap(raw => scala.util.Try(raw.toLong).toEither.left.map(_ => IllegalArgumentException("nonce must be a Long")))
    )

  private def policyFrom(query: Map[String, String], defaultProtection: StateProtection): IO[Throwable, UpgradePolicy] =
    for
      protection <- ZIO.fromEither(StateProtection.parse(query.get("upgradeMode").orElse(query.get("upgrade-mode")).getOrElse(defaultProtection.operatorValue)).left.map(IllegalArgumentException(_)))
      fallback <- ZIO.fromEither(query.get("fallback").map(parseFallback).getOrElse(Right(FallbackPolicy.Forbidden)).left.map(IllegalArgumentException(_)))
      policy = UpgradePolicy(protection, fallback)
      _ <- ZIO.fromEither(policy.validate.left.map(error => IllegalArgumentException(error.message)))
    yield policy

  private def parseFallback(value: String): Either[String, FallbackPolicy] =
    value.trim.toLowerCase match
      case "forbidden" => Right(FallbackPolicy.Forbidden)
      case "allow-last-state" | "allowlaststate" => Right(FallbackPolicy.AllowLastState)
      case other => Left(s"unknown fallback policy: $other (use forbidden or allow-last-state)")

  private def persist(store: StateStore, record: StateRecord): IO[Throwable, Unit] =
    store.getJournal(record.key).flatMap(previous => store.putJournal(StateJournal.merge(previous, record)))

  // 只补默认 namespace/savepoint 目录，不修改调用方明确提供的字段。
  private def prepareResource(resource: ujson.Value, namespace: Namespace, directory: Option[String]): String =
    val metadata = resource.obj.get("metadata").map(_.obj).getOrElse {
      val created = ujson.Obj()
      resource.obj("metadata") = created
      created.obj
    }
    metadata("namespace") = namespace.namespaceValue
    if resource.obj.get("kind").flatMap(_.strOpt).exists(_ == "FlinkDeployment") then
      directory.foreach { value =>
        val spec = resource.obj.get("spec").map(_.obj).getOrElse {
          val created = ujson.Obj()
          resource.obj("spec") = created
          created.obj
        }
        val configuration = spec.get("flinkConfiguration").map(_.obj).getOrElse {
          val created = ujson.Obj()
          spec("flinkConfiguration") = created
          created.obj
        }
        if !configuration.contains("state.savepoints.dir") then configuration("state.savepoints.dir") = value
      }
    resource.render()

  private def ok(body: String): ApiResponse = ApiResponse(200, body)

  private def errorResponse(error: Throwable): ApiResponse =
    val status = error match
      case _: IllegalArgumentException => 400
      case _                           => 502
    ApiResponse(status, errorJson(Option(error.getMessage).getOrElse(error.toString)))

  def errorJson(message: String): String =
    ujson.Obj("error" -> message).render()

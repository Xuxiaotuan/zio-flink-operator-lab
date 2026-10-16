package cn.xuyinyin.flinklab.e2e

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import zio.*
import zio.test.*

/**
  * 访问已经部署的 HTTP 控制面，验证“进程可用”之外的真实边界：
  * Kubernetes 状态后端、PostgreSQL 元数据连接和只读查询路由。
  *
  * 默认跳过，避免普通单测依赖网络；CI/现场验收显式设置
  * ZIO_FLINK_E2E_ENABLED=true 和 ZIO_FLINK_E2E_BASE_URL 后才执行。
  */
object ExternalControlPlaneE2ESpec extends ZIOSpecDefault:
  private val baseUrl = sys.env.getOrElse("ZIO_FLINK_E2E_BASE_URL", "http://127.0.0.1:8080").stripSuffix("/")
  private val namespace = sys.env.getOrElse("ZIO_FLINK_E2E_NAMESPACE", "bigdata-lab")
  // E2E 的目标是已经部署的控制面；显式绕过构建机的外网代理，避免
  // `*.svc.cluster.local` 被错误地送到代理而掩盖真实的集群连通性。
  private val client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).proxy(HttpClient.Builder.NO_PROXY).build()

  private def get(path: String): Task[(Int, ujson.Value)] =
    ZIO.attemptBlocking {
      val request = HttpRequest.newBuilder(URI.create(baseUrl + path)).timeout(Duration.ofSeconds(20)).header("Accept", "application/json").GET().build()
      val response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
      response.statusCode() -> ujson.read(response.body())
    }

  private def encoded(value: String): String = URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20")

  def spec = suite("external control plane e2e")(
    test("health and readiness are served by the deployed control plane") {
      for
        health <- get("/healthz")
        ready <- get("/readyz")
      yield assertTrue(health._1 == 200, health._2("status").str == "ok", ready._1 == 200, ready._2("status").str == "ok")
    },
    test("the deployed process exposes the configured namespace") {
      get("/v1/config").map { case (status, body) => assertTrue(status == 200, body("namespace").str == namespace) }
    },
    test("Kubernetes state and resource list are reachable through the HTTP boundary") {
      for
        state <- get(s"/v1/state?namespace=${encoded(namespace)}")
        deployments <- get(s"/v1/deployments?namespace=${encoded(namespace)}")
        snapshots <- get(s"/v1/snapshots?namespace=${encoded(namespace)}")
      yield assertTrue(
        state._1 == 200,
        state._2("backend").str == "kubernetes",
        state._2("namespace").str == namespace,
        state._2("items").isInstanceOf[ujson.Arr],
        deployments._1 == 200,
        deployments._2("items").isInstanceOf[ujson.Arr],
        snapshots._1 == 200,
        snapshots._2("items").isInstanceOf[ujson.Arr]
      )
    },
    test("catalog and lineage read routes are available") {
      for
        catalogs <- get("/v1/catalogs")
        lineage <- get("/v1/lineage/graph?root=e2e-no-such-dataset")
      yield assertTrue(
        catalogs._1 == 200,
        catalogs._2("items").isInstanceOf[ujson.Arr],
        lineage._1 == 200,
        lineage._2("root").str == "e2e-no-such-dataset",
        lineage._2("items").arr.isEmpty
      )
    }
  ) @@ TestAspect.ifEnv("ZIO_FLINK_E2E_ENABLED")(_.equalsIgnoreCase("true"))

package cn.xuyinyin.flinklab.kubernetes
/** 验证 Java Client 适配器对 Kubernetes HTTP API 的路径、patch、重试和错误分类。 */

import cn.xuyinyin.flinklab.domain.ResourceKind
import cn.xuyinyin.flinklab.domain.FlinkTypes.Namespace
import cn.xuyinyin.flinklab.domain.ResourceUid
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import io.kubernetes.client.util.ClientBuilder
import io.kubernetes.client.util.credentials.AccessTokenAuthentication
import zio.*
import zio.test.*

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters.*

/**
  * Contract tests for the direct Kubernetes API adapter.
  *
  * The fake server deliberately checks the wire contract instead of mocking the
  * generated Kubernetes Java client. This catches incorrect HTTP verbs and
  * media types while keeping the test independent of a real cluster.
  */
object KubernetesApiHttpContractSpec extends ZIOSpecDefault:
  private final case class RequestSnapshot(method: String, path: String, contentType: String, authorization: String, body: String)

  def spec =
    suite("kubernetes api http contract")(
      test("apply uses server-side apply, retries 5xx, and patch uses merge-patch") {
        for
          state <- ZIO.succeed(new FakeServerState)
          server <- ZIO.acquireRelease(ZIO.attempt(startServer(state)))(server => ZIO.succeed(server.stop(0)))
          api = client(server)
          namespace = Namespace.unsafe("analytics")
          resource =
            """{"apiVersion":"flink.apache.org/v1beta1","kind":"FlinkDeployment","metadata":{"name":"orders"}}"""
          savepointPatch = "{\"spec\":{\"job\":{\"savepointTriggerNonce\":9223372036854775807}}}"
          applied <- api.apply(namespace, resource, dryRun = true)
          patched <- api.patch(namespace, ResourceKind.Deployment, "orders", savepointPatch)
          requests <- ZIO.succeed(state.requests.asScala.toList)
        yield assertTrue(
          applied == "{\"ok\":true}",
          patched == "{\"patched\":true}",
          state.applyAttempts.get == 2,
          requests.count(_.method == "PATCH") == 3,
          requests.headOption.exists(request =>
            request.path.startsWith("/apis/flink.apache.org/v1beta1/namespaces/analytics/flinkdeployments/orders") &&
              request.path.contains("fieldManager=zio-flink-operator-lab") &&
              !request.path.contains("force=") &&
              request.path.contains("dryRun=All") &&
              request.contentType.startsWith("application/apply-patch+yaml") &&
              request.authorization == "Bearer fake-token" &&
              request.body == resource
          ),
          requests.lastOption.exists(request =>
            request.method == "PATCH" &&
              request.contentType.startsWith("application/merge-patch+json") &&
              request.body == savepointPatch
          )
        )
      } @@ TestAspect.withLiveClock,
      test("create identifies the update field manager") {
        for
          state <- ZIO.succeed(new FakeServerState)
          server <- ZIO.acquireRelease(ZIO.attempt(startServer(state)))(server => ZIO.succeed(server.stop(0)))
          api = client(server)
          _ <- api.create(Namespace.unsafe("analytics"), """{"kind":"FlinkDeployment","metadata":{"name":"orders"}}""")
          requests <- ZIO.succeed(state.requests.asScala.toList)
        yield assertTrue(requests.size == 1, requests.headOption.exists(request =>
          request.method == "POST" && request.path.contains("fieldManager=zio-flink-operator-lab")))
      },
      test("create preserves the first ambiguous server failure instead of replaying into a conflict") {
        for
          state <- ZIO.succeed(new FakeServerState)
          server <- ZIO.acquireRelease(ZIO.attempt(startServer(state)))(server => ZIO.succeed(server.stop(0)))
          api = client(server)
          result <- api.create(Namespace.unsafe("ambiguous"), """{"kind":"FlinkDeployment","metadata":{"name":"orders"}}""").either
          requests <- ZIO.succeed(state.requests.asScala.toList)
        yield assertTrue(result.left.toOption.exists { case KubernetesApiError(500, _) => true; case _ => false }, requests.size == 1)
      } @@ TestAspect.withLiveClock,
      test("a protected mutation does not replay an ambiguous server failure") {
        for
          state <- ZIO.succeed(new FakeServerState)
          server <- ZIO.acquireRelease(ZIO.attempt(startServer(state)))(server => ZIO.succeed(server.stop(0)))
          api = client(server)
          result <- api.patch(Namespace.unsafe("analytics"), ResourceKind.Deployment, "ambiguous", """{"metadata":{"uid":"uid-1","resourceVersion":"7"},"spec":{"restartNonce":1}}""").either
          requests <- ZIO.succeed(state.requests.asScala.toList)
        yield assertTrue(result.left.toOption.exists { case KubernetesApiError(500, _) => true; case _ => false }, requests.size == 1)
      } @@ TestAspect.withLiveClock,
      test("reads can retry ambiguous server failures") {
        for
          state <- ZIO.succeed(new FakeServerState)
          server <- ZIO.acquireRelease(ZIO.attempt(startServer(state)))(server => ZIO.succeed(server.stop(0)))
          api = client(server)
          result <- api.get(Namespace.unsafe("analytics"), ResourceKind.Deployment, "read-retry")
          requests <- ZIO.succeed(state.requests.asScala.toList)
        yield assertTrue(result == "{\"object\":true}", requests.size == 2)
      } @@ TestAspect.withLiveClock,
      test("get and delete use the named custom-resource endpoint") {
        for
          state <- ZIO.succeed(new FakeServerState)
          server <- ZIO.acquireRelease(ZIO.attempt(startServer(state)))(server => ZIO.succeed(server.stop(0)))
          api = client(server)
          namespace = Namespace.unsafe("analytics")
          got <- api.get(namespace, ResourceKind.SessionJob, "orders-job")
          deleted <- api.delete(namespace, ResourceKind.SessionJob, "orders-job")
          requests <- ZIO.succeed(state.requests.asScala.toList)
        yield assertTrue(
          got == "{\"object\":true}",
          deleted == "{\"deleted\":true}",
          requests.map(_.method) == List("GET", "DELETE"),
          requests.forall(_.path == "/apis/flink.apache.org/v1beta1/namespaces/analytics/flinksessionjobs/orders-job")
        )
      },
      test("list uses the collection custom-resource endpoint") {
        for
          state <- ZIO.succeed(new FakeServerState)
          server <- ZIO.acquireRelease(ZIO.attempt(startServer(state)))(server => ZIO.succeed(server.stop(0)))
          api = client(server)
          listed <- api.list(Namespace.unsafe("analytics"), ResourceKind.StateSnapshot)
          requests <- ZIO.succeed(state.requests.asScala.toList)
        yield assertTrue(
          listed == "{\"object\":true}",
          requests.size == 1,
          requests.headOption.exists(request => request.method == "GET" && request.path == "/apis/flink.apache.org/v1beta1/namespaces/analytics/flinkstatesnapshots")
        )
      },
      test("list sends active and legacy label selectors to the API server") {
        for
          state <- ZIO.succeed(new FakeServerState)
          server <- ZIO.acquireRelease(ZIO.attempt(startServer(state)))(server => ZIO.succeed(server.stop(0)))
          api = client(server)
          _ <- api.list(Namespace.unsafe("analytics"), ResourceKind.Operation, Some("xxt.io/operation-active=true"))
          _ <- api.list(Namespace.unsafe("analytics"), ResourceKind.Operation, Some("!xxt.io/operation-active"))
          requests <- ZIO.succeed(state.requests.asScala.toList)
          paths = requests.map(request => java.net.URLDecoder.decode(request.path, StandardCharsets.UTF_8))
        yield assertTrue(requests.forall(_.method == "GET"), paths == List(
          "/apis/flink.apache.org/v1beta1/namespaces/analytics/flinkoperations?labelSelector=xxt.io/operation-active=true",
          "/apis/flink.apache.org/v1beta1/namespaces/analytics/flinkoperations?labelSelector=!xxt.io/operation-active"))
      },
      test("delete carries a UID precondition when one is supplied") {
        for
          state <- ZIO.succeed(new FakeServerState)
          server <- ZIO.acquireRelease(ZIO.attempt(startServer(state)))(server => ZIO.succeed(server.stop(0)))
          api = client(server)
          _ <- api.delete(Namespace.unsafe("analytics"), ResourceKind.Deployment, "orders", ResourceUid.from("uid-1").toOption, Some("7"))
          requests <- ZIO.succeed(state.requests.asScala.toList)
        yield assertTrue(requests.headOption.exists(request => request.method == "DELETE" && request.body.contains("\"uid\":\"uid-1\"") && request.body.contains("\"resourceVersion\":\"7\"")))
      },
      test("does not retry a forbidden response") {
        for
          state <- ZIO.succeed(new FakeServerState)
          server <- ZIO.acquireRelease(ZIO.attempt(startServer(state)))(server => ZIO.succeed(server.stop(0)))
          api = client(server)
          result <- api.get(Namespace.unsafe("analytics"), ResourceKind.Deployment, "forbidden").either
          requests <- ZIO.succeed(state.requests.asScala.toList)
        yield assertTrue(result.isLeft, requests.size == 1, requests.headOption.exists(_.method == "GET"))
      },
      test("retries 429 until the retry budget is exhausted") {
        for
          state <- ZIO.succeed(new FakeServerState)
          server <- ZIO.acquireRelease(ZIO.attempt(startServer(state)))(server => ZIO.succeed(server.stop(0)))
          api = client(server)
          result <- api.get(Namespace.unsafe("analytics"), ResourceKind.Deployment, "throttled").either
          requests <- ZIO.succeed(state.requests.asScala.toList)
        yield assertTrue(result.isLeft, requests.size == 3, requests.forall(_.method == "GET"))
      } @@ TestAspect.withLiveClock
    )

  private def client(server: HttpServer): KubernetesApiLive =
    val apiClient =
      ClientBuilder.standard()
        .setBasePath(s"http://127.0.0.1:${server.getAddress.getPort}")
        .setAuthentication(new AccessTokenAuthentication("fake-token"))
        .setVerifyingSsl(false)
        .build()
    new KubernetesApiLive(KubernetesApiSettings(maxRetries = 2), apiClient)

  private def startServer(state: FakeServerState): HttpServer =
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/", new HttpHandler:
      override def handle(exchange: HttpExchange): Unit =
        try
          val body = String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
          val path = exchange.getRequestURI.toString
          val contentType = Option(exchange.getRequestHeaders.getFirst("Content-Type")).getOrElse("")
          val authorization = Option(exchange.getRequestHeaders.getFirst("Authorization")).getOrElse("")
          state.requests.add(RequestSnapshot(exchange.getRequestMethod, path, contentType, authorization, body))
          val (status, response) =
            if path.contains("/forbidden") then (403, "{\"error\":\"forbidden\"}")
            else if path.contains("/throttled") then (429, "{\"error\":\"too many requests\"}")
            else if path.contains("/ambiguous") then
              if state.requests.size() == 1 then (500, "{\"error\":\"response after possible commit\"}")
              else (409, "{\"error\":\"already exists or resource version changed\"}")
            else if path.contains("/read-retry") && state.requests.size() == 1 then (500, "{\"error\":\"temporary\"}")
            else if exchange.getRequestMethod == "PATCH" && contentType.startsWith("application/apply-patch+yaml") then
              val attempt = state.applyAttempts.incrementAndGet()
              if attempt == 1 then (500, "{\"error\":\"temporary\"}")
              else (200, "{\"ok\":true}")
            else if exchange.getRequestMethod == "PATCH" then (200, "{\"patched\":true}")
            else if exchange.getRequestMethod == "GET" then (200, "{\"object\":true}")
            else if exchange.getRequestMethod == "DELETE" then (200, "{\"deleted\":true}")
            else if exchange.getRequestMethod == "POST" then (201, "{\"created\":true}")
            else (404, "{\"error\":\"not found\"}")
          val responseBytes = response.getBytes(StandardCharsets.UTF_8)
          exchange.sendResponseHeaders(status, responseBytes.length.toLong)
          exchange.getResponseBody.write(responseBytes)
        finally
          exchange.close()
    )
    server.start()
    server

  private final class FakeServerState:
    val requests = new ConcurrentLinkedQueue[RequestSnapshot]()
    val applyAttempts = new AtomicInteger(0)

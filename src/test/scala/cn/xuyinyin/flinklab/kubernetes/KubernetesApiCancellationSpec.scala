package cn.xuyinyin.flinklab.kubernetes

import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.operation.KubernetesResourceCoordinator
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.kubernetes.client.util.ClientBuilder
import okhttp3.{Call, Connection, EventListener}
import zio.*
import zio.test.*

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

/** 使用真实 TCP/OkHttp 连接，验证取消传到传输层，且不会重放结果未知的写入。 */
object KubernetesApiCancellationSpec extends ZIOSpecDefault:
  private val ns = Namespace.unsafe("analytics")
  private val target = ResourceRef(ns, ResourceKind.Deployment, DeploymentName.unsafe("orders"))
  private final class Probe(val partialBody: Boolean):
    val received = new CountDownLatch(1)
    val unblock = new CountDownLatch(1)
    val canceled = new CountDownLatch(1)
    val released = new CountDownLatch(1)
    val mutations = new AtomicInteger()
    val cancelBeforeUnlock = new AtomicBoolean(false)
    val executor = Executors.newCachedThreadPool()
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.setExecutor(executor)
    server.createContext("/", exchange => handle(exchange))
    server.start()
    val client = new ClientBuilder().setBasePath(s"http://127.0.0.1:${server.getAddress.getPort}").build()
    client.setHttpClient(client.getHttpClient.newBuilder().eventListener(new EventListener:
      override def canceled(call: Call): Unit =
        if call.request().url().encodedPath().contains("flinkdeployments") then Probe.this.canceled.countDown()
      override def connectionReleased(call: Call, connection: Connection): Unit =
        if call.request().url().encodedPath().contains("flinkdeployments") then released.countDown()
    ).build())
    val api = new KubernetesApiLive(KubernetesApiSettings(maxRetries = 2), client)
    private def send(exchange: HttpExchange, code: Int, raw: String): Unit =
      val bytes = raw.getBytes(UTF_8)
      exchange.sendResponseHeaders(code, bytes.length)
      exchange.getResponseBody.write(bytes)
    private def handle(exchange: HttpExchange): Unit =
      try
        exchange.getRequestBody.readAllBytes()
        if exchange.getRequestURI.getPath.contains("flinkoperationlocks") then
          exchange.getRequestMethod match
            case "POST" => send(exchange, 201, """{"metadata":{"uid":"lock-uid","resourceVersion":"1"}}""")
            case "PATCH" => send(exchange, 409, "lease ownership lost")
            case "DELETE" =>
              cancelBeforeUnlock.set(canceled.getCount == 0)
              send(exchange, 200, "{}")
            case _ => send(exchange, 404, "{}")
        else
          mutations.incrementAndGet()
          if partialBody then
            exchange.sendResponseHeaders(200, 100)
            exchange.getResponseBody.write('{'.toInt)
            exchange.getResponseBody.flush()
          received.countDown()
          unblock.await(55, TimeUnit.SECONDS)
          if !partialBody then send(exchange, 200, "{}")
      catch case _: java.io.IOException => () // 对端取消会关闭 socket，属于本测试预期。
      finally exchange.close()
    def close(): Unit =
      unblock.countDown()
      server.stop(0)
      executor.shutdownNow()
      ()

  private def probe(partialBody: Boolean) = ZIO.acquireRelease(ZIO.attempt(new Probe(partialBody)))(value => ZIO.succeed(value.close()))
  private def arrived(latch: CountDownLatch) = ZIO.attemptBlocking(latch.await(2, TimeUnit.SECONDS))

  def spec = suite("kubernetes transport cancellation")(
    test("interrupt cancels a request waiting for response headers") { cancelRequest(partialBody = false) },
    test("interrupt cancels and closes a response while reading its body") { cancelRequest(partialBody = true) },
    test("lease loss cancels the in-flight mutation before releasing the lock") {
      for
        p <- probe(partialBody = true)
        fiber <- new KubernetesResourceCoordinator(p.api).withLock(target, OperationId.generate())(
          p.api.patch(ns, ResourceKind.Deployment, "orders", "{}").mapError(error => ControlPlaneError.StoreFailure(error.toString))
        ).either.fork
        received <- arrived(p.received)
        _ <- ZIO.sleep(41.seconds)
        canceled <- arrived(p.canceled)
        _ <- ZIO.succeed(p.unblock.countDown())
        result <- fiber.join
      yield assertTrue(received, canceled, p.cancelBeforeUnlock.get(), result.isLeft, p.mutations.get() == 1)
    } @@ TestAspect.withLiveClock @@ TestAspect.timeout(55.seconds)
  ) @@ TestAspect.sequential @@ TestAspect.timeout(60.seconds)

  private def cancelRequest(partialBody: Boolean) =
    for
      p <- probe(partialBody)
      fiber <- p.api.patch(ns, ResourceKind.Deployment, "orders", "{}").fork
      received <- arrived(p.received)
      interrupt <- fiber.interrupt.fork
      canceled <- arrived(p.canceled)
      // 先观察取消，再放开服务端；旧实现即使收到正常响应才退出，也不能蒙混通过。
      _ <- ZIO.succeed(p.unblock.countDown())
      exit <- interrupt.join
      released <- arrived(p.released)
    yield assertTrue(received, canceled, released, exit.isInterrupted, p.mutations.get() == 1)

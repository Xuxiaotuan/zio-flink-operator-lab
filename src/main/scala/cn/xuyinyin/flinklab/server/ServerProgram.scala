package cn.xuyinyin.flinklab.server

/** HTTP 服务启动器：创建 Java HttpServer、绑定 ZIO 依赖并为每个请求调用控制面路由。 */
import cn.xuyinyin.flinklab.kubernetes.KubernetesApi
import cn.xuyinyin.flinklab.state.{StatePoller, StateStore}
import cn.xuyinyin.flinklab.operation.{AsyncOperationWorker, OperationStore}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import zio.*

import java.net.{InetSocketAddress, URI, URLDecoder}
import java.nio.charset.StandardCharsets
import java.util.concurrent.{ExecutorService, Executors}

final case class HttpServerSettings(host: String = "0.0.0.0", port: Int = 8080, threads: Int = 8)

object HttpServerSettings:
  def fromEnv(env: Map[String, String]): HttpServerSettings =
    HttpServerSettings(
      host = env.getOrElse("ZIO_FLINK_SERVER_HOST", "0.0.0.0"),
      port = env.get("ZIO_FLINK_SERVER_PORT").flatMap(_.toIntOption).getOrElse(8080),
      threads = env.get("ZIO_FLINK_SERVER_THREADS").flatMap(_.toIntOption).filter(_ > 0).getOrElse(8)
    )

object ServerProgram:
  /** 初始化数据库/状态后启动 HTTP Server 与后台状态轮询。 */
  def run: ZIO[KubernetesApi & StateStore & AsyncOperationWorker & OperationStore, Throwable, Unit] =
    ZIO.scoped {
      for
        api <- ZIO.service[KubernetesApi]
        store <- ZIO.service[StateStore]
        worker <- ZIO.service[AsyncOperationWorker]
        operationStore <- ZIO.service[OperationStore]
        _ <- operationStore.initialize
        _ <- store.initialize
        _ <- StatePoller.run.forkScoped
        settings = HttpServerSettings.fromEnv(sys.env)
        running <- ZIO.acquireRelease(start(settings, api, store, operationStore, worker)) { case (server, executor) =>
          ZIO.attempt(server.stop(0)).ignore *> ZIO.succeed(executor.shutdown())
        }
        _ <- Console.printLine(s"zio-flink-operator server listening on ${settings.host}:${settings.port}")
        _ <- ZIO.never
      yield ()
    }

  /** 用资源作用域管理 HttpServer 和线程池，服务退出时一定关闭。 */
  private def start(settings: HttpServerSettings, api: KubernetesApi, store: StateStore, operationStore: OperationStore, worker: AsyncOperationWorker): IO[Throwable, (HttpServer, ExecutorService)] =
    ZIO.attempt {
      val server = HttpServer.create(new InetSocketAddress(settings.host, settings.port), 0)
      val executor = Executors.newFixedThreadPool(settings.threads)
      server.setExecutor(executor)
      server.createContext("/", new HttpHandler:
        override def handle(exchange: HttpExchange): Unit =
          try
            val request = ApiRequest(
              method = exchange.getRequestMethod,
              path = exchange.getRequestURI.getPath,
              query = queryParameters(exchange.getRequestURI),
              body = String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
            )
            val response = Unsafe.unsafe { implicit unsafe =>
              Runtime.default.unsafe.run(
                WebUi.handle(request).flatMap {
                  case Some(response) => ZIO.succeed(response)
                  case None => KubernetesHttpApi.handleWith(api, store, operationStore, worker, request, KubernetesHttpSettings.fromEnv(sys.env))
                }
              ).getOrThrowFiberFailure()
            }
            val bytes = response.body.getBytes(StandardCharsets.UTF_8)
            exchange.getResponseHeaders.set("Content-Type", response.contentType + "; charset=utf-8")
            exchange.sendResponseHeaders(response.status, bytes.length.toLong)
            val output = exchange.getResponseBody
            try output.write(bytes)
            finally output.close()
          catch
            case error: Throwable =>
              val bytes = KubernetesHttpApi.errorJson(Option(error.getMessage).getOrElse(error.toString)).getBytes(StandardCharsets.UTF_8)
              exchange.getResponseHeaders.set("Content-Type", "application/json; charset=utf-8")
              exchange.sendResponseHeaders(500, bytes.length.toLong)
              exchange.getResponseBody.write(bytes)
              exchange.close()
          finally exchange.close()
      )
      server.start()
      (server, executor)
    }

  private def queryParameters(uri: URI): Map[String, String] =
    Option(uri.getRawQuery).toList.flatMap(_.split('&').toList).flatMap { pair =>
      pair.split("=", 2).toList match
        case key :: value :: Nil => Some(URLDecoder.decode(key, "UTF-8") -> URLDecoder.decode(value, "UTF-8"))
        case key :: Nil if key.nonEmpty => Some(URLDecoder.decode(key, "UTF-8") -> "")
        case _ => None
    }.toMap

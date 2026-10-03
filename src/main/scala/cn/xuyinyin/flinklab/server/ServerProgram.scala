package cn.xuyinyin.flinklab.server

import cn.xuyinyin.flinklab.kubernetes.KubernetesApi
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
  def run: ZIO[KubernetesApi, Throwable, Unit] =
    ZIO.scoped {
      for
        api <- ZIO.service[KubernetesApi]
        settings = HttpServerSettings.fromEnv(sys.env)
        running <- ZIO.acquireRelease(start(settings, api)) { case (server, executor) =>
          ZIO.attempt(server.stop(0)).ignore *> ZIO.succeed(executor.shutdown())
        }
        _ <- Console.printLine(s"zio-flink-operator server listening on ${settings.host}:${settings.port}")
        _ <- ZIO.never
      yield ()
    }

  private def start(settings: HttpServerSettings, api: KubernetesApi): IO[Throwable, (HttpServer, ExecutorService)] =
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
                KubernetesHttpApi.handleWith(api, request, KubernetesHttpSettings.fromEnv(sys.env))
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

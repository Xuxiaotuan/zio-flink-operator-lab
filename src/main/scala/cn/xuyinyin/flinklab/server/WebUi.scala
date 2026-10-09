package cn.xuyinyin.flinklab.server

import zio.*
import java.nio.charset.StandardCharsets

/** 固定白名单读取 JAR 内的前端资源；请求路径绝不参与文件路径拼接。 */
object WebUi:
  private val assets = Map(
    "/" -> ("/web/index.html", "text/html"),
    "/app.js" -> ("/web/app.js", "text/javascript"),
    "/styles.css" -> ("/web/styles.css", "text/css")
  )

  def handle(request: ApiRequest): UIO[Option[ApiResponse]] =
    assets.get(request.path).filter(_ => request.method == "GET") match
      case None => ZIO.none
      case Some((resource, contentType)) =>
        ZIO.scoped {
          ZIO.acquireRelease(ZIO.attemptBlocking(Option(getClass.getResourceAsStream(resource)).getOrElse(
            throw IllegalStateException(s"packaged web asset missing: $resource")
          )))(stream => ZIO.attemptBlocking(stream.close()).ignore).flatMap { stream =>
            ZIO.attemptBlocking(Some(ApiResponse(200, String(stream.readAllBytes(), StandardCharsets.UTF_8), contentType, Map("Cache-Control" -> "no-store"))))
          }
        }.catchAll(_ => ZIO.some(ApiResponse(500, KubernetesHttpApi.errorJson("web assets are unavailable"))))

package cn.xuyinyin.flinklab.server

import zio.*
import zio.test.*

object WebUiSpec extends ZIOSpecDefault:
  private def response(path: String) =
    WebUi.handle(ApiRequest("GET", path)).map(_.getOrElse(ApiResponse(404, "missing")))

  def spec = suite("web ui static assets")(
    test("serves the browser entrypoint with the platform markers") {
      response("/").map { result =>
        assertTrue(
          result.status == 200,
          result.contentType == "text/html",
          result.body.contains("ZIO Flink Platform"),
          result.body.contains("/v1/deployments"),
          result.body.contains("operationId")
        )
      }
    },
    test("serves the browser script and stylesheet") {
      for
        script <- response("/app.js")
        styles <- response("/styles.css")
      yield assertTrue(
        script.status == 200,
        script.contentType == "text/javascript",
        script.body.contains("requestId"),
        script.body.contains("encodeURIComponent"),
        script.body.contains("/v1/operations/"),
        script.body.contains("COMPLETED"),
        script.body.contains("FAILED"),
        script.body.contains("TIMEDOUT"),
        script.body.contains("UNCERTAIN"),
        styles.status == 200,
        styles.contentType == "text/css",
        styles.body.contains("--accent")
      )
    },
    test("unknown asset is not served") {
      for
        unknown <- WebUi.handle(ApiRequest("GET", "/unknown.js"))
        absolute <- WebUi.handle(ApiRequest("GET", "/etc/passwd"))
        traversal <- WebUi.handle(ApiRequest("GET", "/web/../app.js"))
      yield assertTrue(unknown.isEmpty, absolute.isEmpty, traversal.isEmpty)
    }
  )

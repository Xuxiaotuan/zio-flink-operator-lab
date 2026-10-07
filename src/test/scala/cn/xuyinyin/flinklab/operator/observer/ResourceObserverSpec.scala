package cn.xuyinyin.flinklab.operator.observer
/** 验证 list/watch、resourceVersion 和 410 relist 观察协议。 */

import cn.xuyinyin.flinklab.cli.ResourceKind
import cn.xuyinyin.flinklab.domain.FlinkTypes.Namespace
import cn.xuyinyin.flinklab.kubernetes.KubernetesApi
import cn.xuyinyin.flinklab.operator.watch.{WatchEvent, WatchEventType}
import zio.*
import zio.stream.*
import zio.test.*

object ResourceObserverSpec extends ZIOSpecDefault:
  private val namespace = Namespace.unsafe("analytics")

  def spec = suite("resource observer")(
    test("starts from list resourceVersion and emits a watch event") {
      val listJson = """{"items":[{"kind":"FlinkDeployment","metadata":{"name":"orders","uid":"uid-1","resourceVersion":"2","generation":3},"status":{"lifecycleState":"STABLE","jobStatus":{"state":"RUNNING"}}}],"metadata":{"resourceVersion":"2"}}"""
      val modified = WatchEvent.fromJson(ujson.read("""{"type":"MODIFIED","object":{"kind":"FlinkDeployment","metadata":{"name":"orders","uid":"uid-1","resourceVersion":"3","generation":3},"status":{"lifecycleState":"STABLE","jobStatus":{"state":"RUNNING"}}}}""")).toOption.get
      val api = new KubernetesApi:
        def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.succeed("")
        def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
        def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
        def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.succeed("")
        override def list(namespace: Namespace, kind: ResourceKind) = ZIO.succeed(listJson)
        override def watchFrom(namespace: Namespace, kind: ResourceKind, name: Option[String], resourceVersion: Option[String]) =
          if resourceVersion.contains("2") then ZStream.fromChunk(Chunk.single(modified)) else ZStream.fail(new IllegalArgumentException("watch must use list resourceVersion"))
      for
        values <- ResourceObserver.live(api).observe(namespace, ResourceKind.Deployment, Some("orders")).take(2).runCollect
      yield assertTrue(values.map(_.resourceVersion) == Chunk(Some("2"), Some("3")))
    },
    test("re-lists after Kubernetes resourceVersion expiry") {
      val initial = """{"items":[{"kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"2"},"status":{"lifecycleState":"STABLE","jobStatus":{"state":"RUNNING"}}}],"metadata":{"resourceVersion":"2"}}"""
      val relisted = """{"items":[{"kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"4"},"status":{"lifecycleState":"STABLE","jobStatus":{"state":"RUNNING"}}}],"metadata":{"resourceVersion":"4"}}"""
      val gone = WatchEvent(WatchEventType.Error, ujson.Obj("code" -> 410, "message" -> "too old resource version"))
      val api = new KubernetesApi:
        var lists = 0
        def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.succeed("")
        def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
        def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
        def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.succeed("")
        override def list(namespace: Namespace, kind: ResourceKind) =
          ZIO.succeed { lists += 1; if lists == 1 then initial else relisted }
        override def watchFrom(namespace: Namespace, kind: ResourceKind, name: Option[String], resourceVersion: Option[String]) =
          if resourceVersion.contains("2") then ZStream.fail(cn.xuyinyin.flinklab.kubernetes.KubernetesApiError(410, "too old"))
          else ZStream.empty
      for
        values <- ResourceObserver.live(api).observe(namespace, ResourceKind.Deployment, Some("orders")).take(2).runCollect
      yield assertTrue(values.map(_.resourceVersion) == Chunk(Some("2"), Some("4")))
    }
  )

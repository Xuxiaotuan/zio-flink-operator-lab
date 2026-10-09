package cn.xuyinyin.flinklab.server
/** 验证 dry-run、幂等、字段白名单和危险变更不会绕过 worker。 */

import cn.xuyinyin.flinklab.application.FlinkOperationFactory
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.kubernetes.KubernetesApi
import cn.xuyinyin.flinklab.operation.*
import cn.xuyinyin.flinklab.state.KubernetesStateStore
import zio.*
import zio.test.*

object MutationSafetySpec extends ZIOSpecDefault:
  private val settings = KubernetesHttpSettings("analytics")
  private val body = """{"kind":"FlinkDeployment","metadata":{"name":"orders"},"spec":{"image":"flink:1.20.1","job":{"jarURI":"local:///job.jar","upgradeMode":"savepoint","state":"suspended"},"flinkConfiguration":{"state.savepoints.dir":"s3://snapshots/orders","execution.checkpointing.interval":"10s"}}}"""

  private def check(request: ApiRequest) =
    for
      calls <- Ref.make(Vector.empty[String])
      api = new KubernetesApi:
        def apply(namespace: Namespace, resource: String, dryRun: Boolean) = calls.update(_ :+ s"apply:$dryRun:$resource").as(resource)
        def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.fail(new IllegalStateException("unexpected get"))
        def delete(namespace: Namespace, kind: ResourceKind, name: String) = calls.update(_ :+ "delete").as("{}")
        def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = calls.update(_ :+ "patch").as("{}")
      store <- InMemoryOperationStore.make
      worker = new AsyncOperationWorker:
        def submit(requestId: RequestId, operation: FlinkOperation) = calls.update(_ :+ "submit").as(AcceptedOperation(OperationId.generate(), requestId, java.time.Instant.now()))
        def process(id: OperationId) = ZIO.fail(ControlPlaneError.OperationNotFound(id))
      response <- KubernetesHttpApi.handleWith(api, KubernetesStateStore(api), store, worker, request, settings)
      recorded <- calls.get
      operations <- store.list
    yield (response, recorded, operations)

  def spec = suite("mutation safety")(
    test("production HTTP deployment dry-run never queues an operation") {
      check(ApiRequest("POST", "/v1/deployments", Map("dryRun" -> "true"), body)).map { (response, calls, operations) =>
        assertTrue(response.status == 200, !calls.contains("submit"), calls.size == 1, calls.headOption.exists(_.startsWith("apply:true:")), operations.isEmpty)
      }
    },
    test("production HTTP snapshot dry-run never queues an operation") {
      check(ApiRequest("POST", "/v1/snapshots", Map("dryRun" -> "true"), """{"targetKind":"deployment","targetName":"orders","snapshotName":"orders-sp","type":"savepoint"}""")).map { (response, calls, operations) =>
        assertTrue(response.status == 200, !calls.contains("submit"), calls.headOption.exists(_.startsWith("apply:true:")), operations.isEmpty)
      }
    },
    test("unsupported dry-run mutations fail before any side effect") {
      val requests = List(
        ApiRequest("DELETE", "/v1/deployments/orders"), ApiRequest("DELETE", "/v1/snapshots/orders-sp"),
        ApiRequest("POST", "/v1/deployments/orders/restart"), ApiRequest("POST", "/v1/deployments/orders/resume"),
        ApiRequest("POST", "/v1/deployments/orders/suspend-savepoint"), ApiRequest("POST", "/v1/deployments/orders/savepoint"),
        ApiRequest("POST", "/v1/deployments/orders/upgrade", body = body)
      ).map(_.copy(query = Map("dryRun" -> "true")))
      ZIO.foreach(requests)(check).map(results => assertTrue(results.forall { (response, calls, _) => response.status == 400 && calls.isEmpty }))
    },
    test("a misspelled dry-run value is rejected instead of executing") {
      check(ApiRequest("POST", "/v1/deployments", Map("dryRun" -> "treu"), body)).map { (response, calls, _) => assertTrue(response.status == 400, calls.isEmpty) }
    },
    test("HTTP parsing preserves job protection, state and checkpoint configuration") {
      val result = FlinkOperationFactory.fromDeploymentJson("analytics", body).map {
        case FlinkOperation.Deploy(spec) => spec.resource
        case _ => ujson.Null
      }
      assertTrue(result.exists(value => value("spec")("job")("upgradeMode").str == "savepoint" && value("spec")("job")("state").str == "suspended" && value("spec")("flinkConfiguration").obj.get("execution.checkpointing.interval").contains(ujson.Str("10s"))))
    },
    test("HTTP parsing preserves podTemplate secret injection") {
      val value = ujson.read(body)
      value("spec")("podTemplate") = ujson.Obj(
        "spec" -> ujson.Obj(
          "containers" -> ujson.Arr(
            ujson.Obj(
              "name" -> "flink-main-container",
              "env" -> ujson.Arr(
                ujson.Obj("name" -> "AWS_ACCESS_KEY_ID", "valueFrom" -> ujson.Obj("secretKeyRef" -> ujson.Obj("name" -> "rustfs-credentials", "key" -> "access-key")))
              )
            )
          )
        )
      )
      val result = FlinkOperationFactory.fromDeploymentJson("analytics", value.render()).map {
        case FlinkOperation.Deploy(spec) => spec.resource
        case _ => ujson.Null
      }
      assertTrue(result.exists(_.obj.get("spec").flatMap(_.objOpt).flatMap(_.get("podTemplate")).nonEmpty))
    },
    test("unknown deployment fields cannot be silently discarded") {
      val value = ujson.read(body)
      value("spec")("podTemplte") = ujson.Obj()
      assertTrue(FlinkOperationFactory.fromDeploymentJson("analytics", value.render()).isLeft)
    },
    test("a conflicting HTTP upgrade policy is rejected before enqueue") {
      check(ApiRequest("POST", "/v1/deployments/orders/upgrade", Map("upgradeMode" -> "stateless"), body)).map { (response, calls, _) => assertTrue(response.status == 400, calls.isEmpty) }
    }
  )

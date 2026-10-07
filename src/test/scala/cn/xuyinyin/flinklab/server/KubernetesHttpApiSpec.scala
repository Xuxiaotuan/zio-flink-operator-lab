package cn.xuyinyin.flinklab.server
/** 验证 HTTP 查询、状态、快照和 operation 生命周期接口。 */

import cn.xuyinyin.flinklab.kubernetes.FakeKubernetesApi
import cn.xuyinyin.flinklab.cli.ResourceKind
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.operation.{AcceptedOperation, AsyncOperationWorker}
import cn.xuyinyin.flinklab.operation.InMemoryOperationStore
import cn.xuyinyin.flinklab.state.KubernetesStateStore
import zio.*
import zio.test.*

object KubernetesHttpApiSpec extends ZIOSpecDefault:
  private val settings = KubernetesHttpSettings("analytics", Some("s3://flink-savepoints/zio-flink-operator/local/"))
  private val deployment =
    """{"apiVersion":"flink.apache.org/v1beta1","kind":"FlinkDeployment","metadata":{"name":"orders","namespace":"default"}}"""

  def spec = suite("kubernetes http api")(
    test("serves health and readiness without Kubernetes access") {
      for
        fake <- FakeKubernetesApi.make
        health <- KubernetesHttpApi.handleWith(fake, ApiRequest("GET", "/healthz"), settings)
        ready <- KubernetesHttpApi.handleWith(fake, ApiRequest("GET", "/readyz"), settings)
      yield assertTrue(health.status == 200, ready.status == 200, health.body == "{\"status\":\"ok\"}")
    },
    test("applies a deployment through the KubernetesApi port") {
      for
        fake <- FakeKubernetesApi.make
        response <- KubernetesHttpApi.handleWith(fake, ApiRequest("POST", "/v1/deployments", Map("dryRun" -> "true", "namespace" -> "analytics"), deployment), settings)
        records <- fake.records.get
      yield assertTrue(
        response.status == 200,
        records.exists(_.startsWith("apply:analytics:flinkdeployment:orders:")),
        records.exists(_.contains("state.savepoints.dir")),
        records.exists(_.contains("s3://flink-savepoints/zio-flink-operator/local/"))
      )
    },
    test("sends a string nonce without losing Long precision") {
      for
        fake <- FakeKubernetesApi.make
        response <- KubernetesHttpApi.handleWith(fake, ApiRequest("POST", "/v1/deployments/orders/savepoint", Map.empty, """{"nonce":"9007199254740993"}"""), settings)
        records <- fake.records.get
      yield assertTrue(
        response.status == 200,
        records.exists(_.contains("\"savepointTriggerNonce\":9007199254740993"))
      )
    },
    test("rejects an invalid savepoint nonce before calling Kubernetes") {
      for
        fake <- FakeKubernetesApi.make
        response <- KubernetesHttpApi.handleWith(fake, ApiRequest("POST", "/v1/deployments/orders/savepoint", Map.empty, """{"nonce":"not-a-long"}"""), settings)
        records <- fake.records.get
      yield assertTrue(response.status == 400, records.isEmpty)
    },
    test("returns normalized deployment checkpoint and savepoint status") {
      for
        fake <- FakeKubernetesApi.make
        response <- KubernetesHttpApi.handleWith(fake, ApiRequest("GET", "/v1/deployments/orders/status"), settings)
        json = ujson.read(response.body)
      yield assertTrue(
        response.status == 200,
        json("jobState").str == "RUNNING",
        json("checkpoint")("lastCheckpoint")("triggerNonce").str == "7",
        json("savepoint")("lastSavepoint")("location").str == "s3://flink-savepoints/orders"
      )
    },
    test("creates, lists and deletes a FlinkStateSnapshot") {
      for
        fake <- FakeKubernetesApi.make
        body = """{"targetKind":"FlinkDeployment","targetName":"orders","snapshotName":"orders-sp-1","type":"savepoint"}"""
        created <- KubernetesHttpApi.handleWith(fake, ApiRequest("POST", "/v1/snapshots", Map("namespace" -> "analytics", "dryRun" -> "true"), body), settings)
        listed <- KubernetesHttpApi.handleWith(fake, ApiRequest("GET", "/v1/snapshots", Map("namespace" -> "analytics")), settings)
        deleted <- KubernetesHttpApi.handleWith(fake, ApiRequest("DELETE", "/v1/snapshots/orders-sp-1", Map("namespace" -> "analytics")), settings)
        records <- fake.records.get
      yield assertTrue(
        created.status == 200,
        records.exists(_.contains("apply:analytics:flinkstatesnapshot:orders-sp-1")),
        listed.status == 200,
        ujson.read(listed.body)("resourceVersion").str == "2",
        ujson.read(listed.body)("items").arr.head("state").str == "COMPLETED",
        deleted.status == 200,
        records.exists(_.startsWith("delete:analytics:flinkstatesnapshot:orders-sp-1"))
      )
    },
    test("exposes the Kubernetes CR state backend to every replica") {
      for
        fake <- FakeKubernetesApi.make
        response <- KubernetesHttpApi.handleWith(fake, ApiRequest("GET", "/v1/state", Map("namespace" -> "analytics")), settings)
        json = ujson.read(response.body)
      yield assertTrue(
        response.status == 200,
        json("backend").str == "kubernetes",
        json("namespace").str == "analytics",
        json("items").arr.size == 3
      )
    },
    test("routes deployment submission through the typed operation worker") {
      for
        fake <- FakeKubernetesApi.make
        worker = new AsyncOperationWorker:
          def submit(requestId: RequestId, operation: FlinkOperation) = ZIO.succeed(AcceptedOperation(OperationId.from("op-1").toOption.get, requestId, java.time.Instant.parse("2026-10-03T00:00:00Z")))
          def process(id: OperationId) = ZIO.fail(ControlPlaneError.OperationNotFound(id))
        response <- KubernetesHttpApi.handleWith(fake, KubernetesStateStore(fake), worker, ApiRequest("POST", "/v1/deployments", Map("namespace" -> "analytics"), deployment), settings)
        json = ujson.read(response.body)
      yield assertTrue(response.status == 202, json("operationId").str == "op-1")
    },
    test("returns operation lifecycle from the operation store") {
      for
        fake <- FakeKubernetesApi.make
        store <- InMemoryOperationStore.make
        resource = ResourceRef(cn.xuyinyin.flinklab.domain.FlinkTypes.Namespace.unsafe("analytics"), ResourceKind.Deployment, cn.xuyinyin.flinklab.domain.FlinkTypes.DeploymentName.unsafe("orders"))
        operation = Operation.accepted(RequestId.from("req-1").toOption.get, FlinkOperation.Resume(resource), resource, java.time.Instant.parse("2026-10-03T00:00:00Z"))
        _ <- store.create(operation)
        worker = new AsyncOperationWorker:
          def submit(requestId: RequestId, operation: FlinkOperation) = ZIO.succeed(AcceptedOperation(OperationId.generate(), requestId, java.time.Instant.now()))
          def process(id: OperationId) = ZIO.fail(ControlPlaneError.OperationNotFound(id))
        response <- KubernetesHttpApi.handleWith(fake, KubernetesStateStore(fake), store, worker, ApiRequest("GET", s"/v1/operations/${operation.id.operationIdValue}"), settings)
        json = ujson.read(response.body)
      yield assertTrue(response.status == 200, json("state").str == "ACCEPTED", json("events").arr.nonEmpty)
    }
  )

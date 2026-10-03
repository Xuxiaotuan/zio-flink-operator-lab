package cn.xuyinyin.flinklab.server

import cn.xuyinyin.flinklab.kubernetes.FakeKubernetesApi
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
    }
  )

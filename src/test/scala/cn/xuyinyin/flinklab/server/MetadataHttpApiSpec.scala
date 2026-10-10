package cn.xuyinyin.flinklab.server

import cn.xuyinyin.flinklab.kubernetes.FakeKubernetesApi
import cn.xuyinyin.flinklab.metadata.*
import cn.xuyinyin.flinklab.state.KubernetesStateStore
import cn.xuyinyin.flinklab.operation.AsyncOperationWorker
import zio.*
import zio.test.*

object MetadataHttpApiSpec extends ZIOSpecDefault:
  private val settings = KubernetesHttpSettings("analytics", None)
  private val catalogBody = """{"id":"warehouse","type":"postgres","endpoint":"jdbc:postgresql://db/xxt","database":"xxt","credentialRef":"secret/pg"}"""
  private val snapshotBody = """{"catalogId":"warehouse","schema":"public","version":"v1","observedAt":"2026-10-10T00:00:00Z","tables":[{"catalogId":"warehouse","schema":"public","name":"orders","observedAt":"2026-10-10T00:00:00Z","columns":[{"name":"id","dataType":"BIGINT","nullable":false}]}]}"""
  def spec = suite("metadata http api")(
    test("registers catalog, imports snapshot, lists tables and never returns secrets") {
      for
        fake <- FakeKubernetesApi.make
        metadata <- InMemoryMetadataStore.make
        worker = new AsyncOperationWorker:
          def submit(requestId: cn.xuyinyin.flinklab.domain.RequestId, operation: cn.xuyinyin.flinklab.domain.FlinkOperation) = ZIO.fail(cn.xuyinyin.flinklab.domain.ControlPlaneError.OperationNotFound(cn.xuyinyin.flinklab.domain.OperationId.generate()))
          def process(id: cn.xuyinyin.flinklab.domain.OperationId) = ZIO.fail(cn.xuyinyin.flinklab.domain.ControlPlaneError.OperationNotFound(id))
        created <- KubernetesHttpApi.handleWith(fake, KubernetesStateStore(fake), metadata, worker, ApiRequest("POST", "/v1/catalogs", body = catalogBody), settings)
        imported <- KubernetesHttpApi.handleWith(fake, KubernetesStateStore(fake), metadata, worker, ApiRequest("POST", "/v1/catalogs/warehouse/snapshots", body = snapshotBody), settings)
        schemas <- KubernetesHttpApi.handleWith(fake, KubernetesStateStore(fake), metadata, worker, ApiRequest("GET", "/v1/catalogs/warehouse/schemas"), settings)
        tables <- KubernetesHttpApi.handleWith(fake, KubernetesStateStore(fake), metadata, worker, ApiRequest("GET", "/v1/catalogs/warehouse/tables", Map("schema" -> "public")), settings)
      yield assertTrue(created.status == 201, imported.status == 201, schemas.status == 200, schemas.body.contains("public"), tables.status == 200, tables.body.contains("orders"), !created.body.contains("asd123456"), !created.body.contains("password"))
    },
    test("persists static SQL lineage and exposes evidence source") {
      for
        fake <- FakeKubernetesApi.make
        metadata <- InMemoryMetadataStore.make
        worker = new AsyncOperationWorker:
          def submit(requestId: cn.xuyinyin.flinklab.domain.RequestId, operation: cn.xuyinyin.flinklab.domain.FlinkOperation) = ZIO.fail(cn.xuyinyin.flinklab.domain.ControlPlaneError.OperationNotFound(cn.xuyinyin.flinklab.domain.OperationId.generate()))
          def process(id: cn.xuyinyin.flinklab.domain.OperationId) = ZIO.fail(cn.xuyinyin.flinklab.domain.ControlPlaneError.OperationNotFound(id))
        response <- KubernetesHttpApi.handleWith(fake, KubernetesStateStore(fake), metadata, worker, ApiRequest("POST", "/v1/lineage/sql", body = """{"sql":"INSERT INTO analytics.summary SELECT * FROM raw.orders","jobId":"orders"}"""), settings)
        graph <- KubernetesHttpApi.handleWith(fake, KubernetesStateStore(fake), metadata, worker, ApiRequest("GET", "/v1/lineage/graph", Map("root" -> "analytics.summary")), settings)
      yield assertTrue(response.status == 201, response.body.contains("SQL_STATIC"), graph.status == 200, graph.body.contains("raw.orders"))
    },
    test("invalid catalog input returns 400") {
      for
        fake <- FakeKubernetesApi.make
        metadata <- InMemoryMetadataStore.make
        worker = new AsyncOperationWorker:
          def submit(requestId: cn.xuyinyin.flinklab.domain.RequestId, operation: cn.xuyinyin.flinklab.domain.FlinkOperation) = ZIO.fail(cn.xuyinyin.flinklab.domain.ControlPlaneError.OperationNotFound(cn.xuyinyin.flinklab.domain.OperationId.generate()))
          def process(id: cn.xuyinyin.flinklab.domain.OperationId) = ZIO.fail(cn.xuyinyin.flinklab.domain.ControlPlaneError.OperationNotFound(id))
        response <- KubernetesHttpApi.handleWith(fake, KubernetesStateStore(fake), metadata, worker, ApiRequest("POST", "/v1/catalogs", body = """{"id":"bad/name","type":"postgres","endpoint":"x","database":"x"}"""), settings)
      yield assertTrue(response.status == 400)
    }
  )

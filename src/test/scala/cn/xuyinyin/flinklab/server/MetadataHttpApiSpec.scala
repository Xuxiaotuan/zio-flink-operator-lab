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
        response <- KubernetesHttpApi.handleWith(fake, KubernetesStateStore(fake), metadata, worker, ApiRequest("POST", "/v1/lineage/sql", body = """{"sql":"INSERT INTO analytics.summary SELECT * FROM `raw`.orders","jobId":"orders"}"""), settings)
        graph <- KubernetesHttpApi.handleWith(fake, KubernetesStateStore(fake), metadata, worker, ApiRequest("GET", "/v1/lineage/graph", Map("root" -> "analytics.summary")), settings)
      yield assertTrue(response.status == 201, response.body.contains("SQL_STATIC"), graph.status == 200, graph.body.contains("raw.orders"))
    },
    test("storage failure and request conflict preserve their HTTP meaning") {
      for
        fake <- FakeKubernetesApi.make
        metadata <- InMemoryMetadataStore.make
        failure = cn.xuyinyin.flinklab.domain.ControlPlaneError.StoreFailure("database unavailable")
        defWorker = (error: cn.xuyinyin.flinklab.domain.ControlPlaneError) => new AsyncOperationWorker:
          def submit(requestId: cn.xuyinyin.flinklab.domain.RequestId, operation: cn.xuyinyin.flinklab.domain.FlinkOperation) = ZIO.fail(error)
          def process(id: cn.xuyinyin.flinklab.domain.OperationId) = ZIO.fail(error)
        unavailable <- KubernetesHttpApi.handleWith(fake, KubernetesStateStore(fake), metadata, defWorker(failure), ApiRequest("POST", "/v1/deployments/orders/resume"), settings)
        resource = cn.xuyinyin.flinklab.domain.ResourceRef(cn.xuyinyin.flinklab.domain.FlinkTypes.Namespace.unsafe("analytics"), cn.xuyinyin.flinklab.domain.ResourceKind.Deployment, cn.xuyinyin.flinklab.domain.FlinkTypes.DeploymentName.unsafe("orders"))
        busy <- KubernetesHttpApi.handleWith(fake, KubernetesStateStore(fake), metadata, defWorker(cn.xuyinyin.flinklab.domain.ControlPlaneError.ResourceBusy(resource)), ApiRequest("POST", "/v1/deployments/orders/resume"), settings)
      yield assertTrue(unavailable.status == 503, busy.status == 409)
    },
    test("readiness fails when worker supervision reports an unavailable consumer") {
      for
        fake <- FakeKubernetesApi.make
        metadata <- InMemoryMetadataStore.make
        worker = new AsyncOperationWorker:
          def submit(requestId: cn.xuyinyin.flinklab.domain.RequestId, operation: cn.xuyinyin.flinklab.domain.FlinkOperation) = ZIO.fail(cn.xuyinyin.flinklab.domain.ControlPlaneError.StoreFailure("unavailable"))
          def process(id: cn.xuyinyin.flinklab.domain.OperationId) = ZIO.fail(cn.xuyinyin.flinklab.domain.ControlPlaneError.OperationNotFound(id))
          override def readiness = ZIO.succeed(false)
        ready <- KubernetesHttpApi.handleWith(fake, KubernetesStateStore(fake), metadata, worker, ApiRequest("GET", "/readyz"), settings)
        health <- KubernetesHttpApi.handleWith(fake, KubernetesStateStore(fake), metadata, worker, ApiRequest("GET", "/healthz"), settings)
      yield assertTrue(ready.status == 503, health.status == 200)
    },
    test("readiness deadline responds before an uninterruptible dependency finishes") {
      for
        fake <- FakeKubernetesApi.make
        delegate <- InMemoryMetadataStore.make
        started <- Promise.make[Nothing, Unit]
        release <- Promise.make[Nothing, Unit]
        metadata = new MetadataStore:
          override def check = (started.succeed(()) *> release.await).uninterruptible
          def listCatalogs = delegate.listCatalogs
          def createCatalog(c: CatalogRegistration) = delegate.createCatalog(c)
          def listTables(id: String, schema: Option[String]) = delegate.listTables(id, schema)
          def getTable(id: String, schema: String, name: String) = delegate.getTable(id, schema, name)
          def listSnapshots(id: String) = delegate.listSnapshots(id)
          def saveSnapshot(s: SchemaSnapshot) = delegate.saveSnapshot(s)
          def saveLineage(e: LineageEdge) = delegate.saveLineage(e)
          def graph(root: String) = delegate.graph(root)
        worker = new AsyncOperationWorker:
          def submit(id: cn.xuyinyin.flinklab.domain.RequestId, o: cn.xuyinyin.flinklab.domain.FlinkOperation) = ZIO.fail(cn.xuyinyin.flinklab.domain.ControlPlaneError.StoreFailure("unused"))
          def process(id: cn.xuyinyin.flinklab.domain.OperationId) = ZIO.fail(cn.xuyinyin.flinklab.domain.ControlPlaneError.OperationNotFound(id))
        pending <- KubernetesHttpApi.handleWith(fake, KubernetesStateStore(fake), metadata, worker, ApiRequest("GET", "/readyz"), settings).fork
        _ <- started.await
        _ <- TestClock.adjust(3.seconds)
        atDeadline <- pending.poll
        _ <- release.succeed(())
        response <- pending.join
      yield assertTrue(atDeadline.exists { case Exit.Success(value) => value.status == 503; case _ => false }, response.status == 503)
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

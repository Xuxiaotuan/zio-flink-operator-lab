package cn.xuyinyin.flinklab.operation

import cn.xuyinyin.flinklab.application.DefaultVerificationEngine
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.kubernetes.{KubernetesApi, KubernetesApiError}
import cn.xuyinyin.flinklab.operator.observer.{ResourceObservation, ResourceObserver}
import cn.xuyinyin.flinklab.operator.watch.WatchEventType
import zio.*
import zio.stream.*
import zio.test.*
import java.time.Instant

object AsyncOperationWorkerReliabilitySpec extends ZIOSpecDefault:
  private val ns = Namespace.unsafe("analytics")
  private val name = DeploymentName.unsafe("orders")
  private val uid = ResourceUid.from("original").toOption.get
  private val target = ResourceRef(ns, ResourceKind.Deployment, name)
  private val deploy = FlinkOperation.Deploy(FlinkDeploymentSpec(ns, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.Job", 1)))
  private val ready = """{"kind":"FlinkDeployment","metadata":{"name":"orders","uid":"original","resourceVersion":"8","generation":3},"status":{"jobStatus":{"state":"RUNNING"},"reconciliationStatus":{"state":"DEPLOYED"},"observedGeneration":3}}"""
  private val receipt = """{"metadata":{"uid":"original","resourceVersion":"8","generation":3}}"""
  private val absent = ZIO.fail(KubernetesApiError(404, "not found"))
  private def api(read: IO[Throwable, String], write: String = receipt, patchRecord: String => UIO[Unit] = _ => ZIO.unit, deleteRecord: Option[ResourceUid] => UIO[Unit] = _ => ZIO.unit): KubernetesApi = new KubernetesApi:
    def get(namespace: Namespace, kind: ResourceKind, name: String) = read
    override def create(namespace: Namespace, resource: String) = apply(namespace, resource, dryRun = false)
    def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.succeed(write)
    def patch(namespace: Namespace, kind: ResourceKind, name: String, value: String) = patchRecord(value).as(write)
    def delete(namespace: Namespace, kind: ResourceKind, name: String) = deleteRecord(None).as("""{"kind":"Status","status":"Success"}""")
    override def delete(namespace: Namespace, kind: ResourceKind, name: String, uid: Option[ResourceUid]) = deleteRecord(uid).as("""{"kind":"Status","status":"Success"}""")
  private def observer(raw: String = ready): ResourceObserver = new ResourceObserver:
    def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]) =
      val value = ujson.read(raw)
      ZStream.succeed(ResourceObservation(WatchEventType.Added, ResourceKind.Deployment, "orders", Some("8"), Some(value("metadata")("uid").str), Some(3), Some(3), raw))
  private val silent = new ResourceObserver:
    def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]) = ZStream.never
  private def accept(store: OperationStore, command: FlinkOperation, request: String) =
    new DefaultFlinkControlPlane(store).accept(RequestId.from(request).toOption.get, command)
  private def submitted(store: OperationStore, id: OperationId, submittedUid: Option[ResourceUid] = Some(uid), generation: Long = 3L) =
    store.transition(id, OperationEvent.ValidationStarted(Instant.now())) *>
      store.transition(id, OperationEvent.ValidationPassed(Instant.now())) *>
      store.transition(id, OperationEvent.Submitted(Instant.now(), Generation.from(generation).toOption, ResourceVersion.from("8").toOption, submittedUid))

  def spec = suite("worker reliability regressions")(
    test("absent mutation target fails promptly even when watch never emits") {
      for
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        accepted <- accept(store, FlinkOperation.Resume(target), "missing-target")
        worker = new DefaultAsyncOperationWorker(api(absent), silent, DefaultVerificationEngine, store, mutex)
        result <- Live.live(worker.process(accepted.operationId).either.timeout(300.millis))
        stored <- store.get(accepted.operationId)
      yield assertTrue(result.exists(_.isLeft), stored.exists(_.state == OperationState.Failed))
    },
    test("mutation patches carry the GET resource UID and resourceVersion") {
      for
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        patches <- Ref.make(Vector.empty[String])
        accepted <- accept(store, FlinkOperation.Resume(target), "bound-patch")
        worker = new DefaultAsyncOperationWorker(api(ZIO.succeed(ready), patchRecord = value => patches.update(_ :+ value)), observer(), DefaultVerificationEngine, store, mutex)
        completed <- worker.process(accepted.operationId)
        values <- patches.get
      yield assertTrue(completed.state == OperationState.Completed, values.size == 1, values.headOption.exists { raw =>
        val value = ujson.read(raw)
        value.obj.get("metadata").exists(m => m.obj.get("uid").contains(ujson.Str("original")) && m.obj.get("resourceVersion").contains(ujson.Str("8")))
      })
    },
    test("submitted deployment cannot complete from replacement UID") {
      for
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        accepted <- accept(store, deploy, "replacement-deploy")
        worker = new DefaultAsyncOperationWorker(api(absent), observer(ready.replace("original", "replacement")), DefaultVerificationEngine, store, mutex)
        result <- worker.process(accepted.operationId).either
        stored <- store.get(accepted.operationId)
      yield assertTrue(result.isLeft, stored.exists(_.state != OperationState.Completed))
    },
    test("recovery binds verification to persisted Submitted UID") {
      for
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        accepted <- accept(store, deploy, "recovery-replacement")
        _ <- submitted(store, accepted.operationId)
        worker = new DefaultAsyncOperationWorker(api(absent), observer(ready.replace("original", "replacement")), DefaultVerificationEngine, store, mutex)
        result <- worker.process(accepted.operationId).either
      yield assertTrue(result.isLeft)
    },
    test("a successful mutation response without identity is uncertain") {
      for
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        accepted <- accept(store, deploy, "missing-receipt-identity")
        worker = new DefaultAsyncOperationWorker(api(absent, "{}"), observer(), DefaultVerificationEngine, store, mutex)
        _ <- worker.process(accepted.operationId).either
        stored <- store.get(accepted.operationId)
      yield assertTrue(stored.exists(_.state == OperationState.Uncertain))
    },
    test("delete completed before watch opens is proved by GET absence") {
      for
        reads <- Ref.make(0)
        deletes <- Ref.make(Vector.empty[Option[ResourceUid]])
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        accepted <- accept(store, FlinkOperation.Delete(target, DeletePolicy.Force), "delete-race")
        read = reads.getAndUpdate(_ + 1).flatMap(n => if n == 0 then ZIO.succeed(ready) else absent)
        worker = new DefaultAsyncOperationWorker(api(read, deleteRecord = value => deletes.update(_ :+ value)), silent, DefaultVerificationEngine, store, mutex)
        result <- Live.live(worker.process(accepted.operationId).either.timeout(300.millis))
        stored <- store.get(accepted.operationId)
        deleteUids <- deletes.get
      yield assertTrue(deleteUids == Vector(Some(uid)), result.exists(_.exists(_.state == OperationState.Completed)), stored.exists(_.events.exists {
        case OperationEvent.Submitted(_, _, _, value) => value.contains(uid)
        case _ => false
      }))
    },
    test("recovered delete uses GET absence without repeating deletion") {
      for
        deletes <- Ref.make(0)
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        accepted <- accept(store, FlinkOperation.Delete(target, DeletePolicy.Force), "recover-delete")
        _ <- submitted(store, accepted.operationId)
        worker = new DefaultAsyncOperationWorker(api(absent, deleteRecord = _ => deletes.update(_ + 1)), silent, DefaultVerificationEngine, store, mutex)
        result <- Live.live(worker.process(accepted.operationId).either.timeout(300.millis))
        writes <- deletes.get
      yield assertTrue(writes == 0, result.exists(_.exists(_.state == OperationState.Completed)))
    },
    test("snapshot recovery uses its UID and completion path without deployment observedGeneration") {
      for
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        snapshotName = DeploymentName.unsafe("orders-recovered-savepoint")
        command = FlinkOperation.Snapshot(target, SnapshotPolicy(SnapshotType.Savepoint, Some(snapshotName)))
        accepted <- accept(store, command, "recover-snapshot")
        _ <- submitted(store, accepted.operationId, generation = 1L)
        snapshots = new ResourceObserver:
          def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]) =
            ZStream.succeed(ResourceObservation(WatchEventType.Added, ResourceKind.StateSnapshot, snapshotName.nameValue, Some("8"), Some("original"), Some(1), None,
              """{"kind":"FlinkStateSnapshot","metadata":{"name":"orders-recovered-savepoint","uid":"original","generation":1,"resourceVersion":"8"},"status":{"state":"COMPLETED","path":"s3://savepoints/recovered"}}"""))
        worker = new DefaultAsyncOperationWorker(api(absent), snapshots, DefaultVerificationEngine, store, mutex)
        result <- worker.process(accepted.operationId).either
      yield assertTrue(result.exists(_.state == OperationState.Completed))
    },
    test("an identity-free recovered delete completes only on authoritative absence") {
      for
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        accepted <- accept(store, FlinkOperation.Delete(target, DeletePolicy.Force), "recover-absent-unbound-delete")
        _ <- submitted(store, accepted.operationId, None)
        worker = new DefaultAsyncOperationWorker(api(absent), silent, DefaultVerificationEngine, store, mutex)
        result <- worker.process(accepted.operationId).either
      yield assertTrue(result.exists(_.state == OperationState.Completed))
    },
    test("a mutation receipt cannot replace the pre-read target UID") {
      for
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        accepted <- accept(store, FlinkOperation.Resume(target), "changed-receipt-identity")
        worker = new DefaultAsyncOperationWorker(api(ZIO.succeed(ready), receipt.replace("original", "replacement")), observer(ready.replace("original", "replacement")), DefaultVerificationEngine, store, mutex)
        _ <- worker.process(accepted.operationId).either
        stored <- store.get(accepted.operationId)
      yield assertTrue(stored.exists(_.state == OperationState.Uncertain))
    },
    test("non-object mutation receipt is uncertain rather than killing the worker") {
      for
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        accepted <- accept(store, deploy, "scalar-receipt")
        worker = new DefaultAsyncOperationWorker(api(absent, "[]"), observer(), DefaultVerificationEngine, store, mutex)
        _ <- worker.process(accepted.operationId).exit
        stored <- store.get(accepted.operationId)
      yield assertTrue(stored.exists(_.state == OperationState.Uncertain))
    },
    test("an absent deployment uses create so a concurrent object cannot be overwritten") {
      for
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        calls <- Ref.make(Vector.empty[String])
        createApi = new KubernetesApi:
          def get(namespace: Namespace, kind: ResourceKind, name: String) = absent
          override def create(namespace: Namespace, resource: String) = calls.update(_ :+ "create").as(receipt)
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = calls.update(_ :+ "apply").as(receipt)
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.succeed(receipt)
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("{}")
        accepted <- accept(store, deploy, "absent-deploy-create")
        worker = new DefaultAsyncOperationWorker(createApi, observer(), DefaultVerificationEngine, store, mutex)
        completed <- worker.process(accepted.operationId)
        actual <- calls.get
      yield assertTrue(completed.state == OperationState.Completed, actual == Vector("create"))
    },
    test("the live consumer pool progresses a second resource while the first watch blocks") {
      ZIO.scoped {
        for
          store <- InMemoryOperationStore.make
          watching <- Promise.make[Nothing, Unit]
          liveApi = new KubernetesApi:
            def get(namespace: Namespace, kind: ResourceKind, name: String) = absent
            override def create(namespace: Namespace, resource: String) = apply(namespace, resource, dryRun = false)
            def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.succeed(receipt)
            def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.succeed(receipt)
            def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("{}")
            override def list(namespace: Namespace, kind: ResourceKind) = ZIO.succeed("""{"metadata":{"resourceVersion":"8"},"items":[]}""")
            override def watchFrom(namespace: Namespace, kind: ResourceKind, name: Option[String], resourceVersion: Option[String]) =
              ZStream.fromZIO(watching.succeed(())).drain ++ ZStream.never
          worker <- AsyncOperationWorker.live.build.provideSomeLayer[Scope](
            ZLayer.succeed(liveApi: KubernetesApi) ++ ZLayer.succeed(store: OperationStore) ++
              ZLayer.succeed(new cn.xuyinyin.flinklab.application.DefaultPolicyEngine: cn.xuyinyin.flinklab.application.PolicyEngine) ++
              ZLayer.succeed(InMemoryResourceCoordinator: ResourceCoordinator)
          ).map(_.get[AsyncOperationWorker])
          _ <- worker.submit(RequestId.from("pool-first").toOption.get, deploy)
          _ <- watching.await
          second <- worker.submit(RequestId.from("pool-second").toOption.get, FlinkOperation.Resume(target.copy(name = DeploymentName.unsafe("other"))))
          finished = store.get(second.operationId).repeatUntil(_.exists(_.state == OperationState.Failed))
          result <- Live.live(finished.timeout(300.millis))
        yield assertTrue(result.flatten.exists(_.state == OperationState.Failed))
      }
    },
    test("recovered resume never completes from a finished job or resubmits the mutation") {
      for
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        writes <- Ref.make(0)
        observed <- Promise.make[Nothing, Unit]
        accepted <- accept(store, FlinkOperation.Resume(target), "resume-finished-recovery")
        _ <- submitted(store, accepted.operationId)
        finished = ready.replace("RUNNING", "FINISHED")
        finishedObserver = new ResourceObserver:
          def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]) =
            observer(finished).observe(namespace, kind, name) ++ ZStream.fromZIO(observed.succeed(())).drain ++ ZStream.never
        worker = new DefaultAsyncOperationWorker(api(ZIO.succeed(finished), patchRecord = _ => writes.update(_ + 1)), finishedObserver, DefaultVerificationEngine, store, mutex)
        fiber <- worker.process(accepted.operationId).either.fork
        _ <- observed.await
        _ <- TestClock.adjust(181.seconds)
        result <- fiber.join
        stored <- store.get(accepted.operationId)
        count <- writes.get
      yield assertTrue(result.isLeft, stored.exists(_.state == OperationState.TimedOut), count == 0)
    },
    test("GET requests that never return terminalize within the verification deadline") {
      for
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        entered <- Promise.make[Nothing, Unit]
        accepted <- accept(store, FlinkOperation.Resume(target), "get-timeout")
        worker = new DefaultAsyncOperationWorker(api(entered.succeed(()) *> ZIO.never), silent, DefaultVerificationEngine, store, mutex)
        fiber <- worker.process(accepted.operationId).either.fork
        _ <- entered.await
        _ <- TestClock.adjust(181.seconds)
        result <- fiber.join
        stored <- store.get(accepted.operationId)
      yield assertTrue(result.isLeft, stored.exists(_.state == OperationState.TimedOut))
    },
    test("unrelated resource progresses while another operation waits for verification") {
      for
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        started <- Promise.make[Nothing, Unit]
        blockedObserver = new ResourceObserver:
          def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]) = ZStream.fromZIO(started.succeed(())).drain ++ ZStream.never
        first <- accept(store, deploy, "first-waiting")
        other = FlinkOperation.Resume(target.copy(name = DeploymentName.unsafe("other")))
        second <- accept(store, other, "second-independent")
        worker = new DefaultAsyncOperationWorker(api(absent), blockedObserver, DefaultVerificationEngine, store, mutex)
        fiber <- worker.process(first.operationId).fork
        _ <- started.await
        result <- Live.live(worker.process(second.operationId).either.timeout(300.millis))
        _ <- fiber.interrupt
      yield assertTrue(result.exists(_.isLeft))
    }
  ) @@ TestAspect.sequential

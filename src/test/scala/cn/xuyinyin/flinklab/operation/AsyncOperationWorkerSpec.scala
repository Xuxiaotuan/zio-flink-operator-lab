package cn.xuyinyin.flinklab.operation
/** 验证异步 worker 的提交、观察、验证、恢复和 FallbackDetected 生命周期。 */

import cn.xuyinyin.flinklab.application.DefaultVerificationEngine
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.kubernetes.KubernetesApi
import cn.xuyinyin.flinklab.operator.observer.{ResourceObservation, ResourceObserver}
import cn.xuyinyin.flinklab.operator.watch.WatchEventType
import zio.*
import zio.stream.*
import zio.test.*
import java.time.Instant

object AsyncOperationWorkerSpec extends ZIOSpecDefault:
  private val namespace = Namespace.unsafe("analytics")
  private val name = DeploymentName.unsafe("orders")
  private val operation = FlinkOperation.Deploy(FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.WordCount", 1)))

  def spec = suite("async operation worker")(
    test("a replica that cannot acquire the lease leaves the operation untouched") {
      for
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        accepted <- new DefaultFlinkControlPlane(store).accept(RequestId.from("req-busy-replica").toOption.get, operation)
        coordinator = new ResourceCoordinator:
          def withLock[A](resource: ResourceRef, operationId: OperationId)(effect: IO[ControlPlaneError, A]) = ZIO.fail(ControlPlaneError.ResourceBusy(resource))
        api = new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.fail(IllegalStateException("must not submit"))
          def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.succeed("")
        observer = new ResourceObserver:
          def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]) = ZStream.empty
        worker = new DefaultAsyncOperationWorker(api, observer, DefaultVerificationEngine, store, mutex, coordinator = coordinator)
        result <- worker.process(accepted.operationId).either
        stored <- store.get(accepted.operationId)
      yield assertTrue(result.isLeft, stored.exists(_.state == OperationState.Accepted), stored.exists(_.events.size == 1))
    },
    test("recovery after the durable submission boundary never repeats a write") {
      for
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        accepted <- new DefaultFlinkControlPlane(store).accept(RequestId.from("req-write-gap").toOption.get, operation)
        _ <- store.transition(accepted.operationId, OperationEvent.ValidationStarted(Instant.now()))
        _ <- store.transition(accepted.operationId, OperationEvent.ValidationPassed(Instant.now()))
        calls <- Ref.make(0)
        api = new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = calls.update(_ + 1).as("{}")
          def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.succeed("")
        observer = new ResourceObserver:
          def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]) = ZStream.empty
        worker = new DefaultAsyncOperationWorker(api, observer, DefaultVerificationEngine, store, mutex)
        _ <- worker.process(accepted.operationId).either
        stored <- store.get(accepted.operationId)
        writes <- calls.get
      yield assertTrue(writes == 0, stored.exists(_.state == OperationState.Uncertain))
    },
    test("submits, observes and completes a deployment operation") {
      for
        api <- ZIO.succeed(new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.succeed("""{"metadata":{"resourceVersion":"8","generation":3}}""")
          def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.succeed("")
        )
        observer = new ResourceObserver:
          def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]) =
            ZStream.succeed(ResourceObservation(WatchEventType.Added, ResourceKind.Deployment, "orders", Some("8"), Some("uid"), Some(3), Some(3), """{"kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"8","uid":"uid","generation":3},"status":{"lifecycleState":"STABLE","jobManagerDeploymentStatus":"READY","jobStatus":{"state":"RUNNING"},"reconciliationStatus":{"state":"DEPLOYED"},"observedGeneration":3}}"""))
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        worker = new DefaultAsyncOperationWorker(api, observer, DefaultVerificationEngine, store, mutex, new cn.xuyinyin.flinklab.application.DefaultPolicyEngine)
        accepted <- worker.submit(RequestId.from("req-1").toOption.get, operation)
        completed <- worker.process(accepted.operationId)
      yield assertTrue(completed.state == OperationState.Completed, completed.events.exists(_.isInstanceOf[OperationEvent.VerificationSucceeded]))
    },
    test("creates and verifies a typed FlinkStateSnapshot") {
      for
        records <- Ref.make(Vector.empty[String])
        api <- ZIO.succeed(new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = records.update(_ :+ resource).as("""{"metadata":{"resourceVersion":"9","generation":1}}""")
          def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.succeed("")
        )
        snapshotName = DeploymentName.unsafe("orders-savepoint")
        observer = new ResourceObserver:
          def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]) =
            ZStream.succeed(ResourceObservation(WatchEventType.Added, ResourceKind.StateSnapshot, snapshotName.nameValue, Some("9"), Some("snapshot-uid"), None, None, """{"kind":"FlinkStateSnapshot","metadata":{"name":"orders-savepoint","resourceVersion":"9","uid":"snapshot-uid"},"status":{"state":"COMPLETED","path":"s3://flink-savepoints/orders-savepoint"}}"""))
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        worker = new DefaultAsyncOperationWorker(api, observer, DefaultVerificationEngine, store, mutex, new cn.xuyinyin.flinklab.application.DefaultPolicyEngine)
        operation = FlinkOperation.Snapshot(ResourceRef(namespace, ResourceKind.Deployment, name), SnapshotPolicy(SnapshotType.Savepoint, Some(snapshotName)))
        accepted <- worker.submit(RequestId.from("req-snapshot").toOption.get, operation)
        completed <- worker.process(accepted.operationId)
        applied <- records.get
      yield assertTrue(completed.state == OperationState.Completed, applied.exists(_.contains("FlinkStateSnapshot")), applied.exists(_.contains("orders-savepoint")))
    },
    test("preserves an operator rejection instead of masking it as a generation mismatch") {
      for
        api <- ZIO.succeed(new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.succeed("""{"metadata":{"generation":1,"resourceVersion":"2"}}""")
          def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.succeed("")
        )
        observer = new ResourceObserver:
          def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]) =
            ZStream.succeed(ResourceObservation(WatchEventType.Modified, ResourceKind.Deployment, "orders", Some("3"), Some("uid"), Some(1), None, """{"kind":"FlinkDeployment","metadata":{"name":"orders","generation":1},"status":{"error":"spec.serviceAccount must be defined","reconciliationStatus":{"state":"UPGRADING"}}}"""))
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        worker = new DefaultAsyncOperationWorker(api, observer, DefaultVerificationEngine, store, mutex)
        accepted <- new DefaultFlinkControlPlane(store).accept(RequestId.from("req-operator-rejection").toOption.get, operation)
        _ <- worker.process(accepted.operationId).either
        stored <- store.get(accepted.operationId)
      yield assertTrue(stored.exists(_.state == OperationState.Failed), stored.exists(_.events.exists {
        case OperationEvent.Failed(_, reason) => reason.contains("serviceAccount")
        case _ => false
      }))
    },
    test("terminalizes an abandoned snapshot instead of waiting for a timeout") {
      for
        api <- ZIO.succeed(new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.succeed("""{"metadata":{"resourceVersion":"10"}}""")
          def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.succeed("")
        )
        snapshotName = DeploymentName.unsafe("orders-abandoned")
        observer = new ResourceObserver:
          def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]) =
            ZStream.succeed(ResourceObservation(WatchEventType.Modified, ResourceKind.StateSnapshot, snapshotName.nameValue, Some("10"), Some("snapshot-uid"), None, None, """{"kind":"FlinkStateSnapshot","metadata":{"name":"orders-abandoned","resourceVersion":"10","uid":"snapshot-uid"},"status":{"state":"ABANDONED","error":"job is not running"}}"""))
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        worker = new DefaultAsyncOperationWorker(api, observer, DefaultVerificationEngine, store, mutex, new cn.xuyinyin.flinklab.application.DefaultPolicyEngine)
        operation = FlinkOperation.Snapshot(ResourceRef(namespace, ResourceKind.Deployment, name), SnapshotPolicy(SnapshotType.Savepoint, Some(snapshotName)))
        accepted <- worker.submit(RequestId.from("req-abandoned").toOption.get, operation)
        _ <- worker.process(accepted.operationId).either
        stored <- store.get(accepted.operationId)
      yield assertTrue(stored.exists(_.state == OperationState.Failed), stored.exists(_.events.exists(_.toString.contains("job is not running"))))
    },
    test("applies the validated upgrade protection and fallback policy") {
      for
        records <- Ref.make(Vector.empty[String])
        api <- ZIO.succeed(new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = records.update(_ :+ resource).as("""{"metadata":{"resourceVersion":"11","generation":2}}""")
          def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.succeed("")
        )
        observer = new ResourceObserver:
          def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]) =
            ZStream.succeed(ResourceObservation(WatchEventType.Modified, ResourceKind.Deployment, "orders", Some("11"), Some("uid"), Some(2), Some(2), """{"kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"11","uid":"uid","generation":2},"spec":{"flinkConfiguration":{"state.savepoints.dir":"s3://savepoints/orders"}},"status":{"lifecycleState":"STABLE","jobManagerDeploymentStatus":"READY","jobStatus":{"state":"RUNNING","savepointInfo":{"lastSavepoint":{"location":"s3://savepoints/orders/savepoint-1"}}},"reconciliationStatus":{"state":"DEPLOYED","lastReconciledSpec":"{\"spec\":{\"job\":{\"upgradeMode\":\"savepoint\"}}}"},"observedGeneration":2}}"""))
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        worker = new DefaultAsyncOperationWorker(api, observer, DefaultVerificationEngine, store, mutex, new cn.xuyinyin.flinklab.application.DefaultPolicyEngine)
        spec = FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.WordCount", 1), savepointDirectory = Some("s3://savepoints/orders"))
        operation = FlinkOperation.Upgrade(ResourceRef(namespace, ResourceKind.Deployment, name), spec, UpgradePolicy(StateProtection.Savepoint, FallbackPolicy.Forbidden))
        accepted <- worker.submit(RequestId.from("req-upgrade").toOption.get, operation)
        completed <- worker.process(accepted.operationId)
        applied <- records.get
      yield assertTrue(completed.state == OperationState.Completed, applied.exists(raw => raw.contains("\"upgradeMode\":\"savepoint\"") && raw.contains("kubernetes.operator.job.upgrade.last-state-fallback.enabled")))
    },
    test("records fallback detected when the operator explicitly reconciles last-state") {
      for
        api <- ZIO.succeed(new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.succeed("""{"metadata":{"resourceVersion":"11","generation":2}}""")
          def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.succeed("")
        )
        observer = new ResourceObserver:
          def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]) =
            ZStream.succeed(ResourceObservation(WatchEventType.Modified, ResourceKind.Deployment, "orders", Some("11"), Some("uid"), Some(2), Some(2), """{"kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"11","uid":"uid","generation":2},"spec":{"flinkConfiguration":{"state.savepoints.dir":"s3://savepoints/orders"}},"status":{"jobStatus":{"state":"RUNNING"},"reconciliationStatus":{"state":"DEPLOYED","lastReconciledSpec":"{\"spec\":{\"job\":{\"upgradeMode\":\"last-state\"}}}"},"observedGeneration":2}}"""))
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        worker = new DefaultAsyncOperationWorker(api, observer, DefaultVerificationEngine, store, mutex, new cn.xuyinyin.flinklab.application.DefaultPolicyEngine)
        spec = FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.WordCount", 1, StateProtection.Savepoint), savepointDirectory = Some("s3://savepoints/orders"))
        operation = FlinkOperation.Upgrade(ResourceRef(namespace, ResourceKind.Deployment, name), spec, UpgradePolicy(StateProtection.Savepoint, FallbackPolicy.AllowLastState))
        accepted <- worker.submit(RequestId.from("req-fallback").toOption.get, operation)
        completed <- worker.process(accepted.operationId)
      yield assertTrue(completed.state == OperationState.Completed, completed.events.exists(_.isInstanceOf[OperationEvent.FallbackDetected]))
    },
    test("resumes a verifying operation from its persisted event chain") {
      for
        api <- ZIO.succeed(new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.succeed("""{"metadata":{"resourceVersion":"8","generation":3}}""")
          def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.succeed("")
        )
        observer = new ResourceObserver:
          def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]) =
            ZStream.succeed(ResourceObservation(WatchEventType.Modified, ResourceKind.Deployment, "orders", Some("8"), Some("uid"), Some(3), Some(3), """{"kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"8","uid":"uid","generation":3},"status":{"lifecycleState":"STABLE","jobManagerDeploymentStatus":"READY","jobStatus":{"state":"RUNNING"},"reconciliationStatus":{"state":"DEPLOYED"},"observedGeneration":3}}"""))
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        worker = new DefaultAsyncOperationWorker(api, observer, DefaultVerificationEngine, store, mutex, new cn.xuyinyin.flinklab.application.DefaultPolicyEngine)
        accepted <- new DefaultFlinkControlPlane(store).accept(RequestId.from("req-recovery").toOption.get, operation)
        _ <- store.transition(accepted.operationId, OperationEvent.ValidationStarted(Instant.now()))
        _ <- store.transition(accepted.operationId, OperationEvent.ValidationPassed(Instant.now()))
        _ <- store.transition(accepted.operationId, OperationEvent.Submitted(Instant.now(), Generation.from(3).toOption, ResourceVersion.from("8").toOption))
        _ <- store.transition(accepted.operationId, OperationEvent.WaitingForObservation(Instant.now()))
        _ <- store.transition(accepted.operationId, OperationEvent.VerificationStarted(Instant.now()))
        completed <- worker.process(accepted.operationId)
      yield assertTrue(completed.state == OperationState.Completed, completed.events.exists(_.isInstanceOf[OperationEvent.VerificationSucceeded]))
    },
    test("resumes a submitted operation through waiting before verification") {
      for
        api <- ZIO.succeed(new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.fail(UnsupportedOperationException("must not resubmit"))
          def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.succeed("")
        )
        observer = new ResourceObserver:
          def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]) =
            ZStream.succeed(ResourceObservation(WatchEventType.Modified, ResourceKind.Deployment, "orders", Some("8"), Some("uid"), Some(3), Some(3), """{"kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"8","uid":"uid","generation":3},"status":{"lifecycleState":"STABLE","jobManagerDeploymentStatus":"READY","jobStatus":{"state":"RUNNING"},"reconciliationStatus":{"state":"DEPLOYED"},"observedGeneration":3}}"""))
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        worker = new DefaultAsyncOperationWorker(api, observer, DefaultVerificationEngine, store, mutex)
        accepted <- new DefaultFlinkControlPlane(store).accept(RequestId.from("req-submitted-recovery").toOption.get, operation)
        _ <- store.transition(accepted.operationId, OperationEvent.ValidationStarted(Instant.now()))
        _ <- store.transition(accepted.operationId, OperationEvent.ValidationPassed(Instant.now()))
        _ <- store.transition(accepted.operationId, OperationEvent.Submitted(Instant.now(), Generation.from(3).toOption, ResourceVersion.from("8").toOption))
        completed <- worker.process(accepted.operationId)
      yield assertTrue(completed.state == OperationState.Completed, completed.events.exists(_.isInstanceOf[OperationEvent.WaitingForObservation]), completed.events.exists(_.isInstanceOf[OperationEvent.VerificationStarted]))
    },
    test("classifies an explicit Kubernetes 4xx submission rejection as failed") {
      for
        api <- ZIO.succeed(new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.fail(cn.xuyinyin.flinklab.kubernetes.KubernetesApiError(403, "forbidden"))
          def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.succeed("")
        )
        observer = new ResourceObserver:
          def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]) = ZStream.empty
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        worker = new DefaultAsyncOperationWorker(api, observer, DefaultVerificationEngine, store, mutex)
        accepted <- new DefaultFlinkControlPlane(store).accept(RequestId.from("req-forbidden").toOption.get, operation)
        result <- worker.process(accepted.operationId).either
        stored <- store.get(accepted.operationId)
      yield assertTrue(result.isLeft, stored.exists(_.state == OperationState.Failed), stored.exists(_.events.exists {
        case OperationEvent.Failed(_, reason) => reason.contains("HTTP 403")
        case _ => false
      }))
    },
    test("records fallback after recovering an upgrade verification") {
      for
        api <- ZIO.succeed(new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.fail(UnsupportedOperationException("must not resubmit"))
          def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.succeed("")
        )
        observer = new ResourceObserver:
          def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]) =
            ZStream.succeed(ResourceObservation(WatchEventType.Modified, ResourceKind.Deployment, "orders", Some("11"), Some("uid"), Some(2), Some(2), """{"kind":"FlinkDeployment","metadata":{"name":"orders","generation":2},"status":{"jobStatus":{"state":"RUNNING"},"reconciliationStatus":{"state":"DEPLOYED","lastReconciledSpec":"{\"spec\":{\"job\":{\"upgradeMode\":\"last-state\"}}}"},"observedGeneration":2}}"""))
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        worker = new DefaultAsyncOperationWorker(api, observer, DefaultVerificationEngine, store, mutex)
        spec = FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.WordCount", 1, StateProtection.Savepoint))
        upgrade = FlinkOperation.Upgrade(ResourceRef(namespace, ResourceKind.Deployment, name), spec, UpgradePolicy(StateProtection.Savepoint, FallbackPolicy.AllowLastState))
        accepted <- new DefaultFlinkControlPlane(store).accept(RequestId.from("req-recovered-fallback").toOption.get, upgrade)
        _ <- store.transition(accepted.operationId, OperationEvent.ValidationStarted(Instant.now()))
        _ <- store.transition(accepted.operationId, OperationEvent.ValidationPassed(Instant.now()))
        _ <- store.transition(accepted.operationId, OperationEvent.Submitted(Instant.now(), Generation.from(2).toOption, ResourceVersion.from("11").toOption))
        _ <- store.transition(accepted.operationId, OperationEvent.WaitingForObservation(Instant.now()))
        _ <- store.transition(accepted.operationId, OperationEvent.VerificationStarted(Instant.now()))
        completed <- worker.process(accepted.operationId)
      yield assertTrue(completed.state == OperationState.Completed, completed.events.count(_.isInstanceOf[OperationEvent.FallbackDetected]) == 1)
    },
    test("fails an upgrade when a ready observation omits protection evidence") {
      for
        api <- ZIO.succeed(new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.succeed("""{"metadata":{"resourceVersion":"11","generation":2}}""")
          def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("")
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.succeed("")
        )
        observer = new ResourceObserver:
          def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]) =
            ZStream.succeed(ResourceObservation(WatchEventType.Modified, ResourceKind.Deployment, "orders", Some("11"), Some("uid"), Some(2), Some(2), """{"kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"11","uid":"uid","generation":2},"spec":{"flinkConfiguration":{"state.savepoints.dir":"s3://savepoints/orders"}},"status":{"jobStatus":{"state":"RUNNING"},"reconciliationStatus":{"state":"DEPLOYED"},"observedGeneration":2}}"""))
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        worker = new DefaultAsyncOperationWorker(api, observer, DefaultVerificationEngine, store, mutex, new cn.xuyinyin.flinklab.application.DefaultPolicyEngine)
        spec = FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.WordCount", 1, StateProtection.Savepoint), savepointDirectory = Some("s3://savepoints/orders"))
        operation = FlinkOperation.Upgrade(ResourceRef(namespace, ResourceKind.Deployment, name), spec, UpgradePolicy(StateProtection.Savepoint, FallbackPolicy.Forbidden))
        accepted <- new DefaultFlinkControlPlane(store).accept(RequestId.from("req-missing-protection").toOption.get, operation)
        result <- worker.process(accepted.operationId).either
        stored <- store.get(accepted.operationId)
      yield assertTrue(result.isLeft, stored.exists(_.state == OperationState.Failed))
    }
  )

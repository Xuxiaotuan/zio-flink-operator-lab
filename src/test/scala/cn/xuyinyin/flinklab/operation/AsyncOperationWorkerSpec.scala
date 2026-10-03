package cn.xuyinyin.flinklab.operation

import cn.xuyinyin.flinklab.application.DefaultVerificationEngine
import cn.xuyinyin.flinklab.cli.ResourceKind
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.kubernetes.KubernetesApi
import cn.xuyinyin.flinklab.operator.observer.{ResourceObservation, ResourceObserver}
import cn.xuyinyin.flinklab.operator.watch.WatchEventType
import zio.*
import zio.stream.*
import zio.test.*

object AsyncOperationWorkerSpec extends ZIOSpecDefault:
  private val namespace = Namespace.unsafe("analytics")
  private val name = DeploymentName.unsafe("orders")
  private val operation = FlinkOperation.Deploy(FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.WordCount", 1)))

  def spec = suite("async operation worker")(
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
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = records.update(_ :+ resource).as("""{"metadata":{"resourceVersion":"9"}}""")
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
    }
  )

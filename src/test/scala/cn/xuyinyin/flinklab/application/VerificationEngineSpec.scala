package cn.xuyinyin.flinklab.application

import cn.xuyinyin.flinklab.cli.ResourceKind
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.operator.observer.ResourceObservation
import cn.xuyinyin.flinklab.operator.watch.WatchEventType
import zio.test.*

object VerificationEngineSpec extends ZIOSpecDefault:
  private val namespace = Namespace.unsafe("analytics")
  private val name = DeploymentName.unsafe("orders")
  private val ref = ResourceRef(namespace, ResourceKind.Deployment, name)

  def spec = suite("verification engine")(
    test("accepts a ready observation for deploy") {
      val operation = FlinkOperation.Deploy(FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.WordCount", 1)))
      val observation = ResourceObservation(WatchEventType.Added, ResourceKind.Deployment, "orders", Some("9"), Some("uid"), Some(3), Some(3), """{"kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"9","uid":"uid","generation":3},"status":{"lifecycleState":"STABLE","jobManagerDeploymentStatus":"READY","jobStatus":{"state":"RUNNING"},"reconciliationStatus":{"state":"DEPLOYED"},"observedGeneration":3}}""")
      assertTrue(DefaultVerificationEngine.verify(operation, ref, Evidence.fromObservation(namespace, observation)).isRight)
    },
    test("accepts a naturally finished ready job") {
      val operation = FlinkOperation.Deploy(FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.WordCount", 1)))
      val observation = ResourceObservation(WatchEventType.Modified, ResourceKind.Deployment, "orders", Some("9"), Some("uid"), Some(3), Some(3), """{"kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"9","uid":"uid","generation":3},"status":{"lifecycleState":"STABLE","jobManagerDeploymentStatus":"READY","jobStatus":{"state":"FINISHED"},"reconciliationStatus":{"state":"DEPLOYED"},"observedGeneration":3}}""")
      assertTrue(DefaultVerificationEngine.verify(operation, ref, Evidence.fromObservation(namespace, observation)).isRight)
    },
    test("rejects a stale observed generation") {
      val operation = FlinkOperation.Deploy(FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.WordCount", 1)))
      val observation = ResourceObservation(WatchEventType.Added, ResourceKind.Deployment, "orders", Some("9"), Some("uid"), Some(3), Some(2), """{"kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"9","uid":"uid","generation":3},"status":{"lifecycleState":"STABLE","jobManagerDeploymentStatus":"READY","jobStatus":{"state":"RUNNING"},"reconciliationStatus":{"state":"DEPLOYED"},"observedGeneration":2}}""")
      assertTrue(DefaultVerificationEngine.verify(operation, ref, Evidence.fromObservation(namespace, observation)).isLeft)
    },
    test("rejects evidence from an older submitted generation") {
      val operation = FlinkOperation.Deploy(FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.WordCount", 1)))
      val observation = ResourceObservation(WatchEventType.Added, ResourceKind.Deployment, "orders", Some("9"), Some("uid"), Some(3), Some(3), """{"kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"9","uid":"uid","generation":3},"status":{"lifecycleState":"STABLE","jobManagerDeploymentStatus":"READY","jobStatus":{"state":"RUNNING"},"reconciliationStatus":{"state":"DEPLOYED"},"observedGeneration":3}}""")
      assertTrue(DefaultVerificationEngine.verify(operation, ref, Evidence.fromObservation(namespace, observation), Generation.from(4).toOption).isLeft)
    },
    test("treats an operator status error as a deterministic verification failure") {
      val operation = FlinkOperation.Deploy(FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.WordCount", 1)))
      val observation = ResourceObservation(WatchEventType.Modified, ResourceKind.Deployment, "orders", Some("10"), Some("uid"), Some(3), Some(3), """{"kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"10","uid":"uid","generation":3},"status":{"lifecycleState":"FAILED","error":"operator rejected the resource","reconciliationStatus":{"state":"UPGRADING"},"observedGeneration":3}}""")
      val evidence = Evidence.fromObservation(namespace, observation)
      assertTrue(evidence.reconciliation == ReconciliationState.Error, DefaultVerificationEngine.verify(operation, ref, evidence).isLeft)
    },
    test("requires a completed snapshot to expose its result path") {
      val snapshotName = DeploymentName.unsafe("orders-snapshot")
      val operation = FlinkOperation.Snapshot(ref, SnapshotPolicy(SnapshotType.Savepoint, Some(snapshotName)))
      val observation = ResourceObservation(WatchEventType.Modified, ResourceKind.StateSnapshot, snapshotName.nameValue, Some("9"), Some("snapshot-uid"), None, None, """{"kind":"FlinkStateSnapshot","metadata":{"name":"orders-snapshot","uid":"snapshot-uid","resourceVersion":"9"},"status":{"state":"COMPLETED"}}""")
      assertTrue(DefaultVerificationEngine.verify(operation, ResourceRef(namespace, ResourceKind.StateSnapshot, snapshotName, ResourceUid.from("snapshot-uid").toOption), Evidence.fromObservation(namespace, observation)).isLeft)
    }
  )

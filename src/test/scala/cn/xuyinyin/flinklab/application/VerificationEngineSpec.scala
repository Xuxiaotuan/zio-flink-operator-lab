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
    },
    test("accepts an explicit last-state fallback only when policy allows it") {
      val spec = FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.WordCount", 1, StateProtection.Savepoint))
      val operation = FlinkOperation.Upgrade(ref, spec, UpgradePolicy(StateProtection.Savepoint, FallbackPolicy.AllowLastState))
      val observation = ResourceObservation(WatchEventType.Modified, ResourceKind.Deployment, "orders", Some("11"), Some("uid"), Some(4), Some(4), """{"kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"11","uid":"uid","generation":4},"status":{"jobStatus":{"state":"RUNNING"},"reconciliationStatus":{"state":"DEPLOYED","lastReconciledSpec":"{\"spec\":{\"job\":{\"upgradeMode\":\"last-state\"}}}"},"observedGeneration":4}}""")
      val evidence = Evidence.fromObservation(namespace, observation)
      val result = DefaultVerificationEngine.verify(operation, ref, evidence)
      assertTrue(evidence.actualProtection.contains(ActualProtection.LastState), result.isRight)
    },
    test("rejects an explicit last-state fallback when forbidden") {
      val spec = FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.WordCount", 1, StateProtection.Savepoint))
      val operation = FlinkOperation.Upgrade(ref, spec, UpgradePolicy(StateProtection.Savepoint, FallbackPolicy.Forbidden))
      val observation = ResourceObservation(WatchEventType.Modified, ResourceKind.Deployment, "orders", Some("11"), Some("uid"), Some(4), Some(4), """{"kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"11","uid":"uid","generation":4},"status":{"jobStatus":{"state":"RUNNING"},"reconciliationStatus":{"state":"DEPLOYED","lastReconciledSpec":"{\"spec\":{\"job\":{\"upgradeMode\":\"last-state\"}}}"},"observedGeneration":4}}""")
      assertTrue(DefaultVerificationEngine.verify(operation, ref, Evidence.fromObservation(namespace, observation)).isLeft)
    },
    test("rejects strict savepoint upgrade when actual protection evidence is missing") {
      val spec = FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.WordCount", 1, StateProtection.Savepoint))
      val operation = FlinkOperation.Upgrade(ref, spec, UpgradePolicy(StateProtection.Savepoint, FallbackPolicy.Forbidden))
      val observation = ResourceObservation(WatchEventType.Modified, ResourceKind.Deployment, "orders", Some("11"), Some("uid"), Some(4), Some(4), """{"kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"11","uid":"uid","generation":4},"status":{"jobStatus":{"state":"RUNNING"},"reconciliationStatus":{"state":"DEPLOYED"},"observedGeneration":4}}""")
      val result = DefaultVerificationEngine.verify(operation, ref, Evidence.fromObservation(namespace, observation))
      assertTrue(result.isLeft, result.left.toOption.exists(_.message.contains("protection evidence")))
    },
    test("accepts the operator upgrade savepoint path as protection evidence") {
      val spec = FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.WordCount", 1, StateProtection.Savepoint))
      val operation = FlinkOperation.Upgrade(ref, spec, UpgradePolicy(StateProtection.Savepoint, FallbackPolicy.Forbidden))
      val observation = ResourceObservation(WatchEventType.Modified, ResourceKind.Deployment, "orders", Some("12"), Some("uid"), Some(5), Some(5), """{"kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"12","uid":"uid","generation":5},"status":{"jobStatus":{"state":"RUNNING","savepointInfo":{},"upgradeSavepointPath":"s3://bucket/upgrade-sp"},"reconciliationStatus":{"state":"DEPLOYED","lastReconciledSpec":"{\"spec\":{\"job\":{\"upgradeMode\":\"savepoint\"}}}"},"observedGeneration":5}}""")
      val evidence = Evidence.fromObservation(namespace, observation)
      assertTrue(evidence.actualProtection.contains(ActualProtection.Savepoint(SnapshotPath.from("s3://bucket/upgrade-sp").toOption.get)), DefaultVerificationEngine.verify(operation, ref, evidence).isRight)
    },
    test("does not treat missing protection evidence as an allowed fallback") {
      val spec = FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.WordCount", 1, StateProtection.Savepoint))
      val operation = FlinkOperation.Upgrade(ref, spec, UpgradePolicy(StateProtection.Savepoint, FallbackPolicy.AllowLastState))
      val observation = ResourceObservation(WatchEventType.Modified, ResourceKind.Deployment, "orders", Some("11"), Some("uid"), Some(4), Some(4), """{"kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"11","uid":"uid","generation":4},"status":{"jobStatus":{"state":"RUNNING"},"reconciliationStatus":{"state":"DEPLOYED"},"observedGeneration":4}}""")
      assertTrue(DefaultVerificationEngine.verify(operation, ref, Evidence.fromObservation(namespace, observation)).isLeft)
    }
  )

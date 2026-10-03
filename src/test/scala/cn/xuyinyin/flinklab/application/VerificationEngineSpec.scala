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
    test("rejects a stale observed generation") {
      val operation = FlinkOperation.Deploy(FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.WordCount", 1)))
      val observation = ResourceObservation(WatchEventType.Added, ResourceKind.Deployment, "orders", Some("9"), Some("uid"), Some(3), Some(2), """{"kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"9","uid":"uid","generation":3},"status":{"lifecycleState":"STABLE","jobManagerDeploymentStatus":"READY","jobStatus":{"state":"RUNNING"},"reconciliationStatus":{"state":"DEPLOYED"},"observedGeneration":2}}""")
      assertTrue(DefaultVerificationEngine.verify(operation, ref, Evidence.fromObservation(namespace, observation)).isLeft)
    },
    test("rejects evidence from an older submitted generation") {
      val operation = FlinkOperation.Deploy(FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.WordCount", 1)))
      val observation = ResourceObservation(WatchEventType.Added, ResourceKind.Deployment, "orders", Some("9"), Some("uid"), Some(3), Some(3), """{"kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"9","uid":"uid","generation":3},"status":{"lifecycleState":"STABLE","jobManagerDeploymentStatus":"READY","jobStatus":{"state":"RUNNING"},"reconciliationStatus":{"state":"DEPLOYED"},"observedGeneration":3}}""")
      assertTrue(DefaultVerificationEngine.verify(operation, ref, Evidence.fromObservation(namespace, observation), Generation.from(4).toOption).isLeft)
    }
  )

package cn.xuyinyin.flinklab.operation

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

/** 从真正的 worker 执行到持久化恢复，防止普通 upsert 绕过保护或恢复后放宽完成条件。 */
object RestoreSafetySpec extends ZIOSpecDefault:
  private val ns = Namespace.unsafe("analytics")
  private val ref = ResourceRef(ns, ResourceKind.Deployment, DeploymentName.unsafe("orders"))
  private val path = "s3://snapshots/requested"
  private val deploymentSpec = FlinkDeploymentSpec(ns, ref.name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.Job", 1,
    StateProtection.Savepoint, initialSavepointPath = SnapshotPath.from(path).toOption, allowNonRestoredState = Some(false), savepointRedeployNonce = Some(5L)))
  private def raw(location: String = path, nonce: Long = 5, generation: Int = 4, state: String = "RUNNING") =
    ujson.Obj("kind" -> "FlinkDeployment", "metadata" -> ujson.Obj("name" -> "orders", "namespace" -> "analytics", "uid" -> "original", "resourceVersion" -> "8", "generation" -> generation),
      "spec" -> ujson.Obj("job" -> ujson.Obj("savepointRedeployNonce" -> ujson.Num(nonce.toDouble))),
      "status" -> ujson.Obj("observedGeneration" -> generation, "jobStatus" -> ujson.Obj("state" -> state), "reconciliationStatus" -> ujson.Obj("state" -> "DEPLOYED",
        "lastReconciledSpec" -> ujson.Obj("spec" -> ujson.Obj("job" -> ujson.Obj("state" -> "running", "upgradeMode" -> "savepoint", "initialSavepointPath" -> location, "savepointRedeployNonce" -> ujson.Num(nonce.toDouble)),
          "flinkConfiguration" -> ujson.Obj("kubernetes.operator.job.upgrade.last-state-fallback.enabled" -> "false"))).render()))).render()
  private def api(patches: Ref[List[String]], currentNonce: Long = 4): KubernetesApi = new KubernetesApi:
    def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.dieMessage("unexpected apply")
    def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed(raw(nonce = currentNonce, generation = 3, state = "SUSPENDED"))
    def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.dieMessage("unexpected delete")
    def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = patches.update(_ :+ patch).as("""{"metadata":{"uid":"original","resourceVersion":"9","generation":4}}""")
  private def observer(location: String = path, nonce: Long = 5): ResourceObserver = new ResourceObserver:
    def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]) =
      ZStream.succeed(ResourceObservation(WatchEventType.Modified, ResourceKind.Deployment, "orders", Some("9"), Some("original"), Some(4), Some(4), raw(location, nonce)))

  def spec = suite("restore safety through worker")(
    test("ordinary deploy rejects an existing target before mutation") {
      for
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        writes <- Ref.make(List.empty[String])
        ordinary = FlinkOperation.Deploy(deploymentSpec.copy(job = deploymentSpec.job.copy(initialSavepointPath = None, savepointRedeployNonce = None)))
        accepted <- new DefaultFlinkControlPlane(store).accept(RequestId.generate(), ordinary)
        result <- new DefaultAsyncOperationWorker(api(writes), observer(), DefaultVerificationEngine, store, mutex).process(accepted.operationId).either
        actual <- writes.get
      yield assertTrue(result.isLeft, actual.isEmpty)
    },
    test("explicit restore from suspension preserves the requested path nonce and boolean") {
      for
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        writes <- Ref.make(List.empty[String])
        accepted <- new DefaultFlinkControlPlane(store).accept(RequestId.generate(), FlinkOperation.Deploy(deploymentSpec))
        result <- new DefaultAsyncOperationWorker(api(writes), observer(), DefaultVerificationEngine, store, mutex).process(accepted.operationId)
        actual <- writes.get
        restored <- store.get(accepted.operationId).map(_.flatMap(op => OperationCodec.fromJson(OperationCodec.json(op)).toOption))
      yield assertTrue(result.state == OperationState.Completed, actual.size == 1, actual.headOption.exists { raw =>
        val value = ujson.read(raw)("spec")
        value("job")("initialSavepointPath").str == path && value("job")("savepointRedeployNonce").num == 5 &&
          !value("job")("allowNonRestoredState").bool && value("flinkConfiguration")("kubernetes.operator.job.upgrade.last-state-fallback.enabled").str == "false"
      }, restored.exists(_.command == FlinkOperation.Deploy(deploymentSpec)))
    },
    test("an old redeploy nonce is rejected before mutation") {
      for
        store <- InMemoryOperationStore.make
        mutex <- OperationMutex.make
        writes <- Ref.make(List.empty[String])
        accepted <- new DefaultFlinkControlPlane(store).accept(RequestId.generate(), FlinkOperation.Deploy(deploymentSpec))
        result <- new DefaultAsyncOperationWorker(api(writes, currentNonce = 5), observer(), DefaultVerificationEngine, store, mutex).process(accepted.operationId).either
        actual <- writes.get
      yield assertTrue(result.isLeft, actual.isEmpty)
    },
    test("recovered restore rejects wrong savepoint or nonce without repeating the write") {
      ZIO.foreach(List(("s3://snapshots/other", 5L), (path, 4L))) { (location, nonce) =>
        for
          store <- InMemoryOperationStore.make
          mutex <- OperationMutex.make
          writes <- Ref.make(List.empty[String])
          seen <- Promise.make[Nothing, Unit]
          accepted <- new DefaultFlinkControlPlane(store).accept(RequestId.generate(), FlinkOperation.Deploy(deploymentSpec))
          _ <- store.transition(accepted.operationId, OperationEvent.ValidationStarted(Instant.now()))
          _ <- store.transition(accepted.operationId, OperationEvent.ValidationPassed(Instant.now()))
          _ <- store.transition(accepted.operationId, OperationEvent.Submitted(Instant.now(), Generation.from(4).toOption, ResourceVersion.from("9").toOption, ResourceUid.from("original").toOption))
          observations = new ResourceObserver:
            def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]) =
              observer(location, nonce).observe(namespace, kind, name) ++ ZStream.fromZIO(seen.succeed(())).drain ++ ZStream.never
          fiber <- new DefaultAsyncOperationWorker(api(writes), observations, DefaultVerificationEngine, store, mutex).process(accepted.operationId).either.fork
          _ <- seen.await
          _ <- TestClock.adjust(181.seconds)
          result <- fiber.join
          stored <- store.get(accepted.operationId)
          actual <- writes.get
        yield result.isLeft && stored.exists(_.state == OperationState.TimedOut) && actual.isEmpty
      }.map(results => assertTrue(results.forall(identity)))
    }
  )

package cn.xuyinyin.flinklab.operation
/** 验证 FlinkOperation CR 的持久化和 resourceVersion 更新契约。 */

import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.kubernetes.KubernetesApi
import zio.*
import zio.test.*

import java.time.Instant

object KubernetesOperationStoreSpec extends ZIOSpecDefault:
  private val namespace = Namespace.unsafe("analytics")
  private val name = DeploymentName.unsafe("orders")
  private val target = ResourceRef(namespace, ResourceKind.Deployment, name)
  private val operation = Operation.accepted(RequestId.from("req-k8s").toOption.get, FlinkOperation.Resume(target), target, Instant.parse("2026-10-03T00:00:00Z"))

  def spec = suite("kubernetes operation store")(
    test("round trips operation lifecycle through a Kubernetes CR") {
      for
        raw <- Ref.make(Option.empty[String])
        api = new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = raw.set(Some(resource)).as(resource)
          override def create(namespace: Namespace, resource: String) = raw.set(Some(resource)).as(resource)
          def get(namespace: Namespace, kind: ResourceKind, name: String) = raw.get.map(_.getOrElse(""))
          override def list(namespace: Namespace, kind: ResourceKind) = raw.get.map(value => s"{\"items\":[${value.getOrElse("{}")}]}")
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = raw.set(None).as("deleted")
          override def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = raw.set(Some(patch)).as(patch)
        store = new KubernetesOperationStore(api)
        _ <- store.create(operation)
        updated <- store.transition(operation.id, OperationEvent.ValidationStarted(Instant.parse("2026-10-03T00:00:01Z")))
        loaded <- store.get(operation.id)
      yield assertTrue(updated.state == OperationState.Validating, loaded.exists(_.state == OperationState.Validating))
    },
    test("re-reads a conflicted CR and preserves concurrent audit events") {
      for
        attempts <- Ref.make(0)
        valid = operation.advance(OperationEvent.ValidationStarted(Instant.now())).toOption.get
        concurrent = valid.advance(OperationEvent.ValidationPassed(Instant.now())).toOption.get
        raw <- Ref.make(KubernetesOperationResource.render(valid, Some("1")))
        api = new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.fail(UnsupportedOperationException())
          def get(namespace: Namespace, kind: ResourceKind, name: String) = raw.get
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.fail(UnsupportedOperationException())
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) =
            attempts.updateAndGet(_ + 1).flatMap {
              case 1 => raw.set(KubernetesOperationResource.render(concurrent, Some("2"))) *> ZIO.fail(cn.xuyinyin.flinklab.kubernetes.KubernetesApiError(409, "conflict"))
              case _ if KubernetesOperationResource.resourceVersion(patch).contains("2") => raw.set(patch).as(patch)
              case _ => ZIO.fail(IllegalStateException("stale resourceVersion reused"))
            }
        result <- new KubernetesOperationStore(api).transition(operation.id, OperationEvent.Submitted(Instant.now(), None, None)).either
        count <- attempts.get
      yield assertTrue(result.exists(_.state == OperationState.Submitted), result.exists(_.events.exists(_.isInstanceOf[OperationEvent.ValidationPassed])), count == 2)
    },
    test("preserves the typed operation payload in the CR") {
      val rendered = KubernetesOperationResource.render(operation)
      assertTrue(rendered.contains("FlinkOperation"), rendered.contains(operation.id.operationIdValue), KubernetesOperationResource.parse(rendered).contains(operation))
    }
  )

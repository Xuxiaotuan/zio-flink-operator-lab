package cn.xuyinyin.flinklab.operation

import cn.xuyinyin.flinklab.cli.ResourceKind
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
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.succeed("")
        store = new KubernetesOperationStore(api)
        _ <- store.create(operation)
        updated <- store.transition(operation.id, OperationEvent.ValidationStarted(Instant.parse("2026-10-03T00:00:01Z")))
        loaded <- store.get(operation.id)
      yield assertTrue(updated.state == OperationState.Validating, loaded.exists(_.state == OperationState.Validating))
    },
    test("preserves the typed operation payload in the CR") {
      val rendered = KubernetesOperationResource.render(operation)
      assertTrue(rendered.contains("FlinkOperation"), rendered.contains(operation.id.operationIdValue), KubernetesOperationResource.parse(rendered).contains(operation))
    }
  )

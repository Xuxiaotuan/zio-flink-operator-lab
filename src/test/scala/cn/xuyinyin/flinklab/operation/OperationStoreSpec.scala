package cn.xuyinyin.flinklab.operation

import cn.xuyinyin.flinklab.cli.ResourceKind
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import zio.*
import zio.test.*

import java.time.Instant

object OperationStoreSpec extends ZIOSpecDefault:
  private val namespace = Namespace.unsafe("analytics")
  private val name = DeploymentName.unsafe("orders")
  private val resource = ResourceRef(namespace, ResourceKind.Deployment, name)
  private val operation = Operation.accepted(
    RequestId.from("req-1").toOption.get,
    FlinkOperation.Resume(resource),
    resource,
    Instant.parse("2026-10-03T00:00:00Z")
  )

  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("operation store")(
      test("creates and transitions an operation atomically") {
        for
          store <- ZIO.service[OperationStore]
          _ <- store.create(operation)
          updated <- store.transition(operation.id, OperationEvent.ValidationStarted(Instant.parse("2026-10-03T00:00:01Z")))
          current <- store.get(operation.id)
        yield assertTrue(updated.state == OperationState.Validating, current.map(_.events.size).contains(2))
      },
      test("rejects a duplicate operation") {
        for
          store <- ZIO.service[OperationStore]
          first <- store.create(operation).either
          second <- store.create(operation).either
        yield assertTrue(first.isRight, second.left.exists(_.isInstanceOf[ControlPlaneError.OperationAlreadyExists]))
      },
      test("accepts an operation without claiming verification") {
        for
          accepted <- ZIO.serviceWithZIO[FlinkControlPlane](_.accept(RequestId.from("req-2").toOption.get, FlinkOperation.Resume(resource)))
          stored <- ZIO.serviceWithZIO[FlinkControlPlane](_.get(accepted.operationId))
        yield assertTrue(stored.exists(_.state == OperationState.Accepted), accepted.requestId.requestIdValue == "req-2")
      }
    ).provide(InMemoryOperationStore.layer, FlinkControlPlane.live)

package cn.xuyinyin.flinklab.operation
/** 验证内存 OperationStore 的幂等、状态推进和资源互斥。 */

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
      test("concurrent different commands cannot reuse the same request id after create conflict") {
        for
          delegate <- InMemoryOperationStore.make
          lookups <- Ref.make(0)
          bothRead <- Promise.make[Nothing, Unit]
          store = new OperationStore:
            def create(value: Operation) = delegate.create(value)
            def transition(id: OperationId, event: OperationEvent) = delegate.transition(id, event)
            def get(id: OperationId) = delegate.get(id)
            def list = ZIO.succeed(Nil)
            override def findByRequestId(id: RequestId) = lookups.updateAndGet(_ + 1).flatMap {
              case count if count <= 2 => bothRead.succeed(()).when(count == 2) *> bothRead.await.as(None)
              case _ => delegate.findByRequestId(id)
            }
          controlPlane = new DefaultFlinkControlPlane(store)
          requestId = RequestId.from("concurrent-request").toOption.get
          results <- controlPlane.accept(requestId, FlinkOperation.Resume(resource)).either.zipPar(
            controlPlane.accept(requestId, FlinkOperation.Delete(resource, DeletePolicy.Graceful)).either)
          stored <- delegate.list
        yield assertTrue(List(results._1, results._2).count(_.isRight) == 1,
          List(results._1, results._2).count(_.left.exists(_.message.contains("different operation"))) == 1, stored.size == 1)
      },
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
      },
      test("reuses an existing operation for the same request id") {
        for
          controlPlane <- ZIO.service[FlinkControlPlane]
          first <- controlPlane.accept(RequestId.from("req-idempotent").toOption.get, FlinkOperation.Resume(resource))
          second <- controlPlane.accept(RequestId.from("req-idempotent").toOption.get, FlinkOperation.Resume(resource))
        yield assertTrue(first.operationId == second.operationId)
      },
      test("rejects a different active operation for the same resource") {
        for
          controlPlane <- ZIO.service[FlinkControlPlane]
          first <- controlPlane.accept(RequestId.from("req-old").toOption.get, FlinkOperation.Resume(resource))
          second <- controlPlane.accept(RequestId.from("req-new").toOption.get, FlinkOperation.Restart(resource, UpgradePolicy(StateProtection.LastState, FallbackPolicy.Forbidden))).either
        yield assertTrue(second.left.exists(_.isInstanceOf[ControlPlaneError.ResourceBusy]), first.operationId.operationIdValue.nonEmpty)
      },
      test("same request id with different operation is a conflict") {
        for
          controlPlane <- ZIO.service[FlinkControlPlane]
          _ <- controlPlane.accept(RequestId.from("req-conflict").toOption.get, FlinkOperation.Resume(resource))
          result <- controlPlane.accept(RequestId.from("req-conflict").toOption.get, FlinkOperation.Delete(resource, DeletePolicy.Graceful)).either
        yield assertTrue(result.left.exists(_.message.contains("different operation")))
      }
    ).provide(InMemoryOperationStore.layer, FlinkControlPlane.live)

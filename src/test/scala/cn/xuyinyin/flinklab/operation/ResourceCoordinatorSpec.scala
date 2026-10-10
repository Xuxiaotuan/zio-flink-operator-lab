package cn.xuyinyin.flinklab.operation
/** 验证锁租约创建、过期接管、续租和 UID 保护释放。 */

import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.kubernetes.{KubernetesApi, KubernetesApiError}
import zio.*
import zio.test.*

object ResourceCoordinatorSpec extends ZIOSpecDefault:
  private val namespace = Namespace.unsafe("analytics")
  private val target = ResourceRef(namespace, ResourceKind.Deployment, DeploymentName.unsafe("orders"))

  def spec = suite("kubernetes resource coordinator")(
    test("takes over an expired lock using its UID and then releases its own lock") {
      for
        calls <- Ref.make(Vector.empty[String])
        operationId = OperationId.from("new-operation").toOption.get
        api = new KubernetesApi:
          private val expired = "2020-01-01T00:00:00Z"
          private var creates = 0
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.fail(UnsupportedOperationException())
          override def create(namespace: Namespace, resource: String) =
            calls.update(_ :+ "create") *> ZIO.succeed { creates += 1 }.flatMap(_ => if creates == 1 then ZIO.fail(KubernetesApiError(409, "already exists")) else ZIO.succeed("""{"metadata":{"uid":"new-lock"}}"""))
          override def get(namespace: Namespace, kind: ResourceKind, name: String) =
            calls.update(_ :+ "get") *> ZIO.succeed(s"""{"metadata":{"uid":"old-lock","resourceVersion":"7"},"spec":{"operationId":"old-operation","leaseUntil":"$expired"}}""")
          override def delete(namespace: Namespace, kind: ResourceKind, name: String) = calls.update(_ :+ "delete-by-name").as("deleted")
          override def delete(namespace: Namespace, kind: ResourceKind, name: String, uid: Option[ResourceUid]) =
            calls.update(_ :+ s"delete:${uid.map(_.resourceUidValue).getOrElse("")}").as("deleted")
          override def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.succeed("")
        result <- new KubernetesResourceCoordinator(api).withLock(target, operationId)(ZIO.succeed("ok"))
        observed <- calls.get
      yield assertTrue(result == "ok", observed == Vector("create", "get", "delete:old-lock", "create", "delete:new-lock"))
    },
    test("renews a held lease with UID and resourceVersion preconditions") {
      for
        patches <- Ref.make(Vector.empty[String])
        entered <- Promise.make[Nothing, Unit]
        finished <- Promise.make[Nothing, String]
        api = new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.fail(UnsupportedOperationException())
          override def create(namespace: Namespace, resource: String) = ZIO.succeed("""{"metadata":{"uid":"held-lock","resourceVersion":"7"}}""")
          def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("""{"metadata":{"uid":"held-lock","resourceVersion":"7"}}""")
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("deleted")
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = patches.update(_ :+ patch).as("renewed")
        fiber <- new KubernetesResourceCoordinator(api).withLock(target, OperationId.from("renew-operation").toOption.get)(entered.succeed(()) *> finished.await).fork
        _ <- entered.await
        _ <- TestClock.adjust(40.seconds)
        observed <- patches.get
        _ <- finished.succeed("ok")
        result <- fiber.join
      yield assertTrue(result == "ok", observed.exists(raw => raw.contains("\"uid\":\"held-lock\"") && raw.contains("\"resourceVersion\":\"7\"") && raw.contains("leaseUntil")))
    },
    test("interrupts the effect when ownership is lost during lease renewal") {
      for
        entered <- Promise.make[Nothing, Unit]
        finished <- Promise.make[Nothing, String]
        api = new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.fail(UnsupportedOperationException())
          override def create(namespace: Namespace, resource: String) = ZIO.succeed("""{"metadata":{"uid":"old-lock","resourceVersion":"7"}}""")
          def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("""{"metadata":{"uid":"replacement-lock","resourceVersion":"8"}}""")
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("deleted")
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.fail(KubernetesApiError(409, "lock ownership changed"))
        fiber <- new KubernetesResourceCoordinator(api).withLock(target, OperationId.from("lost-operation").toOption.get)(entered.succeed(()) *> finished.await).either.fork
        _ <- entered.await
        _ <- TestClock.adjust(40.seconds)
        _ <- finished.succeed("ok")
        result <- fiber.join
      yield assertTrue(result.isLeft)
    },
    test("does not steal a lease renewed after the expiry read") {
      for
        unsafeDeletes <- Ref.make(0)
        entered <- Ref.make(false)
        api = new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.fail(UnsupportedOperationException())
          override def create(namespace: Namespace, resource: String) = ZIO.fail(KubernetesApiError(409, "exists"))
          def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed("""{"metadata":{"uid":"same-uid","resourceVersion":"old-rv"},"spec":{"leaseUntil":"2020-01-01T00:00:00Z"}}""")
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = unsafeDeletes.update(_ + 1).as("deleted")
          override def delete(namespace: Namespace, kind: ResourceKind, name: String, uid: Option[ResourceUid], resourceVersion: Option[String]) = ZIO.fail(KubernetesApiError(409, "resourceVersion changed"))
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.fail(UnsupportedOperationException())
        _ <- new KubernetesResourceCoordinator(api).withLock(target, OperationId.from("contender").toOption.get)(entered.set(true)).either
        deletes <- unsafeDeletes.get
        executed <- entered.get
      yield assertTrue(deletes == 0, !executed)
    },
    test("rejects a live lock without deleting it") {
      for
        calls <- Ref.make(Vector.empty[String])
        operationId = OperationId.from("new-operation").toOption.get
        api = new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.fail(UnsupportedOperationException())
          override def create(namespace: Namespace, resource: String) = calls.update(_ :+ "create") *> ZIO.fail(KubernetesApiError(409, "already exists"))
          override def get(namespace: Namespace, kind: ResourceKind, name: String) = calls.update(_ :+ "get") *> ZIO.succeed("""{"metadata":{"uid":"live-lock"},"spec":{"leaseUntil":"2999-01-01T00:00:00Z"}}""")
          override def delete(namespace: Namespace, kind: ResourceKind, name: String) = calls.update(_ :+ "delete-by-name").as("deleted")
          override def delete(namespace: Namespace, kind: ResourceKind, name: String, uid: Option[ResourceUid]) = calls.update(_ :+ "delete").as("deleted")
          override def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.succeed("")
        result <- new KubernetesResourceCoordinator(api).withLock(target, operationId)(ZIO.succeed("ok")).either
        observed <- calls.get
      yield assertTrue(result.isLeft, observed == Vector("create", "get"))
    }
  )

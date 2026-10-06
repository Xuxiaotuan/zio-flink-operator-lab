package cn.xuyinyin.flinklab.operation

import cn.xuyinyin.flinklab.cli.ResourceKind
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

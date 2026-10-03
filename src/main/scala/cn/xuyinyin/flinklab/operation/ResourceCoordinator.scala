package cn.xuyinyin.flinklab.operation

import cn.xuyinyin.flinklab.cli.ResourceKind
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.kubernetes.{KubernetesApi, KubernetesApiError}
import zio.*

trait ResourceCoordinator:
  def withLock[A](resource: ResourceRef, operationId: OperationId)(effect: IO[ControlPlaneError, A]): IO[ControlPlaneError, A]

object InMemoryResourceCoordinator extends ResourceCoordinator:
  override def withLock[A](resource: ResourceRef, operationId: OperationId)(effect: IO[ControlPlaneError, A]): IO[ControlPlaneError, A] = effect

final class KubernetesResourceCoordinator(api: KubernetesApi) extends ResourceCoordinator:
  override def withLock[A](resource: ResourceRef, operationId: OperationId)(effect: IO[ControlPlaneError, A]): IO[ControlPlaneError, A] =
    val lockName = lockNameFor(resource)
    acquire(resource, operationId, lockName) *> effect.ensuring(release(resource.namespace, lockName))

  private def acquire(resource: ResourceRef, operationId: OperationId, lockName: DeploymentName): IO[ControlPlaneError, Unit] =
    api.create(resource.namespace, render(resource, operationId, lockName))
      .mapError {
        case KubernetesApiError(409, _) => ControlPlaneError.ResourceBusy(resource)
        case error => ControlPlaneError.StoreFailure(Option(error.getMessage).getOrElse(error.toString))
      }
      .unit

  private def release(namespace: Namespace, lockName: DeploymentName): UIO[Unit] =
    api.delete(namespace, ResourceKind.OperationLock, lockName.nameValue).ignore

  private def lockNameFor(resource: ResourceRef): DeploymentName =
    val raw = s"${resource.namespace.namespaceValue}-${resource.kind.apiResource}-${resource.name.nameValue}"
    val digest = java.security.MessageDigest.getInstance("SHA-256").digest(raw.getBytes(java.nio.charset.StandardCharsets.UTF_8)).map(byte => f"$byte%02x").mkString.take(16)
    DeploymentName.unsafe(s"flink-lock-$digest")

  private def render(resource: ResourceRef, operationId: OperationId, lockName: DeploymentName): String =
    ujson.Obj(
      "apiVersion" -> "flink.apache.org/v1beta1",
      "kind" -> "FlinkOperationLock",
      "metadata" -> ujson.Obj("name" -> lockName.nameValue, "namespace" -> resource.namespace.namespaceValue),
      "spec" -> ujson.Obj(
        "operationId" -> operationId.operationIdValue,
        "target" -> resource.json
      )
    ).render()

object ResourceCoordinator:
  val live: ZLayer[KubernetesApi, Nothing, ResourceCoordinator] =
    ZLayer.fromFunction(new KubernetesResourceCoordinator(_))

package cn.xuyinyin.flinklab.operation

/** 资源互斥协调器：通过 FlinkOperationLock CR 为同一 Flink 资源建立可续租、可接管的租约。 */
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.kubernetes.{KubernetesApi, KubernetesApiError}
import zio.*

import java.time.{Duration, Instant}

/** 为同一资源的变更提供互斥执行边界。 */
trait ResourceCoordinator:
  def withLock[A](resource: ResourceRef, operationId: OperationId)(effect: IO[ControlPlaneError, A]): IO[ControlPlaneError, A]

object InMemoryResourceCoordinator extends ResourceCoordinator:
  /** 获取租约、后台续租，副作用结束后按 UID 释放，避免误删新持有者的锁。 */
  override def withLock[A](resource: ResourceRef, operationId: OperationId)(effect: IO[ControlPlaneError, A]): IO[ControlPlaneError, A] = effect

final class KubernetesResourceCoordinator(api: KubernetesApi) extends ResourceCoordinator:
  private val leaseDuration = Duration.ofSeconds(sys.env.get("ZIO_FLINK_LOCK_LEASE_SECONDS").flatMap(_.toLongOption).getOrElse(120L).max(1L))
  private val renewInterval = Duration.ofMillis((leaseDuration.toMillis / 3L).max(1000L))

  /** 获取租约、后台续租，副作用结束后按 UID 释放，避免误删新持有者的锁。 */
  override def withLock[A](resource: ResourceRef, operationId: OperationId)(effect: IO[ControlPlaneError, A]): IO[ControlPlaneError, A] =
    val lockName = lockNameFor(resource)
    acquire(resource, operationId, lockName).flatMap { lock =>
      for
        current <- Ref.make(lock)
        // renew 与 effect 竞速；续租失败会中断副作用，避免失去所有权后继续操作。
        result <- renew(resource.namespace, lockName, current).forever.raceFirst(effect).ensuring(release(resource.namespace, lockName, lock.uid))
      yield result
    }

  private final case class LockLease(uid: Option[ResourceUid], resourceVersion: Option[String])
  private final case class ExistingLock(uid: Option[ResourceUid], resourceVersion: Option[String], expiresAt: Instant)

  // 创建冲突时只接管已过期的锁；活跃锁直接返回 ResourceBusy。
  private def acquire(resource: ResourceRef, operationId: OperationId, lockName: DeploymentName): IO[ControlPlaneError, LockLease] =
    createLock(resource, operationId, lockName).either.flatMap {
      case Right(lock) => ZIO.succeed(lock)
      case Left(KubernetesApiError(409, _)) =>
        api.get(resource.namespace, ResourceKind.OperationLock, lockName.nameValue)
          .flatMap(raw =>
            parseLock(raw) match
              case Some(existing) if existing.expiresAt.isBefore(Instant.now()) =>
                api.delete(resource.namespace, ResourceKind.OperationLock, lockName.nameValue, existing.uid, existing.resourceVersion)
                  .unit *> createLock(resource, operationId, lockName).mapError {
                    case KubernetesApiError(409, _) => ControlPlaneError.ResourceBusy(resource)
                    case error => toStoreFailure(error)
                  }
              case Some(_) => ZIO.fail(ControlPlaneError.ResourceBusy(resource))
              case None => ZIO.fail(ControlPlaneError.ResourceBusy(resource))
          )
          .mapError {
            case error: ControlPlaneError => error
            case error => toStoreFailure(error)
          }
      case Left(error) => ZIO.fail(toStoreFailure(error))
    }

  private def createLock(resource: ResourceRef, operationId: OperationId, lockName: DeploymentName): IO[Throwable, LockLease] =
    val leaseUntil = Instant.now().plus(leaseDuration)
    api.create(resource.namespace, render(resource, operationId, lockName, leaseUntil))
      .map(raw => LockLease(metadataUid(raw), metadataResourceVersion(raw)))

  // 续租 patch 带上 UID/resourceVersion，锁已被接管时会触发错误而停止副作用。
  private def renew(namespace: Namespace, lockName: DeploymentName, current: Ref[LockLease]): IO[ControlPlaneError, Unit] =
    (ZIO.sleep(zio.Duration.fromMillis(renewInterval.toMillis)) *>
      current.get.flatMap { lease =>
        api.patch(namespace, ResourceKind.OperationLock, lockName.nameValue, renderRenewal(lease, Instant.now().plus(leaseDuration)))
          .mapError(toStoreFailure)
          .flatMap(raw => current.update(currentLease => currentLease.copy(resourceVersion = metadataResourceVersion(raw).orElse(currentLease.resourceVersion))))
      })

  private def renderRenewal(lease: LockLease, leaseUntil: Instant): String =
    val metadata = ujson.Obj()
    lease.uid.foreach(value => metadata("uid") = value.resourceUidValue)
    lease.resourceVersion.foreach(value => metadata("resourceVersion") = value)
    ujson.Obj("metadata" -> metadata, "spec" -> ujson.Obj("leaseUntil" -> leaseUntil.toString)).render()

  private def release(namespace: Namespace, lockName: DeploymentName, uid: Option[ResourceUid]): UIO[Unit] =
    api.delete(namespace, ResourceKind.OperationLock, lockName.nameValue, uid).ignore

  private def lockNameFor(resource: ResourceRef): DeploymentName =
    val raw = s"${resource.namespace.namespaceValue}-${resource.kind.apiResource}-${resource.name.nameValue}"
    val digest = java.security.MessageDigest.getInstance("SHA-256").digest(raw.getBytes(java.nio.charset.StandardCharsets.UTF_8)).map(byte => f"$byte%02x").mkString.take(16)
    DeploymentName.unsafe(s"flink-lock-$digest")

  private def render(resource: ResourceRef, operationId: OperationId, lockName: DeploymentName, leaseUntil: Instant): String =
    ujson.Obj(
      "apiVersion" -> "flink.apache.org/v1beta1",
      "kind" -> "FlinkOperationLock",
      "metadata" -> ujson.Obj("name" -> lockName.nameValue, "namespace" -> resource.namespace.namespaceValue),
      "spec" -> ujson.Obj(
        "operationId" -> operationId.operationIdValue,
        "target" -> resource.json,
        "leaseUntil" -> leaseUntil.toString
      )
    ).render()

  private def parseLock(raw: String): Option[ExistingLock] =
    scala.util.Try(ujson.read(raw)).toOption.flatMap { value =>
      val metadata = value.obj.get("metadata").flatMap(_.objOpt)
      val spec = value.obj.get("spec").flatMap(_.objOpt)
      val uid = metadata.flatMap(_.get("uid")).flatMap(_.strOpt).flatMap(ResourceUid.from(_).toOption)
      val expiresAt = spec.flatMap(_.get("leaseUntil")).flatMap(_.strOpt).flatMap(value => scala.util.Try(Instant.parse(value)).toOption)
      expiresAt.map(value => ExistingLock(uid, metadata.flatMap(_.get("resourceVersion")).flatMap(_.strOpt), value))
    }

  private def metadataUid(raw: String): Option[ResourceUid] =
    scala.util.Try(ujson.read(raw)).toOption
      .flatMap(_.obj.get("metadata").flatMap(_.objOpt))
      .flatMap(_.get("uid")).flatMap(_.strOpt)
      .flatMap(ResourceUid.from(_).toOption)

  private def metadataResourceVersion(raw: String): Option[String] =
    scala.util.Try(ujson.read(raw)).toOption
      .flatMap(_.obj.get("metadata").flatMap(_.objOpt))
      .flatMap(_.get("resourceVersion")).flatMap(_.strOpt)

  private def toStoreFailure(error: Any): ControlPlaneError =
    error match
      case value: ControlPlaneError => value
      case value: Throwable => ControlPlaneError.StoreFailure(Option(value.getMessage).getOrElse(value.toString))
      case value => ControlPlaneError.StoreFailure(value.toString)

object ResourceCoordinator:
  val live: ZLayer[KubernetesApi, Nothing, ResourceCoordinator] =
    ZLayer.fromFunction(new KubernetesResourceCoordinator(_))

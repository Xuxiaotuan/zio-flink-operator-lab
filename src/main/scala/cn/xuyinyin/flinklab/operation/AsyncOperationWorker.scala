package cn.xuyinyin.flinklab.operation

/** 异步操作 worker：负责操作生命周期、锁、提交、观察、验证、超时和故障恢复。 */
import cn.xuyinyin.flinklab.application.{DefaultPolicyEngine, DefaultVerificationEngine, Evidence, PolicyEngine, VerificationEngine}
import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.kubernetes.{KubernetesApi, KubernetesApiError}
import cn.xuyinyin.flinklab.operator.observer.ResourceObserver
import zio.*

import java.time.Instant

/** 异步操作入口；submit 只接受请求，process 负责推进生命周期。 */
trait AsyncOperationWorker:
  def submit(requestId: RequestId, operation: FlinkOperation): IO[ControlPlaneError, AcceptedOperation]
  def process(id: OperationId): IO[ControlPlaneError, Operation]

/** A process-level mutex protects reconcile side effects; the durable store protects transitions across replicas. */
final class OperationMutex(private val semaphore: Semaphore):
  def withPermit[A](effect: => IO[ControlPlaneError, A]): IO[ControlPlaneError, A] = semaphore.withPermit(effect)

object OperationMutex:
  val make: UIO[OperationMutex] = Semaphore.make(1).map(new OperationMutex(_))

final class DefaultAsyncOperationWorker(
    api: KubernetesApi,
    observer: ResourceObserver,
    verifier: VerificationEngine,
    store: OperationStore,
    mutex: OperationMutex,
    policy: PolicyEngine = new DefaultPolicyEngine,
    coordinator: ResourceCoordinator = InMemoryResourceCoordinator,
    scheduler: Option[Queue[OperationId]] = None
) extends AsyncOperationWorker:
  private val controlPlane = new DefaultFlinkControlPlane(store)
  private val verificationTimeout = sys.env.get("ZIO_FLINK_VERIFICATION_TIMEOUT_SECONDS").flatMap(_.toLongOption).filter(_ > 0).getOrElse(180L).seconds

  /** 创建幂等 operation，并按配置异步排队或启动后台 fiber。 */
  override def submit(requestId: RequestId, operation: FlinkOperation): IO[ControlPlaneError, AcceptedOperation] =
    for
      accepted <- controlPlane.accept(requestId, operation)
      _ <- scheduler match
        case Some(queue) => queue.offer(accepted.operationId).unit
        case None => process(accepted.operationId).forkDaemon.unit
    yield accepted

  /** 先获得跨副本租约，再重新读取操作；竞争失败的副本没有写审计事件的权限。 */
  override def process(id: OperationId): IO[ControlPlaneError, Operation] = mutex.withPermit {
    getRequired(id).flatMap { candidate =>
      if !AsyncOperationWorker.isActive(candidate) then ZIO.succeed(candidate)
      else coordinator.withLock(candidate.resource, id) {
        getRequired(id).flatMap { current =>
          val execute = current.state match
            case OperationState.Accepted =>
              transition(id, OperationEvent.ValidationStarted(Instant.now())).flatMap(runOperation(id, _))
            case OperationState.Validating if current.events.exists(_.isInstanceOf[OperationEvent.ValidationPassed]) =>
              // ValidationPassed 是持久化的写入边界：没有 Submitted 回执时无法排除请求已生效。
              ZIO.fail(ControlPlaneError.UncertainFailure("submission acknowledgement is missing; refusing to repeat a possible Kubernetes write"))
            case OperationState.Validating => runOperation(id, current)
            case OperationState.Submitted | OperationState.WaitingForObservation | OperationState.Reconciling | OperationState.Verifying =>
              resumeOperation(id, current)
            case _ => ZIO.succeed(current)
          execute.catchAll {
            // 持久化失败不能解释成业务失败；由下一轮扫描重新读取，再决定是否恢复或保留不确定。
            case error: ControlPlaneError.StoreFailure => ZIO.fail(error)
            case error => terminalize(id, error) *> ZIO.fail(error)
          }
        }
      }
    }
  }

  // Pod 重启或重复投递时，从已持久化的事件阶段恢复，而不是重新提交资源。
  private def resumeOperation(id: OperationId, current: Operation): IO[ControlPlaneError, Operation] =
    for
      _ <- current.state match
        // 将 Submitted 的两个合法推进集中在同一分支，便于审查恢复顺序。
        case OperationState.Submitted =>
          transition(id, OperationEvent.WaitingForObservation(Instant.now())).unit *>
            transition(id, OperationEvent.VerificationStarted(Instant.now())).unit
        case OperationState.WaitingForObservation | OperationState.Reconciling =>
          transition(id, OperationEvent.VerificationStarted(Instant.now())).unit
        case _ => ZIO.unit
      target = observationTarget(id, current)
      expectedGeneration = current.events.collect { case OperationEvent.Submitted(_, generation, _, _) => generation }.lastOption.flatten
      // 继续使用提交时记录的 generation，避免旧版本 READY 被误认成当前操作成功。
      evidence <- awaitVerification(current.command, target, expectedGeneration)
      _ <- transition(id, OperationEvent.Observed(Instant.now(), evidence.observedGeneration))
      _ <- recordFallback(id, current, evidence)
      result <- transition(id, OperationEvent.VerificationSucceeded(Instant.now(), Some(evidence.audit("recovered verification"))))
    yield result

  // 新 operation 的标准顺序：观察 -> 策略校验 -> 提交 -> 观察 -> 证据验证。
  private def runOperation(id: OperationId, current: Operation): IO[ControlPlaneError, Operation] =
    for
      observed <- currentObservation(current)
      validated <- policy.validate(current.command, observed)
      _ <- transition(id, OperationEvent.ValidationPassed(Instant.now()))
      // 只有策略验证通过后才允许对 Kubernetes 产生写副作用。
      response <- submitToKubernetes(id, validated.operation).mapError(submissionError)
      responseMetadata = metadata(response)
      _ <- transition(id, OperationEvent.Submitted(Instant.now(), responseMetadata._1, responseMetadata._2, responseMetadata._3))
      _ <- transition(id, OperationEvent.WaitingForObservation(Instant.now()))
      completed <- current.command match
        case FlinkOperation.Delete(_, _) =>
          // 删除的成功证据是 Deleted 事件，或确认 UID 已被新对象替换。
          for
            _ <- transition(id, OperationEvent.VerificationStarted(Instant.now()))
            deleteTarget = validated.operation match
              case FlinkOperation.Delete(target, _) => target
              case _ => current.resource
            _ <- if response == "already absent" then ZIO.unit else awaitDeletion(deleteTarget)
            result <- transition(id, OperationEvent.VerificationSucceeded(Instant.now()))
          yield result
        case _ =>
          val target = observationTarget(id, current).copy(uid = current.command match
            case FlinkOperation.Snapshot(_, _) => responseMetadata._3
            case _ => None)
          for
            _ <- transition(id, OperationEvent.VerificationStarted(Instant.now()))
            expectedGeneration = current.command match
              case FlinkOperation.Snapshot(_, _) => None
              case _ => metadata(response)._1
            // 快照依靠 Snapshot CR 的 UID/状态/路径，普通变更依靠 generation。
            evidence <- awaitVerification(current.command, target, expectedGeneration)
            _ <- transition(id, OperationEvent.Observed(Instant.now(), evidence.observedGeneration))
            _ <- recordFallback(id, current, evidence)
            result <- transition(id, OperationEvent.VerificationSucceeded(Instant.now(), Some(evidence.audit("verification succeeded"))))
          yield result
    yield completed

  private def getRequired(id: OperationId): IO[ControlPlaneError, Operation] =
    store.get(id).flatMap(ZIO.fromOption(_).orElseFail(ControlPlaneError.OperationNotFound(id)))

  private def currentObservation(operation: Operation): IO[ControlPlaneError, Option[ObservedJobState]] =
    operation.command match
      case FlinkOperation.Deploy(_) | FlinkOperation.Snapshot(_, _) => ZIO.succeed(None)
      case _ =>
        observer.observe(operation.resource.namespace, operation.resource.kind, Some(operation.resource.name.nameValue)).take(1).runHead
          .map(_.map(observation => observedState(operation.resource, Evidence.fromObservation(operation.resource.namespace, observation))))
          .mapError(error => ControlPlaneError.VerificationFailed(Option(error.getMessage).getOrElse(error.toString)))

  private def observationTarget(id: OperationId, operation: Operation): ResourceRef =
    operation.command match
      case FlinkOperation.Snapshot(target, policy) =>
        val uid = operation.events.collect { case OperationEvent.Submitted(_, _, _, value) => value }.lastOption.flatten
        ResourceRef(target.namespace, ResourceKind.StateSnapshot, policy.snapshotName.getOrElse(snapshotName(target.name, id)), uid)
      case _ => operation.resource

  private def snapshotName(target: DeploymentName, id: OperationId): DeploymentName =
    val suffix = s"-snapshot-${id.operationIdValue.take(8)}"
    val base = target.nameValue.take((63 - suffix.length).max(1))
    DeploymentName.unsafe((base + suffix).take(63))

  private def observedState(resource: ResourceRef, evidence: Evidence): ObservedJobState =
    ObservedJobState(resource, evidence.jobState, evidence.reconciliation, evidence.generation, evidence.observedGeneration, evidence.savepointDirectory, evidence.snapshotPath)

  private def awaitVerification(operation: FlinkOperation, target: ResourceRef, expectedGeneration: Option[Generation]): IO[ControlPlaneError, Evidence] =
    // 验证流会跳过“证据暂缺”的中间状态，只在明确成功或确定失败时停止。
    observer.observe(target.namespace, target.kind, Some(target.name.nameValue))
      .map(observation => Evidence.fromObservation(target.namespace, observation))
      .map { evidence =>
        verifier.verify(operation, target, evidence, expectedGeneration) match
          case Right(result) => Some(Right(result.evidence))
          // Job FAILED、Operator ERROR 等不可恢复状态立即结束等待。
          case Left(error) if deterministicFailure(evidence) => Some(Left(error))
          case Left(_) => None
      }
      .collectSome
      .runHead
      .mapError(error => ControlPlaneError.VerificationFailed(Option(error.getMessage).getOrElse(error.toString)))
      .flatMap {
        case Some(Right(evidence)) => ZIO.succeed(evidence)
        case Some(Left(error)) => ZIO.fail(error)
        case None => ZIO.fail(ControlPlaneError.VerificationFailed("verification stream completed before the submitted generation was observed"))
      }
      .timeoutFail(ControlPlaneError.VerificationTimedOut("verification timed out while waiting for the submitted generation"))(verificationTimeout)

  private def deterministicFailure(evidence: Evidence): Boolean =
    evidence.jobState == JobState.Failed || evidence.reconciliation == ReconciliationState.Error || evidence.snapshotState.exists { state =>
      state.toUpperCase match
        case "FAILED" | "ABANDONED" | "ERROR" => true
        case _ => false
    }

  private def awaitDeletion(target: ResourceRef): IO[ControlPlaneError, Unit] =
    observer.observe(target.namespace, target.kind, Some(target.name.nameValue))
      .filter { observation =>
        observation.eventType == cn.xuyinyin.flinklab.operator.watch.WatchEventType.Deleted ||
          (target.uid.nonEmpty && observation.uid.nonEmpty && observation.uid != target.uid.map(_.resourceUidValue))
      }
      .runHead
      .mapError(error => ControlPlaneError.VerificationFailed(Option(error.getMessage).getOrElse(error.toString)))
      .flatMap(value => ZIO.when(value.isEmpty)(ZIO.fail(ControlPlaneError.VerificationFailed("delete observation stream completed before the target was deleted"))))
      .timeoutFail(ControlPlaneError.VerificationTimedOut("delete verification timed out"))(verificationTimeout)
      .unit

  private def transition(id: OperationId, event: OperationEvent): IO[ControlPlaneError, Operation] = store.transition(id, event)

  private def recordFallback(id: OperationId, operation: Operation, evidence: Evidence): IO[ControlPlaneError, Unit] =
    operation.command match
      case FlinkOperation.Upgrade(_, _, policy) if policy.fallback == FallbackPolicy.AllowLastState && evidence.actualProtection.contains(ActualProtection.LastState) && !evidenceMatches(policy.protection, evidence.actualProtection.get) && !operation.events.exists(_.isInstanceOf[OperationEvent.FallbackDetected]) =>
        transition(id, OperationEvent.FallbackDetected(Instant.now(), policy.protection, ActualProtection.LastState)).unit
      case _ => ZIO.unit

  private def evidenceMatches(requested: StateProtection, actual: ActualProtection): Boolean =
    (requested, actual) match
      case (StateProtection.Stateless, ActualProtection.EmptyState) => true
      case (StateProtection.LastState, ActualProtection.LastState) => true
      case (StateProtection.Savepoint, ActualProtection.Savepoint(_)) => true
      case _ => false

  private def terminalize(id: OperationId, error: ControlPlaneError): IO[ControlPlaneError, Unit] =
    val event = error match
      case ControlPlaneError.VerificationTimedOut(reason) => OperationEvent.TimedOut(Instant.now(), reason)
      case ControlPlaneError.UncertainFailure(reason) => OperationEvent.Uncertain(Instant.now(), reason)
      case _ => OperationEvent.Failed(Instant.now(), error.message)
    store.get(id).flatMap {
      case Some(operation) if Set(OperationState.Completed, OperationState.Failed, OperationState.TimedOut, OperationState.Superseded, OperationState.Uncertain).contains(operation.state) => ZIO.unit
      case Some(_) => store.transition(id, event).unit
      case None => ZIO.unit
    }

  private def submissionError(error: Throwable): ControlPlaneError =
    error match
      case _: java.io.IOException => ControlPlaneError.UncertainFailure(Option(error.getMessage).getOrElse(error.toString))
      // 4xx 是 Kubernetes 已明确拒绝的结果，不能伪装成“未知”；只有限流或服务端/网络异常才保留 UNCERTAIN。
      case KubernetesApiError(status, _) if status == 429 || status >= 500 => ControlPlaneError.UncertainFailure(Option(error.getMessage).getOrElse(error.toString))
      case _: cn.xuyinyin.flinklab.kubernetes.KubernetesApiError => ControlPlaneError.VerificationFailed(Option(error.getMessage).getOrElse(error.toString))
      case _ => ControlPlaneError.VerificationFailed(Option(error.getMessage).getOrElse(error.toString))

  private def submitToKubernetes(id: OperationId, operation: FlinkOperation): IO[Throwable, String] = operation match
    case FlinkOperation.Deploy(spec) => api.apply(spec.namespace, spec.resource.render(), dryRun = false)
    case FlinkOperation.Upgrade(target, spec, _) => api.apply(target.namespace, spec.resource.render(), dryRun = false)
    case FlinkOperation.Suspend(target, policy) => api.patch(target.namespace, target.kind, target.name.nameValue, if policy == SuspendPolicy.KeepState then cn.xuyinyin.flinklab.operator.savepoint.SavepointPatch.suspend else """{"spec":{"job":{"state":"suspended","upgradeMode":"stateless"}}}""")
    case FlinkOperation.Resume(target) => api.patch(target.namespace, target.kind, target.name.nameValue, """{"spec":{"job":{"state":"running"}}}""")
    case FlinkOperation.Restart(target, policy) =>
      val fallback = policy.fallback == FallbackPolicy.AllowLastState
      api.patch(target.namespace, target.kind, target.name.nameValue, s"""{"spec":{"restartNonce":${java.lang.System.currentTimeMillis()},"job":{"upgradeMode":"${policy.protection.operatorValue}"},"flinkConfiguration":{"kubernetes.operator.job.upgrade.last-state-fallback.enabled":"$fallback"}}}""")
    case FlinkOperation.Snapshot(target, policy) =>
      val spec = FlinkStateSnapshotSpec(target.namespace, policy.snapshotName.getOrElse(snapshotName(target.name, id)), target.kind, target.name, policy.snapshotType)
      val checkName = policy.snapshotName match
        case None => ZIO.unit
        case Some(name) =>
          api.get(target.namespace, ResourceKind.StateSnapshot, name.nameValue).either.flatMap {
            case Left(KubernetesApiError(404, _)) => ZIO.unit
            case Left(error) => ZIO.fail(error)
            case Right(raw) if raw.trim.isEmpty => ZIO.unit
            case Right(_) => ZIO.fail(IllegalStateException(s"snapshot resource already exists: ${name.nameValue}"))
          }
      checkName *> api.apply(target.namespace, spec.resource.render(), dryRun = false)
    case FlinkOperation.Delete(target, _) =>
      api.delete(target.namespace, target.kind, target.name.nameValue, target.uid).catchSome { case KubernetesApiError(404, _) => ZIO.succeed("already absent") }

  private def metadata(response: String): (Option[Generation], Option[ResourceVersion], Option[ResourceUid]) =
    scala.util.Try(ujson.read(response)).toOption.flatMap(_.obj.get("metadata").flatMap(_.objOpt)).map { metadata =>
      val generation = metadata.get("generation").flatMap(value => value.numOpt.map(_.toLong).orElse(value.strOpt.flatMap(_.toLongOption))).flatMap(Generation.from(_).toOption)
      val resourceVersion = metadata.get("resourceVersion").flatMap(_.strOpt).flatMap(ResourceVersion.from(_).toOption)
      val uid = metadata.get("uid").flatMap(_.strOpt).flatMap(ResourceUid.from(_).toOption)
      (generation, resourceVersion, uid)
    }.getOrElse((None, None, None))

object AsyncOperationWorker:
  private[operation] def isActive(operation: Operation): Boolean =
    Set(OperationState.Accepted, OperationState.Validating, OperationState.Submitted, OperationState.WaitingForObservation, OperationState.Reconciling, OperationState.Verifying).contains(operation.state)

  val live: ZLayer[KubernetesApi & OperationStore & PolicyEngine & ResourceCoordinator, Nothing, AsyncOperationWorker] =
    ZLayer.scoped {
      for
        api <- ZIO.service[KubernetesApi]
        store <- ZIO.service[OperationStore]
        policy <- ZIO.service[PolicyEngine]
        coordinator <- ZIO.service[ResourceCoordinator]
        mutex <- OperationMutex.make
        queue <- Queue.dropping[OperationId](256)
        worker = new DefaultAsyncOperationWorker(api, cn.xuyinyin.flinklab.operator.observer.ResourceObserver.live(api), DefaultVerificationEngine, store, mutex, policy, coordinator, Some(queue))
        // 队列只负责低延迟调度，持久化 Operation 才是工作来源；满队列、进程退出都不会丢失请求。
        _ <- (store.list.flatMap(operations => ZIO.foreachDiscard(operations.filter(isActive))(operation => queue.offer(operation.id)))
          .catchAll(error => Console.printLineError("operation recovery scan failed: " + error.message).ignore) *>
          ZIO.sleep(5.seconds)).forever.forkScoped
        _ <- queue.take.flatMap(worker.process).catchAll {
          case _: ControlPlaneError.ResourceBusy => ZIO.unit // 另一副本持有租约，留给下一轮扫描。
          case error => Console.printLineError(s"operation worker failed: ${error.message}").ignore
        }.forever.forkScoped
      yield worker
    }

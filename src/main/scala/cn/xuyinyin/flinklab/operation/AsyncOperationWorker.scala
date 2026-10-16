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
  def readiness: UIO[Boolean] = ZIO.succeed(true)

/** 同一资源在本进程内互斥，不阻塞其他资源；跨副本仍由持久化租约协调。 */
final class OperationMutex(private val semaphore: Semaphore):
  def withPermit[A](effect: => IO[ControlPlaneError, A]): IO[ControlPlaneError, A] = semaphore.withPermit(effect)
  private val active = java.util.concurrent.ConcurrentHashMap.newKeySet[String]()
  def withResource[A](resource: ResourceRef)(effect: => IO[ControlPlaneError, A]): IO[ControlPlaneError, A] =
    val key = s"${resource.namespace.namespaceValue}/${resource.kind}/${resource.name.nameValue}"
    ZIO.acquireReleaseWith(
      ZIO.succeed(active.add(key)).flatMap(acquired => ZIO.fail(ControlPlaneError.ResourceBusy(resource)).unless(acquired))
    )(_ => ZIO.succeed(active.remove(key)).unit)(_ => effect)

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
    scheduler: Option[Queue[OperationId]] = None,
    enqueue: Option[OperationId => UIO[Unit]] = None
) extends AsyncOperationWorker:
  private val controlPlane = new DefaultFlinkControlPlane(store)
  private val verificationTimeout = sys.env.get("ZIO_FLINK_VERIFICATION_TIMEOUT_SECONDS").flatMap(_.toLongOption).filter(_ > 0).getOrElse(180L).seconds

  /** 创建幂等 operation，并按配置异步排队或启动后台 fiber。 */
  override def submit(requestId: RequestId, operation: FlinkOperation): IO[ControlPlaneError, AcceptedOperation] =
    for
      accepted <- controlPlane.accept(requestId, operation)
      _ <- enqueue match
        case Some(schedule) => schedule(accepted.operationId)
        case None => scheduler match
          case Some(queue) => queue.offer(accepted.operationId).unit
          case None => process(accepted.operationId).forkDaemon.unit
    yield accepted

  /** 先获得跨副本租约，再重新读取操作；竞争失败的副本没有写审计事件的权限。 */
  override def process(id: OperationId): IO[ControlPlaneError, Operation] =
    getRequired(id).flatMap { candidate =>
      if !AsyncOperationWorker.isActive(candidate) then ZIO.succeed(candidate)
      else mutex.withResource(candidate.resource) { coordinator.withLock(candidate.resource, id) {
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
      }}
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
      expectedGeneration = current.command match
        case FlinkOperation.Snapshot(_, _) => None
        case _ => current.events.collect { case OperationEvent.Submitted(_, generation, _, _) => generation }.lastOption.flatten
      _ <- current.command match
        case FlinkOperation.Delete(_, _) => ZIO.unit // 历史删除回执即使缺少 UID，GET 确认不存在仍是充分证据。
        case _ => ensureSubmittedIdentity(current.command, target, expectedGeneration)
      result <- current.command match
        case FlinkOperation.Delete(_, _) =>
          awaitDeletion(target) *> transition(id, OperationEvent.VerificationSucceeded(Instant.now()))
        case _ =>
          for
            evidence <- awaitVerification(current.command, target, expectedGeneration)
            _ <- transition(id, OperationEvent.Observed(Instant.now(), evidence.observedGeneration))
            _ <- recordFallback(id, current, evidence)
            completed <- transition(id, OperationEvent.VerificationSucceeded(Instant.now(), Some(evidence.audit("recovered verification"))))
          yield completed
    yield result

  // 新 operation 的标准顺序：观察 -> 策略校验 -> 提交 -> 观察 -> 证据验证。
  private def runOperation(id: OperationId, current: Operation): IO[ControlPlaneError, Operation] =
    for
      observation <- getObservation(current.resource)
      observed = observation.map { value =>
        val evidence = Evidence.fromObservation(current.resource.namespace, value)
        observedState(evidence.resource, evidence)
      }
      validated <- policy.validate(current.command, observed)
      _ <- validateMutationIdentity(current, observed)
      _ <- ZIO.fail(ControlPlaneError.InvalidPolicy("observed mutation target resourceVersion is required")).when(observation.exists(_.resourceVersion.isEmpty))
      _ <- transition(id, OperationEvent.ValidationPassed(Instant.now()))
      // 只有策略验证通过后才允许对 Kubernetes 产生写副作用。
      response <- submitToKubernetes(id, validated.operation, observed, observation.flatMap(_.resourceVersion)).mapError(submissionError)
        .timeoutFail(ControlPlaneError.UncertainFailure("Kubernetes submission timed out; refusing to repeat a possible write"))(verificationTimeout)
      responseMetadata = current.command match
        case FlinkOperation.Delete(_, _) => (None, None, observed.flatMap(_.resource.uid).orElse(current.resource.uid))
        case _ => metadata(response)
      _ <- current.command match
        case FlinkOperation.Delete(_, _) if response == "already absent" => ZIO.unit
        case _ => ensureSubmittedIdentity(current.command, current.resource.copy(uid = responseMetadata._3), responseMetadata._1)
      _ <- current.command match
        case FlinkOperation.Snapshot(_, _) | FlinkOperation.Delete(_, _) => ZIO.unit
        case _ => ZIO.fail(ControlPlaneError.UncertainFailure("mutation response UID differs from the validated target"))
          .when(observed.exists(value => responseMetadata._3 != value.resource.uid))
      _ <- transition(id, OperationEvent.Submitted(Instant.now(), responseMetadata._1, responseMetadata._2, responseMetadata._3))
      _ <- transition(id, OperationEvent.WaitingForObservation(Instant.now()))
      completed <- current.command match
        case FlinkOperation.Delete(_, _) =>
          // 删除的成功证据是 GET 确认不存在，或原 UID 已被新对象替换。
          for
            _ <- transition(id, OperationEvent.VerificationStarted(Instant.now()))
            deleteTarget = current.resource.copy(uid = responseMetadata._3)
            _ <- if response == "already absent" then ZIO.unit else awaitDeletion(deleteTarget)
            result <- transition(id, OperationEvent.VerificationSucceeded(Instant.now()))
          yield result
        case _ =>
          val target = observationTarget(id, current).copy(uid = responseMetadata._3)
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

  private def getObservation(target: ResourceRef): IO[ControlPlaneError, Option[cn.xuyinyin.flinklab.operator.observer.ResourceObservation]] =
    api.get(target.namespace, target.kind, target.name.nameValue)
      .flatMap(ResourceObserver.fromGet(target.kind, _)).map(Some(_))
      .catchSome { case KubernetesApiError(404, _) => ZIO.succeed(None) }
      .mapError(error => ControlPlaneError.VerificationFailed(Option(error.getMessage).getOrElse(error.toString)))
      .timeoutFail(ControlPlaneError.VerificationTimedOut("Kubernetes GET timed out"))(verificationTimeout)

  private def validateMutationIdentity(operation: Operation, observed: Option[ObservedJobState]): IO[ControlPlaneError, Unit] =
    val mayBeAbsent = operation.command match
      case FlinkOperation.Deploy(_) | FlinkOperation.Delete(_, _) => true
      case _ => false
    for
      _ <- ZIO.fail(ControlPlaneError.InvalidPolicy("operation target does not exist")).when(observed.isEmpty && !mayBeAbsent)
      _ <- ZIO.fail(ControlPlaneError.InvalidPolicy("observed resource identity does not match operation target")).when(observed.exists(value => value.resource.namespace != operation.resource.namespace || value.resource.kind != operation.resource.kind || value.resource.name != operation.resource.name))
      _ <- ZIO.fail(ControlPlaneError.InvalidPolicy("observed mutation target UID is required")).when(observed.exists(_.resource.uid.isEmpty))
      _ <- ZIO.fail(ControlPlaneError.InvalidPolicy("operation target UID does not match the observed resource")).when(operation.resource.uid.nonEmpty && observed.exists(_.resource.uid != operation.resource.uid))
    yield ()

  private def ensureSubmittedIdentity(operation: FlinkOperation, target: ResourceRef, generation: Option[Generation]): IO[ControlPlaneError, Unit] =
    val needsGeneration = operation match
      case FlinkOperation.Snapshot(_, _) | FlinkOperation.Delete(_, _) => false
      case _ => true
    ZIO.fail(ControlPlaneError.UncertainFailure("submitted resource identity metadata is missing; refusing unbound verification"))
      .when(target.uid.isEmpty || (needsGeneration && generation.isEmpty)).unit

  private def observationTarget(id: OperationId, operation: Operation): ResourceRef =
    operation.command match
      case FlinkOperation.Snapshot(target, policy) =>
        val uid = operation.events.collect { case OperationEvent.Submitted(_, _, _, value) => value }.lastOption.flatten
        ResourceRef(target.namespace, ResourceKind.StateSnapshot, policy.snapshotName.getOrElse(snapshotName(target.name, id)), uid)
      case _ =>
        val uid = operation.events.collect { case OperationEvent.Submitted(_, _, _, value) => value }.lastOption.flatten
        operation.resource.copy(uid = uid)

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
          case Left(error) if (target.uid.nonEmpty && evidence.uid != target.uid) || deterministicFailure(evidence) => Some(Left(deterministicError(evidence, error)))
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
    def poll: IO[ControlPlaneError, Unit] =
      getObservation(target).flatMap {
        case None => ZIO.unit
        case Some(observation) if target.uid.nonEmpty && observation.uid.nonEmpty && observation.uid != target.uid.map(_.resourceUidValue) => ZIO.unit
        case Some(_) if target.uid.isEmpty => ZIO.fail(ControlPlaneError.UncertainFailure("delete submission UID is missing and the target still exists"))
        case Some(_) => ZIO.sleep(100.millis) *> poll
      }
    poll.timeoutFail(ControlPlaneError.VerificationTimedOut("delete verification timed out"))(verificationTimeout)

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

  /** Operator 已明确拒绝时优先保留 CR 的 error 字段，避免被 generation 校验遮蔽。 */
  private def deterministicError(evidence: Evidence, fallback: ControlPlaneError): ControlPlaneError =
    val message = scala.util.Try(ujson.read(evidence.raw)).toOption
      .flatMap(_.obj.get("status").flatMap(_.objOpt))
      .flatMap(_.get("error").flatMap(_.strOpt))
      .filter(_.trim.nonEmpty)
      .getOrElse(fallback.message)
    ControlPlaneError.VerificationFailed(message)

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

  private def submitToKubernetes(id: OperationId, operation: FlinkOperation, observed: Option[ObservedJobState], resourceVersion: Option[String]): IO[Throwable, String] =
    def bound(raw: String): String =
      val value = ujson.read(raw)
      observed.foreach { current =>
        val fields = value.obj.getOrElseUpdate("metadata", ujson.Obj())
        current.resource.uid.foreach(uid => fields("uid") = uid.resourceUidValue)
        resourceVersion.foreach(rv => fields("resourceVersion") = rv)
      }
      value.render()
    operation match
      case FlinkOperation.Deploy(spec) =>
        if observed.isEmpty then api.create(spec.namespace, spec.resource.render())
        else api.apply(spec.namespace, bound(spec.resource.render()), dryRun = false)
      case FlinkOperation.Upgrade(target, spec, _) => api.apply(target.namespace, bound(spec.resource.render()), dryRun = false)
      case FlinkOperation.Suspend(target, policy) => api.patch(target.namespace, target.kind, target.name.nameValue, bound(if policy == SuspendPolicy.KeepState then cn.xuyinyin.flinklab.operator.savepoint.SavepointPatch.suspend else """{"spec":{"job":{"state":"suspended","upgradeMode":"stateless"}}}"""))
      case FlinkOperation.Resume(target) => api.patch(target.namespace, target.kind, target.name.nameValue, bound("""{"spec":{"job":{"state":"running"}}}"""))
      case FlinkOperation.Restart(target, policy) =>
        val fallback = policy.fallback == FallbackPolicy.AllowLastState
        api.patch(target.namespace, target.kind, target.name.nameValue, bound(s"""{"spec":{"restartNonce":${java.lang.System.currentTimeMillis()},"job":{"upgradeMode":"${policy.protection.operatorValue}"},"flinkConfiguration":{"kubernetes.operator.job.upgrade.last-state-fallback.enabled":"$fallback"}}}"""))
      case FlinkOperation.Snapshot(target, policy) =>
        val spec = FlinkStateSnapshotSpec(target.namespace, policy.snapshotName.getOrElse(snapshotName(target.name, id)), target.kind, target.name, policy.snapshotType)
        // POST 原子地拒绝名称冲突，避免 GET 与创建之间的竞态覆盖已有快照。
        api.create(target.namespace, spec.resource.render())
      case FlinkOperation.Delete(target, _) =>
        if observed.isEmpty then ZIO.succeed("already absent")
        else api.delete(target.namespace, target.kind, target.name.nameValue, observed.flatMap(_.resource.uid).orElse(target.uid)).catchSome { case KubernetesApiError(404, _) => ZIO.succeed("already absent") }
  private def metadata(response: String): (Option[Generation], Option[ResourceVersion], Option[ResourceUid]) =
    scala.util.Try(ujson.read(response).obj.get("metadata").flatMap(_.objOpt)).toOption.flatten.map { metadata =>
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
        pending <- Ref.make(Set.empty[OperationId])
        cursor <- Ref.make(0)
        recoveryHealthy <- Ref.make(false)
        enqueue = (id: OperationId) =>
          (pending.modify(ids => if ids.contains(id) then (false, ids) else (true, ids + id)).flatMap { fresh =>
            if !fresh then ZIO.unit
            else queue.offer(id).flatMap(offered => ZIO.unless(offered)(pending.update(_ - id))).unit
          }).uninterruptible
        worker = new DefaultAsyncOperationWorker(api, ResourceObserver.live(api), DefaultVerificationEngine, store, mutex, policy, coordinator, Some(queue), Some(enqueue))
        // 同时去重排队与执行中的 ID；轮换扫描起点，让满队列溢出的操作有机会入队。
        scanner <- (store.listActive.timeoutFail(ControlPlaneError.StoreFailure("operation recovery scan timed out"))(10.seconds).flatMap { operations =>
          val active = operations.filter(isActive)
          cursor.modify { previous =>
            val offset = if active.isEmpty then 0 else previous % active.size
            (active.drop(offset) ++ active.take(offset), offset + 256)
          }.flatMap(ordered => ZIO.foreachDiscard(ordered)(operation => enqueue(operation.id))) *> recoveryHealthy.set(true)
        }.catchAll(error => recoveryHealthy.set(false) *> Console.printLineError("operation recovery scan failed: " + error.message).ignore) *>
          ZIO.sleep(5.seconds)).forever.forkScoped
        parallelism = sys.env.get("ZIO_FLINK_WORKER_PARALLELISM").flatMap(_.toIntOption).getOrElse(4).max(1).min(64)
        consumers <- ZIO.foreach(1 to parallelism) { _ =>
          queue.take.flatMap { id =>
            worker.process(id).catchAll {
              case _: ControlPlaneError.ResourceBusy => ZIO.unit // 下一轮持久化扫描重试租约竞争。
              case error => Console.printLineError(s"operation worker failed: ${error.message}").ignore
            }.ensuring(pending.update(_ - id))
          }.forever.forkScoped
        }
      yield new AsyncOperationWorker:
        def submit(requestId: RequestId, operation: FlinkOperation) = worker.submit(requestId, operation)
        def process(id: OperationId) = worker.process(id)
        override def readiness: UIO[Boolean] =
          for
            healthy <- recoveryHealthy.get
            scan <- scanner.poll
            workers <- ZIO.foreach(consumers)(_.poll)
          yield healthy && scan.isEmpty && workers.forall(_.isEmpty)
    }

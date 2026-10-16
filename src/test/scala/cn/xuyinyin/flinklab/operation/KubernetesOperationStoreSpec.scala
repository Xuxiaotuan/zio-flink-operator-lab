package cn.xuyinyin.flinklab.operation
/** 验证 FlinkOperation CR 的持久化和 resourceVersion 更新契约。 */

import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.kubernetes.KubernetesApi
import zio.*
import zio.test.*

import java.time.Instant

object KubernetesOperationStoreSpec extends ZIOSpecDefault:
  private val namespace = Namespace.unsafe(sys.env.getOrElse("FLINK_NAMESPACE", "default"))
  private val name = DeploymentName.unsafe("orders")
  private val target = ResourceRef(namespace, ResourceKind.Deployment, name)
  private val operation = Operation.accepted(RequestId.from("req-k8s").toOption.get, FlinkOperation.Resume(target), target, Instant.parse("2026-10-03T00:00:00Z"))

  def spec = suite("kubernetes operation store")(
    test("finds a request by its deterministic resource name without listing operation history") {
      for
        names <- Ref.make(List.empty[String])
        raw <- Ref.make(KubernetesOperationResource.render(operation))
        api = new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.fail(UnsupportedOperationException())
          def get(namespace: Namespace, kind: ResourceKind, name: String) = names.update(_ :+ name) *> raw.get
          override def list(namespace: Namespace, kind: ResourceKind) = ZIO.fail(IllegalStateException("history must not be listed"))
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.fail(UnsupportedOperationException())
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.fail(UnsupportedOperationException())
        store = new KubernetesOperationStore(api)
        matched <- store.findByRequestId(operation.requestId).either
        _ <- raw.set(KubernetesOperationResource.render(operation.copy(requestId = RequestId.generate())))
        mismatched <- store.findByRequestId(operation.requestId).either
        requestedNames <- names.get
      yield assertTrue(matched.contains(Some(operation)), mismatched.contains(None),
        requestedNames == List.fill(2)(OperationId.forRequest(operation.requestId).operationIdValue))
    },
    test("lists indexed active operations and unlabeled legacy operations without listing history") {
      val legacy = Operation.accepted(RequestId.from("legacy-request").toOption.get, FlinkOperation.Resume(target), target, Instant.now())
      // 终态也必须有合法历史，不能直接修改 state 伪造完成。
      val terminal = legacy.advance(OperationEvent.Failed(Instant.now(), "historical terminal")).toOption.get.copy(id = OperationId.generate())
      def response(values: Operation*) = ujson.Obj("items" -> ujson.Arr.from(values.map(value => ujson.read(KubernetesOperationResource.render(value))))).render()
      for
        selectors <- Ref.make(List.empty[Option[String]])
        api = new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = ZIO.fail(UnsupportedOperationException())
          def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.fail(UnsupportedOperationException())
          override def list(namespace: Namespace, kind: ResourceKind) = ZIO.fail(IllegalStateException("unfiltered history must not be listed"))
          override def list(namespace: Namespace, kind: ResourceKind, selector: Option[String]) =
            selectors.update(_ :+ selector) *> (selector match
              case Some("xxt.io/operation-active=true") => ZIO.succeed(response(operation))
              case Some("!xxt.io/operation-active") => ZIO.succeed(response(operation, legacy, terminal))
              case _ => ZIO.fail(IllegalArgumentException("unexpected selector")))
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.fail(UnsupportedOperationException())
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.fail(UnsupportedOperationException())
        result <- new KubernetesOperationStore(api).listActive.either
        requestedSelectors <- selectors.get
      yield assertTrue(result.map(_.map(_.id).toSet).contains(Set(operation.id, legacy.id)), result.exists(_.size == 2),
        requestedSelectors == List(Some("xxt.io/operation-active=true"), Some("!xxt.io/operation-active")))
    },
    test("renders a queryable active label for every lifecycle state") {
      val terminalStates = Set(OperationState.Completed, OperationState.Failed, OperationState.Superseded, OperationState.TimedOut, OperationState.Uncertain)
      val labels = OperationState.values.toList.map { state =>
        val json = ujson.read(KubernetesOperationResource.render(operation.copy(state = state)))
        json("metadata")("labels").obj.get("xxt.io/operation-active").flatMap(_.strOpt).contains((!terminalStates.contains(state)).toString)
      }
      assertTrue(labels.forall(identity))
    },
    test("rejects a target outside the configured operation namespace before persistence") {
      val foreignNamespace = Namespace.unsafe(if namespace.namespaceValue == "other" then "foreign" else "other")
      val foreignTarget = target.copy(namespace = foreignNamespace)
      val foreignOperation = Operation.accepted(RequestId.generate(), FlinkOperation.Resume(foreignTarget), foreignTarget, Instant.now())
      for
        writes <- Ref.make(0)
        api = new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = writes.update(_ + 1).as(resource)
          override def create(namespace: Namespace, resource: String) = writes.update(_ + 1).as(resource)
          def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.fail(UnsupportedOperationException())
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.fail(UnsupportedOperationException())
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = ZIO.fail(UnsupportedOperationException())
        result <- new KubernetesOperationStore(api).create(foreignOperation).either
        count <- writes.get
      yield assertTrue(result.left.exists(_.message.contains("namespace")), count == 0)
    },
    test("hashes arbitrary request ids into valid stable Kubernetes label values") {
      val requestId = RequestId.from("client:/" + "long-request/" * 12).toOption.get
      val value = operation.copy(requestId = requestId)
      val first = ujson.read(KubernetesOperationResource.render(value))("metadata")("labels")("xxt.io/request-id").str
      val second = ujson.read(KubernetesOperationResource.render(value))("metadata")("labels")("xxt.io/request-id").str
      assertTrue(first.length <= 63, first.matches("[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?"), first == second,
        KubernetesOperationResource.parse(KubernetesOperationResource.render(value)).contains(value))
    },
    test("rejects corrupt persisted history before any transition write") {
      val value = ujson.read(KubernetesOperationResource.render(operation))
      value("spec")("operation")("state") = "COMPLETED"
      for
        writes <- Ref.make(0)
        api = new KubernetesApi:
          def apply(namespace: Namespace, resource: String, dryRun: Boolean) = writes.update(_ + 1).as(resource)
          def get(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.succeed(value.render())
          override def list(namespace: Namespace, kind: ResourceKind) = ZIO.succeed(ujson.Obj("items" -> ujson.Arr(value)).render())
          def delete(namespace: Namespace, kind: ResourceKind, name: String) = ZIO.fail(UnsupportedOperationException())
          def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String) = writes.update(_ + 1).as(patch)
        store = new KubernetesOperationStore(api)
        loaded <- store.get(operation.id).either
        listed <- store.list.either
        advanced <- store.transition(operation.id, OperationEvent.ValidationStarted(Instant.now())).either
        count <- writes.get
      yield assertTrue(List(loaded, listed, advanced).forall(_.left.exists(_.isInstanceOf[ControlPlaneError.StoreFailure])), count == 0)
    },
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

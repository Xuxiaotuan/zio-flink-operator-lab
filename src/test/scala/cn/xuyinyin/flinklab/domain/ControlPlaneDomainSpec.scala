package cn.xuyinyin.flinklab.domain
/** 验证领域模型、事件审计 JSON 和操作状态机。 */

import cn.xuyinyin.flinklab.domain.ControlPlaneError.InvalidTransition
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import cn.xuyinyin.flinklab.operation.OperationCodec
import zio.Scope
import zio.test.*

import java.time.Instant

object ControlPlaneDomainSpec extends ZIOSpecDefault:
  private val namespace = Namespace.unsafe("analytics")
  private val name = DeploymentName.unsafe("orders")
  private val resource = ResourceRef(namespace, ResourceKind.Deployment, name)

  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("control plane domain")(
      test("renders a typed state protection as the operator upgrade mode") {
        val jar = JobJarUri.unsafe("local:///job.jar")
        val job = FlinkJob(jar, "example.WordCount", 2, StateProtection.Savepoint)
        assertTrue(job.json("upgradeMode").str == "savepoint")
      },
      test("round trips last-state operations from the Kubernetes operation CR") {
        val spec = FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.WordCount", 1, StateProtection.LastState))
        val operation = Operation.accepted(RequestId.from("req-last-state").toOption.get, FlinkOperation.Upgrade(resource, spec, UpgradePolicy(StateProtection.LastState, FallbackPolicy.AllowLastState)), resource, Instant.parse("2026-10-03T00:00:00Z"))
        assertTrue(OperationCodec.fromJson(OperationCodec.json(operation)).isRight)
      },
      test("round trips the savepoint redeploy nonce in an operation CR") {
        val job = FlinkJob(
          JobJarUri.unsafe("local:///job.jar"),
          "example.WordCount",
          1,
          StateProtection.Savepoint,
          initialSavepointPath = Some(SnapshotPath.from("s3://bucket/savepoint-1").toOption.get),
          savepointRedeployNonce = Some(2L)
        )
        val deployment = FlinkDeploymentSpec(namespace, name, "flink:1.20.1", "v1_20", job)
        val operation = Operation.accepted(RequestId.from("req-savepoint-redeploy").toOption.get, FlinkOperation.Deploy(deployment), resource, Instant.parse("2026-10-03T00:00:00Z"))
        val encoded = ujson.read(OperationCodec.json(operation))
        val restored = OperationCodec.fromJson(encoded.render())
        assertTrue(
          encoded("command")("spec")("spec")("job")("savepointRedeployNonce").num == 2,
          restored.toOption.exists {
            case Operation(_, _, FlinkOperation.Deploy(spec), _, _, _, _, _) =>
              spec.job.initialSavepointPath.exists(_.snapshotPathValue == "s3://bucket/savepoint-1") && spec.job.savepointRedeployNonce.contains(2L)
            case _ => false
          }
        )
      },
      test("rejects an invalid stateless fallback policy") {
        assertTrue(UpgradePolicy(StateProtection.Stateless, FallbackPolicy.AllowLastState).validate.isLeft)
      },
      test("keeps operation transitions explicit") {
        val now = Instant.parse("2026-10-03T00:00:00Z")
        val operation = Operation.accepted(RequestId.from("req-1").toOption.get, FlinkOperation.Resume(resource), resource, now)
        val started = operation.advance(OperationEvent.ValidationStarted(now.plusSeconds(1)))
        val illegal = operation.advance(OperationEvent.VerificationSucceeded(now.plusSeconds(1)))
        assertTrue(
          started.toOption.exists(_.state == OperationState.Validating),
          illegal.left.exists(_.isInstanceOf[InvalidTransition])
        )
      },
      test("serializes operation history as audit data") {
        val now = Instant.parse("2026-10-03T00:00:00Z")
        val operation = Operation.accepted(RequestId.from("req-1").toOption.get, FlinkOperation.Resume(resource), resource, now)
        val json = operation.json
        assertTrue(
          json("operationId").str.nonEmpty,
          json("requestId").str == "req-1",
          json("events").arr.head("type").str == "ACCEPTED"
        )
      },
      test("persists submitted identity and verification evidence") {
        val now = Instant.parse("2026-10-03T00:00:00Z")
        val uid = ResourceUid.from("snapshot-uid").toOption.get
        val operation = Operation.accepted(RequestId.from("req-evidence").toOption.get, FlinkOperation.Resume(resource), resource, now)
        val submitted = operation.advance(OperationEvent.ValidationStarted(now.plusSeconds(1))).toOption.get
          .advance(OperationEvent.ValidationPassed(now.plusSeconds(2))).toOption.get
          .advance(OperationEvent.Submitted(now.plusSeconds(3), Generation.from(4).toOption, ResourceVersion.from("7").toOption, Some(uid))).toOption.get
          .advance(OperationEvent.WaitingForObservation(now.plusSeconds(4))).toOption.get
          .advance(OperationEvent.VerificationStarted(now.plusSeconds(5))).toOption.get
          .advance(OperationEvent.VerificationSucceeded(now.plusSeconds(6), Some(VerificationEvidence(resource.copy(uid = Some(uid)), ResourceVersion.from("8").toOption, Some(uid), Generation.from(4).toOption, Generation.from(4).toOption, None, None, "job is running")))).toOption.get
        val restored = OperationCodec.fromJson(OperationCodec.json(submitted))
        assertTrue(
          submitted.events.exists(_.toString.contains("snapshot-uid")),
          restored.isRight,
          restored.toOption.exists(_.events.exists(_.toString.contains("job is running")))
        )
      }
    )

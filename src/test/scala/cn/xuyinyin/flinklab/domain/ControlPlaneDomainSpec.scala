package cn.xuyinyin.flinklab.domain

import cn.xuyinyin.flinklab.cli.ResourceKind
import cn.xuyinyin.flinklab.domain.ControlPlaneError.InvalidTransition
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
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
      }
    )

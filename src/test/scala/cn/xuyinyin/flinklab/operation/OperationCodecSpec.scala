package cn.xuyinyin.flinklab.operation

import cn.xuyinyin.flinklab.domain.*
import cn.xuyinyin.flinklab.domain.FlinkTypes.*
import zio.test.*

import java.time.Instant

/** 持久化边界的回归测试：不允许重启后改变意图，或把损坏历史当成成功。 */
object OperationCodecSpec extends ZIOSpecDefault:
  private val at = Instant.parse("2026-10-16T00:00:00Z")
  private val ref = ResourceRef(Namespace.unsafe("analytics"), ResourceKind.Deployment, DeploymentName.unsafe("orders"))
  private def accepted(command: FlinkOperation) = Operation.accepted(RequestId.from("codec-test").toOption.get, command, ref, at)
  private def encoded = ujson.read(OperationCodec.json(accepted(FlinkOperation.Resume(ref))))
  private def completed = List[OperationEvent](
    OperationEvent.ValidationStarted(at.plusSeconds(1)), OperationEvent.ValidationPassed(at.plusSeconds(2)),
    OperationEvent.Submitted(at.plusSeconds(3), None, None), OperationEvent.WaitingForObservation(at.plusSeconds(4)),
    OperationEvent.VerificationStarted(at.plusSeconds(5)),
    OperationEvent.VerificationSucceeded(at.plusSeconds(6), Some(VerificationEvidence(ref, None, None, None, None, None, None, "running")))
  ).foldLeft(accepted(FlinkOperation.Resume(ref)))((op, event) => op.advance(event).toOption.get)

  def spec = suite("operation codec persistence contract")(
    test("preserves every suspend and delete policy") {
      val commands = SuspendPolicy.values.toList.map(FlinkOperation.Suspend(ref, _)) ++ DeletePolicy.values.toList.map(FlinkOperation.Delete(ref, _))
      assertTrue(commands.forall(command => OperationCodec.fromJson(OperationCodec.json(accepted(command))).toOption.exists(_.command == command)))
    },
    test("versions new records and rejects unsupported versions") {
      val value = encoded
      val version = value.obj.get("schemaVersion")
      value("schemaVersion") = 999
      assertTrue(version.contains(ujson.Num(1)), OperationCodec.fromJson(value.render()).isLeft)
    },
    test("reads the documented versionless policy defaults only for legacy records") {
      val value = ujson.read(OperationCodec.json(accepted(FlinkOperation.Suspend(ref, SuspendPolicy.KeepState))))
      value.obj.remove("schemaVersion")
      value("command").obj.remove("policy")
      val legacy = OperationCodec.fromJson(value.render())
      value("schemaVersion") = 1
      assertTrue(legacy.toOption.exists(_.command == FlinkOperation.Suspend(ref, SuspendPolicy.KeepState)), OperationCodec.fromJson(value.render()).isLeft)
    },
    test("keeps legacy delete and submitted identity sentinels readable") {
      val value = ujson.read(OperationCodec.json(accepted(FlinkOperation.Delete(ref, DeletePolicy.Graceful))))
      value.obj.remove("schemaVersion")
      value("command").obj.remove("policy")
      val history = ujson.read(OperationCodec.json(completed))
      history.obj.remove("schemaVersion")
      assertTrue(
        OperationCodec.fromJson(value.render()).toOption.exists(_.command == FlinkOperation.Delete(ref, DeletePolicy.Graceful)),
        OperationCodec.fromJson(history.render()) == Right(completed)
      )
    },
    test("preserves protection, fallback, target identity and snapshot options") {
      val identified = ref.copy(uid = ResourceUid.from("original-uid").toOption)
      val spec = FlinkDeploymentSpec(ref.namespace, ref.name, "flink:1.20.1", "v1_20", FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.Job", 2))
      val policies = for protection <- StateProtection.values.toList; fallback <- FallbackPolicy.values.toList if UpgradePolicy(protection, fallback).validate.isRight yield UpgradePolicy(protection, fallback)
      val commands = policies.flatMap(policy => List(FlinkOperation.Upgrade(identified, spec, policy), FlinkOperation.Restart(identified, policy))) ++
        SnapshotType.values.toList.map(kind => FlinkOperation.Snapshot(identified, SnapshotPolicy(kind, Some(DeploymentName.unsafe("orders-snapshot")))))
      assertTrue(commands.forall(command => OperationCodec.fromJson(OperationCodec.json(accepted(command))).toOption.exists(_.command == command)))
    },
    test("rejects unknown explicit policies instead of falling back") {
      val values = List(FlinkOperation.Suspend(ref, SuspendPolicy.KeepState), FlinkOperation.Delete(ref, DeletePolicy.Graceful), FlinkOperation.Restart(ref, UpgradePolicy(StateProtection.Savepoint, FallbackPolicy.Forbidden))).map { command =>
        val value = ujson.read(OperationCodec.json(accepted(command)))
        value("command")(if command.isInstanceOf[FlinkOperation.Restart] then "fallback" else "policy") = "FUTURE_POLICY"
        OperationCodec.fromJson(value.render())
      }
      assertTrue(values.forall(_.isLeft))
    },
    test("rejects unknown events rather than manufacturing uncertainty") {
      val value = encoded
      value("events")(0)("type") = "FUTURE_EVENT"
      assertTrue(OperationCodec.fromJson(value.render()).isLeft)
    },
    test("replays history and checks the stored state and timestamps") {
      val values = List("state", "createdAt", "updatedAt").map { field =>
        val value = encoded
        value(field) = (if field == "state" then "COMPLETED" else at.plusSeconds(30).toString)
        OperationCodec.fromJson(value.render())
      }
      val empty = encoded
      empty("events") = ujson.Arr()
      val illegal = encoded
      illegal("events").arr += ujson.Obj("type" -> "VERIFICATION_SUCCEEDED", "at" -> at.toString)
      assertTrue(values.forall(_.isLeft), OperationCodec.fromJson(empty.render()).isLeft, OperationCodec.fromJson(illegal.render()).isLeft)
    },
    test("round trips a complete history including absent submitted identity") {
      assertTrue(OperationCodec.fromJson(OperationCodec.json(completed)) == Right(completed))
    },
    test("rejects corrupt present evidence and optional identity fields") {
      val badEvidence = ujson.read(OperationCodec.json(completed))
      badEvidence("events").arr.last("evidence") = ujson.Obj("reason" -> "missing resource")
      val badUid = encoded
      badUid("resource")("uid") = " "
      val badGeneration = ujson.read(OperationCodec.json(completed))
      badGeneration("events")(3)("generation") = 1.5
      val badEvidenceUid = ujson.read(OperationCodec.json(completed))
      badEvidenceUid("events").arr.last("evidence")("uid") = ujson.Null
      assertTrue(List(badEvidence, badUid, badGeneration, badEvidenceUid).forall(value => OperationCodec.fromJson(value.render()).isLeft))
    },
    test("rejects invalid evidence generations and fallback event values") {
      val invalid = List[ujson.Value](ujson.Num(-1), ujson.Num(1.5), ujson.Str("9223372036854775808"), ujson.Null).map { raw =>
        val value = ujson.read(OperationCodec.json(completed))
        value("events").arr.last("evidence")("generation") = raw
        OperationCodec.fromJson(value.render())
      }
      val value = ujson.read(OperationCodec.json(completed))
      value("events")(5) = ujson.Obj("type" -> "FALLBACK_DETECTED", "at" -> at.plusSeconds(5).toString, "requestedProtection" -> "Savepoint", "actualProtection" -> "FUTURE_MODE")
      assertTrue(invalid.forall(_.isLeft), OperationCodec.fromJson(value.render()).isLeft)
    },
    test("preserves restore boolean and rejects malformed restore options") {
      val job = FlinkJob(JobJarUri.unsafe("local:///job.jar"), "example.Job", 1, allowNonRestoredState = Some(true))
      val spec = FlinkDeploymentSpec(ref.namespace, ref.name, "flink:1.20.1", "v1_20", job)
      val op = accepted(FlinkOperation.Deploy(spec))
      val value = ujson.read(OperationCodec.json(op))
      value("command")("spec")("spec")("job")("savepointRedeployNonce") = "broken"
      assertTrue(OperationCodec.fromJson(OperationCodec.json(op)).toOption.exists(_.command == op.command), OperationCodec.fromJson(value.render()).isLeft)
    }
  )

package cn.xuyinyin.flinklab.operator.watch

import zio.test.*

object WatchModelSpec extends ZIOSpecDefault:
  def spec =
    suite("watch model")(
      test("decodes a modified Flink status event") {
        val event = WatchEvent.fromJson(
          ujson.read("""{
            "type": "MODIFIED",
            "object": {
              "metadata": {"name": "orders", "resourceVersion": "12"},
              "status": {
                "lifecycleState": "STABLE",
                "jobManagerDeploymentStatus": "READY",
                "jobStatus": {"state": "RUNNING", "savepointInfo": {"lastSavepoint": {"location": "s3://bucket/sp-12"}}},
                "conditions": [{"type": "Running", "status": "True", "message": "ready"}]
              }
            }
          }""")
        )
        assertTrue(
          event.isRight,
          event.toOption.exists(_.eventType == WatchEventType.Modified),
          event.toOption.flatMap(_.snapshot.toOption).exists(snapshot =>
            snapshot.name == "orders" &&
              snapshot.resourceVersion.contains("12") &&
              snapshot.lifecycleState.contains("STABLE") &&
              snapshot.jobState.contains("RUNNING") &&
              snapshot.deploymentStatus.contains("READY") &&
              snapshot.lastSavepointLocation.contains("s3://bucket/sp-12")
          )
        )
      },
      test("rejects a watch object without metadata name") {
        val event = WatchEvent.fromJson(ujson.Obj("type" -> "ADDED", "object" -> ujson.Obj("status" -> ujson.Obj())))
        assertTrue(event.isLeft)
      },
      test("keeps resource versions opaque and follows watch arrival order") {
        val current = FlinkStatusSnapshot("orders", Some("10"), Some("STABLE"), Some("RUNNING"), Some("READY"), None)
        val next = current.copy(resourceVersion = Some("2"), lifecycleState = Some("FAILED"))
        val merged = StatusReducer.merge(Map("orders" -> current), next)
        assertTrue(merged("orders") == next)
      },
      test("decodes checkpoint, savepoint history and conditions") {
        val parsed = FlinkStatusSnapshot.fromJsonString("""{
          "kind":"FlinkDeployment","metadata":{"name":"orders","resourceVersion":"20"},
          "status":{"lifecycleState":"STABLE","error":"","reconciliationStatus":{"state":"DEPLOYED"},
            "jobStatus":{"jobId":"job-1","jobName":"orders","state":"RUNNING","upgradeSavepointPath":"s3://bucket/upgrade",
              "checkpointInfo":{"formatType":"FULL","lastCheckpoint":{"timeStamp":"100","triggerNonce":"7","triggerType":"PERIODIC"},"triggerId":"cp-1"},
              "savepointInfo":{"formatType":"CANONICAL","lastSavepoint":{"location":"s3://bucket/sp-2","triggerNonce":"6"},"savepointHistory":[{"location":"s3://bucket/sp-1"}]}},
            "conditions":[{"type":"Running","status":"True","reason":"Ready","message":"ready"}]}
        }""")
        assertTrue(
          parsed.toOption.exists(snapshot =>
            snapshot.kind.contains("FlinkDeployment") &&
              snapshot.jobId.contains("job-1") &&
              snapshot.checkpoint.flatMap(_.lastCheckpoint).flatMap(_.triggerNonce).contains("7") &&
              snapshot.savepoint.exists(_.history.size == 1) &&
              snapshot.savepoint.flatMap(_.upgradeSavepointPath).contains("s3://bucket/upgrade") &&
              snapshot.conditions.head.message.contains("ready")
          )
        )
      },
      test("decodes completed and failed state snapshots") {
        val completed = FlinkStateSnapshotStatus.fromJsonString("""{"kind":"FlinkStateSnapshot","metadata":{"name":"sp-1","resourceVersion":"2"},"spec":{"jobReference":{"kind":"FlinkDeployment","name":"orders"}},"status":{"state":"COMPLETED","path":"s3://bucket/sp-1"}}""")
        val failed = FlinkStateSnapshotStatus.fromJsonString("""{"kind":"FlinkStateSnapshot","metadata":{"name":"sp-2"},"status":{"state":"FAILED","error":"checkpoint failed","failures":2}}""")
        assertTrue(
          completed.toOption.flatMap(_.path).contains("s3://bucket/sp-1"),
          completed.toOption.flatMap(_.jobReferenceName).contains("orders"),
          failed.toOption.flatMap(_.state).contains("FAILED"),
          failed.toOption.flatMap(_.error).contains("checkpoint failed")
        )
      }
    )

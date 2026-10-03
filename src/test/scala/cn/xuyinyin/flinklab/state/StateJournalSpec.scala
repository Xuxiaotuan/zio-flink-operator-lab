package cn.xuyinyin.flinklab.state

import cn.xuyinyin.flinklab.cli.ResourceKind
import cn.xuyinyin.flinklab.domain.FlinkTypes.Namespace
import zio.test.*

object StateJournalSpec extends ZIOSpecDefault:
  private def record(state: String, rv: String = "opaque-rv"): StateRecord =
    StateRecord.fromJson(Namespace.unsafe("analytics"), ResourceKind.Deployment,
      s"""{"metadata":{"name":"orders","uid":"uid-1","resourceVersion":"$rv"},"status":{"jobStatus":{"state":"$state"}}}""").toOption.get

  def spec = suite("state journal")(
    test("records changed status and deduplicates unchanged polls") {
      val first = StateJournal.merge(None, record("RUNNING"))
      val duplicate = StateJournal.merge(Some(first), record("RUNNING", "different-opaque-rv"))
      val finished = StateJournal.merge(Some(duplicate), record("FINISHED"))
      assertTrue(first.history.size == 1, duplicate.history.size == 1, finished.history.size == 2,
        finished.history.last("status")("jobStatus")("state").str == "FINISHED")
    },
    test("retains deletion as lifecycle evidence and survives serialization") {
      val deleted = StateJournal.markDeleted(StateJournal.merge(None, record("RUNNING")))
      val restored = StateJournal.fromJson(deleted.json)
      assertTrue(restored.deleted, restored.history.last("event").str == "DELETED", restored.record.key.name == "orders")
    },
    test("bounds retained lifecycle observations") {
      val result = (1 to 140).foldLeft(Option.empty[StateJournal])((previous, index) => Some(StateJournal.merge(previous, record(s"S$index"))))
      assertTrue(result.get.history.size == 100)
    }
  )

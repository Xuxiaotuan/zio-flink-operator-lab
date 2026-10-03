package cn.xuyinyin.flinklab.operator.savepoint

import zio.test.*

object SavepointPatchSpec extends ZIOSpecDefault:
  def spec = suite("savepoint patch")(
    test("trigger changes only the nonce") {
      val json = ujson.read(SavepointPatch.trigger(42L))
      assertTrue(json == ujson.Obj("spec" -> ujson.Obj("job" -> ujson.Obj("savepointTriggerNonce" -> 42))))
    },
    test("suspend asks the Operator to stop with savepoint, without a competing nonce trigger") {
      val json = ujson.read(SavepointPatch.suspend)
      assertTrue(
        json("spec")("job")("state").str == "suspended",
        json("spec")("job")("upgradeMode").str == "savepoint",
        !json("spec")("job").obj.contains("savepointTriggerNonce")
      )
    },
    test("does not round a Long nonce through a floating point number") {
      assertTrue(SavepointPatch.trigger(9007199254740993L).contains("9007199254740993"))
    }
  )

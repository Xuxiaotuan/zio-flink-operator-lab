package cn.xuyinyin.flinklab.cli

import zio.test.*

object CommandSpec extends ZIOSpecDefault:
  def spec =
    suite("command parser")(
      test("parses a dry-run apply") {
        val result = Command.parse(List("apply", "deployment", "--name", "orders", "--namespace", "analytics", "--dry-run"))
        assertTrue(result == Right(Command.Apply(ResourceKind.Deployment, Map("name" -> "orders", "namespace" -> "analytics"), dryRun = true)))
      },
      test("parses a watch command") {
        val result = Command.parse(List("watch", "deployment", "--name", "orders"))
        assertTrue(result == Right(Command.Watch(ResourceKind.Deployment, Map("name" -> "orders"))))
      },
      test("parses a savepoint command") {
        val result = Command.parse(List("savepoint", "deployment", "--name", "orders", "--nonce", "42"))
        assertTrue(result == Right(Command.Savepoint(ResourceKind.Deployment, Map("name" -> "orders", "nonce" -> "42"))))
      },
      test("rejects an option without a value") {
        assertTrue(Command.parse(List("render", "deployment", "--name")).isLeft)
      },
      test("does not allow dry-run to change delete semantics") {
        assertTrue(Command.parse(List("delete", "deployment", "--name", "orders", "--dry-run")).left.exists(_.contains("only valid for apply")))
      },
      test("rejects unknown options") {
        assertTrue(Command.parse(List("status", "deployment", "--name", "orders", "--typo", "value")).left.exists(_.contains("unknown option")))
      }
    )

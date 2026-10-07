package cn.xuyinyin.flinklab.operator.watch
/** 验证 ZStream watch 的重试、重连、reducer 和错误传播。 */

import zio.*
import zio.stream.*
import zio.test.*

object WatchStreamSpec extends ZIOSpecDefault:
  private val runningEvent = """{"type":"MODIFIED","object":{"metadata":{"name":"orders","resourceVersion":"2"},"status":{"lifecycleState":"STABLE","jobStatus":{"state":"RUNNING"}}}}"""
  private val finishedEvent = """{"type":"MODIFIED","object":{"metadata":{"name":"orders","resourceVersion":"3"},"status":{"lifecycleState":"STABLE","jobStatus":{"state":"FINISHED"}}}}"""

  def spec =
    suite("watch stream")(
      test("turns newline JSON into watch events") {
        for
          events <- WatchStream.fromLines(ZStream.fromIterable(List(runningEvent))).runCollect
        yield assertTrue(events.size == 1, events.head.eventType == WatchEventType.Modified)
      },
      test("emits a merged status map for every valid event") {
        for
          states <- WatchStream.latestStatuses(
            WatchStream.fromLines(ZStream.fromIterable(List(runningEvent, finishedEvent)))
          ).runCollect
        yield assertTrue(
          states.size == 2,
          states.last("orders").jobState.contains("FINISHED")
        )
      },
      test("removes a deleted resource from the status projection") {
        val deleted = """{"type":"DELETED","object":{"metadata":{"name":"orders","resourceVersion":"4"},"status":{"jobStatus":{"state":"FINISHED"}}}}"""
        for
          states <- WatchStream.latestStatuses(
            WatchStream.fromLines(ZStream.fromIterable(List(runningEvent, deleted)))
          ).runCollect
        yield assertTrue(states.last.get("orders").isEmpty)
      },
      test("ignores bookmark events in the status projection") {
        val bookmark = """{"type":"BOOKMARK","object":{"metadata":{"resourceVersion":"4"}}}"""
        for
          states <- WatchStream.latestStatuses(
            WatchStream.fromLines(ZStream.fromIterable(List(runningEvent, bookmark, finishedEvent)))
          ).runCollect
        yield assertTrue(states.size == 2, states.last("orders").jobState.contains("FINISHED"))
      },
      test("fails instead of hiding Kubernetes error events") {
        val error = WatchEvent(WatchEventType.Error, ujson.Obj("message" -> "resource version expired"))
        for
          result <- WatchStream.latestStatuses(ZStream.succeed(error)).runCollect.either
        yield assertTrue(result.isLeft)
      },
      test("projects state snapshot events") {
        val created = """{"type":"ADDED","object":{"kind":"FlinkStateSnapshot","metadata":{"name":"sp-1"},"spec":{"jobReference":{"kind":"FlinkDeployment","name":"orders"}},"status":{"state":"IN_PROGRESS"}}}"""
        val completed = """{"type":"MODIFIED","object":{"kind":"FlinkStateSnapshot","metadata":{"name":"sp-1"},"spec":{"jobReference":{"kind":"FlinkDeployment","name":"orders"}},"status":{"state":"COMPLETED","path":"s3://bucket/sp-1"}}}"""
        for
          states <- WatchStream.latestSnapshots(WatchStream.fromLines(ZStream.fromIterable(List(created, completed)))).runCollect
        yield assertTrue(states.last("sp-1").state.contains("COMPLETED"), states.last("sp-1").path.contains("s3://bucket/sp-1"))
      },
      test("retries a failed source up to the configured limit") {
        for
          attempts <- Ref.make(0)
          result <- Retrying
            .stream(
              ZStream.unwrap(
                attempts.updateAndGet(_ + 1).map { attempt =>
                  if attempt < 3 then ZStream.fail(new RuntimeException("transient"))
                  else ZStream.succeed("ready")
                }
              ),
              RetryPolicy(maxRetries = 2, initialDelay = Duration.Zero)
            )
            .runCollect
          count <- attempts.get
        yield assertTrue(result.toList == List("ready"), count == 3)
      },
      test("does not retry an error rejected by the policy") {
        for
          attempts <- Ref.make(0)
          result <- Retrying
            .stream(
              ZStream.unwrap(attempts.updateAndGet(_ + 1).map(_ => ZStream.fail(new IllegalArgumentException("permanent")))),
              RetryPolicy(maxRetries = 2, initialDelay = Duration.Zero, shouldRetry = _ => false)
            )
            .runCollect
            .either
          count <- attempts.get
        yield assertTrue(result.isLeft, count == 1)
      },
      test("does not retry beyond the configured limit") {
        for
          attempts <- Ref.make(0)
          result <- Retrying
            .stream(
              ZStream.unwrap(attempts.updateAndGet(_ + 1).map(_ => ZStream.fail(new RuntimeException("permanent")))),
              RetryPolicy(maxRetries = 2, initialDelay = Duration.Zero)
            )
            .runCollect
            .either
          count <- attempts.get
        yield assertTrue(result.isLeft, count == 3)
      },
      test("reconnects after a normally completed watch stream") {
        for
          attempts <- Ref.make(0)
          values <- Retrying
            .reconnect(
              ZStream.unwrap(attempts.updateAndGet(_ + 1).map(attempt => ZStream.succeed(attempt))),
              RetryPolicy(maxRetries = 2, initialDelay = Duration.Zero)
            )
            .take(3)
            .runCollect
          count <- attempts.get
        yield assertTrue(values.toList == List(1, 2, 3), count == 3)
      },
      test("retries failures and reconnects after completion") {
        for
          attempts <- Ref.make(0)
          values <- Retrying
            .resilient(
              ZStream.unwrap(
                attempts.updateAndGet(_ + 1).map { attempt =>
                  if attempt == 1 then ZStream.fail(new RuntimeException("transient"))
                  else ZStream.succeed(attempt)
                }
              ),
              RetryPolicy(maxRetries = 3, initialDelay = Duration.Zero)
            )
            .take(3)
            .runCollect
        yield assertTrue(values.toList == List(2, 3, 4))
      }
    )

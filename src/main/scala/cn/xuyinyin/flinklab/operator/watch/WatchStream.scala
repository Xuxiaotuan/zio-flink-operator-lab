package cn.xuyinyin.flinklab.operator.watch

import zio.*
import zio.stream.*

final case class RetryPolicy(maxRetries: Int, initialDelay: Duration, shouldRetry: Throwable => Boolean = _ => true)

object Retrying:
  def stream[A](source: => ZStream[Any, Throwable, A], policy: RetryPolicy): ZStream[Any, Throwable, A] =
    val schedule = (Schedule.exponential(policy.initialDelay) && Schedule.recurs(policy.maxRetries)).whileInput(policy.shouldRetry)
    source.retry(schedule)

  def reconnect[A](source: => ZStream[Any, Throwable, A], policy: RetryPolicy): ZStream[Any, Throwable, A] =
    source.repeat(Schedule.spaced(policy.initialDelay) && Schedule.recurs(policy.maxRetries))

  def resilient[A](source: => ZStream[Any, Throwable, A], policy: RetryPolicy): ZStream[Any, Throwable, A] =
    stream(source, policy).repeat(Schedule.spaced(policy.initialDelay))

object WatchStream:
  def fromLines(lines: ZStream[Any, Throwable, String]): ZStream[Any, Throwable, WatchEvent] =
    lines.mapZIO { line =>
      ZIO.fromEither(WatchEvent.fromJson(ujson.read(line))).mapError(message => IllegalArgumentException(message)).flatMap {
        case event if event.eventType == WatchEventType.Error =>
          ZIO.fail(IllegalArgumentException(errorMessage(event)))
        case event => ZIO.succeed(event)
      }
    }

  def latestStatuses(events: ZStream[Any, Throwable, WatchEvent]): ZStream[Any, Throwable, Map[String, FlinkStatusSnapshot]] =
    events
      .mapZIO {
        case event if event.eventType == WatchEventType.Error => ZIO.fail(IllegalArgumentException(errorMessage(event)))
        case event if event.eventType == WatchEventType.Bookmark => ZIO.succeed(None)
        case event => ZIO.succeed(Some(event))
      }
      .collectSome
      .scan(Map.empty[String, FlinkStatusSnapshot]) { (current, event) =>
        StatusReducer.mergeEvent(current, event).fold(_ => current, identity)
      }
      .drop(1)

  def latestSnapshots(events: ZStream[Any, Throwable, WatchEvent]): ZStream[Any, Throwable, Map[String, FlinkStateSnapshotStatus]] =
    events
      .mapZIO {
        case event if event.eventType == WatchEventType.Error => ZIO.fail(IllegalArgumentException(errorMessage(event)))
        case event if event.eventType == WatchEventType.Bookmark => ZIO.succeed(None)
        case event => ZIO.succeed(Some(event))
      }
      .collectSome
      .scan(Map.empty[String, FlinkStateSnapshotStatus]) { (current, event) =>
        StateSnapshotReducer.mergeEvent(current, event).fold(_ => current, identity)
      }
      .drop(1)

  private def errorMessage(event: WatchEvent): String =
    event.resource.obj.get("message").map(_.str).filter(_.nonEmpty).getOrElse("Kubernetes watch returned an error event")

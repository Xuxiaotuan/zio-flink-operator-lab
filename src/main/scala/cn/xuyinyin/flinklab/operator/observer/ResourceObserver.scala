package cn.xuyinyin.flinklab.operator.observer

/** 资源观察器：执行 list → watch，处理 resourceVersion、410 Gone 重列和断线重连。 */
import cn.xuyinyin.flinklab.domain.ResourceKind
import cn.xuyinyin.flinklab.domain.FlinkTypes.Namespace
import cn.xuyinyin.flinklab.kubernetes.{KubernetesApi, KubernetesApiError}
import cn.xuyinyin.flinklab.operator.watch.{WatchEvent, WatchEventType}
import zio.*
import zio.stream.*

/** A normalized object observation. The resourceVersion is opaque to the observer. */
final case class ResourceObservation(
    eventType: WatchEventType,
    kind: ResourceKind,
    name: String,
    resourceVersion: Option[String],
    uid: Option[String],
    generation: Option[Long],
    observedGeneration: Option[Long],
    payload: String
)

/** 将 list/watch 事件转换成统一的资源观察流。 */
trait ResourceObserver:
  def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]): ZStream[Any, Throwable, ResourceObservation]

object ResourceObserver:
  def live(api: KubernetesApi): ResourceObserver = new DefaultResourceObserver(api)

final class DefaultResourceObserver(api: KubernetesApi) extends ResourceObserver:
  /** 每个 cycle 先 list 获取快照和 resourceVersion，再从该版本开始 watch。 */
  override def observe(namespace: Namespace, kind: ResourceKind, name: Option[String]): ZStream[Any, Throwable, ResourceObservation] =
    // cycle 是可重入的：410 重新 list，正常断线从新的 list 继续。
    def cycle: ZStream[Any, Throwable, ResourceObservation] =
      ZStream.unwrap {
        api.list(namespace, kind).map(parseList(_, kind)).map { listed =>
          // 先发出 list 快照，再消费从该 resourceVersion 开始的增量事件。
          val initial = ZStream.fromChunk(Chunk.fromIterable(listed.items.filter(item => name.forall(_ == item.name))))
          val watched = api.watchFrom(namespace, kind, name, listed.resourceVersion)
            .mapZIO {
              case event if event.eventType == WatchEventType.Bookmark => ZIO.succeed(None)
              case event => fromEvent(event, kind).map(Some(_))
            }
            .collectSome
            .catchAll {
              // 410 表示游标过期，必须重新 list；不能拿旧 resourceVersion 重试。
              case error: KubernetesApiError if error.status == 410 => cycle
              // 临时错误让本轮流结束，由 reconnect 重新建立 list/watch。
              case error: KubernetesApiError if error.status == 429 || error.status >= 500 => ZStream.empty
              case _: java.io.IOException => ZStream.empty
              case error => ZStream.fail(error)
            }
          initial ++ watched ++ reconnect
        }
      }
    def reconnect: ZStream[Any, Throwable, ResourceObservation] =
      ZStream.fromZIO(ZIO.sleep(100.millis)) *> cycle
    cycle

  private final case class Listed(items: List[ResourceObservation], resourceVersion: Option[String])

  private def parseList(raw: String, kind: ResourceKind): Listed =
    val value = ujson.read(raw)
    val metadata = value.obj.get("metadata").flatMap(_.objOpt).getOrElse(Map.empty[String, ujson.Value])
    val resourceVersion = metadata.get("resourceVersion").flatMap(value => stringValue(Some(value)))
    val items = value.obj.get("items").flatMap(_.arrOpt).map(_.toList).getOrElse(Nil).map { item =>
      fromResource(WatchEventType.Added, item, kind)
    }
    Listed(items, resourceVersion)

  private def fromEvent(event: WatchEvent, kind: ResourceKind): IO[Throwable, ResourceObservation] =
    if event.eventType == WatchEventType.Bookmark then ZIO.fail(IllegalArgumentException("bookmark does not carry a resource observation"))
    else if event.eventType == WatchEventType.Error then ZIO.fail(IllegalArgumentException("watch error event was not classified by KubernetesApi"))
    else ZIO.attempt(fromResource(event.eventType, event.resource, kind))

  private def fromResource(eventType: WatchEventType, value: ujson.Value, kind: ResourceKind): ResourceObservation =
    val metadata = value.obj.get("metadata").flatMap(_.objOpt).getOrElse(Map.empty[String, ujson.Value])
    val name = stringValue(metadata.get("name")).filter(_.nonEmpty).getOrElse(throw IllegalArgumentException("observed resource metadata.name is required"))
    ResourceObservation(
      eventType,
      kind,
      name,
      stringValue(metadata.get("resourceVersion")),
      stringValue(metadata.get("uid")),
      longValue(metadata.get("generation")),
      value.obj.get("status").flatMap(_.objOpt).flatMap(_.get("observedGeneration")).flatMap(value => longValue(Some(value))),
      value.render()
    )

  private def stringValue(value: Option[ujson.Value]): Option[String] = value.flatMap(item => item.strOpt.orElse(item.numOpt.map(_.toString))).filter(_.nonEmpty)
  private def longValue(value: Option[ujson.Value]): Option[Long] = value.flatMap(item => item.numOpt.map(_.toLong).orElse(item.strOpt.flatMap(_.toLongOption)))

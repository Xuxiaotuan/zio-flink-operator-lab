package cn.xuyinyin.flinklab.state

/** 状态轮询器：定期读取 Kubernetes CR，并把最新状态合并到选定的共享状态后端。 */
import cn.xuyinyin.flinklab.domain.ResourceKind
import cn.xuyinyin.flinklab.domain.FlinkTypes.Namespace
import cn.xuyinyin.flinklab.kubernetes.KubernetesApi
import zio.*

final case class StatePollerSettings(namespace: Namespace, interval: Duration)

object StatePollerSettings:
  def fromEnv(env: Map[String, String]): Either[String, StatePollerSettings] =
    for
      namespace <- Namespace.from(env.getOrElse("FLINK_NAMESPACE", "default"))
      seconds = env.get("ZIO_FLINK_STATE_POLL_INTERVAL_SECONDS").flatMap(_.toIntOption).getOrElse(15)
      _ <- Either.cond(seconds > 0, (), "ZIO_FLINK_STATE_POLL_INTERVAL_SECONDS must be positive")
    yield StatePollerSettings(namespace, seconds.seconds)

/** Periodically reconciles the Kubernetes CR status into the configured shared state store. */
object StatePoller:
  val kinds = List(ResourceKind.Deployment, ResourceKind.SessionJob, ResourceKind.StateSnapshot)

  def run: ZIO[KubernetesApi & StateStore, Nothing, Nothing] =
    val settings = StatePollerSettings.fromEnv(sys.env) match
      case Right(value) => value
      case Left(error) => throw IllegalArgumentException(error)
    ZIO.service[KubernetesApi].flatMap { api =>
      ZIO.service[StateStore].flatMap { store =>
        (pollOnce(api, store, settings.namespace)
          .catchAll(error => ZIO.logWarning(s"state poll failed: ${Option(error.getMessage).getOrElse(error.toString)}")) *>
          Clock.sleep(settings.interval)).forever
      }
    }

  /** 单轮读取三类 Flink CR，并以 resourceVersion 合并进共享状态。 */
  def pollOnce(api: KubernetesApi, store: StateStore, namespace: Namespace): IO[Throwable, Unit] =
    for
      records <- ZIO.foreach(kinds) { kind =>
        api.list(namespace, kind).flatMap(raw =>
          ZIO.fromEither(StateRecord.fromCollection(namespace, kind, raw).left.map(IllegalArgumentException(_)))
        )
      }.map(_.flatten)
      _ <- ZIO.foreachDiscard(records)(record => persist(store, record))
      _ <- markMissing(store, namespace, records)
    yield ()

  private def persist(store: StateStore, record: StateRecord): IO[Throwable, Unit] =
    store.getJournal(record.key).flatMap(previous => store.putJournal(StateJournal.merge(previous, record)))

  private def markMissing(store: StateStore, namespace: Namespace, current: List[StateRecord]): IO[Throwable, Unit] =
    if store.backend != StateBackend.Postgres then ZIO.unit
    else
      val currentKeys = current.map(_.key).toSet
      store.listJournals(namespace).flatMap(existing =>
        ZIO.foreachDiscard(existing.filter(journal => !journal.deleted && !currentKeys.contains(journal.key))) { journal =>
          store.putJournal(StateJournal.markDeleted(journal))
        }
      )

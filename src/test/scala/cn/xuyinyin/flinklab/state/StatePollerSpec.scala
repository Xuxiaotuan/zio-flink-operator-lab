package cn.xuyinyin.flinklab.state
/** 验证轮询器合并多种 Flink CR 并检查轮询间隔配置。 */

import cn.xuyinyin.flinklab.domain.FlinkTypes.Namespace
import cn.xuyinyin.flinklab.kubernetes.FakeKubernetesApi
import zio.*
import zio.test.*

object StatePollerSpec extends ZIOSpecDefault:
  private final class MemoryStore(ref: Ref[Map[StateKey, StateJournal]]) extends StateStore:
    override val backend: StateBackend = StateBackend.Kubernetes
    override def putJournal(journal: StateJournal): UIO[Unit] = ref.update(_.updated(journal.key, journal))
    override def getJournal(key: StateKey): UIO[Option[StateJournal]] = ref.get.map(_.get(key))
    override def listJournals(namespace: Namespace): UIO[List[StateJournal]] =
      ref.get.map(_.values.filter(_.key.namespace == namespace).toList)
    override def delete(key: StateKey): UIO[Unit] = ref.update(_ - key)

  def spec = suite("state poller")(
    test("merges all watched resource kinds into the shared store") {
      for
        fake <- FakeKubernetesApi.make
        ref <- Ref.make(Map.empty[StateKey, StateJournal])
        store = new MemoryStore(ref)
        namespace <- ZIO.fromEither(Namespace.from("analytics")).foldZIO(error => ZIO.dieMessage(error), ZIO.succeed(_))
        _ <- StatePoller.pollOnce(fake, store, namespace)
        journals <- ref.get
      yield assertTrue(
        journals.size == 3,
        journals.keys.forall(_.namespace == namespace),
        journals.values.forall(_.history.nonEmpty)
      )
    },
    test("rejects a non-positive polling interval") {
      val result = StatePollerSettings.fromEnv(Map("ZIO_FLINK_STATE_POLL_INTERVAL_SECONDS" -> "0"))
      assertTrue(result.isLeft)
    }
  )

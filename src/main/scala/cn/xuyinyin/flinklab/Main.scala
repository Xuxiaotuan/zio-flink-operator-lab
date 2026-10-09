package cn.xuyinyin.flinklab

/** HTTP 控制面入口：启动唯一的 ServerProgram，所有 Flink 操作通过 HTTP API 进入。 */
import cn.xuyinyin.flinklab.application.PolicyEngine
import cn.xuyinyin.flinklab.kubernetes.KubernetesApi
import cn.xuyinyin.flinklab.operation.{AsyncOperationWorker, OperationStore, ResourceCoordinator}
import cn.xuyinyin.flinklab.server.ServerProgram
import cn.xuyinyin.flinklab.state.StateStore
import zio.*

object Main extends ZIOAppDefault:
  /** 组装 HTTP 服务依赖；命令行参数不再承载业务操作。 */
  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    val serverLayer = ZLayer.make[KubernetesApi & StateStore & OperationStore & AsyncOperationWorker](
      KubernetesApi.live,
      OperationStore.live,
      StateStore.live,
      PolicyEngine.live,
      ResourceCoordinator.live,
      AsyncOperationWorker.live
    )
    ServerProgram.run.provideSome[ZIOAppArgs](serverLayer)

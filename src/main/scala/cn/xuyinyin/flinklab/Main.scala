package cn.xuyinyin.flinklab

/** 主程序入口：根据命令行参数选择一次性 CLI 操作或长期运行的 HTTP 控制面。 */
import cn.xuyinyin.flinklab.cli.Command
import cn.xuyinyin.flinklab.kubernetes.KubernetesApi
import cn.xuyinyin.flinklab.operator.OperatorProgram
import cn.xuyinyin.flinklab.server.ServerProgram
import cn.xuyinyin.flinklab.state.StateStore
import cn.xuyinyin.flinklab.operation.{AsyncOperationWorker, OperationStore, ResourceCoordinator}
import cn.xuyinyin.flinklab.application.PolicyEngine
import zio.*

object Main extends ZIOAppDefault:
  /** 组装运行时依赖；serve 走 HTTP 服务，其它参数走 CLI/worker。 */
  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    for
      args <- getArgs
      _ <- args.toList match
        case "serve" :: Nil =>
          val serverLayer = ZLayer.make[KubernetesApi & StateStore & OperationStore & AsyncOperationWorker](KubernetesApi.live, OperationStore.live, StateStore.live, PolicyEngine.live, ResourceCoordinator.live, AsyncOperationWorker.live)
          ServerProgram.run.provideSome[ZIOAppArgs](serverLayer)
        case values =>
          for
            command <- ZIO.fromEither(Command.parse(values)).mapError(message => IllegalArgumentException(message))
            commandLayer = ZLayer.make[KubernetesApi & OperationStore & AsyncOperationWorker](KubernetesApi.live, OperationStore.live, PolicyEngine.live, ResourceCoordinator.live, AsyncOperationWorker.live)
            result <- OperatorProgram.executeWithWorker(command).provideSome[ZIOAppArgs](commandLayer)
          yield result
    yield ()

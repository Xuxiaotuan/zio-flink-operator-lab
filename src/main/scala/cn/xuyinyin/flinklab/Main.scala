package cn.xuyinyin.flinklab

import cn.xuyinyin.flinklab.cli.Command
import cn.xuyinyin.flinklab.kubernetes.KubernetesApi
import cn.xuyinyin.flinklab.operator.OperatorProgram
import cn.xuyinyin.flinklab.server.ServerProgram
import cn.xuyinyin.flinklab.state.StateStore
import cn.xuyinyin.flinklab.operation.{AsyncOperationWorker, OperationStore}
import cn.xuyinyin.flinklab.application.PolicyEngine
import zio.*

object Main extends ZIOAppDefault:
  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    for
      args <- getArgs
      _ <- args.toList match
        case "serve" :: Nil =>
          val serverLayer = ZLayer.make[KubernetesApi & StateStore & OperationStore & AsyncOperationWorker](KubernetesApi.live, OperationStore.live, StateStore.live, PolicyEngine.live, AsyncOperationWorker.live)
          ServerProgram.run.provideSome[ZIOAppArgs](serverLayer)
        case values =>
          for
            command <- ZIO.fromEither(Command.parse(values)).mapError(message => IllegalArgumentException(message))
            commandLayer = ZLayer.make[KubernetesApi & OperationStore & AsyncOperationWorker](KubernetesApi.live, OperationStore.live, PolicyEngine.live, AsyncOperationWorker.live)
            result <- OperatorProgram.executeWithWorker(command).provideSome[ZIOAppArgs](commandLayer)
          yield result
    yield ()

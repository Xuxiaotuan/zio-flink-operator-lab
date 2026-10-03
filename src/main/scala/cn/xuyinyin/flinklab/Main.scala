package cn.xuyinyin.flinklab

import cn.xuyinyin.flinklab.cli.Command
import cn.xuyinyin.flinklab.kubernetes.KubernetesApi
import cn.xuyinyin.flinklab.operator.OperatorProgram
import cn.xuyinyin.flinklab.server.ServerProgram
import zio.*

object Main extends ZIOAppDefault:
  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    for
      args <- getArgs
      _ <- args.toList match
        case "serve" :: Nil => ServerProgram.run.provideSome[ZIOAppArgs](KubernetesApi.live)
        case values =>
          for
            command <- ZIO.fromEither(Command.parse(values)).mapError(message => IllegalArgumentException(message))
            result <- OperatorProgram.execute(command).provideSome[ZIOAppArgs](KubernetesApi.live)
          yield result
    yield ()

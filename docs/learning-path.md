# 学习路线

学习目标服务于同一个结果：能够用 Scala 3 和 ZIO 编写、测试和维护 Flink Operator 控制程序。

## 1. Scala 3 类型边界

阅读 `domain/FlinkTypes.scala`、`domain/FlinkResources.scala` 和 `cli/Command.scala`。

- opaque type 区分 namespace、资源名和 JAR URI。
- extension method 提供领域值的受控读取。
- enum 表示 Deployment、SessionJob、StateSnapshot 和快照类型。
- `StateProtection`、`FallbackPolicy`、`OperationState` 和 `OperationEvent` 表示业务约束、状态机和审计历史。
- `Either` 表示 CLI 和资源校验失败。
- 用一个 JSON 编码练习理解 `given/using`，不把上下文参数扩散到服务边界。

## 2. ZIO 效果与依赖

阅读 `OperatorProgram.execute` 和 `KubernetesApi`：

```scala
def execute(command: Command): ZIO[KubernetesApi, Throwable, Unit]
```

`KubernetesApi` 是环境，`Throwable` 是失败通道，`Unit` 是成功值。生产实现通过 `ZLayer` 提供，fake 实现用于测试。构造效果不会自动连接集群，只有运行时执行才会产生副作用。

## 3. Future、并发和资源

把一个 Kubernetes 请求分别用 Future 和 ZIO 描述，比较执行时机、失败传播、取消和资源关闭。Java client 的阻塞调用必须进入 blocking 线程池。watch 通过 Scope 关闭底层连接，不能只依赖 fiber 结束。

## 4. ZStream、Ref 和重试

阅读 `operator/watch/WatchModel.scala` 和 `WatchStream.scala`：

- 事件按到达顺序合并为最新状态。
- `DELETED` 移除资源，`BOOKMARK` 不改变状态，`ERROR` 进入失败通道。
- `Schedule` 控制失败重试和 EOF 重连上限。
- resourceVersion 是不透明字符串。

可靠 watcher 还需要保存 list 的 resourceVersion，并在 410 Gone 后 relist；完成这部分后再把它纳入生产验收。

## 5. 快照与状态证据

阅读 `FlinkStateSnapshotSpec`、`FlinkStateSnapshotStatus` 和 [状态监控](monitoring.md)，区分：

1. CR 已提交。
2. Operator 已接受并处理请求。
3. snapshot 已完成并写出路径。
4. 作业已经从该状态恢复并产生正确业务结果。

这四个结果需要不同的证据。API 返回 200 或 CR 出现旧的 lastSavepoint 不能替代快照完成证据。

## 6. 类型化控制面

阅读 `domain/ControlPlaneDomain.scala`、`application/PolicyEngine.scala` 和 `operation/OperationStore.scala`：

- `FlinkOperation` 把 CLI、HTTP 和未来 UI 的操作统一成同一个领域命令。
- `PolicyEngine` 把未验证操作转换成 `ValidatedOperation`。
- `OperationStateMachine` 拒绝非法状态跳转，并保留 `Failed`、`TimedOut`、`Uncertain` 和 `Superseded`。
- `OperationStore` 通过 ZIO `Ref` 保证单实例内的检查、转换和写入原子完成；分布式持久化和 Lease 锁放到后续阶段。

学习重点是：`ZIO[R, E, A]` 的 `E` 不再只写 `Throwable`，操作策略和状态机错误使用 `ControlPlaneError`；接受操作也不等于验证完成。

# 设计与协议

项目只有一个核心：用 ZIO 编写 Flink 控制程序，通过 Kubernetes API 操作 Flink Kubernetes Operator，完成任务提交、状态观察和状态快照。

项目北极星是：**类型化、可审计、可验证结果的 Flink 操作控制面**。Scala 3 类型负责表达哪些操作和状态保护策略是合法的；ZIO 负责 Kubernetes 副作用、错误通道、资源生命周期、并发和重试。

```text
CLI / HTTP
    |
    v
ZIO OperatorProgram
    |
    v
Typed FlinkControlPlane
    |
    +-- PolicyEngine
    +-- OperationStore / OperationStateMachine
    +-- ResourceObserver (list → watch → 410 relist)
    +-- VerificationEngine / Evidence
    +-- AsyncOperationWorker / OperationMutex
    |
    v
KubernetesApi
    |
    v
Kubernetes API Server
    |
    v
Flink Kubernetes Operator
    |
    v
FlinkDeployment / FlinkSessionJob / FlinkStateSnapshot
```

对应的可交互图：

- [架构图](diagrams/architecture.html)
- [数据流图](diagrams/dataflow.html)

## 当前已落地的控制面边界

`domain/ControlPlaneDomain.scala` 定义了操作控制面的核心类型：

| 类型 | 作用 |
| --- | --- |
| `FlinkOperation` | Deploy、Upgrade、Suspend、Resume、Restart、Snapshot、Delete 的统一命令模型 |
| `StateProtection` | `Stateless`、`LastState`、`Savepoint`，映射到 Operator 的 `upgradeMode` |
| `FallbackPolicy` | 明确 savepoint 失败后是否允许 last-state |
| `OperationState` | Accepted、Validating、Submitted、Reconciling、Verifying、Completed、Failed、TimedOut、Uncertain、Superseded |
| `OperationEvent` | 不可变操作历史，记录提交、观察、快照、验证和不确定结果 |
| `RequestId`、`OperationId`、`ResourceUid` | 分离请求重试、控制面操作和 Kubernetes 资源实例 |

`FlinkOperationFactory` 把 CLI apply 和 HTTP 写请求转换成同一个 `FlinkOperation`。`PolicyEngine` 负责升级策略校验；`AsyncOperationWorker` 负责状态推进、Kubernetes 提交、观察和验证。`OperationStore` 默认使用 Kubernetes `FlinkOperation` CR，`ZIO_FLINK_OPERATION_STORE=postgres` 时才使用 PostgreSQL，`memory` 只用于单进程测试。Kubernetes 适配器仍是唯一提交边界。

请求接受和操作完成是两个结果。HTTP/CLI 写请求先返回 `AcceptedOperation`，worker 随后推进 `Accepted → Validating → Submitted → WaitingForObservation → Reconciling → Verifying → Completed`。PolicyEngine 不通过时不会写 Kubernetes。验证会持续观察，只有 `Evidence` 同时满足资源身份、UID、提交后的 generation、observedGeneration、reconciliation 和实际 Job 状态时才进入 `Completed`；确定失败进入 `Failed`，等待超时进入 `TimedOut`，提交请求结果不确定进入 `Uncertain`。

严格 savepoint 策略的语义是：`StateProtection.Savepoint + FallbackPolicy.Forbidden` 在没有 savepoint 存储证据时直接拒绝；即使 Operator 后续表现为 last-state，也不能把操作标记为成功。网络中断使用 `Uncertain`，被新操作覆盖使用 `Superseded`，超时使用 `TimedOut`。

## 当前实现边界

1. CLI apply 和 HTTP 写请求统一生成 `FlinkOperation`，服务返回 `operationId`。
2. `ResourceObserver` 先 list，再用 list 的 `resourceVersion` watch；收到 410 Gone 会重新 list。
3. `VerificationEngine` 使用 `Evidence` 校验 UID、generation、observedGeneration、reconciliation 和 Job 状态。
4. `AsyncOperationWorker` 执行提交、观察、验证和失败转移；Kubernetes `FlinkOperationLock` CR 保护跨副本的资源级互斥，`OperationMutex` 保护进程内副作用。PostgreSQL 后端使用行锁保护共享操作记录。
5. 真实 checkpoint/savepoint 路径和恢复结果仍需目标集群带 RustFS 插件和 Secret 的作业验收。

## 资源边界

| 资源 | 用途 |
| --- | --- |
| `FlinkDeployment` | 当前 CLI 模板创建 Application Cluster 并运行一个 Flink Job |
| `FlinkSessionJob` | 把 Job 提交到已有的 Session Cluster |
| `FlinkStateSnapshot` | 请求 savepoint 或 checkpoint，并观察快照状态 |

所有资源都通过 Kubernetes API Server 提交。程序不调用 `kubectl`，不直接操作 JobManager Pod。

`apply` 使用 Server-Side Apply，旧的 `savepoint` 与 `suspend-savepoint` 使用局部 merge patch，作为兼容入口保留。typed checkpoint/savepoint operation 使用确定名称创建 `FlinkStateSnapshot`，随后只观察这一个 Snapshot CR；Operator 记录请求、结果路径和失败信息。

## 状态来源

任务状态来自 Flink CR 的 `status`：

- 生命周期：`lifecycleState`、`jobManagerDeploymentStatus`、`reconciliationStatus`。
- Job：`jobStatus.jobId`、`jobStatus.jobName`、`jobStatus.state`、`startTime`、`updateTime`。
- 条件与错误：`conditions`、`status.error`。
- checkpoint 摘要：`jobStatus.checkpointInfo`。
- savepoint 摘要：`jobStatus.savepointInfo`，包括最后路径和历史记录。

快照请求的状态来自 `FlinkStateSnapshot.status`，包括 `state`、`path`、`error`、`failures`、`triggerId` 和时间戳。字段以目标集群安装的 CRD 为准。

当前监控是 Operator CR 状态和 checkpoint/savepoint 摘要。逐个 checkpoint 的 task/subtask 明细需要额外接入 Flink REST，本项目当前没有把它混入 Kubernetes API 适配器。

## ZIO 边界

底层 Kubernetes 互操作仍使用 `ZIO[KubernetesApi, Throwable, A]`，方便保留 Java Client 的原始错误；业务策略和操作状态使用 `ControlPlaneError`，不把所有错误降级成 `Throwable`。`KubernetesApi` 是副作用端口，`KubernetesApiLive` 是生产实现，fake API 用于测试。

`ZStream` 消费 Kubernetes watch 事件，reducer 按事件到达顺序维护状态。`Scope` 负责关闭 watch，`Schedule` 负责有界重试，`ZLayer` 负责组装 Kubernetes 客户端。HTTP 服务另外运行一个 ZIO 后台轮询器，按固定间隔 list 三类 Flink CR，把状态合并到共享状态后端。

资源版本始终按不透明字符串处理。`ResourceObserver` 已实现 list → resourceVersion watch 和 410 Gone relist；resourceVersion 按不透明字符串传递。Kubernetes 模式的跨进程操作状态由 `FlinkOperation` CR 持久化，`requestId` 通过 CR label 查询实现幂等；显式选择 PostgreSQL 时才使用 PostgreSQL OperationStore。数据库读取或反序列化失败会返回 `StoreFailure`，不会伪装成“没有 operation”。watch 游标在每次 relist 后从 API Server 重新获得。

## 多副本与状态后端

HTTP 副本不保存本地会话状态，Service 可以把请求路由到任意副本。资源状态后端由 `ZIO_FLINK_STATE_BACKEND` 选择，操作审计由 `ZIO_FLINK_OPERATION_STORE` 选择；Kubernetes 模式默认使用 API Server 中的 CR 共享 operation 生命周期：

| 模式 | 统一状态来源 | 用途 |
| --- | --- | --- |
| `kubernetes` | FlinkDeployment、FlinkSessionJob、FlinkStateSnapshot、FlinkOperation CR | Kubernetes 部署默认模式，CR 是事实源，副本直接读取 API Server |
| `postgres` | `zio_flink_operator_state` 表 | 裸机运行时保存最新状态、资源版本和最多 100 条生命周期事件，副本读取同一张表 |

PostgreSQL 只保存状态观测和资源版本，不保存 Flink checkpoint/savepoint 二进制。savepoint 仍由 Flink 运行时写入 RustFS。两种模式都需要 Kubernetes API，因为本项目的提交和 Operator 状态来源始终是 Kubernetes API。

状态接口：

- `GET /v1/state?namespace=<namespace>`：列出当前后端中的状态记录。
- `GET /v1/state/<kind>/<name>?namespace=<namespace>`：读取一条状态记录。

Kubernetes 模式不会额外引入 ConfigMap 缓存，避免 CR 与缓存出现双重事实源。裸机或外部审计场景可以显式配置 PostgreSQL；裸机 PostgreSQL 模式的 upsert 使用 Kubernetes `resourceVersion` 防止较旧观测覆盖较新观测。

当前已完成目标集群 PipelineRun、Flink Operator reconcile、两副本调度和 Kubernetes CR 统一 operation 状态的现场验收。HTTP dry-run、策略与最终 Flink 请求一致性、稳定 requestId、活动资源互斥、Operation CR resourceVersion 条件更新、worker 重启后的阶段恢复、快照 UID/结果路径校验、删除 UID 前置条件、watch 正常 EOF 重连以及验证 Evidence 审计已有代码和回归测试。仍未完成的是 checkpoint/savepoint 成功写入 RustFS、从 savepoint 恢复后的业务连续性、跨节点故障演练、锁租约接管，以及 FallbackDetected 的完整生产语义。单元测试、构建和 kustomize 渲染不能替代这些现场验收。

本地 OrbStack 只有一个节点，两个 Pod 只能证明进程副本和 Service 路由。目标集群清单使用一个 NodePort Service、两个副本、hostname 反亲和和 `DoNotSchedule`；两台机器的跨节点调度需要目标集群可达、节点标签正常、Operator 已安装，并通过滚动重启和状态接口验证。每个副本都轮询同一 Kubernetes API，状态统一来自 CR；这会按副本数增加 list 请求量，后续高规模场景应增加 leader election 或集中式 watch。

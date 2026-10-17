# 设计与协议

项目只有一个核心：用 ZIO 编写 Flink 控制程序，通过 Kubernetes API 操作 Flink Kubernetes Operator，完成任务提交、状态观察和状态快照。

项目北极星是：**类型化、可审计、可验证结果的 Flink 操作控制面**。Scala 3 类型负责表达哪些操作和状态保护策略是合法的；ZIO 负责 Kubernetes 副作用、错误通道、资源生命周期、并发和重试。

```text
HTTP API
    |
    v
ZIO ServerProgram
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

`FlinkOperationFactory` 把 HTTP 写请求转换成同一个 `FlinkOperation`。`PolicyEngine` 负责升级策略校验；`AsyncOperationWorker` 负责状态推进、Kubernetes 提交、观察和验证。`OperationStore` 默认使用 Kubernetes `FlinkOperation` CR，`ZIO_FLINK_OPERATION_STORE=postgres` 时才使用 PostgreSQL，`memory` 只用于单进程测试。Kubernetes 适配器仍是唯一提交边界。

普通 Deploy 只创建资源：观察到目标已存在时在写入前拒绝；观察后发生名称竞争时，由 API Server 的 POST 409 拒绝。已有资源的普通变更必须走 Upgrade，统一执行运行状态、状态保护、fallback 规范化和保护证据校验。Upgrade 与显式恢复使用带 UID/resourceVersion 的 merge patch，版本变化时返回冲突；不使用强制字段接管。

请求接受和操作完成是两个结果。HTTP 写请求先返回 `AcceptedOperation`，worker 随后推进 `Accepted → Validating → Submitted → WaitingForObservation → Reconciling → Verifying → Completed`。PolicyEngine 不通过时不会写 Kubernetes。验证会持续观察，只有 `Evidence` 同时满足资源身份、UID、提交后的 generation、observedGeneration、reconciliation 和实际 Job 状态时才进入 `Completed`；确定失败进入 `Failed`，等待超时进入 `TimedOut`，提交请求结果不确定进入 `Uncertain`。

严格 savepoint 策略的语义是：`StateProtection.Savepoint` 只有在 Evidence 明确包含 savepoint 保护和结果路径时才完成；Operator 的 `lastReconciledSpec` 缺失或显式表现为非 savepoint 时拒绝。`AllowLastState` 只允许明确观察到 `last-state` 的结果，并追加 `FALLBACK_DETECTED` 审计事件，不把未知证据当作 fallback。网络中断使用 `Uncertain`，被新操作覆盖使用 `Superseded`，超时使用 `TimedOut`。

暂停还需区分 Flink Job 状态和 Operator 生命周期：savepoint 停止的 Job 可能为 `FINISHED`。只有 `lifecycleState=SUSPENDED` 且 `lastReconciledSpec.spec.job.state=suspended` 时才归一为暂停；严格暂停仍须通过 generation、reconciliation 和 savepoint 证据校验。

## 预览与指定恢复

`dryRun` 只在 HTTP 边界解析一次，大小写及首尾 ASCII 空白均接受；非法值返回 400。预览只支持 POST deployment/snapshot apply，向底层明确传 `dryRun=true`，不创建 operation、不排队、不产生实际 CR。适配器负责发送 Kubernetes `dryRun=All`。

已有目标仍可通过 `POST /v1/deployments` 指定 savepoint 恢复，必须同时声明 `initialSavepointPath`、正的安全整数 `savepointRedeployNonce`、`upgradeMode=savepoint` 和 `state=running`。任一恢复字段出现就按恢复校验，字段缺失不能退回普通 Deploy。已有目标的 nonce 必须大于当前 CR 中的 nonce；相同 requestId 重试复用原 operation，不提高 nonce。只做普通 Upgrade 时应移除旧恢复字段，避免把复制的 CR 当成一次新的恢复请求。

恢复提交强制禁止 last-state fallback；Verifier 和重启后的 worker 从同一持久化命令读取 path/nonce，要求匹配 Operator `lastReconciledSpec` 中的路径、nonce、savepoint 模式和禁止 fallback 配置，再结合 UID/generation/READY/Job 状态完成验证。目标为暂停状态时也允许显式恢复。CR 证据证明 Operator 已协调该声明；文件可读和示例 keyed state 连续仍需 Flink/RustFS 现场验收，不能由 READY 单独推断。

## 操作持久化与恢复约束

`OperationCodec` 是 Kubernetes CR 与 PostgreSQL 共用的持久化边界。新写入记录携带 `schemaVersion: 1`，显式保存 Suspend/Delete 策略、升级保护与 fallback、恢复参数和提交身份。未知版本、未知事件或策略、损坏的 Evidence、非整数 generation/nonce 都拒绝读取，并由 store 返回 `StoreFailure`。程序不会把坏数据改写成成功或自动生成 `Uncertain` 事件。

读取时从 `Accepted` 重放完整历史，使用同一个 `OperationStateMachine` 校验每次转换，再核对持久化 state、createdAt 与 updatedAt。事件时间不要求跨节点单调递增；createdAt 必须对应首个 Accepted，updatedAt 必须对应末事件。缺失的历史证据仍保持缺失，不能被补成成功依据。

无版本记录只兼容已知旧编码：缺失 Suspend 策略解释为 `KeepState`，缺失 Delete 策略解释为 `Graceful`；旧 Submitted/Observed 的 generation `0` 和空 resourceVersion 表示未返回身份。旧编码已经丢掉的 `DiscardState` / `Force` 无法从 JSON 反推。需要这些语义的历史操作应按原请求和外部证据核对，不能宣称可无损恢复。新版本缺失策略或任何版本显式写入未知策略都拒绝读取。

Resume 必须观察到对应 UID、提交 generation/observedGeneration 的 `RUNNING + READY`。旧作业 `FINISHED` 不算恢复成功；若有界作业在观察到 RUNNING 前已经结束，当前会继续观察并超时，不用 FINISHED 猜测本次恢复结果。Deploy 对自然结束的 `FINISHED + READY` 仍维持原有判据。

Kubernetes 普通 HTTP 请求将 ZIO 中断传给对应 OkHttp `Call.cancel()`，响应体读取在 `finally` 中关闭。Watch 在等待响应头之前登记 Call 的关闭责任，建连和等事件均显式可取消；Scope 结束会取消 Call 并关闭 Watch，取消后迟到的 Watch 也会关闭。续租失败会先发出本地取消再释放锁；取消无法撤销已经到达 API Server 的写入，因此仍依赖 UID/resourceVersion 前置条件、确定名称创建和未知写入结果不重放。持久化 `ValidationPassed` 后没有提交回执时，恢复仍进入 `Uncertain`，不会重复提交。

## 当前实现边界

1. HTTP 写请求统一生成 `FlinkOperation`，服务返回 `operationId`。
2. `ResourceObserver` 先 list，再用 list 的 `resourceVersion` watch；收到 410 Gone 会重新 list。
3. `VerificationEngine` 使用 `Evidence` 校验 UID、generation、observedGeneration、reconciliation 和 Job 状态。
4. `AsyncOperationWorker` 执行提交、观察、验证和失败转移；Kubernetes `FlinkOperationLock` CR 保护跨副本的资源级互斥，带 `leaseUntil` 的锁会持续续租，过期锁可按 UID 接管，`OperationMutex` 保护进程内副作用。PostgreSQL 后端使用行锁保护共享操作记录。
5. 真实 checkpoint/savepoint 路径和恢复结果需要目标集群带 RustFS 插件、Secret 和 Registry pull secret 的作业验收。

## 资源边界

| 资源 | 用途 |
| --- | --- |
| `FlinkDeployment` | 当前 HTTP 模板创建 Application Cluster 并运行一个 Flink Job |
| `FlinkSessionJob` | 把 Job 提交到已有的 Session Cluster |
| `FlinkStateSnapshot` | 请求 savepoint 或 checkpoint，并观察快照状态 |

所有资源都通过 Kubernetes API Server 提交。程序不调用 `kubectl`，不直接操作 JobManager Pod。

普通 Deploy 使用 POST 创建，Upgrade 与显式恢复使用 UID/resourceVersion 约束的 merge patch；仅预览通过 Server-Side Apply 加 `dryRun=All` 返回资源。typed checkpoint/savepoint operation 使用确定名称创建 `FlinkStateSnapshot`，随后只观察这一个 Snapshot CR。Operator 记录请求、结果路径和失败信息。

## 状态来源

任务状态来自 Flink CR 的 `status`：

- 生命周期：`lifecycleState`、`jobManagerDeploymentStatus`、`reconciliationStatus`。
- Job：`jobStatus.jobId`、`jobStatus.jobName`、`jobStatus.state`、`startTime`、`updateTime`。
- 条件与错误：`conditions`、`status.error`。
- checkpoint 摘要：`jobStatus.checkpointInfo`。
- savepoint 摘要：`jobStatus.savepointInfo`，包括最后路径和历史记录；升级 Savepoint 还可能由 Operator 写入 `jobStatus.upgradeSavepointPath`，验证器将两者都作为结果路径来源。

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

目标集群的发布版本、操作 ID、RustFS 文件核对、恢复和锁接管结果统一记录在 [测试与证据](testing.md)，不以历史构建替代当前版本的验收。单元测试、构建和清单渲染不能替代真实集群证据，控制面读请求指标也不能外推为 Flink 作业吞吐容量。

本地 OrbStack 只有一个节点，API 两个 Pod 和前端一个 Pod 只能证明进程副本与 Service 路由。目标集群清单使用前端 NodePort、API ClusterIP、API 两个副本和前端一个副本；两台机器的跨节点调度需要目标集群可达、节点标签正常、Operator 已安装，并通过滚动重启和状态接口验证。每个 API 副本都轮询同一 Kubernetes API，状态统一来自 CR；这会按 API 副本数增加 list 请求量，后续高规模场景应增加 leader election 或集中式 watch。

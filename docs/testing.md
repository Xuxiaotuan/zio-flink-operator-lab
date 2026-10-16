# 测试与证据

测试区分四个层次：程序生成资源、API Server 接受资源、Operator reconcile、Flink 任务和快照真实完成。低层测试不能替代高层运行证据。

## 本地验证

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
sbt -batch test
sbt -batch assembly
mvn -B -f job/pom.xml package -DskipTests
```

### 2026-10-16 修复验证

- 当前代码 `9804df5` 的 `sbt -batch test assembly`：175 个测试通过，6 个按开关跳过，assembly 成功。跳过项是 4 个外部 HTTP E2E 和 2 个独立 PostgreSQL 契约用例；部署后的流水线另行开启并通过全部 4 个外部 HTTP E2E。
- 修复覆盖 POST 创建与 UID/resourceVersion 约束的 merge patch 更新、写入响应丢失后不自动重放、严格 Restart/Suspend 保护证据、Operator `SUSPENDED/FINISHED` 暂停组合、UID/resourceVersion 绑定、删除及快照恢复、幂等创建竞争、缺失资源、有界并发、活动操作查询、HTTP 错误分类和 readiness 响应时限。
- Flink 1.20.1 语法树解析回归覆盖别名逗号连接、子查询、CTE、CTAS、残缺 SQL 及不支持节点；assembly 中两个 JSON_QUERY 补丁类已核对采用 Flink 的实现。
- 前端镜像构建通过，其中执行的 Node 测试为 19/19。

打包后的可复现验收：

```sh
sbt -batch assembly
python3 src/test/scripts/packaged-http-contract.py
```

脚本需要 Docker，自动创建仅绑定回环地址的临时 PostgreSQL 和本地 Kubernetes 协议替身，再启动两个实际 JAR 进程。已验证 A 写入 Catalog/Schema/静态血缘、B 读取，跨进程 requestId 幂等和缺失目标 FAILED 查询；使用 JDK 17 启动的两个进程中，暂停临时 PostgreSQL 后，`/readyz` 在约 3.02 秒返回 503，`/healthz` 保持 200。脚本最后删除自己创建的容器并退出两个服务进程，本轮复查无临时资源残留。

这是实际 HTTP 进程、真实 PostgreSQL 与 Kubernetes 协议替身的集成证据。真实集群结果单独记录在下文，二者不能相互替代。

## 最新真实验收（2026-10-16）

发布代码为 `9804df5`。PipelineRun `zio-flink-operator-lab-nlkcj`（Jenkins `#47`）为 `Succeeded` / `Finished: SUCCESS`，包含 175 个本地测试、assembly、镜像构建推送、前后端 rollout，以及部署后 4/4 HTTP E2E；独立 PostgreSQL 契约的两项仍按环境开关跳过。API/UI 镜像标签为 `build-47-9804df52f67f`。

环境为 `bigdata-lab`、Kubernetes `v1.23.17`、Flink Operator `1.16.1`、Flink `1.20.1`。API 两副本分别在 `xjw`、`xxt`，最终均 Ready、重启 0 次；前端单副本 Ready。两台节点的 `:30882/` 返回与仓库相同的 HTML。验收作业为 `zio-e2e-20261016-749673-stateful`，使用 `StatefulCounterJob` 和独立前缀 `s3://flink-savepoints/zio-e2e-20261016-749673/`。

### 操作与实际结果

验收随修复滚动进行：提交与初始快照在 `#45`，首次成功升级和 Pod 故障在 `#46`；暂停重测、过期锁接管、指定恢复、最终双副本共享及清理在 `#47`。以下操作始终关联同一测试 CR UID。

| 场景 | operationId | 最终状态 |
| --- | --- | --- |
| 提交作业 | `50a4cbd6-92bf-354c-bed8-a8b1ae9cbdec` | `COMPLETED` |
| 手动 checkpoint | `9b741e17-4e89-3b9d-b751-8c92faf6ae86` | `COMPLETED` |
| 手动 savepoint | `d9bd2060-0276-374b-b2e4-2f6a7e5c478d` | `COMPLETED` |
| 严格升级 + 删除一个 API Pod | `7189fa8b-6f7b-3ac5-b143-cf549942f957` | `COMPLETED` |
| savepoint 暂停 | `a558aea9-1256-3aaa-9f0c-e3122a3e18cb` | `COMPLETED` |
| 恢复运行 | `f3e12746-3c12-3fb8-a941-9e71d640cc53` | `COMPLETED` |
| 过期锁接管 + 严格重启 | `0d43ba84-c638-33b3-87d5-88e17e29d214` | `COMPLETED` |
| 指定 savepoint 恢复 | `6219c5e3-4283-3202-8c44-fe21a0352bb2` | `COMPLETED` |
| 删除测试作业 | `bdeeb690-8b5b-347c-a831-cc2196bcbf8e` | `COMPLETED` |

- 自动 checkpoint、手动 checkpoint/savepoint 均有实际结果路径；在保存前、升级后、暂停恢复后、接管重启后和指定恢复后，分别观察到至少 2 个已完成 checkpoint，结果 `_metadata` 的 RustFS HEAD 均为 200。Flink 默认只保留最近一次 checkpoint，第一份手动 checkpoint 在检查前已被周期 checkpoint 淘汰；将测试配置 `state.checkpoints.num-retained=10` 后复验通过。`COMPLETED` 不能作为文件永远保留的保证。
- 严格升级从 parallelism 1 改为 2，CR UID 始终为 `268e6d69-97d8-420e-90fa-ee62a0cbbe09`；Job ID 从 `f8dcdb948eaa48919f420e92c6b7b655` 变为 `b2a84cd0c5d4107363944bc006d08914`。operation 为 `VERIFYING` 时删除 xjw 上一个 API Pod，随后通过服务查询同一 operation 到 `COMPLETED`。这证明本次操作在一个 API Pod 删除期间完成，不单凭 Pod 接收请求就推断它持有租约。
- 过期锁 `flink-lock-b8a68274a0f26853` 的旧 UID `cda3009c-cb52-47cf-8e97-83e81ad305ce` 被新 UID `b2220693-1465-459d-9512-1d1a2c97ec05` 替换，新 `spec.operationId` 为 `0d43ba84-c638-33b3-87d5-88e17e29d214`，重启最终完成。初次验收脚本使用了复数资源类型计算锁键，未覆盖真实锁；错误测试锁已删除，以上结果来自改用源码中的单数资源键后重跑。
- 指定恢复源为 `s3://flink-savepoints/zio-e2e-20261016-749673/savepoints/savepoint-f8dcdb-c77e59d0dd00`。JobManager 日志和 Flink REST `latest.restored.external_path` 都等于该路径；恢复后的 Job ID 为 `735dbddc78edc2fe56020701b9cbe3f8`，并继续完成 checkpoint。保存前记录的 ValueState 计数为 `10585`，恢复后捕获的最小值为 `10732`，确认未从零开始。该示例只证明 keyed ValueState 恢复，不证明真实业务源位点或端到端 exactly-once。
- 直连两个不同节点的最终 API Pod，验证 A 写 Schema Snapshot `v2`、B 读取 `orders_peer_v2`，且两副本读到完全相同的已完成 operation。Catalog/Schema/SQL 静态血缘写入实际 PostgreSQL，CTE + 逗号 JOIN 提取出两个来源表。此部署的状态和 operation 后端为 Kubernetes；本轮没有重新部署 PostgreSQL OperationStore 双节点模式，其历史证据与本地双进程契约分开保留。

### 现场失败与修复

- Pipeline `#44` 的部署后 E2E 失败：非绝对 Service FQDN 在该集群的搜索域下命中 FakeDNS；同一请求用 ClusterIP 或末尾带 `.` 的 FQDN 则成功。流水线及 RustFS 示例 endpoint 改为绝对 DNS 后，`#45`、`#46`、`#47` 均成功。
- 升级 operation `4f1fc3fe-29b5-349a-bde1-bb6f95e1b242` 为 `FAILED`：POST 的 Update 字段所有权与 SSA Apply 冲突，即使 fieldManager 同名仍返回 409。`470de4e` 将已有资源写入改为 UID/resourceVersion 约束的 merge patch，补充先失败后通过的回归测试，真实升级复验通过；没有启用强制字段接管。
- 暂停 operation `193e9cf8-f288-375d-a26e-ec6774dcf42b` 为 `TIMEDOUT`：Operator 实际返回 `lifecycleState=SUSPENDED`、`jobStatus.state=FINISHED`。`9804df5` 同时核对生命周期与已协调的 suspended 声明，再结合 savepoint/generation 证据判定；缺生命周期、仍 RUNNING、缺保护路径或旧 generation 的回归均拒绝。修复后从运行状态重新暂停与恢复通过，未改写旧失败记录。
- 单副本 UI 使用 `maxSurge=0/maxUnavailable=1`，发布时出现短暂连接拒绝。验收只恢复查询原 operationId，没有重发写请求；该部署不承诺前端发布期间零中断。

### 清理与边界

通过 HTTP 删除 operation `bdeeb690-8b5b-347c-a831-cc2196bcbf8e` 完成后，确认测试 JobManager/TaskManager、Service、Deployment、ReplicaSet、ConfigMap 均无残留；共删除 8 个快照 CR、14 个操作记录、测试锁、RustFS 独立前缀下 32 个对象，以及 PostgreSQL 中 2 条血缘、2 条 Schema Snapshot、1 条 Catalog。源 savepoint 文件已经清理，上述 HEAD 200 是删除前证据。`orders`、`wordcount-demo`、`zio-word-count` 的 UID 与验收前完全相同。

清理时先删除 Job，导致手动 savepoint 的默认文件销毁 finalizer 找不到原 Job；只对该测试快照设置 `disposeOnDelete=false` 后，Operator 正常完成 CR 清理，再显式删除自己的 RustFS 前缀，未强行移除 finalizer。后续验收应先处理 Snapshot 的文件保留/销毁策略，再删除原 Job。

本轮没有整台宿主机故障注入、生产容量压测或多小时稳定性验证。验收中使用 Flink REST 读取 checkpoint 和恢复证据，不表示控制面新增直接操纵 JobManager 的执行通道。

## 历史基线与现场记录

当前本地执行结果以本轮实际命令为准。新增覆盖 ResourceObserver 的 resourceVersion/410 relist、PolicyEngine 接入、提交 generation 等待验证、Snapshot CR、Kubernetes Operation CR、资源锁、HTTP typed operation、AsyncOperationWorker、PostgreSQL OperationStore 幂等键和 operation lifecycle 查询；本轮增加 Catalog/Schema Snapshot、PostgreSQL 元数据适配器、表级 SQL_STATIC 血缘解析和目录/血缘 HTTP 契约。

2026-10-10/11 历史基线：`sbt -batch test` 通过 129 个测试、5 个按条件跳过（4 个外部 E2E 加 1 个未配置 PostgreSQL 的契约用例）；`sbt -batch assembly` 成功生成 assembly（本轮 SHA-1 `20e591858e98bcbdaf9dc8f043cd4df8d7896cc3`）；`node --test src/test/web/app.test.mjs` 通过 19 个测试，`node --check src/main/resources/web/app.js` 通过，`mvn -B -f job/pom.xml package -DskipTests` 成功；前后端镜像构建通过，Nginx 在为本地校验容器补充 `zio-flink-operator=127.0.0.1` upstream 映射后 `nginx -t` 通过。

以下本地结果只说明代码级契约和构建通过，不能替代现场验收。目标集群的 PipelineRun、Operator reconcile、两副本统一 operation 状态、checkpoint/savepoint 写入 RustFS、savepoint 恢复、单副本故障演练、锁租约接管和 PostgreSQL 双节点运行已有现场证据；HTTP dry-run、策略请求一致性、活动资源互斥、Operation resourceVersion CAS、worker 阶段恢复、快照 UID/路径校验、删除 UID 前置条件、watch EOF 重连和 Evidence 审计也有回归测试。控制面压力与长稳结果见 2026-10-08 历史记录，本轮未重跑容量测试。

历史现场记录：StatefulCounterJob 已通过 RustFS checkpoint、FlinkStateSnapshot savepoint 和新 Deployment 恢复；该记录保留用于说明早期 Operator 行为，当前恢复验收以 2026-10-16 记录为准。

### 历史现场验收（2026-10-10）

- PipelineRun `zio-flink-operator-lab-jd8bc`（Jenkins build `#37`）成功，提交 `c44502a`；目标 Deployment 镜像为 `build-37-c44502a7cb3f`，两个副本分别在 `xjw`、`xxt`，均 `1/1`、重启 0 次。
- 第一轮恢复请求 `zio-e2e-restore-20261010-v2` 没有产生资源副作用，Operation 进入 `UNCERTAIN`；原因是 ujson 把 Scala `Long` 渲染成字符串，API Server 拒绝 `savepointRedeployNonce` 的 CRD 类型。该失败促成显式 `ujson.Num` 编码和数值类型回归测试。
- 修复后的 Operation `cc2b7a3f-0188-342f-8f05-62851aca2dac` 为 `COMPLETED`，事件包含 `SUBMITTED → WAITING_FOR_OBSERVATION → OBSERVED → VERIFICATION_SUCCEEDED`；generation/observedGeneration 都为 `3`，`lastReconciledSpec` 包含 `upgradeMode=savepoint`、源 Savepoint 和 `savepointRedeployNonce=1`。
- Flink Job ID 从 `e1a10ea8e1efb2936b3f176fd60a6e80` 变为 `c4758c4cf3758a58df9384d511e1c205`。JobManager 日志记录 `Restoring job ... from Savepoint`，Flink REST 显示 `restored=1`、`completed=10`，最新 checkpoint 路径为 `s3://flink-savepoints/zio-e2e-20261010/checkpoints/c4758c4cf3758a58df9384d511e1c205/chk-15`；源 Savepoint 的 RustFS `_metadata` HEAD 返回 HTTP 200。
- 验证完成后删除了本轮 `zio-e2e-*` 的 FlinkDeployment、FlinkStateSnapshot、FlinkOperation、JobManager、TaskManager 和 Service；复查没有临时资源残留。RustFS Savepoint 对象保留为审计和恢复证据。

### 前后端拆分验收（2026-10-10）

- PipelineRun `zio-flink-operator-lab-qmp6n`（Jenkins build `#39`）成功，目标镜像为 API `build-39-fb007b454494` 和前端 `zio-flink-operator-ui:build-39-fb007b454494`。
- `zio-flink-operator` API Service 已为 ClusterIP，两个 API Pod 分别运行在 `xjw`、`xxt`；`zio-flink-operator-ui` 为单副本 Deployment，运行在 `xjw`，NodePort `30882` 对外提供页面和 Nginx 反向代理。
- 两个节点的 `http://<node>:30882/` 均返回 `ZIO Flink Platform` 页面；`/healthz`、`/readyz` 和经代理的 `/v1/config` 均返回成功，namespace 为 `bigdata-lab`。

现场故障演练：删除 xjw 节点上的一个控制面 Pod 后，xxt 节点副本持续返回 `{"status":"ok"}`，Deployment 自动补回 xjw 副本并恢复 `2/2`；Flink 作业和 Kubernetes CR 未受影响。
- 覆盖 HTTP/domain、类型化控制面、FlinkDeployment/FlinkStateSnapshot JSON、fake Kubernetes API、HTTP wire contract、HTTP 控制面状态/快照接口、统一状态后端配置与接口、watch 状态投影、checkpoint/savepoint 字段、回退审计、锁租约续期与接管、重试策略。
- Maven Job：应以本轮命令的 `BUILD SUCCESS` 为准。
- `job/target/zio-flink-wordcount-0.1.0.jar` 同时包含 `StatefulCounterJob`；目标集群现场测试应使用 `examples/flinkdeployment-stateful.json`，确认 checkpoint 计数、savepoint `status.path` 和恢复后 `stateful-counter` 日志连续。

测试按职责分组：HTTP/domain、typed control-plane、policy、operation store/worker、operation factory、ResourceObserver、fake Kubernetes API、HTTP contract、watch/retry、状态后端和状态轮询。

这些测试默认不连接真实 Kubernetes。HTTP contract test 使用本地 HTTP server，只验证 Kubernetes Java Client 的请求协议。

### 外部 HTTP E2E

`ExternalControlPlaneE2ESpec` 是只读的部署后冒烟测试，访问已经部署的 HTTP 控制面，检查健康检查、namespace 配置、Kubernetes 状态后端、部署/快照和 Catalog/Lineage 查询。它不验证元数据写入持久化、跨副本写后读或真实 Flink 作业完成。它不创建或删除 Flink 资源，因此不会留下临时 CR、Pod、Service 或对象存储数据。

默认关闭，避免普通单测访问网络：

```sh
sbt -batch "testOnly cn.xuyinyin.flinklab.e2e.ExternalControlPlaneE2ESpec"
```

显式开启后访问真实 NodePort 或端口转发地址：

```sh
ZIO_FLINK_E2E_ENABLED=true \
ZIO_FLINK_E2E_BASE_URL=http://<任一节点>:30882 \
ZIO_FLINK_E2E_NAMESPACE=bigdata-lab \
sbt -batch "testOnly cn.xuyinyin.flinklab.e2e.ExternalControlPlaneE2ESpec"
```

2026-10-11 曾使用 `http://100.82.226.63:30882` 对当时部署的目标集群运行该命令，4 个测试全部通过；测试客户端显式绕过构建机代理，直接验证 NodePort/Service 的真实连通性。

KubeSphere Pipeline 在 API/UI rollout 完成后执行同一套测试，使用集群内的 `zio-flink-operator.<namespace>.svc.cluster.local.:8080`，该阶段检查本次部署后的 Service、查询权限和接口连通性；PostgreSQL 存储契约必须另外在独立测试库执行。服务 DNS、RBAC 或依赖网络不可达时，流水线应保留为 `external_blocked`，不能把本地单测结果当作现场通过。

## 历史目标集群验证

以下为早期环境和功能记录，当前发布以 2026-10-16 验收为准：

- K8s `v1.23.17`，两节点 `xjw`、`xxt`；Flink Kubernetes Operator `1.16.1` 已 Ready，并只 watch `bigdata-lab`。
- `zio-flink-operator` 两个 API Pod 分别调度到 `xjw`、`xxt`；前端 `zio-flink-operator-ui` 单副本通过 NodePort `30882` 对外提供页面和反向代理，从两台节点访问 `/healthz` 均返回 `{"status":"ok"}`。
- 从两台节点访问 `/v1/state?namespace=bigdata-lab` 都读到同一个 `zio-word-count` CR，backend 为 `kubernetes`，生命周期为 `STABLE`。
- 通过 HTTP 提交 `FlinkDeployment` 后，Operator 让 JobManager、TaskManager 进入 Ready，Job 进入 `FINISHED`，并产生 jobId `475c0a0e218426706464212870d4e5cf`。

### 历史现场验收（2026-10-07）

- PipelineRun `zio-flink-operator-lab-jddn6`（Jenkins build `#30`）成功：提交 `a4ffa3b` 的 112 个测试、assembly、控制面镜像和 Stateful Job 镜像均完成；目标 Deployment rollout 到 `build-30-a4ffa3bfb705`，两个副本分别位于 xjw、xxt，两台 NodePort 的 `/healthz` 均返回 `{"status":"ok"}`。
- 首次使用 build `#29` 做真实严格 Savepoint 升级时，operation `b34bf428-6a80-387f-8e96-b95ba49d11dd` 正确进入 `FAILED`，原因是 Operator 的升级结果路径位于 `status.jobStatus.upgradeSavepointPath`，旧观察逻辑只读取 `savepointInfo.lastSavepoint.location`，且不能把 READY 但证据暂缺视为确定失败。该现场失败促成 `a4ffa3b` 修复，并保留为回归依据。
- build `#30` 部署后，真实 `zio-e2e-strict-savepoint` 通过 HTTP 提交严格 Savepoint 升级；operation `966c9524-0494-373c-991a-ed2ccc30f27e` 最终为 `COMPLETED`，事件包含 `SUBMITTED → WAITING_FOR_OBSERVATION → OBSERVED → VERIFICATION_SUCCEEDED`，generation/observedGeneration 均为 `3`，审计证据保存路径 `s3://flink-savepoints/zio-e2e-strict/savepoints/savepoint-176ced-43a90de992e0`。
- 同一 Job 的 Flink 日志记录从该 Savepoint 恢复，并连续完成 checkpoint `302` 至 `318`；RustFS 对该路径的 `_metadata` HEAD 返回 HTTP 200。验证后通过 operation `40fe3b54-1380-37df-bd6f-24e3411e96ef` 删除 Deployment，两个升级 Snapshot CR 也已删除；复查没有 `zio-e2e-*` 或 smoke 的 FlinkDeployment、FlinkStateSnapshot、Pod、Service、ReplicaSet、Deployment 或 Lock 残留。对应的 FlinkOperation CR 和 RustFS 快照对象保留为审计和恢复证据。

### 历史多节点锁、PostgreSQL 与稳定性验收（2026-10-08）

- 真实租约接管：先在 `bigdata-lab` 创建已过期的 `FlinkOperationLock/flink-lock-874c2d79e17088bf`，旧 operation 为 `zio-e2e-lock-old`；通过双副本控制面提交后，operation `9afbee58-823b-3d6e-8f0e-4fdad1a9574d` 创建新锁 UID、更新 `spec.operationId` 并延长 `leaseUntil`，最终进入 `COMPLETED`。随后删除 operation `4a383749-387e-35bd-aadf-45eb38e9dbc3` 完成清理，Lock、Deployment、Snapshot、Pod 和 Service 均无残留。
- PostgreSQL 双节点：临时双副本控制面分别调度到 `xxt`、`xjw`，使用 PostgreSQL 状态与 operation store。两台 NodePort（`30884`）的 `/v1/state?namespace=bigdata-lab` 均返回 `backend=postgres` 和相同 `resourceVersion=205312379`；同一个 requestId 在两台入口得到 operation `6001acb1-e3c0-362d-8a25-d610bcc356be`，两台查询均为 `COMPLETED`，事件链一致为 `ACCEPTED → VALIDATION_STARTED → VALIDATION_PASSED → SUBMITTED → WAITING_FOR_OBSERVATION → VERIFICATION_STARTED → OBSERVED → VERIFICATION_SUCCEEDED`。清理 operation `67658226-1eae-30d4-8a83-d3201b878a8b` 完成后，临时 Deployment、Service、Secret 和测试表已删除。
- 压力基线：16 并发、60 秒、两节点交替访问健康和状态接口，共 9507 次请求，成功率 100%。`/healthz` 7610 次，P50/P95/P99/最大延迟为 `20.4/159.0/255.2/425.2 ms`；`/v1/state` 1897 次，P50/P95/P99/最大延迟为 `58.9/512.2/745.4/930.2 ms`。按成功率 ≥99%、健康 P95 ≤500ms、状态 P95 ≤2000ms 的本轮基线通过。
- 长稳基线：10 分钟、每 5 秒从两台 NodePort 访问 `/healthz` 和 `/v1/state`，共 468 次请求全部成功，错误率 `0%`，P95 `66.4ms`，最大 `451.7ms`；两个控制面 Pod 全程 `restartCount=0`。这是控制面读请求稳定性证据，不是 Flink 作业吞吐、故障注入或多小时生产 SLO 证明。

- PipelineRun `zio-flink-operator-lab-wjclx`（Jenkins build `#27`）成功：提交 `0561654` 的测试、assembly、控制面镜像和 Stateful Job 镜像均完成，目标 Deployment rollout 到 `build-27-0561654c8687`。
- PipelineRun `zio-flink-operator-lab-wh67d`（Jenkins build `#28`）成功：提交 `0e72734` 的 108 个测试、assembly、控制面镜像和 Stateful Job 镜像均完成，目标 Deployment rollout 到 `build-28-0e72734e935c`；两副本分别位于 xxt、xjw。

- PipelineRun `zio-flink-operator-lab-582f4`（Jenkins build `#24`）成功：包含 StatefulCounterJob 的 Maven 打包、带 S3 插件的 Flink Job 镜像构建/推送，以及控制面 Deployment rollout。控制面镜像为 `build-24-ba815f757367`，测试 Job 镜像为 `100.97.53.78:5001/xxt/zio-flink-stateful-job:build-24-ba815f757367`。
- PipelineRun `zio-flink-operator-lab-96csb`（Jenkins build `#23`）成功：提交 `7e0a00b` 的测试、assembly、镜像推送和目标 Deployment rollout 均通过。最终镜像为 `build-23-7e0a00bfa84d`。
- 新镜像 Deployment 为 `2/2`，两个 Pod 分别运行在 `xxt` 和 `xjw`；NodePort `30882` 的 `/healthz` 和 `/readyz` 均返回 `{"status":"ok"}`。
- 使用 `dryrun-7e0a00b` 在目标集群执行 HTTP `POST /v1/deployments?namespace=bigdata-lab&dryRun=true`，API 返回 dry-run 对象预览；随后查询 `FlinkDeployment/dryrun-7e0a00b` 返回 `NotFound`，证明该请求未创建业务 CR，也未进入 operation worker。
- PipelineRun `zio-flink-operator-lab-jlkxm`（Jenkins build `#22`）成功：提交 `4232b75` 的测试、assembly、镜像推送和目标 Deployment rollout 均通过。最终镜像为 `build-22-4232b75b9b9a`。
- Deployment 为 `2/2`，两个 Pod 分别运行在 `xxt` 和 `xjw`；两个 NodePort 地址的 `/healthz` 均返回 `{"status":"ok"}`。
- 请求 `smoke-20261006-5` 的 operation `c3f6530c-9777-43b4-bbb5-8b35a831cc9a` 在两个节点返回完全相同的 `COMPLETED` 事件链：`SUBMITTED → WAITING_FOR_OBSERVATION → OBSERVED → VERIFICATION_SUCCEEDED`。
- 对应 `FlinkDeployment/zio-control-plane-smoke-5` 状态为 `READY/FINISHED/DEPLOYED`。这证明了 HTTP 提交、Operation CR、resourceVersion/watch 观察、Operator reconcile、自然结束任务判定和两副本统一读取状态。

早期现场证据覆盖了运行中 Job 的 checkpoint、savepoint、RustFS 结果路径和 savepoint 恢复连续性；对已结束 Job 的 savepoint operation `dfa0f661-22ad-4f07-9ccd-77101c6d95f3` 仍保留为负向契约证据，审计事件记录 Operator 的 `ABANDONED` 原因（目标 Job 不在运行）。

此前 build `#11` 的 Source 阶段曾因 GitHub 连接重置而 `external_blocked`；后续 PipelineRun `#22` 已成功，说明当时的已推送基线具备 CI/CD 证据。这些是历史记录，本轮 CI/CD 以 2026-10-16 的 PipelineRun 为准。

## 测试覆盖

| 层级 | 能证明 | 不能证明 |
| --- | --- | --- |
| Scala/domain/HTTP | 参数、资源 JSON、快照类型和 patch | API Server 接受资源 |
| Fake Kubernetes API | 业务调用的资源类型、namespace、name、body 和顺序 | Java Client 的真实 HTTP 请求 |
| HTTP wire contract | SSA、merge patch、list/get/delete、重试和错误分类 | 当前集群权限、CRD 版本和 Operator 行为 |
| HTTP control-plane tests | deployment status、snapshot create/list/get/delete、状态字段序列化 | 真实 Flink 作业和快照结果 |
| Server-side dry-run | API Server 接受资源和权限 | Operator reconcile、Pod、Job、checkpoint/savepoint |
| 集群集成 | CR status、Pod、Job、快照路径和恢复结果 | 生产 HA、长期稳定性和业务正确性 |

## 当前状态模型

FlinkDeployment 和 FlinkSessionJob 的监控摘要包括 lifecycle、Job 状态、JobManager 状态、conditions、error、reconciliation、checkpointInfo 和 savepointInfo。

FlinkStateSnapshot 的监控摘要包括目标 Job、state、path、error、failures、triggerId 和时间戳。checkpoint/savepoint 请求只有在对应 Snapshot CR 完成并出现结果路径后，才算完成。

逐个 checkpoint 的大小、耗时、完成数量和 task/subtask 明细需要 Flink REST；当前测试没有把这些数据伪装成 Kubernetes CR 字段。

## 本地部署证据

已验证的本地部署检查：

```sh
kubectl apply --dry-run=client -k deploy/local
kubectl apply -k deploy/local
kubectl -n flink-lineage-test rollout status deployment/zio-flink-operator --timeout=120s
```

本地 API Deployment 为两个副本、前端 Deployment 为一个副本，OrbStack 当前只有一个节点。健康检查和 deployment dry-run 可以验证服务进程、Service、RBAC 和 API Server 访问；不能证明跨节点高可用或真实 Flink Job。

## 真实集群验收

在目标集群按以下顺序记录证据：

1. `apply --dry-run` 返回成功。
2. 真实 apply 返回成功，CR 进入 Operator 处理状态。
3. JobManager、TaskManager 和 Job 进入预期状态。
4. `FlinkStateSnapshot` 的 state、path、error、failures 与触发请求一致。
5. 从 savepoint 恢复后，Job 状态和业务状态连续。

如果只拿到 HTTP 200、CR 已创建或旧的 `lastSavepoint`，标记为 `evidence_incomplete`。如果被集群、Operator、权限、存储或 kubeconfig 阻塞，标记为 `external_blocked`，保留阻塞条件。


### 工作台入门提交回归

工作台默认模板是带 `serviceAccount: flink` 的无状态 WordCount，不声明 savepoint/last-state，因此不会触发 Flink Operator 对 checkpoint 目录的要求。选择 `savepoint` 或 `last-state` 时，前端会在请求发出前检查 checkpoint 与 savepoint 目录；最终仍以 Operator 校验为准。

NodePort 使用 HTTP 时，工作台会在 `crypto.randomUUID()` 不可用的浏览器环境中回退到 `crypto.getRandomValues()` 生成 UUID v4，并把 requestId 写入输入框。网络失败重试复用同一 requestId，用户点击“生成新 ID”才开始新的操作意图。

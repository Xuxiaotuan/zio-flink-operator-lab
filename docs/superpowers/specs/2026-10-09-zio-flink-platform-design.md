# ZIO Flink Platform 设计规格

## 目标

构建一个基于 Scala 3 + ZIO 的 HTTP-only Flink 开发与运行平台，覆盖 SQL 开发、作业编译、Catalog/元数据、血缘、发布、状态观察和 checkpoint/savepoint 管理，作为当前 Flink Kubernetes 场景下 StreamPark/Dinky 的精简替代。

平台的核心不是功能数量，而是：**所有作业变更都经过统一的 `FlinkOperation`，并以 Kubernetes、Flink 和 RustFS 证据确认结果。**

## 产品边界

支持：

- Flink Kubernetes Operator；
- Kubernetes Application/Session 相关 Flink 资源；
- SQL 文档、版本、校验和编译；
- Job Artifact 生成和 RustFS 存储；
- Catalog、Schema Snapshot 和表字段查询；
- SQL 静态血缘和运行期血缘事件；
- 作业发布、升级、暂停、恢复、重启、删除；
- checkpoint/savepoint 请求、恢复和证据校验；
- 两个或多个 HTTP 副本共享 Kubernetes/PostgreSQL 状态；
- 浏览器前端和 HTTP/SSE API。

不支持：

- Spark、YARN、Standalone 等非 Kubernetes 执行模式；
- 多租户；
- 用户认证和授权；
- 公网暴露；
- 通用工作流引擎；
- 重新实现 Flink SQL Planner、Kubernetes Operator 或 Flink Runtime；
- 让 StreamPark、Dinky 和本平台同时修改同一个 Flink 资源。

不做认证授权意味着平台是受信任内网工具。部署必须通过私有网络、Kubernetes NetworkPolicy 或受限 Ingress 保护，不能直接暴露到公网。

## 核心不变量

1. Kubernetes Flink Operator 是 Flink 资源的唯一执行者；ZIO 服务只通过 Kubernetes API 提交和观察 CR。
2. SQL、Artifact、Jar 和前端提交最终都转换为同一个 `FlinkOperation`。
3. API Server 接受请求只产生 `Accepted`，不能直接表示业务完成。
4. `Completed` 必须拥有目标资源 UID、提交后 generation、observedGeneration、Operator reconcile 状态、实际 Job 状态和所需快照路径等 Evidence。
5. `StateProtection.Savepoint` 不允许隐式降级到 last-state；无明确证据时只能返回 `Uncertain` 或失败。
6. `requestId` 用于幂等，`operationId` 用于一次控制面操作，`resourceUid` 用于绑定 Kubernetes 资源实例。
7. 多副本不保存本地会话状态；Kubernetes CR 是 Flink 操作状态来源，PostgreSQL 保存 SQL、Catalog、Schema 和 Lineage 查询数据，RustFS 保存 Artifact 和 Flink 状态文件。

## 系统结构

```text
Browser SPA
  ├── SQL Workspace
  ├── Catalog / Metadata
  ├── Lineage
  ├── Release Center
  ├── Job Detail
  └── Operation / Evidence
          │ HTTP / SSE
          ▼
ZIO HTTP Platform
  ├── SQLApplication
  ├── CompileApplication
  ├── MetadataApplication
  ├── LineageApplication
  ├── ReleaseApplication
  ├── OperationWorker
  ├── ResourceObserver
  └── EvidenceVerifier
          │
          ├── Kubernetes API
          ├── PostgreSQL
          └── RustFS
          │
          ▼
Flink Kubernetes Operator → Flink Job
```

前端静态文件和 HTTP API 由同一个 ZIO 服务提供，生产部署不拆分前端服务和后端服务。

## 领域模型

### SqlDocument

```text
id, version, sql, catalogId, databaseName, parameters, checksum, createdAt
```

### JobArtifact

```text
artifactId, sourceDocumentId, uri, sha256, flinkVersion, plannerVersion, schema, createdAt
```

### Catalog / MetadataSnapshot

```text
Catalog: id, type, endpoint, database, credentialRef
MetadataSnapshot: catalogId, schema, tables, columns, version, observedAt
```

### LineageGraph

```text
LineageNode: Dataset | Table | Topic | FlinkJob
LineageEdge: source, target, operation, columns, sourceType, confidence, observedAt
```

`sourceType` 至少包含 `SQL_STATIC`、`RUNTIME_OBSERVED`、`MANUAL` 和 `UNKNOWN`。

### FlinkOperation

```text
operationId, requestId, resourceUid, artifactDigest,
stateProtection, desiredGeneration, lifecycleState,
evidence, auditEvents, createdAt, updatedAt
```

状态流转：

```text
Accepted → Validating → Compiling → ArtifactReady → Submitted
  → WaitingForObservation → Reconciling → Verifying → Completed
```

终态还包括 `Rejected`、`Failed`、`TimedOut`、`Uncertain`、`Superseded` 和 `FallbackDetected`。

## HTTP API

### SQL 和编译

```text
POST /v1/sql/validate
POST /v1/sql/compile
GET  /v1/sql/{documentId}
GET  /v1/sql/{documentId}/versions
```

ZIO 负责编排、校验、重试、日志、Artifact 发布和 digest 校验；Flink Planner/Calcite 或 SQL Gateway 负责 SQL 语义和执行计划，平台不重新实现 Flink 编译器。

### Catalog 和元数据

```text
GET  /v1/catalogs
POST /v1/catalogs
GET  /v1/catalogs/{id}/schemas
GET  /v1/catalogs/{id}/tables/{name}
```

凭据只通过 `credentialRef` 引用 Kubernetes Secret 或 PostgreSQL Secret Store，不写入 SQL、日志、Artifact 或 Git。

### 血缘

```text
GET /v1/lineage/jobs/{jobId}
GET /v1/lineage/datasets/{dataset}
GET /v1/lineage/graph?root=...
```

静态 SQL 血缘和运行时事件必须保留来源和置信度，不能把推断当成运行时事实。

### 发布和操作

```text
POST /v1/releases
POST /v1/releases/{id}/upgrade
POST /v1/releases/{id}/savepoint
POST /v1/releases/{id}/restore
POST /v1/releases/{id}/suspend
POST /v1/releases/{id}/resume
POST /v1/releases/{id}/delete
GET  /v1/operations/{operationId}
GET  /v1/operations/{operationId}/events
GET  /v1/operations/{operationId}/evidence
GET  /v1/operations/{operationId}/events/stream
```

所有写请求返回 `operationId` 和 `Accepted`，由异步 worker 推进生命周期。

## 前端页面

前端只实现平台核心闭环：

1. 总览：作业、操作、失败和快照概览；
2. SQL 工作台：编辑、校验、执行计划、编译和版本；
3. Catalog：数据库、表、字段和 Schema Snapshot；
4. 血缘：作业、表和字段关系及来源；
5. 发布中心：Artifact、策略、Savepoint 升级和恢复；
6. 作业详情：Operator、Job、checkpoint/savepoint 和 RustFS 路径；
7. Operation/Evidence：状态机、事件和完成依据。

前端通过 HTTP API 和 SSE 获取状态，不直接访问 Kubernetes、PostgreSQL 或 RustFS。

## ZIO 设计

- `ZLayer` 注入 Kubernetes、PostgreSQL、RustFS、Compiler、Catalog 和 Lineage 实现；
- `ZStream` 处理 Kubernetes watch、元数据刷新和运行时血缘事件；
- `Schedule` 处理有界重试和 410 relist；
- `Scope` 管理客户端、watch 和数据库资源；
- `Semaphore` 保护进程内副作用；
- Kubernetes `FlinkOperationLock` 保护跨副本资源互斥；
- `Promise` 用于等待 Operation 完成；
- ZIO Test 覆盖 domain、状态机、HTTP wire contract、fake Kubernetes 和存储适配器。

## 存储职责

```text
Kubernetes API: Flink CR、FlinkOperation、FlinkOperationLock
PostgreSQL: SQL、Catalog、Schema Snapshot、Lineage Graph 和查询索引
RustFS: Job Artifact、checkpoint、savepoint、编译日志和构建产物
```

PostgreSQL 不保存 checkpoint/savepoint 二进制；RustFS 不承担分布式锁和 Operation 生命周期。

## 交付顺序

1. 前端壳、静态资源服务、总览、Job/Operation 页面；
2. 前端接入已有发布、快照、状态和 Evidence API；
3. SQL 文档、校验、编译和 Artifact；
4. Catalog 和 Schema Snapshot；
5. SQL 静态血缘；
6. 运行期血缘事件；
7. 真实集群端到端验收、双副本和故障测试；
8. README、部署文档、架构图和数据流图更新。

每一阶段必须有本地测试和对应的真实运行边界记录，不能把编译通过或 API Server 接受 CR 当成 Flink 作业完成。

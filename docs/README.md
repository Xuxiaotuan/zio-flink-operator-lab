# 文档入口

项目核心是：ZIO 通过 Kubernetes API 提交 Flink Operator 资源，并监控任务、checkpoint 和 savepoint 状态。

| 文档 | 内容 |
| --- | --- |
| [运行指南](getting-started.md) | 从构建到提交、观察和快照请求的完整流程 |
| [设计与协议](design.md) | 资源模型、ZIO 边界、Operator 协议和多副本边界 |
| [架构图](diagrams/architecture.html) | 控制面、Kubernetes API、Flink Operator、Flink 和 RustFS 的组件关系 |
| [数据流图](diagrams/dataflow.html) | 提交、reconcile、Job 状态、checkpoint/savepoint 和状态投影 |
| [状态监控](monitoring.md) | CLI/HTTP 入口、状态字段和 checkpoint/savepoint 来源 |
| [部署](deployment.md) | 本地 Kubernetes、RustFS、RBAC 和多副本部署 |
| [测试与证据](testing.md) | 单测、协议测试、集群验证和未验证项 |
| [学习路线](learning-path.md) | 按源码学习 Scala 3、ZIO、ZStream 和 Kubernetes 协议 |

生产代码按 `cli`、`domain`、`kubernetes`、`operator`、`operator/watch`、`operator/savepoint` 和 `server` 分层。每层只负责一种边界，测试目录与生产代码对应。

# 文档入口

项目核心是：ZIO 通过 Kubernetes API 提交 Flink Operator 资源，并监控任务、checkpoint 和 savepoint 状态。

| 文档 | 内容 |
| --- | --- |
| [运行指南](getting-started.md) | 从构建到提交、观察和快照请求的完整流程 |
| [设计与协议](design.md) | 类型化操作、状态保护、Operation 状态机、ZIO 边界、Operator 协议和多副本边界 |
| [架构图](diagrams/architecture.html) | 控制面、Kubernetes API、Flink Operator、Flink 和 RustFS 的组件关系 |
| [Kubernetes 部署图](diagrams/deployment.html) | GitHub、Jenkins、Registry、两副本 Service 和 Flink 运行时 |
| [数据流图](diagrams/dataflow.html) | 提交、reconcile、Job 状态、checkpoint/savepoint 和状态投影 |
| [状态监控](monitoring.md) | CLI/HTTP 入口、状态字段和 checkpoint/savepoint 来源 |
| [部署](deployment.md) | 本地 Kubernetes、RustFS、RBAC 和多副本部署 |
| [测试与证据](testing.md) | 单测、协议测试、集群验证和未验证项 |
| [学习路线](learning-path.md) | 按源码学习 Scala 3、ZIO、ZStream 和 Kubernetes 协议 |

生产代码按 `cli`、`domain`、`application`、`operation`、`kubernetes`、`operator`、`operator/watch`、`operator/savepoint`、`server` 和 `state` 分层。每层只负责一种边界，测试目录与生产代码对应。

README 中保留部署、架构和数据流截图，方便在 GitHub 首页直接理解服务边界。

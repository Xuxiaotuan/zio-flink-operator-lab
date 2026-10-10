# 部署

## 运行形态

HTTP 控制面不保存本地会话状态。多个 ZIO API 副本通过 Kubernetes CR 或 PostgreSQL 状态后端读取统一观测，不使用 RustFS 做锁；浏览器前端是独立的单副本静态服务。

```text
Browser -> zio-flink-operator-ui (1 replica, Nginx)
                                  |
                                  v
                     zio-flink-operator Service (ClusterIP)
                                  |
                                  v
                  zio-flink-operator replicas (2) -> Kubernetes API Server
                                                        |
                                                        v
                                          Flink Kubernetes Operator
```

前端只提供静态页面和同源反向代理，不保存业务状态；ZIO API 副本处理请求，每个写操作由异步 worker 通过 ResourceObserver 观察目标 CR。Kubernetes 部署默认把操作审计写入 `FlinkOperation` CR，副本通过同一个 API Server 共享状态；裸机或外部审计场景才显式选择 PostgreSQL。HTTP 查询接口用于显式观察资源。

## 本地 Kubernetes

本地清单位于 `deploy/local`，包含 namespace、ServiceAccount、RBAC、API Service、两个 API 副本和一个前端副本。

```sh
sbt -batch test
sbt -batch assembly
docker build -t zio-flink-operator-lab:local .
kubectl apply -k deploy/local
kubectl -n flink-lineage-test rollout status deployment/zio-flink-operator --timeout=120s
kubectl -n flink-lineage-test get pods -l app.kubernetes.io/name=zio-flink-operator -o wide
```

本地 OrbStack 使用本机镜像和 `imagePullPolicy: IfNotPresent`。当前本地环境是单节点；API 两个副本和前端一个副本只能证明进程复制和 Service 路由，不证明跨节点高可用。API 两个副本使用 `maxSurge: 0`、`maxUnavailable: 1` 滚动更新，前端保持一个副本。`deploy/local` 设置 `ZIO_FLINK_STATE_BACKEND=kubernetes`。

在两台或更多节点的集群中，Deployment 的副本使用 hostname 反亲和偏好分散调度；Service 不使用会话亲和性，任一副本都能从 API Server 读取相同 CR 状态。

检查服务：

```sh
kubectl -n flink-lineage-test port-forward svc/zio-flink-operator 18080:8080
curl -fsS http://127.0.0.1:18080/healthz
curl -fsS http://127.0.0.1:18080/readyz
```

真实任务提交前还需要在目标 namespace 安装 Flink Operator、CRD 和 watched namespace 配置。只有 CRD 没有 Operator 时，dry-run 可以通过，但不会产生 Flink Job。

## 两节点目标 Kubernetes

目标集群的服务清单位于 `deploy/bigdata-lab`，部署一个两副本 ZIO API Deployment、一个 ClusterIP API Service，以及一个单副本前端 Deployment 和 NodePort Service。API 副本使用 `xjw`、`xxt` 两台节点的 hostname 反亲和偏好，前端只保留一个副本；前端使用 NodePort `30882`，镜像从 Harbor 拉取，RustFS savepoint 前缀为：

```text
s3://flink-savepoints/zio-flink-operator/bigdata-lab/
```

部署前必须先安装 Flink Kubernetes Operator 并让它 watch `bigdata-lab`。部署后检查：

```sh
kubectl apply -k deploy/bigdata-lab
kubectl -n bigdata-lab rollout status deployment/zio-flink-operator --timeout=180s
kubectl -n bigdata-lab rollout status deployment/zio-flink-operator-ui --timeout=180s
kubectl -n bigdata-lab get pods -l app.kubernetes.io/name=zio-flink-operator -o wide
kubectl -n bigdata-lab get pods -l app.kubernetes.io/name=zio-flink-operator-ui -o wide
curl -fsS http://<任一节点>:30882/healthz
open http://<任一节点>:30882/
```

两个 API 副本读取同一个 Kubernetes API Server 和同一组 Flink CR。资源状态和 operation 生命周期默认都来自 Kubernetes CR；每个副本都会轮询，因此状态轮询流量随 API 副本数线性增加。`FlinkOperationLock` CR 按目标资源提供跨副本互斥。设置 `ZIO_FLINK_OPERATION_STORE=postgres` 后，只有 operation 审计切换到 PostgreSQL，Flink 资源状态仍来自 Kubernetes。前端只通过 API Service 访问后端，不参与状态协调。

Flink 冷启动和 TaskManager 调度可能超过短轮询窗口，worker 的提交、删除和验证超时由 `ZIO_FLINK_VERIFICATION_TIMEOUT_SECONDS` 控制，默认 180 秒；现场集群可以按镜像拉取和调度时延调整。

## RustFS

savepoint 使用独立 bucket：

```text
s3://flink-savepoints/zio-flink-operator/
```

目标集群已有 `rustfs` Service（9000）和 `rustfs-credentials` Secret。真实快照验收使用 [examples/flinkdeployment-stateful.json](../examples/flinkdeployment-stateful.json)：它对应 Pipeline 构建的 `zio-flink-stateful-job` 镜像，镜像激活官方 Flink S3 文件系统插件，并通过 `kubernetes.env.secretKeyRef` 将 Secret 注入 Flink Pod；Registry 私有镜像通过 `podTemplate.spec.imagePullSecrets` 使用目标集群已有的 `xxt-harbor-pull`。Flink 1.20 的 S3 插件、endpoint、path-style 和凭据配置遵循 [官方 S3 文件系统文档](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/filesystems/s3/)。

本地默认前缀：

```text
s3://flink-savepoints/zio-flink-operator/local/orbstack/flink-lineage-test/
```

控制面只把 `state.savepoints.dir` 写入 FlinkDeployment；Flink 运行时 Pod 负责实际写入对象。Flink Pod 还需要可达的 S3 endpoint、文件系统插件和 Kubernetes Secret 中的凭据。凭据不能写入 Git、镜像、CR JSON 或日志。

对已存在的 Deployment 触发 savepoint 重部署时，提交的 `FlinkDeployment.spec.job` 必须同时包含 `upgradeMode: "savepoint"`、`initialSavepointPath` 和递增的正整数 `savepointRedeployNonce`。只修改 `initialSavepointPath` 不会触发 Flink Operator 的完整 savepoint 重部署；nonce 每次新的恢复操作都必须变化（同一 requestId 重试保持不变），并且由控制面保留在 Operation 审计 JSON 中。

示例：

```json
{
  "job": {
    "upgradeMode": "savepoint",
    "initialSavepointPath": "s3://flink-savepoints/example/savepoints/savepoint-abc",
    "savepointRedeployNonce": 1
  }
}
```

`deploy/local/rustfs-secret.example.yaml` 只有字段模板和占位符，不能直接作为真实 Secret 使用。checkpoint 与 savepoint 使用不同的存储生命周期，不能把两者混成一个验收结果。

## HTTP 控制面和浏览器工作台

直接运行：

```sh
sbt run
```

容器入口直接启动 ZIO API 服务。服务默认监听 `0.0.0.0:8080`，只提供健康检查和 `/v1/*` API。浏览器工作台由独立的单副本前端容器提供：

```text
http://<任一节点>:30882/
```

前端静态资源由 `frontend/Dockerfile` 构建为 Nginx 镜像。工作台先从只读 `/v1/config` 读取服务默认 namespace，再通过反向代理访问 `/v1/deployments`、`/v1/snapshots`、`/v1/state` 和 `/v1/operations/{id}`，通过 HTTP POST 触发操作，并用有界轮询观察 Operation；初始 `202 ACCEPTED` 不会被显示为完成。

当前版本没有认证和授权。生产或共享集群必须使用私有 Service/Ingress、NetworkPolicy 或其他网络边界，只允许受信任的运维网络访问。相关字段见 [状态监控](monitoring.md)。

## 裸机 PostgreSQL 状态

裸机启动前注入以下环境变量。密码只通过进程环境或 Secret 注入，不写入仓库：

```sh
export ZIO_FLINK_STATE_BACKEND=postgres
export ZIO_FLINK_OPERATION_STORE=postgres
export POSTGRES_HOST=100.82.226.63
export POSTGRES_PORT=30660
export POSTGRES_DB=xxt
export POSTGRES_USER=root
export POSTGRES_PASSWORD='由 Secret 注入'
sbt run
```

服务首次启动会创建 `zio_flink_operator_state` 和 `zio_flink_operations` 表。前者保存 CR 状态观测，后者保存操作状态和审计事件；checkpoint/savepoint 文件仍由 Flink 写入 RustFS。

## 迁移到目标集群

1. 为目标集群准备匹配版本的 Flink Operator、CRD 和 watched namespace。
2. 将镜像推送到目标集群可访问的 registry。
3. 使用目标 namespace 的最小 Role/RoleBinding。
4. 注入 RustFS endpoint 和 Secret，不把凭据写入清单。
5. 先 dry-run，再 apply；分别记录 CR 状态、Flink Pod、Job 状态和快照结果。

目标集群需要单独验证 Operator reconcile、checkpoint、savepoint、恢复、跨节点调度和两个副本读取同一状态。本地部署结果不能替代这些证据。

本轮已在目标集群安装官方 Flink Kubernetes Operator `1.16.1`，命令等价于：

```sh
helm upgrade --install flink-kubernetes-operator \
  flink-operator-repo/flink-kubernetes-operator --version 1.16.1 \
  --namespace flink-operator --create-namespace \
  --set webhook.create=false \
  --set watchNamespaces[0]=bigdata-lab
```

目标集群的 Operator、两副本服务和 Stateful Job 已完成真实验证：两个副本分别调度到 `xjw`、`xxt`，并从两个 NodePort 读取到统一的 operation；checkpoint/savepoint 已写入 RustFS，Savepoint 恢复日志和连续 checkpoint 已核对。另用 PostgreSQL 后端启动了跨节点双副本控制面，两台 NodePort 返回相同 operation 生命周期，测试表和临时资源已在验收后清理。过期 `FlinkOperationLock` 也已在真实 Kubernetes API 中被新操作接管并在完成后释放。`FlinkStateSnapshot` CR 的创建仍不能单独证明对象存储成功，验收必须同时检查 RustFS 结果路径或对象。控制面 HTTP 压力测试 60 秒共 9507 次请求全部成功；10 分钟双节点稳定性测试 468 次请求全部成功、P95 66.4ms、无 Pod 重启。该指标是控制面读请求基线，不代表 Flink 作业吞吐容量。

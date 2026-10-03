# 部署

## 运行形态

HTTP 控制面不保存本地会话状态。多个副本通过 Kubernetes CR 或 PostgreSQL 状态后端读取统一观测，不使用 RustFS 做锁。

```text
Client -> Service -> zio-flink-operator replicas -> Kubernetes API Server
                                                    |
                                                    v
                                      Flink Kubernetes Operator
```

HTTP 副本只处理请求，不在每个副本中启动后台 watch。CLI watch 用于显式观察资源。若以后增加后台 controller，需要单独设计 Kubernetes Lease leader election。

## 本地 Kubernetes

本地清单位于 `deploy/local`，包含 namespace、ServiceAccount、RBAC、Service 和两个 HTTP 副本。

```sh
sbt -batch test
sbt -batch assembly
docker build -t zio-flink-operator-lab:local .
kubectl apply -k deploy/local
kubectl -n flink-lineage-test rollout status deployment/zio-flink-operator --timeout=120s
kubectl -n flink-lineage-test get pods -l app.kubernetes.io/name=zio-flink-operator -o wide
```

本地 OrbStack 使用本机镜像和 `imagePullPolicy: IfNotPresent`。当前本地环境是单节点；两个副本证明进程复制和 Service 路由，不证明跨节点高可用。`deploy/local` 设置 `ZIO_FLINK_STATE_BACKEND=kubernetes`。

在两台或更多节点的集群中，Deployment 的副本使用 hostname 反亲和偏好分散调度；Service 不使用会话亲和性，任一副本都能从 API Server 读取相同 CR 状态。

检查服务：

```sh
kubectl -n flink-lineage-test port-forward svc/zio-flink-operator 18080:8080
curl -fsS http://127.0.0.1:18080/healthz
curl -fsS http://127.0.0.1:18080/readyz
```

真实任务提交前还需要在目标 namespace 安装 Flink Operator、CRD 和 watched namespace 配置。只有 CRD 没有 Operator 时，dry-run 可以通过，但不会产生 Flink Job。

## 两节点目标 Kubernetes

目标集群的服务清单位于 `deploy/bigdata-lab`，只部署一个 `zio-flink-operator` Service 和一个两副本 Deployment。副本使用 `xjw`、`xxt` 两台节点的 hostname 反亲和偏好，滚动更新允许在节点暂时不足时调度；Service 使用 NodePort `30882`，镜像从 Harbor 拉取，RustFS savepoint 前缀为：

```text
s3://flink-savepoints/zio-flink-operator/bigdata-lab/
```

部署前必须先安装 Flink Kubernetes Operator 并让它 watch `bigdata-lab`。部署后检查：

```sh
kubectl apply -k deploy/bigdata-lab
kubectl -n bigdata-lab rollout status deployment/zio-flink-operator --timeout=180s
kubectl -n bigdata-lab get pods -l app.kubernetes.io/name=zio-flink-operator -o wide
curl -fsS http://<任一节点>:30882/healthz
curl -fsS 'http://<任一节点>:30882/v1/state?namespace=bigdata-lab'
```

两个副本读取同一个 Kubernetes API Server 和同一组 Flink CR。每个副本都会轮询，因此状态统一但 Kubernetes list 流量随副本数线性增加；当前版本没有 leader election。

## RustFS

savepoint 使用独立 bucket：

```text
s3://flink-savepoints/zio-flink-operator/
```

本地默认前缀：

```text
s3://flink-savepoints/zio-flink-operator/local/orbstack/flink-lineage-test/
```

控制面只把 `state.savepoints.dir` 写入 FlinkDeployment；Flink 运行时 Pod 负责实际写入对象。Flink Pod 还需要可达的 S3 endpoint、文件系统插件和 Kubernetes Secret 中的凭据。凭据不能写入 Git、镜像、CR JSON 或日志。

`deploy/local/rustfs-secret.example.yaml` 只有字段模板和占位符，不能直接作为真实 Secret 使用。checkpoint 与 savepoint 使用不同的存储生命周期，不能把两者混成一个验收结果。

## HTTP 控制面

直接运行：

```sh
sbt "run serve"
```

容器入口已经是 `serve`。服务默认监听 `0.0.0.0:8080`，相关接口见 [状态监控](monitoring.md)。

## 裸机 PostgreSQL 状态

裸机启动前注入以下环境变量。密码只通过进程环境或 Secret 注入，不写入仓库：

```sh
export ZIO_FLINK_STATE_BACKEND=postgres
export POSTGRES_HOST=100.82.226.63
export POSTGRES_PORT=30660
export POSTGRES_DB=xxt
export POSTGRES_USER=root
export POSTGRES_PASSWORD='由 Secret 注入'
sbt "run serve"
```

服务首次启动会创建 `zio_flink_operator_state` 表。表中保存最新的 CR 状态观测和最多 100 条生命周期事件；checkpoint/savepoint 文件仍由 Flink 写入 RustFS。

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

目标集群的 Operator、两副本服务和 `zio-word-count` 示例 Job 已完成一次运行验证；savepoint 仍需把 RustFS endpoint、S3 插件和凭据以目标 Job 的 Secret 方式接入后再验收。

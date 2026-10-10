# Catalog 与数据血缘

当前版本补齐了目录和血缘的第一条可验证链路：

```text
HTTP 导入 Catalog / Schema Snapshot
             ↓
PostgreSQL 元数据表
             ↓
HTTP 查询 Catalog、表、字段和血缘图
             ↑
Flink SQL → 保守的表级 SQL_STATIC 解析
```

## Catalog

Catalog 只保存连接描述和 `credentialRef`，不会保存密码或 Token。生产环境通过 PostgreSQL 配置启动：

```sh
export ZIO_FLINK_METADATA_JDBC_URL='jdbc:postgresql://<host>:<port>/<database>'
export ZIO_FLINK_METADATA_USER='<secret 注入>'
export ZIO_FLINK_METADATA_PASSWORD='<secret 注入>'
```

注册 Catalog：

```sh
curl -X POST http://127.0.0.1:8080/v1/catalogs \
  -H 'Content-Type: application/json' \
  -d '{"id":"warehouse","type":"postgres","endpoint":"jdbc:postgresql://db/xxt","database":"xxt","credentialRef":"secret/warehouse"}'
```

Schema Snapshot 目前由调用方显式导入，避免服务在没有连接器和凭据时猜测外部数据库结构：

```sh
curl -X POST http://127.0.0.1:8080/v1/catalogs/warehouse/snapshots \
  -H 'Content-Type: application/json' \
  -d @examples/metadata-snapshot.json
```

查询接口：

- `GET /v1/catalogs`
- `GET /v1/catalogs/{id}/schemas`
- `GET /v1/catalogs/{id}/tables?schema=public`
- `GET /v1/catalogs/{id}/tables/{name}?schema=public`

相同 Catalog 配置重复注册保持幂等，冲突配置会被拒绝。一个 Snapshot 表示某个 schema 的完整状态；查询只采用 `observedAt` 最新的快照，空快照会清空该 schema 的当前表清单，迟到快照只补历史。同一观察时间按 `version` 字典序选择，调用方应优先使用递增的观察时间。

## 血缘

当前只接受能明确识别的表级语句：

- `INSERT INTO/OVERWRITE target SELECT ... FROM source JOIN source2 ...`
- `CREATE TABLE target AS SELECT ... FROM source`

提交静态分析：

```sh
curl -X POST http://127.0.0.1:8080/v1/lineage/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql":"INSERT INTO analytics.summary SELECT * FROM raw.orders","jobId":"orders"}'
```

返回的边标记为 `sourceType=SQL_STATIC`、`confidence=0.7`。这代表 SQL 文本推断，不代表 Flink 已经实际读取或写入这些表。复杂 SQL、子查询、CTE、UNION、函数表、逗号连接和无法闭合的注释/字符串会被拒绝，不生成部分血缘。

查询血缘：

```sh
curl 'http://127.0.0.1:8080/v1/lineage/graph?root=analytics.summary'
```

前端的“目录”和“血缘”页面只展示 API 返回的数据；空数据会明确显示为空，不放置演示记录。

## 当前边界

- 目录导入暂时是显式 Snapshot，不是通用 JDBC 自动扫描。
- 血缘暂时是表级 SQL 静态推断，不含字段级血缘。
- `RUNTIME_OBSERVED` / OpenLineage 事件接入尚未实现，不能把 `SQL_STATIC` 当作运行事实。
- 部署必须配置 PostgreSQL；`ZIO_FLINK_METADATA_STORE=memory` 只用于本地单进程学习，不能用于多副本运行。

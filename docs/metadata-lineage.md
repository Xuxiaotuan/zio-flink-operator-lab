# Catalog 与数据血缘

当前版本补齐了目录和血缘的第一条可验证链路：

```text
HTTP 导入 Catalog / Schema Snapshot
             ↓
PostgreSQL 元数据表
             ↓
HTTP 查询 Catalog、表、字段和血缘图
             ↑
Flink 1.20.1 SQL Parser / Calcite AST → 表级 SQL_STATIC 解析
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

SQL 先通过 Apache Flink 1.20.1 的 `FlinkSqlParserImpl` 解析，再遍历 Calcite AST；不再通过正则匹配 `FROM` / `JOIN`。使用 Flink 默认语法兼容配置、反引号标识符和保留大小写的 Java 词法配置。当前可提取的表级语句包括：

- `INSERT INTO/OVERWRITE target SELECT ... FROM source JOIN source2 ...`
- `CREATE TABLE target AS SELECT ... FROM source`
- 显式 JOIN、逗号连接及其别名、FROM 派生表，以及 SELECT / WHERE / JOIN 条件中的子查询。
- 查询内部的非递归 CTE，例如 `INSERT INTO target WITH x AS (SELECT * FROM source) SELECT * FROM x`；按作用域展开到基础表，不把 CTE 名和别名记为数据集，未使用的 CTE 不贡献边。

允许单条语句末尾的分号；引号转义、字符串、注释和完整语句合法性由 Flink 语法解析器处理。CTAS 的连接器属性可以被解析，但不会连接外部系统。

数据集名称当前仍使用以点分隔的字符串。为避免把单个标识符 `` `a.b` `` 与两级名称 `a.b` 混为同一张表，表名、写入目标或 CTE 名中只要有一个标识符分量本身包含点，就会明确拒绝；正常的多级限定名称不受影响。此限制不用于表达式中的字段名。

提交静态分析：

```sh
curl -X POST http://127.0.0.1:8080/v1/lineage/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql":"INSERT INTO analytics.summary SELECT * FROM `raw`.orders","jobId":"orders"}'
```

返回的边标记为 `sourceType=SQL_STATIC`、`confidence=0.7`。这代表 SQL 文本推断，不代表 Flink 已经实际读取或写入这些表。解析失败、多个语句、集合运算（UNION / INTERSECT / EXCEPT）、函数表、LATERAL / UNNEST、时态表、递归或前向 CTE 引用、仅 VALUES 的写入和其他不支持的语句或关系节点会整体拒绝，不生成部分血缘。普通 SELECT 不生成写入边。

实现依赖 `flink-sql-parser:1.20.1`（传递使用未 shaded 的 `calcite-core:1.32.0`），并显式补齐上游声明为 provided / 排除的 Guava `32.1.3-jre` 和 `calcite-linq4j:1.32.0`。版本依据 [Flink 1.20.1 SQL Parser POM](https://github.com/apache/flink/blob/release-1.20.1/flink-table/flink-sql-parser/pom.xml) 和 [Table 模块 POM](https://github.com/apache/flink/blob/release-1.20.1/flink-table/pom.xml)。

查询血缘：

```sh
curl 'http://127.0.0.1:8080/v1/lineage/graph?root=analytics.summary'
```

前端的“目录”和“血缘”页面只展示 API 返回的数据；空数据会明确显示为空，不放置演示记录。

## 当前边界

- 目录导入暂时是显式 Snapshot，不是通用 JDBC 自动扫描。
- 血缘暂时是表级 SQL 静态推断，不含字段级血缘。
- SQL 解析成功只表示语法通过；没有绑定 Catalog、校验表/字段存在性、解析视图定义、验证函数/类型或生成 Flink 执行计划，不承诺 SQL 可以编译或执行。表名保留 SQL 中的大小写和限定部分，不自动补默认 Catalog / Database。
- `RUNTIME_OBSERVED` / OpenLineage 事件接入尚未实现，不能把 `SQL_STATIC` 当作运行事实。
- 部署必须配置 PostgreSQL；`ZIO_FLINK_METADATA_STORE=memory` 只用于本地单进程学习，不能用于多副本运行。

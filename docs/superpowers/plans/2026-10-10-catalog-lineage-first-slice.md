# Catalog and Lineage First Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a real, read-only Catalog and table-level SQL lineage slice to the existing HTTP-only ZIO Flink control plane, with explicit evidence and no fabricated runtime facts.

**Architecture:** Keep one ZIO backend with two replicas and the existing single-replica static frontend. Add a PostgreSQL-backed metadata store for catalog registrations, schema snapshots, and lineage edges; Kubernetes remains the source of Flink operation state and RustFS remains the artifact/state store. The first slice exposes read APIs and a conservative SQL table-lineage parser; frontend pages render only API data and label static inference separately from runtime observation.

**Tech Stack:** Scala 3.3.5, ZIO 2.1.11, JDBC PostgreSQL driver already present, ujson, plain HTML/CSS/JavaScript.

**Spec:** `docs/superpowers/specs/2026-10-09-zio-flink-platform-design.md`

## Global Constraints

- HTTP-only; do not reintroduce CLI paths.
- Kubernetes API remains the only Flink submission boundary.
- PostgreSQL stores catalog, schema snapshot, and lineage query data; it does not store checkpoint/savepoint binaries.
- Do not store catalog passwords or tokens; persist only a credential reference.
- Lineage is table-level first; field-level and runtime OpenLineage ingestion remain deferred.
- Static SQL lineage is marked `SQL_STATIC` with confidence; it must never be presented as runtime fact.
- Existing Flink operation routes and frontend views must keep their current behavior.
- No authentication or authorization is added; document private-network deployment requirements.

## Review Focus

- Invalid catalog identifiers, table names, and SQL input return a clear 4xx response without SQL injection.
- Duplicate catalog registration is deterministic and does not expose credentials.
- A SQL statement with no detectable source/sink tables returns an empty graph with `UNKNOWN` evidence instead of guessed edges.
- A lineage query with no records renders an explicit empty state rather than stale success.
- Existing Kubernetes operation tests and browser tests remain green.

### Task 1: Metadata domain and PostgreSQL schema

**Files:**
- Create: `src/main/scala/cn/xuyinyin/flinklab/metadata/MetadataModel.scala`
- Create: `src/main/scala/cn/xuyinyin/flinklab/metadata/MetadataStore.scala`
- Create: `src/main/scala/cn/xuyinyin/flinklab/metadata/PostgresMetadataStore.scala`
- Test: `src/test/scala/cn/xuyinyin/flinklab/metadata/MetadataModelSpec.scala`
- Test: `src/test/scala/cn/xuyinyin/flinklab/metadata/PostgresMetadataStoreSpec.scala`

**Interfaces:**
- `CatalogId`, `CatalogRegistration`, `TableColumn`, `TableMetadata`, `SchemaSnapshot`, `LineageNode`, `LineageEdge` are validated domain values.
- `MetadataStore.initialize: IO[Throwable, Unit]`.
- `MetadataStore.listCatalogs: IO[ControlPlaneError, List[CatalogRegistration]]`.
- `MetadataStore.createCatalog(catalog): IO[ControlPlaneError, CatalogRegistration]`.
- `MetadataStore.listTables(catalogId, schema): IO[ControlPlaneError, List[TableMetadata]]`.
- `MetadataStore.getTable(catalogId, schema, name): IO[ControlPlaneError, Option[TableMetadata]]`.
- `MetadataStore.saveSnapshot(snapshot): IO[ControlPlaneError, Unit]`.
- `MetadataStore.saveLineage(edge): IO[ControlPlaneError, Unit]`.
- `MetadataStore.graph(root): IO[ControlPlaneError, List[LineageEdge]]`.

- [x] **Step 1: Write failing model and serialization tests**
  - Cover valid/invalid identifiers, credentialRef-only JSON, table columns, and explicit `sourceType`/`confidence`.
- [x] **Step 2: Run focused tests and verify failure**
  - Run `sbt -batch "testOnly cn.xuyinyin.flinklab.metadata.MetadataModelSpec"`.
- [x] **Step 3: Implement models and store interfaces**
  - Keep domain values immutable and reject blank or unsafe identifiers.
- [x] **Step 4: Add PostgreSQL tables and parameterized JDBC operations**
  - Create `zio_flink_catalogs`, `zio_flink_schema_snapshots`, and `zio_flink_lineage_edges`.
  - Keep each complete schema snapshot as versioned JSON; use primary keys and `ON CONFLICT` for idempotent snapshot/edge writes.
- [x] **Step 5: Add in-memory store for unit/HTTP tests and verify**
  - Run model and store tests; PostgreSQL-specific tests must skip with an explicit `evidence_incomplete` message when `ZIO_FLINK_METADATA_TEST_JDBC_URL` is not configured.

### Task 2: Conservative SQL table-lineage extraction

**Files:**
- Create: `src/main/scala/cn/xuyinyin/flinklab/lineage/SqlLineageParser.scala`
- Test: `src/test/scala/cn/xuyinyin/flinklab/lineage/SqlLineageParserSpec.scala`

**Interfaces:**
- `SqlLineageParser.extract(sql, observedAt): Either[String, List[LineageEdge]]`.
- Recognize only table-level `INSERT INTO/OVERWRITE ... SELECT ... FROM ... JOIN ...` and `CREATE TABLE ... AS SELECT ...`.
- Normalize quoted identifiers and emit `SQL_STATIC` edges with confidence `0.7`.
- Never infer field-level edges, external runtime observations, or edges from comments/strings.

- [x] **Step 1: Add failing parser tests**
- [x] **Step 2: Run focused parser test and verify failure**
- [x] **Step 3: Implement bounded tokenizer/regex parser with explicit unsupported cases**
- [x] **Step 4: Verify positive, empty, malformed, and comment/string cases**

### Task 3: HTTP catalog and lineage routes

**Files:**
- Modify: `src/main/scala/cn/xuyinyin/flinklab/server/KubernetesHttpApi.scala`
- Modify: `src/main/scala/cn/xuyinyin/flinklab/Main.scala`
- Modify: `src/main/scala/cn/xuyinyin/flinklab/operation/OperationStore.scala`
- Test: `src/test/scala/cn/xuyinyin/flinklab/server/MetadataHttpApiSpec.scala`

**Interfaces:**
- `GET /v1/catalogs`
- `POST /v1/catalogs` with `{id,type,endpoint,database,credentialRef}`
- `GET /v1/catalogs/{id}/schemas`
- `GET /v1/catalogs/{id}/tables/{name}`
- `GET /v1/lineage/graph?root=...`
- `POST /v1/lineage/sql` with `{sql,jobId?}` to persist static edges and return `sourceType=SQL_STATIC`.
- Responses use existing `ApiResponse` and `errorResponse`; credentials never appear in output.

- [x] **Step 1: Add failing route and wiring tests**
- [x] **Step 2: Run focused HTTP tests and verify failure**
- [x] **Step 3: Inject `MetadataStore` into the HTTP server without changing existing route signatures**
  - Extend the production layer selection alongside OperationStore; retain the in-memory implementation for tests.
- [x] **Step 4: Implement route parsing, validation, and JSON responses**
- [x] **Step 5: Run metadata HTTP tests plus existing HTTP tests**

### Task 4: Catalog and lineage frontend views

**Files:**
- Modify: `src/main/resources/web/index.html`
- Modify: `src/main/resources/web/app.js`
- Modify: `src/main/resources/web/styles.css`
- Modify: `src/test/web/app.test.mjs`
- Modify: `docs/getting-started.md`

**Interfaces:**
- Add navigation views `Catalog` and `Lineage`.
- Browser client methods: `catalogs()`, `tables(catalogId, schema)`, `table(catalogId, schema, name)`, `lineage(root)`, `submitStaticLineage(sql, jobId)`.
- Catalog view lists registered catalogs, tables, columns, and snapshot time.
- Lineage view shows source/target, operation, sourceType, confidence, observedAt; empty and API-error states are explicit.
- Do not add mock records to the page.

- [x] **Step 1: Add failing browser contract assertions**
  - Assert route markers, sourceType/confidence rendering, and empty-state copy.
- [x] **Step 2: Run `node --test src/test/web/app.test.mjs` and verify failure**
- [x] **Step 3: Implement views and API client methods**
- [x] **Step 4: Run browser tests and `node --check src/main/resources/web/app.js`**
- [x] **Step 5: Verify existing overview/jobs/publish/operations views remain intact**

### Task 5: Documentation and verification boundary

**Files:**
- Modify: `README.md`
- Modify: `docs/README.md`
- Modify: `docs/deployment.md`
- Create: `docs/metadata-lineage.md`

- [x] **Step 1: Document API contracts and evidence semantics**
  - Explain PostgreSQL requirement, credentialRef, table-level static lineage, and deferred runtime lineage.
- [x] **Step 2: Add local verification commands**
  - `sbt -batch test assembly`
  - `node --test src/test/web/app.test.mjs`
  - `node --check src/main/resources/web/app.js`
- [x] **Step 3: Run the full local verification and inspect the final diff**
- [x] **Step 4: If PostgreSQL test environment is available, run schema/read/write verification; otherwise mark it `evidence_incomplete`**
- [x] **Step 5: Only after review, commit scoped files and deploy to a non-production namespace**

Implementation note: the code and local verification steps in this plan are complete. Commit, remote push, and target-cluster deployment remain explicit follow-up actions; the current session's Kubernetes API connection timed out, so no remote deployment is claimed.

## Deferred

- SQL compiler/planner and artifact publication.
- Field-level lineage.
- Runtime OpenLineage ingestion.
- Catalog connector discovery from external systems.
- Authentication, authorization, and multi-tenant isolation.

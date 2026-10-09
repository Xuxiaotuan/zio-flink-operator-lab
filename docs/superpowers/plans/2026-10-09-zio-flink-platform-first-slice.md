# ZIO Flink Platform First Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Deliver the first runnable browser-to-control-plane vertical slice: serve a static frontend from the existing ZIO HTTP service, submit/query Flink operations through the existing HTTP contract, and observe deployment/snapshot state without CLI access.

**Architecture:** Keep the current single ZIO service and Kubernetes API boundary. Add classpath-served web assets under `src/main/resources/web`, a small pure `WebUi` asset router, and a browser client that uses the existing `/v1/deployments`, `/v1/snapshots`, `/v1/state`, and `/v1/operations/{id}` endpoints. The UI uses bounded polling for operation refresh in this slice; SSE is a later plan after a streaming response contract exists.

**Tech Stack:** Scala 3.3.5, ZIO 2.1.11, Java `HttpServer`, plain browser HTML/CSS/JavaScript modules, ujson-backed existing HTTP API.

**Spec:** `docs/superpowers/specs/2026-10-09-zio-flink-platform-design.md`

## Global Constraints

- HTTP-only: no CLI command path or browser-to-Kubernetes connection.
- Kubernetes API remains the only Flink submission boundary.
- Existing uncommitted user changes must be preserved; do not reset, clean, or stage unrelated paths.
- No authentication or authorization is implemented; document the private-network deployment boundary.
- Do not add SQL, compiler, Catalog, lineage, or new persistence semantics in this first slice.
- A successful HTTP response to Kubernetes is not presented as Flink completion; the UI must show `Accepted` and observed operation state separately.
- Keep the implementation dependency-free on the frontend; no npm install or CDN runtime dependency.

## Review Focus

- Browser refresh or missing API data must show an explicit error/empty state rather than stale success.
- Deployment names and namespace values must be URL-encoded before requests.
- Repeated submit actions must preserve caller-supplied `requestId` and display the returned `operationId`.
- Static asset paths must not expose arbitrary filesystem reads.
- The UI must not display `Accepted` as `Completed`.

### Task 1: Static web asset contract

**Files:**
- Create: `src/main/resources/web/index.html`
- Create: `src/main/resources/web/app.js`
- Create: `src/main/resources/web/styles.css`
- Test: `src/test/scala/cn/xuyinyin/flinklab/server/WebUiSpec.scala`

**Interfaces:**
- Produces browser entrypoint `/`, script `/app.js`, and stylesheet `/styles.css`.
- `app.js` consumes only the existing JSON API routes and renders explicit `Accepted`, terminal, loading, empty, and error states.

- [x] **Step 1: Write the failing asset contract tests**

Add tests that load `WebUi.handle(ApiRequest("GET", "/"))`, `/app.js`, and `/styles.css`, asserting status 200, content types, and required marker strings (`ZIO Flink Platform`, `/v1/deployments`, `operationId`). Add a missing-asset test expecting 404.

- [x] **Step 2: Run the focused test to verify it fails**

Run: `sbt -batch "testOnly cn.xuyinyin.flinklab.server.WebUiSpec"`
Expected: FAIL because `WebUi` and the web assets do not exist.

- [x] **Step 3: Implement the static browser shell**

Create a dependency-free page with namespace/deployment inputs, deployment list, deployment status, operation details, and action buttons. Keep all API calls in `app.js`; use `encodeURIComponent` for path/query values. Use a small `renderState` function so loading, empty, error, `ACCEPTED`, and terminal states are distinct.

- [x] **Step 4: Run the focused test to verify it passes**

Run: `sbt -batch "testOnly cn.xuyinyin.flinklab.server.WebUiSpec"`
Expected: PASS.

### Task 2: Classpath asset router and HTTP integration

**Files:**
- Create: `src/main/scala/cn/xuyinyin/flinklab/server/WebUi.scala`
- Modify: `src/main/scala/cn/xuyinyin/flinklab/server/ServerProgram.scala`
- Test: `src/test/scala/cn/xuyinyin/flinklab/server/WebUiSpec.scala`

**Interfaces:**
- `WebUi.handle(request: ApiRequest): UIO[Option[ApiResponse]]` returns `Some` only for `/`, `/app.js`, and `/styles.css`.
- Existing API requests continue through `KubernetesHttpApi` unchanged.

- [x] **Step 1: Extend the failing tests for path isolation**

Add assertions that `/etc/passwd`, `/web/../app.js`, and unknown paths return `None`/404 and never read arbitrary resources.

- [x] **Step 2: Run the focused test to verify the new assertions fail**

Run: `sbt -batch "testOnly cn.xuyinyin.flinklab.server.WebUiSpec"`
Expected: FAIL because no isolated asset router exists.

- [x] **Step 3: Implement `WebUi` and wire it before the API route**

Read only fixed classpath resources from `/web/index.html`, `/web/app.js`, and `/web/styles.css`. In `ServerProgram`, try `WebUi.handle` first and call `KubernetesHttpApi.handleWith` only when it returns `None`. Preserve the existing response headers and executor lifecycle.

- [x] **Step 4: Run the focused and existing HTTP tests**

Run: `sbt -batch "testOnly cn.xuyinyin.flinklab.server.WebUiSpec cn.xuyinyin.flinklab.server.KubernetesHttpApiSpec"`
Expected: PASS; existing API routes remain green.

### Task 3: Browser API behavior and static checks

**Files:**
- Modify: `src/main/resources/web/app.js`
- Modify: `src/main/resources/web/index.html`
- Test: `src/test/scala/cn/xuyinyin/flinklab/server/WebUiSpec.scala`

**Interfaces:**
- Browser functions use `GET /v1/deployments`, `GET /v1/deployments/{name}/status`, `POST /v1/deployments/{name}/savepoint`, `POST /v1/deployments/{name}/resume`, `POST /v1/deployments/{name}/suspend-savepoint`, `GET /v1/operations/{id}`, and `GET /v1/state`.
- Submit actions show the returned `operationId` and then poll the operation endpoint at a bounded interval.

- [x] **Step 1: Add contract markers to the failing static test**

Assert the JavaScript includes `encodeURIComponent`, `requestId`, `/v1/operations/`, and separate handling for `ACCEPTED`, `COMPLETED`, `FAILED`, `TIMEDOUT`, and `UNCERTAIN`.

- [x] **Step 2: Run the focused test to verify it fails**

Run: `sbt -batch "testOnly cn.xuyinyin.flinklab.server.WebUiSpec"`
Expected: FAIL until the browser behavior is implemented.

- [x] **Step 3: Implement bounded polling and action wiring**

Use a generated request ID only when the input is empty; otherwise preserve the entered value. Disable a button while its request is in flight, show HTTP errors, and stop polling on terminal states. Do not infer completion from the initial 202 response.

- [x] **Step 4: Run Scala tests and a JavaScript syntax check**

Run: `sbt -batch "testOnly cn.xuyinyin.flinklab.server.WebUiSpec cn.xuyinyin.flinklab.server.KubernetesHttpApiSpec"`
Run: `node --check src/main/resources/web/app.js`
Expected: both commands pass. If `node` is unavailable, record `evidence_incomplete` and retain the Scala asset contract test.

### Task 4: Deployment and documentation integration

**Files:**
- Modify: `README.md`
- Modify: `docs/README.md`
- Modify: `docs/getting-started.md`
- Modify: `docs/deployment.md`
- Modify: `Dockerfile`
- Test: `src/test/scala/cn/xuyinyin/flinklab/server/WebUiSpec.scala`

**Interfaces:**
- Container startup serves `/` from the same HTTP port as the API.
- Documentation uses the exact first-slice routes and states the no-auth private-network boundary.

- [ ] **Step 1: Add documentation assertions**

Extend the asset contract test to assert that the HTML title and docs mention the browser URL and HTTP-only startup; keep docs assertions narrow and stable.

- [ ] **Step 2: Run the focused test to verify it fails**

Run: `sbt -batch "testOnly cn.xuyinyin.flinklab.server.WebUiSpec"`
Expected: FAIL until docs and HTML markers are aligned.

- [x] **Step 3: Update docs and container packaging**

Document `http://127.0.0.1:8080/`, the browser workflow, polling semantics, and the requirement to keep the unauthenticated service on a private network. Ensure assembly/Docker packaging includes `src/main/resources/web` without adding a second service.

- [x] **Step 4: Run the full local verification**

Run: `sbt -batch test assembly`
Run: `node --check src/main/resources/web/app.js`
Expected: all Scala tests and assembly pass; JavaScript syntax check passes when Node is available.

Implementation note: documentation was reviewed manually against the browser URL, HTTP-only startup, polling semantics, and no-auth boundary. A separate runtime/container change was unnecessary because sbt assembly already packages `src/main/resources/web` into the existing single-service JAR.

## Deferred follow-up plans

- SQL document/version and compiler adapter;
- immutable JobArtifact and RustFS publication;
- Catalog and Schema Snapshot persistence;
- SQL static lineage and runtime OpenLineage ingestion;
- SSE streaming contract;
- frontend lineage graph and richer editor;
- real cluster browser acceptance and cleanup.

const TERMINAL = new Set(["COMPLETED", "FAILED", "TIMEDOUT", "SUPERSEDED"]);
const ERROR_STATES = new Set(["FAILED"]);
const UNCERTAIN_STATE = "UNCERTAIN";

export function classifyState(state) {
  const normalized = String(state || "UNKNOWN").toUpperCase();
  if (ERROR_STATES.has(normalized)) return "error";
  if (normalized === "COMPLETED") return "success";
  return "waiting";
}

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

export async function observeOperation(fetchOperation, onUpdate, options = {}) {
  const attempts = options.attempts ?? 90;
  const pause = options.pause ?? (() => sleep(options.interval ?? 2000));
  const signal = options.signal;
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    if (signal?.aborted) return "cancelled";
    const operation = await fetchOperation();
    if (signal?.aborted) return "cancelled";
    onUpdate(operation);
    const state = String(operation?.state || "UNKNOWN").toUpperCase();
    if (TERMINAL.has(state)) return "terminal";
    if (attempt + 1 < attempts) await pause();
  }
  return "limit";
}

export function createClient(fetchImpl = window.fetch.bind(window)) {
  async function request(path, options = {}) {
    const response = await fetchImpl(path, { headers: { "Accept": "application/json", ...(options.headers || {}) }, ...options });
    const text = await response.text();
    let body = {};
    try { body = text ? JSON.parse(text) : {}; } catch { body = { raw: text }; }
    if (!response.ok) throw new Error(`${response.status}: ${body.error || body.raw || "请求失败"}`);
    return body;
  }
  const query = (values) => new URLSearchParams(Object.entries(values).filter(([, value]) => value !== undefined && value !== null && value !== "")).toString();
  return {
    request,
    deployments: namespace => request(`/v1/deployments?${query({ namespace })}`),
    snapshots: namespace => request(`/v1/snapshots?${query({ namespace })}`),
    config: () => request("/v1/config"),
    state: namespace => request(`/v1/state?${query({ namespace })}`),
    status: (namespace, name) => request(`/v1/deployments/${encodeURIComponent(name)}/status?${query({ namespace })}`),
    operation: id => request(`/v1/operations/${encodeURIComponent(id)}`),
    action: (namespace, name, action, requestId) => {
      const policy = action === "restart" ? { upgradeMode: "savepoint", fallback: "forbidden" } : {};
      return request(`/v1/deployments/${encodeURIComponent(name)}/${action}?${query({ namespace, requestId, ...policy })}`, { method: "POST" });
    },
    publish: (namespace, manifest, mode, requestId, dryRun = false) => {
      if (dryRun && mode !== "deploy") throw new Error("dry-run 只支持创建 Deployment；升级请直接提交 Operation");
      const copy = JSON.parse(JSON.stringify(manifest));
      copy.metadata = { ...(copy.metadata || {}), namespace };
      const name = copy.metadata.name;
      if (!name) throw new Error("metadata.name 不能为空");
      const queryValues = dryRun ? { namespace, dryRun: "true" } : { namespace, requestId };
      const route = mode === "upgrade" ? `/v1/deployments/${encodeURIComponent(name)}/upgrade` : "/v1/deployments";
      return request(`${route}?${query(queryValues)}`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(copy) });
    }
  };
}

function byId(id) { return document.getElementById(id); }
function setMessage(element, text, state = "") { if (!element) return; element.textContent = text || ""; element.dataset.state = state; }
function escapeHtml(value) { return String(value ?? "").replace(/[&<>'"]/g, char => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" }[char])); }
function stateBadge(state) { const value = String(state || "UNKNOWN").toUpperCase(); return `<span class="badge" data-state="${classifyState(value)}">${escapeHtml(value)}</span>`; }
function namespace() { return byId("namespace").value.trim() || "default"; }
function requestId(input) { return input.value.trim(); }
function makeRequestId() { return `web-${crypto.randomUUID()}`; }
function manifestTemplate() { return JSON.stringify({ apiVersion: "flink.apache.org/v1beta1", kind: "FlinkDeployment", metadata: { name: "orders" }, spec: { image: "flink:1.20.1", flinkVersion: "v1_20", jobManager: { resource: { cpu: 1, memory: "1024m" } }, taskManager: { resource: { cpu: 1, memory: "1024m" } }, job: { jarURI: "local:///opt/flink/examples/streaming/WordCount.jar", entryClass: "org.apache.flink.streaming.examples.wordcount.WordCount", parallelism: 1, upgradeMode: "savepoint", state: "running" } } }, null, 2); }

function init() {
  const client = createClient();
  let selectedJob = null;
  let pollController = null;
  let latestDeployments = [];
  const app = { client, selectedJob, pollController, latestDeployments };
  byId("manifest").value = manifestTemplate();

  function showView(id) {
    document.querySelectorAll(".view").forEach(view => { view.hidden = view.id !== id; });
    document.querySelectorAll("nav a[data-view]").forEach(link => link.setAttribute("aria-current", link.dataset.view === id ? "page" : "false"));
    byId("current-view").textContent = ({ overview: "总览", jobs: "作业", publish: "发布", operations: "操作记录" })[id] || id;
  }
  function renderJobList(items, target = byId("job-list")) {
    if (!items.length) { target.innerHTML = '<div class="empty">当前 namespace 没有 FlinkDeployment。</div>'; return; }
    target.innerHTML = items.map(item => `<button class="job-choice" type="button" data-job="${escapeHtml(item.name)}" aria-pressed="${selectedJob === item.name}"><strong>${escapeHtml(item.name)}</strong><small>${stateBadge(item.jobState || item.lifecycleState || "UNKNOWN")} ${escapeHtml(item.message || "")}</small></button>`).join("");
    target.querySelectorAll("[data-job]").forEach(button => button.addEventListener("click", () => selectJob(button.dataset.job)));
  }
  function renderMetrics(items, snapshots) {
    byId("metric-total").textContent = items.length;
    byId("metric-running").textContent = items.filter(item => String(item.jobState || "").toUpperCase() === "RUNNING").length;
    byId("metric-failed").textContent = items.filter(item => ["FAILED", "ERROR"].includes(String(item.jobState || item.lifecycleState || "").toUpperCase())).length;
    byId("metric-snapshots").textContent = snapshots.length;
  }
  function renderDetails(status) {
    const values = [["jobState", status.jobState], ["lifecycleState", status.lifecycleState], ["jobId", status.jobId], ["resourceVersion", status.resourceVersion], ["observedGeneration", status.observedGeneration], ["reconciliationState", status.reconciliationState], ["lastSavepoint", status.savepoint?.lastSavepoint?.location || status.savepoint?.location], ["checkpoint", status.checkpoint?.lastCheckpoint?.triggerNonce || status.checkpoint?.lastCheckpoint?.id]];
    byId("job-details").innerHTML = `<dl class="detail-grid">${values.map(([key, value]) => `<div><dt>${escapeHtml(key)}</dt><dd>${value ? escapeHtml(value) : "—"}</dd></div>`).join("")}</dl><div class="note">${escapeHtml(status.error || status.message || "Operator 未报告错误")}</div>`;
  }
  async function refresh() {
    const ns = namespace();
    setMessage(byId("overview-message"), "正在读取 Kubernetes 状态…", "loading");
    try {
      const [deployments, snapshots] = await Promise.all([client.deployments(ns), client.snapshots(ns)]);
      latestDeployments = deployments.items || [];
      app.latestDeployments = latestDeployments;
      renderMetrics(latestDeployments, snapshots.items || []);
      renderJobList(latestDeployments, byId("overview-jobs"));
      renderJobList(latestDeployments);
      byId("jobs-count").textContent = `${latestDeployments.length} 个作业`;
      byId("snapshot-list").innerHTML = (snapshots.items || []).length ? `<div class="table-wrap"><table><thead><tr><th>名称</th><th>类型</th><th>状态</th><th>路径</th></tr></thead><tbody>${snapshots.items.map(item => `<tr><td><code>${escapeHtml(item.name)}</code></td><td>${escapeHtml(item.type || "—")}</td><td>${stateBadge(item.state)}</td><td><code>${escapeHtml(item.path || "—")}</code></td></tr>`).join("")}</tbody></table></div>` : '<div class="empty">当前 namespace 没有快照。</div>';
      byId("updated").textContent = `更新于 ${new Date().toLocaleTimeString()}`;
      setMessage(byId("overview-message"), "状态已更新", "success");
      setMessage(byId("jobs-message"), "状态已更新", "success");
    } catch (error) {
      setMessage(byId("overview-message"), error.message, "error");
      setMessage(byId("jobs-message"), error.message, "error");
    }
  }
  async function selectJob(name) {
    selectedJob = name; app.selectedJob = name;
    byId("job-title").textContent = name;
    byId("job-actions").hidden = false;
    renderJobList(latestDeployments);
    try { renderDetails(await client.status(namespace(), name)); }
    catch (error) { setMessage(byId("jobs-message"), error.message, "error"); }
  }
  async function observe(id, destination = byId("operation-result")) {
    if (pollController) pollController.abort();
    pollController = new AbortController(); app.pollController = pollController;
    byId("stop-poll").disabled = false;
    setMessage(byId("operation-message"), `正在观察 ${id}…`, "waiting");
    destination.innerHTML = "";
    try {
      const result = await observeOperation(() => client.operation(id), operation => {
        const events = (operation.events || []).map(event => `<div class="event"><strong>${escapeHtml(event.type || "event")}</strong><time>${escapeHtml(event.at || "")}</time><div>${escapeHtml(event.reason || "")}</div></div>`).join("");
        destination.innerHTML = `<article class="panel"><div class="panel-heading"><h2>${escapeHtml(operation.operationId || id)}</h2>${stateBadge(operation.state)}</div><dl class="detail-grid"><div><dt>requestId</dt><dd>${escapeHtml(operation.requestId)}</dd></div><div><dt>resource</dt><dd>${escapeHtml(operation.resource?.name || "—")}</dd></div></dl><div>${events || '<div class="empty">暂无审计事件。</div>'}</div></article>`;
        setMessage(byId("operation-message"), `当前状态：${operation.state}`, classifyState(operation.state));
      }, { signal: pollController.signal });
      if (result === "limit") setMessage(byId("operation-message"), `观察窗口结束，当前结果仍需继续查询：${id}`, "waiting");
      if (result === "cancelled") setMessage(byId("operation-message"), "已停止观察。", "waiting");
      if (result === "terminal") refresh();
    } catch (error) {
      // API Server 短暂不可达时保留当前证据，并明确提示用户，不留下未处理的 Promise 异常。
      setMessage(byId("operation-message"), `操作状态读取失败：${error.message}`, "error");
    } finally {
      byId("stop-poll").disabled = true;
    }
  }
  function startOperation(body) { if (body.operationId) { byId("operation-id").value = body.operationId; showView("operations"); observe(body.operationId); } }
  async function runAction(action) {
    if (!selectedJob) return;
    const button = document.querySelector(`[data-action="${action}"]`);
    button.disabled = true; button.dataset.state = "loading";
    try { startOperation(await client.action(namespace(), selectedJob, action, requestId(byId("action-request-id")) || makeRequestId())); }
    catch (error) { setMessage(byId("jobs-message"), error.message, "error"); }
    finally { button.disabled = false; delete button.dataset.state; }
  }
  document.querySelectorAll("nav a[data-view]").forEach(link => link.addEventListener("click", () => showView(link.dataset.view)));
  byId("namespace-form").addEventListener("submit", event => { event.preventDefault(); refresh(); });
  document.querySelectorAll("[data-action]").forEach(button => button.addEventListener("click", () => runAction(button.dataset.action)));
  byId("publish-form").addEventListener("submit", async event => { event.preventDefault(); const submit = byId("submit-publish"); submit.disabled = true; submit.dataset.state = "loading"; try { const body = await client.publish(namespace(), JSON.parse(byId("manifest").value), byId("publish-mode").value, requestId(byId("publish-request-id")) || makeRequestId()); startOperation(body); } catch (error) { setMessage(byId("publish-message"), error.message, "error"); } finally { submit.disabled = false; delete submit.dataset.state; } });
  byId("dry-run").addEventListener("click", async () => { try { const body = await client.publish(namespace(), JSON.parse(byId("manifest").value), byId("publish-mode").value, "", true); byId("dry-run-result").hidden = false; byId("dry-run-json").textContent = JSON.stringify(body, null, 2); setMessage(byId("publish-message"), "Kubernetes dry-run 完成，未创建 operation。", "success"); } catch (error) { setMessage(byId("publish-message"), error.message, "error"); } });
  byId("operation-form").addEventListener("submit", event => { event.preventDefault(); observe(byId("operation-id").value.trim()); });
  byId("stop-poll").addEventListener("click", () => pollController?.abort());
  showView(location.hash.slice(1) || "overview");
  async function bootstrap() {
    try {
      const config = await client.config();
      if (config.namespace) byId("namespace").value = config.namespace;
    } catch (error) {
      setMessage(byId("overview-message"), `未读取服务默认 namespace，使用当前输入值：${error.message}`, "error");
    }
    await refresh();
  }
  bootstrap();
}

if (typeof document !== "undefined") document.addEventListener("DOMContentLoaded", init);

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
    deployment: (namespace, name) => request(`/v1/deployments/${encodeURIComponent(name)}?${query({ namespace })}`),
    status: (namespace, name) => request(`/v1/deployments/${encodeURIComponent(name)}/status?${query({ namespace })}`),
    operation: id => request(`/v1/operations/${encodeURIComponent(id)}`),
    action: (namespace, name, action, requestId) => {
      const policy = action === "restart" ? { upgradeMode: "savepoint", fallback: "forbidden" } : {};
      return request(`/v1/deployments/${encodeURIComponent(name)}/${action}?${query({ namespace, requestId, ...policy })}`, { method: "POST" });
    },
    publish: (namespace, manifest, mode, requestId, dryRun = false) => {
      if (dryRun && mode !== "deploy") throw new Error("dry-run 只支持创建 Deployment；升级请直接提交 Operation");
      validateManifest(manifest);
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
export function resourceStateClass(state) {
  const value = String(state || "UNKNOWN").toUpperCase();
  if (["FAILED", "ERROR", "ABANDONED"].includes(value)) return "error";
  if (["RUNNING", "FINISHED", "STABLE", "READY", "COMPLETED", "SUCCEEDED"].includes(value)) return "success";
  return "waiting";
}

export function snapshotsForJob(items, name) {
  return items.filter(item => item.jobReferenceName === name && item.jobReferenceKind === "FlinkDeployment");
}

export function filterJobs(items, query, state) {
  return items.filter(item => item.name.toLowerCase().includes(query.toLowerCase()) &&
    (!state || (item.jobState || item.lifecycleState || "UNKNOWN").toUpperCase() === state));
}

function resourceBadge(state) {
  const value = String(state || "UNKNOWN").toUpperCase();
  return `<span class="badge" data-state="${resourceStateClass(value)}">${escapeHtml(value)}</span>`;
}

function stateBadge(state) { const value = String(state || "UNKNOWN").toUpperCase(); return `<span class="badge" data-state="${classifyState(value)}">${escapeHtml(value)}</span>`; }
function namespace() { return byId("namespace").value.trim() || "default"; }
function syncNamespace() { const value = namespace(); const target = byId("top-namespace"); if (target) target.textContent = value; }
// randomUUID 仅在安全上下文提供；NodePort HTTP 用同样的密码学随机源生成 UUID v4。
export function makeRequestId(cryptoSource = globalThis.crypto) {
  if (typeof cryptoSource?.randomUUID === "function") return `web-${cryptoSource.randomUUID()}`;
  if (typeof cryptoSource?.getRandomValues !== "function") throw new Error("浏览器不支持安全随机数，请手动填写 requestId");
  const bytes = cryptoSource.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = Array.from(bytes, byte => byte.toString(16).padStart(2, "0")).join("");
  return `web-${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

// 先写回输入框，再发送请求。网络失败后的重试复用同一个幂等键。
export function ensureRequestId(input, generate = makeRequestId) {
  input.value = input.value.trim() || generate();
  return input.value;
}

export function manifestTemplate() {
  return JSON.stringify({
    apiVersion: "flink.apache.org/v1beta1", kind: "FlinkDeployment",
    metadata: { name: "wordcount-demo" },
    spec: {
      image: "flink:1.20.1", flinkVersion: "v1_20", serviceAccount: "flink",
      jobManager: { resource: { cpu: 1, memory: "1024m" } },
      taskManager: { resource: { cpu: 1, memory: "1024m" } },
      job: {
        jarURI: "local:///opt/flink/examples/streaming/WordCount.jar",
        entryClass: "org.apache.flink.streaming.examples.wordcount.WordCount",
        parallelism: 1, upgradeMode: "stateless", state: "running"
      }
    }
  }, null, 2);
}

function validateManifest(manifest) {
  const mode = manifest?.spec?.job?.upgradeMode;
  const config = manifest?.spec?.flinkConfiguration || {};
  const defined = key => typeof config[key] === "string" && config[key].trim();
  // 提前提示常见配置遗漏；不替用户修改 upgradeMode，也不代替 Operator 验证。
  if (["savepoint", "last-state"].includes(mode) && !defined("execution.checkpointing.dir") && !defined("state.checkpoints.dir")) {
    throw new Error(`${mode} 需要 checkpoint 目录：请配置 spec.flinkConfiguration["state.checkpoints.dir"]（或 execution.checkpointing.dir）`);
  }
  if (mode === "savepoint" && !defined("execution.checkpointing.savepoint-dir") && !defined("state.savepoints.dir")) {
    throw new Error('savepoint 需要快照目录：请配置 spec.flinkConfiguration["state.savepoints.dir"]（或 execution.checkpointing.savepoint-dir），并确保存储插件和凭据可用');
  }
}

export function errorMessage(reason) {
  try { const value = JSON.parse(reason); return typeof value?.message === "string" ? value.message : String(reason || ""); }
  catch { return String(reason || ""); }
}

function renderReason(reason) {
  if (!reason) return "";
  const message = errorMessage(reason);
  return `<p class="error-reason">${escapeHtml(message)}</p>${message !== reason ? `<details><summary>原始错误</summary><pre>${escapeHtml(reason)}</pre></details>` : ""}`;
}

function init() {
  const client = createClient();
  let selectedJob = null;
  let pollController = null;
  let latestDeployments = [];
  let latestSnapshots = [];
  let refreshVersion = 0;
  let detailVersion = 0;
  const recentOperations = new Map();
  const app = { client, selectedJob, pollController, latestDeployments };
  byId("manifest").value = manifestTemplate();
  syncNamespace();

  function showView(id) {
    if (!["overview", "jobs", "publish", "operations"].includes(id)) id = "overview";
    document.querySelectorAll(".view").forEach(view => { view.hidden = view.id !== id; });
    document.querySelectorAll("nav a[data-view]").forEach(link => link.setAttribute("aria-current", link.dataset.view === id ? "page" : "false"));
    byId("current-view").textContent = ({ overview: "总览", jobs: "作业", publish: "发布", operations: "操作记录" })[id] || id;
  }
  function renderJobList(items, target = byId("job-list")) {
    target.classList.remove("empty");
    if (target.id === "overview-jobs") items = filterJobs(items, byId("job-search").value.trim(), byId("job-state-filter").value);
    if (!items.length) { target.innerHTML = '<div class="empty">当前命名空间没有作业。</div>'; return; }
    target.classList.remove("empty");
    target.innerHTML = target.id === "overview-jobs" ? `<div class="table-wrap"><table><thead><tr><th>作业名称</th><th>Job 状态</th><th>生命周期</th><th>调谐状态</th><th>TaskManager</th></tr></thead><tbody>${items.map(item => `<tr><td><button type="button" class="link-button" data-job="${escapeHtml(item.name)}">${escapeHtml(item.name)}</button></td><td>${resourceBadge(item.jobState || item.lifecycleState)}</td><td>${escapeHtml(item.lifecycleState || "—")}</td><td>${escapeHtml(item.reconciliationState || "—")}</td><td>${escapeHtml(item.taskManagerReplicas || "—")}</td></tr>`).join("")}</tbody></table></div>` : items.map(item => `<button class="job-choice" type="button" data-job="${escapeHtml(item.name)}" aria-pressed="${selectedJob === item.name}"><strong>${escapeHtml(item.name)}</strong><small>${resourceBadge(item.jobState || item.lifecycleState || "UNKNOWN")} </small></button>`).join("");
    target.querySelectorAll("[data-job]").forEach(button => button.addEventListener("click", () => { location.hash = "jobs"; showView("jobs"); selectJob(button.dataset.job); }));
  }
  function renderMetrics(items, snapshots) {
    byId("metric-total").textContent = items.length;
    byId("metric-running").textContent = items.filter(item => String(item.jobState || "").toUpperCase() === "RUNNING").length;
    byId("metric-failed").textContent = items.filter(item => ["FAILED", "ERROR"].includes(String(item.jobState || item.lifecycleState || "").toUpperCase())).length;
    byId("metric-snapshots").textContent = snapshots.length;
  }
  function renderDetails(status) {
    const values = [["jobState", status.jobState], ["lifecycleState", status.lifecycleState], ["jobId", status.jobId], ["resourceVersion", status.resourceVersion], ["observedGeneration", status.observedGeneration], ["reconciliationState", status.reconciliationState], ["lastSavepoint", status.savepoint?.lastSavepoint?.location || status.savepoint?.location], ["checkpoint", status.checkpoint?.lastCheckpoint?.triggerNonce || status.checkpoint?.lastCheckpoint?.id]];
    byId("job-details").classList.remove("empty");
    byId("job-details").innerHTML = `<dl class="detail-grid">${values.map(([key, value]) => `<div><dt>${escapeHtml(key)}</dt><dd>${value ? escapeHtml(value) : "—"}</dd></div>`).join("")}</dl>${renderReason(status.error)}`;
  }
  function renderSnapshots() {
    const items = snapshotsForJob(latestSnapshots, selectedJob);
    byId("snapshot-list").classList.remove("empty");
    byId("snapshot-list").innerHTML = items.length ? `<div class="table-wrap"><table><thead><tr><th>快照请求</th><th>状态</th><th>路径</th></tr></thead><tbody>${items.map(item => `<tr><td>${escapeHtml(item.name)}</td><td>${resourceBadge(item.state)}</td><td><code>${escapeHtml(item.path || "—")}</code></td></tr>`).join("")}</tbody></table></div>` : '<div class="empty">当前作业没有快照请求记录。</div>';
  }
  function renderRecentOperations() {
    const target = byId("recent-operations");
    target.classList.remove("empty");
    target.innerHTML = [...recentOperations.values()].reverse().map(operation => `<button class="job-choice" type="button" data-operation="${escapeHtml(operation.operationId)}"><span><strong>${escapeHtml(operation.resource?.name || "—")} · ${escapeHtml(operation.operationType || "操作")}</strong><span class="operation-key">${escapeHtml(operation.operationId)}</span></span>${stateBadge(operation.state)}</button>`).join("");
    target.querySelectorAll("[data-operation]").forEach(button => button.addEventListener("click", () => { byId("operation-id").value = button.dataset.operation; observe(button.dataset.operation); }));
  }

  async function refresh() {
    const ns = namespace();
    const version = ++refreshVersion;
    setMessage(byId("overview-message"), "正在读取 Kubernetes 状态…", "loading");
    try {
      const [deployments, snapshots] = await Promise.all([client.deployments(ns), client.snapshots(ns)]);
      if (version !== refreshVersion || ns !== namespace()) return;
      latestSnapshots = snapshots.items || [];
      latestDeployments = deployments.items || [];
      app.latestDeployments = latestDeployments;
      renderMetrics(latestDeployments, snapshots.items || []);
      renderJobList(latestDeployments, byId("overview-jobs"));
      renderJobList(latestDeployments);
      byId("jobs-count").textContent = `${latestDeployments.length} 个作业`;
      if (selectedJob && latestDeployments.some(item => item.name === selectedJob)) await selectJob(selectedJob);

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
    const version = ++detailVersion;
    const ns = namespace();
    byId("job-details").textContent = "正在读取作业…";
    byId("job-config").textContent = "正在读取配置…";
    renderSnapshots();
    const actionEmpty = document.querySelector(".action-empty");
    if (actionEmpty) actionEmpty.hidden = true;
    byId("job-title").textContent = name;
    byId("job-actions").hidden = false;
    renderJobList(latestDeployments);
    try {
      const status = await client.status(ns, name);
      if (version !== detailVersion || ns !== namespace()) return;
      renderDetails(status);
      byId("checkpoint-summary").textContent = "Operator checkpoint 摘要（不含 task/subtask 指标）\n" + JSON.stringify(status.checkpoint || {}, null, 2);
      byId("savepoint-summary").textContent = JSON.stringify(status.savepoint || {}, null, 2);
      const resource = await client.deployment(ns, name);
      if (version === detailVersion && ns === namespace()) byId("job-config").textContent = JSON.stringify(resource.spec || {}, null, 2);
    } catch (error) {
      if (version !== detailVersion || ns !== namespace()) return;
      byId("job-config").textContent = `读取失败：${error.message}`;
      setMessage(byId("jobs-message"), error.message, "error");
    }
  }
  async function observe(id, destination = byId("operation-result")) {
    if (pollController) pollController.abort();
    const controller = new AbortController();
    pollController = controller; app.pollController = controller;
    byId("stop-poll").disabled = false;
    setMessage(byId("operation-message"), `正在观察 ${id}…`, "waiting");
    destination.innerHTML = "";
    try {
      const result = await observeOperation(() => client.operation(id), operation => {
        recentOperations.delete(operation.operationId);
        recentOperations.set(operation.operationId, operation);
        if (recentOperations.size > 20) recentOperations.delete(recentOperations.keys().next().value);
        renderRecentOperations();
        const events = (operation.events || []).map(event => `<div class="event"><strong>${escapeHtml(event.type || "event")}</strong><time>${escapeHtml(event.at || "")}</time>${renderReason(event.reason)}</div>`).join("");
        destination.innerHTML = `<article class="panel"><div class="panel-heading"><h2 class="operation-title">${escapeHtml(operation.operationId || id)}</h2>${stateBadge(operation.state)}</div><dl class="detail-grid"><div><dt>requestId</dt><dd>${escapeHtml(operation.requestId)}</dd></div><div><dt>resource</dt><dd>${escapeHtml(operation.resource?.name || "—")}</dd></div></dl><div class="event-timeline">${events || '<div class="empty">暂无审计事件。</div>'}</div></article>`;
        setMessage(byId("operation-message"), `当前状态：${operation.state}`, classifyState(operation.state));
      }, { signal: controller.signal });
      if (pollController !== controller) return;
      if (result === "limit") setMessage(byId("operation-message"), `观察窗口结束，当前结果仍需继续查询：${id}`, "waiting");
      if (result === "cancelled") setMessage(byId("operation-message"), "已停止观察。", "waiting");
      if (result === "terminal") refresh();
    } catch (error) {
      // API Server 短暂不可达时保留当前证据，并明确提示用户，不留下未处理的 Promise 异常。
      if (pollController === controller) setMessage(byId("operation-message"), `操作状态读取失败：${error.message}`, "error");
    } finally {
      if (pollController === controller) byId("stop-poll").disabled = true;
    }
  }
  function startOperation(body) { if (body.operationId) { byId("operation-id").value = body.operationId; location.hash = "operations"; showView("operations"); observe(body.operationId); } }
  async function runAction(action) {
    if (!selectedJob) return;
    const button = document.querySelector(`[data-action="${action}"]`);
    button.disabled = true; button.dataset.state = "loading";
    try { startOperation(await client.action(namespace(), selectedJob, action, ensureRequestId(byId("action-request-id")))); }
    catch (error) { setMessage(byId("jobs-message"), error.message, "error"); }
    finally { button.disabled = false; delete button.dataset.state; }
  }
  document.querySelectorAll("nav a[data-view]").forEach(link => link.addEventListener("click", () => showView(link.dataset.view)));
  byId("namespace-form").addEventListener("submit", event => { event.preventDefault(); syncNamespace(); selectedJob = null; detailVersion++;
    byId("job-actions").hidden = true;
    document.querySelector(".action-empty").hidden = false;
    byId("job-title").textContent = "作业详情";
    for (const id of ["job-details", "job-config", "checkpoint-summary", "savepoint-summary", "snapshot-list"]) byId(id).textContent = "选择作业查看详情。";
    refresh(); });
  document.querySelectorAll("[data-action]").forEach(button => button.addEventListener("click", () => runAction(button.dataset.action)));
  document.querySelectorAll("[data-tab-target]").forEach(button => button.addEventListener("click", () => {
    const target = button.dataset.tabTarget;
    document.querySelectorAll("[data-tab-target]").forEach(tab => {
      tab.classList.toggle("active", tab === button);
      tab.setAttribute("aria-selected", String(tab === button));
      tab.tabIndex = tab === button ? 0 : -1;
    });
    document.querySelectorAll(".tab-panel").forEach(panel => { panel.hidden = panel.id !== target; });
  }));
  for (const id of ["job-search", "job-state-filter"]) byId(id).addEventListener("input", () => renderJobList(latestDeployments, byId("overview-jobs")));
  const tabs = Array.from(document.querySelectorAll("[data-tab-target]"));
  tabs.forEach((tab, index) => tab.addEventListener("keydown", event => {
    const next = event.key === "ArrowRight" ? (index + 1) % tabs.length : event.key === "ArrowLeft" ? (index + tabs.length - 1) % tabs.length : event.key === "Home" ? 0 : event.key === "End" ? tabs.length - 1 : null;
    if (next !== null) { event.preventDefault(); tabs[next].focus(); tabs[next].click(); }
  }));
  byId("publish-form").addEventListener("submit", async event => { event.preventDefault(); const submit = byId("submit-publish"); submit.disabled = true; submit.dataset.state = "loading"; try { const body = await client.publish(namespace(), JSON.parse(byId("manifest").value), byId("publish-mode").value, ensureRequestId(byId("publish-request-id"))); startOperation(body); } catch (error) { setMessage(byId("publish-message"), error.message, "error"); } finally { submit.disabled = false; delete submit.dataset.state; } });
  byId("dry-run").addEventListener("click", async () => { try { const body = await client.publish(namespace(), JSON.parse(byId("manifest").value), byId("publish-mode").value, "", true); byId("dry-run-result").hidden = false; byId("dry-run-json").textContent = JSON.stringify(body, null, 2); setMessage(byId("publish-message"), "Kubernetes dry-run 完成，未创建 operation。", "success"); } catch (error) { setMessage(byId("publish-message"), error.message, "error"); } });
  byId("operation-form").addEventListener("submit", event => { event.preventDefault(); observe(byId("operation-id").value.trim()); });
  byId("stop-poll").addEventListener("click", () => pollController?.abort());
  // 一个 ID 对应一次操作意图；只有用户点击“生成新 ID”才主动换键。
  document.querySelectorAll("[data-generate-id]").forEach(button => button.addEventListener("click", () => {
    try { byId(button.dataset.generateId).value = makeRequestId(); }
    catch (error) { setMessage(byId(button.dataset.generateId === "publish-request-id" ? "publish-message" : "jobs-message"), error.message, "error"); }
  }));
  for (const id of ["publish-request-id", "action-request-id"]) {
    try { ensureRequestId(byId(id)); } catch { /* 无安全随机源时仍允许手工填写。 */ }
  }
  window.addEventListener("hashchange", () => showView(location.hash.slice(1)));
  showView(location.hash.slice(1) || "overview");
  async function bootstrap() {
    try {
      const config = await client.config();
      if (config.namespace) byId("namespace").value = config.namespace;
      syncNamespace();
    } catch (error) {
      setMessage(byId("overview-message"), `未读取服务默认 namespace，使用当前输入值：${error.message}`, "error");
    }
    await refresh();
  }
  bootstrap();
}

if (typeof document !== "undefined") document.addEventListener("DOMContentLoaded", init);

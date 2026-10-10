import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
const source = await readFile(new URL('../../main/resources/web/app.js', import.meta.url), 'utf8');
const { createClient, observeOperation, classifyState, makeRequestId, ensureRequestId, manifestTemplate, errorMessage, resourceStateClass, filterJobs, snapshotsForJob } = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);

const reply = (status, body) => ({ ok: status >= 200 && status < 300, status, text: async () => JSON.stringify(body) });
test('job action encodes namespace/name, preserves requestId and requests strict savepoint restart', async () => {
  const calls = [];
  const client = createClient(async (url, options) => { calls.push([url, options]); return reply(202, { operationId: 'op-1', state: 'ACCEPTED' }); });
  const result = await client.action('lab space', 'job/name', 'restart', 'retry+key');
  const url = new URL(calls[0][0], 'http://local');
  assert.equal(url.pathname, '/v1/deployments/job%2Fname/restart');
  assert.equal(url.searchParams.get('namespace'), 'lab space');
  assert.equal(url.searchParams.get('requestId'), 'retry+key');
  assert.equal(url.searchParams.get('upgradeMode'), 'savepoint');
  assert.equal(url.searchParams.get('fallback'), 'forbidden');
  assert.equal(result.state, 'ACCEPTED');
  assert.equal(classifyState(result.state), 'waiting');
});
test('Kubernetes error remains an error instead of becoming an empty successful list', async () => {
  const client = createClient(async () => reply(503, { error: 'API unavailable' }));
  await assert.rejects(client.deployments('default'), /503.*API unavailable/);
});
test('client reads the server default namespace configuration', async () => {
  const calls = [];
  const client = createClient(async url => { calls.push(url); return reply(200, { namespace: 'bigdata-lab' }); });
  assert.deepEqual(await client.config(), { namespace: 'bigdata-lab' });
  assert.deepEqual(calls, ['/v1/config']);
});
test('polling observes acceptance until terminal state then stops', async () => {
  const seen = [];
  let calls = 0;
  const result = await observeOperation(async () => ({ state: ['ACCEPTED', 'VERIFYING', 'COMPLETED'][calls++] }), value => seen.push(value.state), { pause: async () => {}, attempts: 5 });
  assert.equal(result, 'terminal');
  assert.deepEqual(seen, ['ACCEPTED', 'VERIFYING', 'COMPLETED']);
  assert.equal(calls, 3);
});
test('polling limits and UNKNOWN never fabricate completion', async () => {
  const states = [];
  const result = await observeOperation(async () => ({ state: 'UNKNOWN' }), value => states.push(value.state), { pause: async () => {}, attempts: 2 });
  assert.equal(result, 'limit');
  assert.deepEqual(states, ['UNKNOWN', 'UNKNOWN']);
  assert.equal(classifyState('UNCERTAIN'), 'waiting');
  assert.equal(classifyState('FAILED'), 'error');
});
test('cancelled observation does not deliver a late response', async () => {
  const controller = new AbortController();
  let delivered = false;
  const result = await observeOperation(async () => { controller.abort(); return { state: 'COMPLETED' }; }, () => { delivered = true; }, { signal: controller.signal });
  assert.equal(result, 'cancelled');
  assert.equal(delivered, false);
});
test('dry-run has no idempotency key and upgrade never rewrites manifest protection', async () => {
  const calls = [];
  const client = createClient(async (url, options) => { calls.push([new URL(url, 'http://local'), JSON.parse(options.body)]); return reply(200, {}); });
  const manifest = { metadata: { name: 'orders' }, spec: { job: { upgradeMode: 'savepoint' }, flinkConfiguration: { 'state.checkpoints.dir': 's3://bucket/cp', 'state.savepoints.dir': 's3://bucket/sp' } } };
  await client.publish('analytics', manifest, 'deploy', 'retry-1', true);
  assert.equal(calls[0][0].pathname, '/v1/deployments');
  assert.equal(calls[0][0].searchParams.get('dryRun'), 'true');
  assert.equal(calls[0][0].searchParams.has('requestId'), false);
  assert.equal(calls[0][1].metadata.namespace, 'analytics');
  assert.equal(calls[0][1].spec.job.upgradeMode, 'savepoint');
  assert.equal(manifest.metadata.namespace, undefined);
});
test('dry-run rejects upgrade before network because the server contract only previews create', async () => {
  let called = false;
  const client = createClient(async () => { called = true; return reply(200, {}); });
  assert.throws(
    () => client.publish('analytics', { metadata: { name: 'orders' }, spec: { job: { upgradeMode: 'savepoint' } } }, 'upgrade', '', true),
    /dry-run.*(Deployment|创建)/i
  );
  assert.equal(called, false);
});

// NodePort 使用 HTTP，必须覆盖 randomUUID 缺失的浏览器环境。
test('request IDs work without randomUUID and retain UUID v4 bits', () => {
  let seed = 0;
  const crypto = { getRandomValues: bytes => { bytes.fill(++seed); return bytes; } };
  const first = makeRequestId(crypto);
  assert.match(first, /^web-[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/);
  assert.notEqual(makeRequestId(crypto), first);
  assert.equal(makeRequestId({ randomUUID: () => 'secure-uuid' }), 'web-secure-uuid');
  assert.throws(() => makeRequestId({}), /手动填写/);
});
test('generated request ID stays visible and is reused after network failure', async () => {
  const input = { value: '' };
  let generated = 0;
  const generate = () => `test-${++generated}`;
  const keys = [];
  const client = createClient(async url => { keys.push(new URL(url, 'http://local').searchParams.get('requestId')); throw new Error('disconnected'); });
  for (let attempt = 0; attempt < 2; attempt++) {
    await assert.rejects(client.publish('lab', JSON.parse(manifestTemplate()), 'deploy', ensureRequestId(input, generate)), /disconnected/);
  }
  assert.equal(input.value, 'test-1');
  assert.deepEqual(keys, ['test-1', 'test-1']);
  input.value = '  explicit-id  ';
  assert.equal(ensureRequestId(input, generate), 'explicit-id');
  assert.equal(generated, 1);
});
test('starter is explicitly stateless with a Flink service account', () => {
  const spec = JSON.parse(manifestTemplate()).spec;
  assert.equal(spec.job.upgradeMode, 'stateless');
  assert.equal(spec.serviceAccount, 'flink');
  assert.equal(spec.job.entryClass, 'org.apache.flink.streaming.examples.wordcount.WordCount');
});
test('stateful manifest missing directories is rejected before submission without changing policy', () => {
  let called = false;
  const client = createClient(async () => { called = true; return reply(202, {}); });
  const manifest = JSON.parse(manifestTemplate());
  manifest.spec.job.upgradeMode = 'savepoint';
  assert.throws(() => client.publish('lab', manifest, 'deploy', 'key'), /checkpoint/);
  manifest.spec.flinkConfiguration = { 'state.checkpoints.dir': 's3://bucket/checkpoints' };
  assert.throws(() => client.publish('lab', manifest, 'deploy', 'key'), /savepoint/);
  assert.equal(called, false);
  assert.equal(manifest.spec.job.upgradeMode, 'savepoint');
});
test('stateful directory aliases are accepted and policy is preserved', async () => {
  const client = createClient(async (_url, options) => reply(202, JSON.parse(options.body)));
  for (const prefix of ['state', 'execution.checkpointing']) {
    const manifest = JSON.parse(manifestTemplate());
    manifest.spec.job.upgradeMode = 'savepoint';
    manifest.spec.flinkConfiguration = prefix === 'state'
      ? { 'state.checkpoints.dir': 's3://bucket/cp', 'state.savepoints.dir': 's3://bucket/sp' }
      : { 'execution.checkpointing.dir': 's3://bucket/cp', 'execution.checkpointing.savepoint-dir': 's3://bucket/sp' };
    assert.equal((await client.publish('lab', manifest, 'upgrade', 'key')).spec.job.upgradeMode, 'savepoint');
  }
});
test('operator JSON errors expose their message and retain plain text errors', () => {
  assert.equal(errorMessage('{"type":"ValidationException","message":"checkpoint missing"}'), 'checkpoint missing');
  assert.equal(errorMessage('plain failure'), 'plain failure');
});

test('job and operation states have separate semantics', () => {
  assert.equal(resourceStateClass('RUNNING'), 'success');
  assert.equal(resourceStateClass('FINISHED'), 'success');
  assert.equal(classifyState('RUNNING'), 'waiting');
  assert.equal(resourceStateClass('FAILED'), 'error');
  assert.equal(resourceStateClass(undefined), 'waiting');
});
test('job filtering combines name and effective status', () => {
  const items = [{name:'Orders',jobState:'RUNNING'}, {name:'failed-order',lifecycleState:'FAILED'}];
  assert.deepEqual(filterJobs(items, 'ORDER', 'FAILED'), [items[1]]);
  assert.equal(filterJobs(items, 'missing', '').length, 0);
});
test('selected job snapshots exclude other resources and session jobs', () => {
  const own = {name:'sp1',jobReferenceName:'orders',jobReferenceKind:'FlinkDeployment'};
  const other = {name:'sp2',jobReferenceName:'orders',jobReferenceKind:'FlinkSessionJob'};
  assert.deepEqual(snapshotsForJob([own,other,{jobReferenceName:'another',jobReferenceKind:'FlinkDeployment'}], 'orders'), [own]);
});
const indexHtml = await readFile(new URL('../../main/resources/web/index.html', import.meta.url), 'utf8');
test('catalog and lineage navigation exposes real API views', () => {
  assert.match(indexHtml, /data-view="catalog"/);
  assert.match(indexHtml, /data-view="lineage"/);
  assert.match(source, /\/v1\/catalogs/);
  assert.match(source, /\/v1\/lineage\/graph/);
  assert.match(source, /SQL_STATIC/);
  assert.match(source, /暂无目录|暂无血缘/);
});
test('catalog and lineage client encodes identifiers and submits static SQL', async () => {
  const calls = [];
  const client = createClient(async (url, options = {}) => { calls.push([url, options]); return reply(200, { items: [] }); });
  await client.catalogs();
  await client.tables('warehouse space', 'public');
  await client.table('warehouse', 'public', 'order/table');
  await client.lineage('raw/orders');
  await client.submitStaticLineage('SELECT 1', 'job-1');
  assert.equal(new URL(calls[1][0], 'http://local').pathname, '/v1/catalogs/warehouse%20space/tables');
  assert.equal(new URL(calls[2][0], 'http://local').pathname, '/v1/catalogs/warehouse/tables/order%2Ftable');
  assert.equal(new URL(calls[3][0], 'http://local').searchParams.get('root'), 'raw/orders');
  assert.equal(new URL(calls[4][0], 'http://local').pathname, '/v1/lineage/sql');
  assert.equal(JSON.parse(calls[4][1].body).jobId, 'job-1');
});

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
const source = await readFile(new URL('../../main/resources/web/app.js', import.meta.url), 'utf8');
const { createClient, observeOperation, classifyState } = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);

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
  const manifest = { metadata: { name: 'orders' }, spec: { job: { upgradeMode: 'savepoint' } } };
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

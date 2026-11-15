import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import http2 from 'node:http2';
import { createHash } from 'node:crypto';
import { startSource } from '../src/source.mjs';
import { run } from '../src/workflow.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..');

async function freshState() {
  return mkdtemp(path.join(tmpdir(), 'wf-restart-'));
}

test('状态落盘后在“进程重启”中继续，已完成项不重复采集', async () => {
  const source = await startSource({ name: '重启续接', kind: 'h2' });
  const stateDir = await freshState();
  try {
    const first = await run({
      source: source.url,
      stateDir,
      input: { transport: 'h2', items: ['plain'], claims: ['paid'] },
    });
    assert.equal(first.complete, true);
    const historyAfterFirst = source.history.length;
    assert.ok(historyAfterFirst > 0);

    // 以独立子进程模拟真正的进程重启：只依赖 stateDir，不带任何调用方内存
    const runner = path.join(here, 'restart-runner.mjs');
    const child = spawn(process.execPath, [runner, source.url, stateDir], { cwd: root, stdio: ['ignore', 'pipe', 'pipe'] });
    let output = '';
    child.stdout.on('data', chunk => { output += chunk; });
    child.stderr.on('data', chunk => { output += chunk; });
    const code = await new Promise(resolve => child.on('exit', resolve));
    assert.equal(code, 0, output);
    const second = JSON.parse(output.trim().split('\n').at(-1));
    assert.equal(second.complete, true);
    assert.deepEqual(second.ids.sort(), ['paid', 'plain']);

    // 重启后没有产生任何新的来源请求：进度完全来自落盘状态
    assert.equal(source.history.length, historyAfterFirst, '重启后不应重复采集已完成资料');

    const persisted = JSON.parse(await readFile(path.join(stateDir, 'result.json'), 'utf8'));
    assert.deepEqual(persisted.ids.sort(), ['paid', 'plain']);
    assert.ok((await readdir(path.join(stateDir, 'visible'))).includes('plain'));
  } finally {
    await source.close();
    await rm(stateDir, { recursive: true, force: true });
  }
});

test('POST 失联后只查收据不重发，供方无重复领取记录', async () => {
  const source = await startSource({ name: '领取幂等', kind: 'h2' });
  const stateDir = await freshState();
  try {
    const result = await run({
      source: source.url,
      stateDir,
      input: { transport: 'h2', claims: ['paid'] },
    });
    assert.equal(result.complete, true);
    const postClaims = source.history.filter(entry => entry.method === 'POST' && entry.path === '/claim/paid');
    assert.equal(postClaims.length, 1, 'POST 只能发一次，失联后必须走收据核对');
    const receiptReads = source.history.filter(entry => entry.path === '/receipts/paid');
    assert.ok(receiptReads.length >= 2, '失联前后都要查收据');
    assert.ok(result.receipts.some(entry => entry.id === 'paid' && entry.receipt.receipt === 'receipt-paid'));
  } finally {
    await source.close();
    await rm(stateDir, { recursive: true, force: true });
  }
});

test('尾部摘要校验失败的正文不进入可见目录并触发重取', async () => {
  // 本地网关：首次返回伪造尾部，第二次返回与正文一致的契约
  const digest = buffer => createHash('sha256').update(buffer).digest('hex');
  const server = http2.createServer();
  const sessions = new Set();
  const counts = new Map();
  server.on('session', session => {
    sessions.add(session);
    session.on('close', () => sessions.delete(session));
    session.on('error', () => {});
  });
  server.on('stream', (stream, headers) => {
    stream.on('error', () => {});
    const id = headers[':path'].split('/').at(-1);
    if (id !== 'trailered') { stream.respond({ ':status': 404 }); stream.end(); return; }
    const count = counts.get(id) ?? 0;
    counts.set(id, count + 1);
    const payload = Buffer.from('尾部校验资料');
    stream.respond(
      { ':status': 200, 'content-length': String(payload.length) },
      { waitForTrailers: true },
    );
    stream.end(payload);
    stream.on('wantTrailers', () => {
      if (count === 0) stream.sendTrailers({ 'x-content-sha256': digest(Buffer.from('被篡改的正文')) });
      else stream.sendTrailers({ 'x-content-sha256': digest(payload) });
    });
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const source = {
    url: `http://127.0.0.1:${server.address().port}`,
    history: [],
  };
  const stateDir = await freshState();
  try {
    const result = await run({ source: source.url, stateDir, input: { transport: 'h2', items: ['trailered'] } });
    assert.deepEqual(result.ids, ['trailered']);
    assert.equal(result.complete, true);
    const attempts = result.attempts.trailered;
    assert.equal(attempts[0].outcome, 'trailer-check-failed');
    assert.equal(attempts[0].integrity.trailerConsistent, false);
    assert.equal(attempts.at(-1).outcome, 'accepted');
    assert.equal(counts.get('trailered'), 2, '校验失败后应重取一次');
    assert.deepEqual(await readdir(path.join(stateDir, 'visible')), ['trailered']);
    assert.deepEqual(await readdir(path.join(stateDir, 'staging')), []);
  } finally {
    for (const session of sessions) session.destroy();
    await new Promise(resolve => server.close(resolve));
    await rm(stateDir, { recursive: true, force: true });
  }
});

test('截断正文绝不发布：visible 目录保持为空且结果标记 unresolved', async () => {
  const source = await startSource({ name: '截断核对', kind: 'h2' });
  const stateDir = await freshState();
  try {
    const result = await run({ source: source.url, stateDir, input: { transport: 'h2', items: ['broken'] } });
    assert.equal(result.complete, false);
    assert.deepEqual(result.ids, []);
    assert.deepEqual(result.unresolved, ['broken']);
    assert.deepEqual(await readdir(path.join(stateDir, 'visible')), []);
    assert.ok(result.attempts.broken.every(attempt => attempt.outcome === 'error' && attempt.reason === 'truncated-body'));
  } finally {
    await source.close();
    await rm(stateDir, { recursive: true, force: true });
  }
});

test('POST 首次被 REFUSED_STREAM 时，查无收据后允许重发并成功', async () => {
  // 首次 POST 拒流（未执行），第二次 POST 成功，收据立即可查
  const server = http2.createServer();
  const sessions = new Set();
  const claimCounts = new Map();   // 已接受的领取次数
  const claimAttempts = new Map(); // 含拒流在内的 POST 次数
  server.on('session', session => {
    sessions.add(session);
    session.on('close', () => sessions.delete(session));
    session.on('error', () => {});
  });
  server.on('stream', (stream, headers) => {
    stream.on('error', () => {});
    const requestPath = headers[':path'];
    if (requestPath.startsWith('/receipts/')) {
      const id = requestPath.split('/').at(-1);
      const claimed = claimCounts.get(id) ?? 0;
      stream.respond({ ':status': claimed > 0 ? 200 : 404 });
      stream.end(JSON.stringify(claimed > 0
        ? { state: 'claimed', location: '/items/' + id, receipt: 'receipt-' + id }
        : { state: 'unknown' }));
      return;
    }
    if (requestPath.startsWith('/claim/')) {
      const id = requestPath.split('/').at(-1);
      const seen = claimAttempts.get(id) ?? 0;
      claimAttempts.set(id, seen + 1);
      if (seen === 0) {
        // 首次 POST 拒流：服务端未处理，不写入领取计数
        stream.close(http2.constants.NGHTTP2_REFUSED_STREAM);
        return;
      }
      claimCounts.set(id, (claimCounts.get(id) ?? 0) + 1);
      stream.respond({ ':status': 200 });
      stream.end('accepted');
      return;
    }
    stream.respond({ ':status': 404 });
    stream.end();
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const url = `http://127.0.0.1:${server.address().port}`;
  const stateDir = await freshState();
  try {
    const result = await run({ source: url, stateDir, input: { transport: 'h2', claims: ['vip'] } });
    assert.equal(result.complete, true);
    assert.deepEqual(result.ids, ['vip']);
    const postAttempts = result.attempts.vip.filter(a => a.method === 'POST');
    assert.equal(postAttempts.length, 2, '拒流（未处理）允许重发一次 POST');
    assert.equal(postAttempts[0].reason, 'refused-stream');
    assert.equal(claimAttempts.get('vip'), 2);
    assert.equal(claimCounts.get('vip'), 1, '只有第二次 POST 被服务端接受，无重复领取');
    assert.equal(result.receipts[0].receipt.receipt, 'receipt-vip');
  } finally {
    for (const session of sessions) session.destroy();
    await new Promise(resolve => server.close(resolve));
    await rm(stateDir, { recursive: true, force: true });
  }
});

test('未知错误不被统一重试：非可重试原因直接落 unresolved 并留证据', async () => {
  const server = http2.createServer();
  const sessions = new Set();
  server.on('session', session => {
    sessions.add(session);
    session.on('close', () => sessions.delete(session));
    session.on('error', () => {});
  });
  server.on('stream', stream => {
    stream.on('error', () => {});
    stream.respond({ ':status': 500 });
    stream.end('网关内部错误');
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const url = `http://127.0.0.1:${server.address().port}`;
  const stateDir = await freshState();
  try {
    const result = await run({ source: url, stateDir, input: { transport: 'h2', items: ['x'] } });
    assert.equal(result.complete, false);
    assert.deepEqual(result.unresolved, ['x']);
    assert.equal(result.attempts.x.length, 1, '500 不做盲目重试');
    assert.equal(result.attempts.x[0].outcome, 'unexpected-status');
  } finally {
    for (const session of sessions) session.destroy();
    await new Promise(resolve => server.close(resolve));
    await rm(stateDir, { recursive: true, force: true });
  }
});

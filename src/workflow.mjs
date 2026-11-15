import { createHash } from 'node:crypto';
import {
  mkdir, writeFile, readFile, rename, rm,
} from 'node:fs/promises';
import path from 'node:path';

import { H2Pool, H2RequestError, Reason } from './h2pool.mjs';

const MAX_ATTEMPTS = 5;
const CONCURRENCY = 2;
const ATTEMPT_TIMEOUT_MS = 8000;
const BACKOFF_MS = [0, 60, 140, 300, 600];

// 可由 GET 安全重取的失败原因；POST 不能凭这些原因直接重发
const RETRIABLE_GET = new Set([
  Reason.REFUSED, Reason.RESET, Reason.TRUNCATED,
  Reason.GOAWAY_REJECTED, Reason.SESSION, Reason.TIMEOUT,
]);
// POST 失联后必须转去查收据的原因（无法从“没读到响应”推断请求未执行）
const RECEIPT_REQUIRED = new Set([
  Reason.RESET, Reason.TRUNCATED, Reason.SESSION, Reason.TIMEOUT,
]);
// 明确未被服务端处理的原因，可在查无收据后直接重发 POST
const SAFE_POST_RETRY = new Set([Reason.REFUSED, Reason.GOAWAY_REJECTED]);

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function atomicWrite(target, payload) {
  const tmp = target + '.tmp-' + process.pid + '-' + Math.random().toString(36).slice(2);
  await writeFile(tmp, payload, { flush: true });
  await rename(tmp, target);
}

function sha256(buffer) {
  return createHash('sha256').update(buffer).digest('hex');
}

function loadState() {
  return {
    version: 1,
    items: {},   // id -> 资料项状态
    claims: {},  // id -> 领取状态
  };
}

function freshItem(id) {
  return { id, phase: 'pending', attempts: [], file: null, receipt: null, error: null };
}

function freshClaim(id, credential) {
  return {
    id, phase: 'pending', credential, attempts: [], receipt: null, error: null,
  };
}

/**
 * 业务入口：source 为本地 h2c 网关，stateDir 为跨次执行保留的状态目录。
 * 不依赖调用方内存：所有进度先落盘，进程重启后从 stateDir 继续。
 */
export async function run({ source, stateDir, input }) {
  if (input?.transport !== 'h2') throw new Error('当前接收端仅支持 h2 传输');
  const visibleDir = path.join(stateDir, 'visible');
  const stagingDir = path.join(stateDir, 'staging');
  const statePath = path.join(stateDir, 'state.json');
  await mkdir(visibleDir, { recursive: true });
  await mkdir(stagingDir, { recursive: true });

  let state = loadState();
  try {
    state = JSON.parse(await readFile(statePath, 'utf8'));
  } catch { /* 首次运行，使用空状态 */ }
  state.items ??= {};
  state.claims ??= {};

  const persist = async () => atomicWrite(statePath, JSON.stringify(state, null, 2));
  await persist();

  const pool = new H2Pool(source);
  try {
    const itemIds = [...new Set(input.items ?? [])];
    const claimIds = [...new Set(input.claims ?? [])];

    // 领取凭据：显式传入优先，否则按 id 生成确定性幂等凭据并随状态落盘
    const credentialOf = id => input.credentials?.[id] ?? 'credential-' + id;
    for (const id of claimIds) if (!state.claims[id]) state.claims[id] = freshClaim(id, credentialOf(id));
    for (const id of itemIds) if (!state.items[id]) state.items[id] = freshItem(id);
    await persist();

    let cursor = 0;
    const workers = Array.from({ length: CONCURRENCY }, async () => {
      while (cursor < claimIds.length + itemIds.length) {
        const index = cursor++;
        if (index < claimIds.length) {
          await processClaim(pool, state, state.claims[claimIds[index]], { persist, visibleDir, stagingDir });
        } else {
          await processItem(pool, state, state.items[itemIds[index - claimIds.length]], { persist, visibleDir, stagingDir });
        }
      }
    });
    await Promise.all(workers);
  } finally {
    await pool.close();
  }

  const result = buildResult(state);
  await atomicWrite(path.join(stateDir, 'result.json'), JSON.stringify(result, null, 2));
  return result;
}

function buildResult(state) {
  const completed = [];
  const unresolved = [];
  const receipts = [];
  const attempts = {};
  for (const [id, entry] of Object.entries(state.claims)) {
    attempts[id] = entry.attempts;
    if (entry.phase === 'complete') {
      completed.push(id);
      receipts.push({ id, kind: 'claim', receipt: entry.receipt });
    } else unresolved.push(id);
  }
  for (const [id, entry] of Object.entries(state.items)) {
    attempts[id] = entry.attempts;
    if (entry.phase === 'complete') {
      completed.push(id);
      receipts.push({ id, kind: 'item', receipt: entry.receipt });
    } else unresolved.push(id);
  }
  return {
    ids: completed.sort(),
    complete: unresolved.length === 0,
    unresolved: unresolved.sort(),
    receipts,
    attempts,
  };
}

async function recordAttempt(state, persist, bucket, id, attempt) {
  const entry = bucket[id];
  entry.attempts.push(attempt);
  await persist(); // 每次尝试先留证据，再决定下一步
}

function backoff(attemptIndex) {
  return sleep(BACKOFF_MS[Math.min(attemptIndex, BACKOFF_MS.length - 1)] ?? 600);
}

// ---------- GET 资料项：可重取，字节数+摘要双校验后才进入可见目录 ----------

async function processItem(pool, state, entry, ctx) {
  const { persist, visibleDir, stagingDir } = ctx;
  while (entry.phase !== 'complete' && entry.attempts.length < MAX_ATTEMPTS) {
    const attemptNo = entry.attempts.length + 1;
    const startedAt = new Date().toISOString();
    let attempt;
    try {
      const response = await pool.request(
        { ':method': 'GET', ':path': '/items/' + encodeURIComponent(entry.id) },
        { timeoutMs: ATTEMPT_TIMEOUT_MS },
      );
      const { buffer, trailers, drainInfo: bodyDrainInfo } = await response.readBody();
      attempt = {
        attempt: attemptNo, method: 'GET', path: '/items/' + entry.id,
        startedAt, status: response.status, streamId: response.streamId,
        generation: response.generation,
        goaway: bodyDrainInfo ?? response.drainInfo,
        bytesReceived: buffer.length,
        trailers,
        outcome: 'response-read',
      };
      if (response.status !== 200) {
        attempt.outcome = 'unexpected-status';
        await recordAttempt(state, persist, state.items, entry.id, attempt);
        entry.phase = 'unresolved';
        entry.error = '来源返回状态 ' + response.status;
        await persist();
        return;
      }
      // 契约校验：content-length 与 x-content-sha256（头部或尾部均可携带）。
      // 头部与尾部同时出现时必须一致，且都要与实际正文吻合——尾部校验失败按完整性拒绝重取。
      const headerLength = response.headers['content-length'];
      const trailerLength = trailers['content-length'];
      const headerDigest = response.headers['x-content-sha256'];
      const trailerDigest = trailers['x-content-sha256'];
      const expectedLength = Number(headerLength ?? trailerLength);
      const expectedDigest = headerDigest ?? trailerDigest;
      const actualDigest = sha256(buffer);
      const lengthOk = Number.isFinite(expectedLength) && buffer.length === expectedLength;
      const digestOk = Boolean(expectedDigest) && actualDigest === String(expectedDigest).toLowerCase();
      const trailerConsistent =
        (trailerLength === undefined || String(trailerLength) === String(buffer.length)) &&
        (trailerDigest === undefined || String(trailerDigest).toLowerCase() === actualDigest);
      const checks = {
        length: lengthOk,
        digest: digestOk,
        trailer: trailerConsistent,
        endStream: true, // readBody 只有收到 END_STREAM 才会 resolve
      };
      attempt.integrity = {
        expectedLength, actualLength: buffer.length,
        expectedDigest, actualDigest,
        trailerPresent: trailerLength !== undefined || trailerDigest !== undefined,
        trailerConsistent,
        passed: lengthOk && digestOk && trailerConsistent,
      };
      if (!checks.length || !checks.digest || !checks.trailer) {
        attempt.outcome = !checks.trailer ? 'trailer-check-failed' : 'integrity-rejected';
        await recordAttempt(state, persist, state.items, entry.id, attempt);
        await rm(path.join(stagingDir, entry.id + '.part'), { force: true });
        await backoff(attemptNo);
        continue;
      }
      // 先写暂存目录，校验全过才原子移动到可见目录——半个文件不会被报告完成
      const staging = path.join(stagingDir, entry.id + '.part');
      const visible = path.join(visibleDir, entry.id);
      await writeFile(staging, buffer, { flush: true });
      await rename(staging, visible);
      const receipt = {
        id: entry.id,
        location: '/items/' + entry.id,
        bytes: buffer.length,
        sha256: actualDigest,
        streamId: response.streamId,
        generation: response.generation,
        completedAt: new Date().toISOString(),
      };
      entry.phase = 'complete';
      entry.file = path.relative(path.dirname(path.dirname(visible)), visible);
      entry.receipt = receipt;
      attempt.outcome = 'accepted';
      await recordAttempt(state, persist, state.items, entry.id, attempt);
      return;
    } catch (error) {
      attempt = {
        attempt: attemptNo, method: 'GET', path: '/items/' + entry.id,
        startedAt, outcome: 'error', reason: error.reason ?? 'unknown',
        code: error.code ?? null, streamId: error.streamId ?? null,
        lastStreamId: error.lastStreamId ?? null, message: error.message,
      };
      await recordAttempt(state, persist, state.items, entry.id, attempt);
      // 清理可能残留的暂存片段
      await rm(path.join(stagingDir, entry.id + '.part'), { force: true });
      if (!(error instanceof H2RequestError) || !RETRIABLE_GET.has(error.reason)) {
        entry.phase = 'unresolved';
        entry.error = error.message;
        await persist();
        return;
      }
      await backoff(attemptNo);
    }
  }
  if (entry.phase !== 'complete') {
    entry.phase = 'unresolved';
    entry.error ??= '重试次数耗尽，完整性契约未满足';
    await persist();
  }
}

// ---------- POST 领取：不能从“没读到响应”推断未执行，先查供方收据 ----------

async function fetchReceipt(pool, id, attemptNo) {
  const startedAt = new Date().toISOString();
  const path = '/receipts/' + encodeURIComponent(id);
  try {
    const response = await pool.request(
      { ':method': 'GET', ':path': path },
      { timeoutMs: ATTEMPT_TIMEOUT_MS },
    );
    const { buffer, drainInfo: bodyDrainInfo } = await response.readBody();
    const body = JSON.parse(buffer.toString('utf8'));
    return {
      ok: true,
      attempt: {
        attempt: attemptNo, method: 'GET', path, startedAt,
        status: response.status, streamId: response.streamId,
        generation: response.generation, goaway: bodyDrainInfo ?? response.drainInfo,
        outcome: 'receipt-' + (response.status === 200 ? 'found' : 'missing'),
      },
      status: response.status,
      body,
    };
  } catch (error) {
    return {
      ok: false,
      attempt: {
        attempt: attemptNo, method: 'GET', path, startedAt,
        outcome: 'error', reason: error.reason ?? 'unknown',
        code: error.code ?? null, streamId: error.streamId ?? null,
        message: error.message,
      },
      error,
    };
  }
}

async function processClaim(pool, state, entry, ctx) {
  const { persist } = ctx;

  while (entry.phase !== 'complete' && entry.attempts.length < MAX_ATTEMPTS) {
    const attemptNo = entry.attempts.length + 1;

    // 第一步：先问供方收据。已有收据就以收据为准，绝不重发 POST
    const receiptLook = await fetchReceipt(pool, entry.id, attemptNo);
    await recordAttempt(state, persist, state.claims, entry.id, receiptLook.attempt);
    if (receiptLook.ok && receiptLook.status === 200 && receiptLook.body.state === 'claimed') {
      entry.phase = 'complete';
      entry.receipt = {
        id: entry.id,
        state: receiptLook.body.state,
        location: receiptLook.body.location,
        receipt: receiptLook.body.receipt,
        confirmedAt: new Date().toISOString(),
      };
      await persist();
      return;
    }
    if (!receiptLook.ok) {
      // 收据查询本身失联：不能发 POST（可能已领取而只是查不到），退避后再查
      await backoff(attemptNo);
      continue;
    }

    // 供方明确表示没有收据（404/unknown）：才考虑发带领据凭据的 POST
    const startedAt = new Date().toISOString();
    const postPath = '/claim/' + encodeURIComponent(entry.id);
    let postAttempt;
    try {
      const response = await pool.request({
        ':method': 'POST',
        ':path': postPath,
        'x-claim-credential': entry.credential,
      }, { body: null, timeoutMs: ATTEMPT_TIMEOUT_MS });
      const { buffer, trailers, drainInfo: bodyDrainInfo } = await response.readBody();
      postAttempt = {
        attempt: attemptNo, method: 'POST', path: postPath,
        startedAt, status: response.status, streamId: response.streamId,
        generation: response.generation, goaway: bodyDrainInfo ?? response.drainInfo,
        trailers, bytesReceived: buffer.length,
        outcome: response.status === 200 || response.status === 201 ? 'claim-accepted' : 'claim-status-' + response.status,
      };
      await recordAttempt(state, persist, state.claims, entry.id, postAttempt);
      if (response.status === 200 || response.status === 201) {
        // 明确成功：仍以收据查询作为最终凭据（下一轮循环即查到）
        await backoff(attemptNo);
        continue;
      }
      if (response.status === 409) {
        // 供方说已领取：转回收据核对，避免凭响应文本自造收据
        await backoff(attemptNo);
        continue;
      }
      entry.phase = 'unresolved';
      entry.error = '领取返回状态 ' + response.status;
      await persist();
      return;
    } catch (error) {
      postAttempt = {
        attempt: attemptNo, method: 'POST', path: postPath,
        startedAt, outcome: 'error', reason: error.reason ?? 'unknown',
        code: error.code ?? null, streamId: error.streamId ?? null,
        lastStreamId: error.lastStreamId ?? null, message: error.message,
      };
      await recordAttempt(state, persist, state.claims, entry.id, postAttempt);
      if (error instanceof H2RequestError && SAFE_POST_RETRY.has(error.reason)) {
        // REFUSED_STREAM / 超出 GOAWAY 边界：流未被服务端处理，查无收据后可重发
        await backoff(attemptNo);
        continue;
      }
      if (error instanceof H2RequestError && RECEIPT_REQUIRED.has(error.reason)) {
        // 连接失败、正文截断、超时：请求可能已执行。不重发，下一轮先查收据
        await backoff(attemptNo);
        continue;
      }
      entry.phase = 'unresolved';
      entry.error = error.message;
      await persist();
      return;
    }
  }
  if (entry.phase !== 'complete') {
    entry.phase = 'unresolved';
    entry.error ??= '重试次数耗尽，未能取得供方收据';
    await persist();
  }
}

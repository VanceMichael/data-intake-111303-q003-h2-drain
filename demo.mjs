import { readFile, mkdtemp, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import assert from 'node:assert/strict';
import { startSource } from './src/source.mjs';

const { run } = await import('./src/workflow.mjs');
setTimeout(() => { console.error('运行超过 75 秒，停止并保留失败状态'); process.exit(124); }, 75000).unref();

const cases = JSON.parse(await readFile(new URL('./fixtures/cases.json', import.meta.url), 'utf8'));
const byMode = Object.fromEntries(cases.map(scenario => [scenario.mode, scenario]));

const proofs = [];

async function execute(mode) {
  const scenario = byMode[mode];
  const source = await startSource(scenario);
  const stateDir = await mkdtemp(path.join(tmpdir(), 'material-demo-'));
  try {
    const result = await run({ source: source.url, stateDir, input: scenario.input });
    const visible = await readdir(path.join(stateDir, 'visible'));
    return { scenario, source, stateDir, result, visible };
  } finally {
    // stateDir 保留到断言之后再清理
  }
}

function summarizeHistory(history) {
  return history.map(entry => ({ method: entry.method, path: entry.path, stream: entry.streamId }));
}

// ---- 证明 1：优雅排空——连接换代不丢资料、不重复领取 ----
{
  // 在契约场景基础上增加一个排空后才调度的条目，使换代连接必然承接新工作
  const drainScenario = {
    ...byMode.drain,
    input: { transport: 'h2', items: ['plain', 'drain', 'after-drain'] },
  };
  const source = await startSource(drainScenario);
  const stateDir = await mkdtemp(path.join(tmpdir(), 'material-demo-'));
  let result;
  let visible;
  try {
    result = await run({ source: source.url, stateDir, input: drainScenario.input });
    visible = await readdir(path.join(stateDir, 'visible'));
    console.log('【证明 1】优雅排空（GOAWAY lastStreamID 边界 + 新旧连接交叠）');
    const generations = new Set();
    for (const attempts of Object.values(result.attempts)) {
      for (const attempt of attempts) if (attempt.generation) generations.add(attempt.generation);
    }
    assert.equal(result.complete, true);
    assert.deepEqual(result.ids, ['after-drain', 'drain', 'plain']);
    assert.ok(generations.size >= 2, '应至少出现两代连接（旧连接排空 + 换代连接）');
    assert.deepEqual(visible.sort(), ['after-drain', 'drain', 'plain']);
    for (const id of result.ids) {
      assert.equal(result.attempts[id].filter(a => a.outcome === 'accepted').length, 1, id + ' 只应成功一次');
    }
    const goawayEvidence = Object.values(result.attempts)
      .flat()
      .filter(attempt => attempt.goaway)
      .map(attempt => ({ id: attempt.path.split('/').at(-1), streamId: attempt.goaway.streamId, lastStreamId: attempt.goaway.lastStreamId, accepted: attempt.goaway.accepted }));
    assert.ok(goawayEvidence.length >= 2, '交叠期间完成回调必须带 GOAWAY 边界证据');
    const afterDrain = result.attempts['after-drain'][0];
    assert.ok(afterDrain.generation >= 2, '排空后的新工作必须落在换代连接');
    assert.equal(afterDrain.goaway, null);
    console.log('  换代连接代数：', [...generations].sort().join(' -> '));
    console.log('  GOAWAY 边界证据：', JSON.stringify(goawayEvidence));
    console.log('  实际来源访问：', JSON.stringify(summarizeHistory(source.history)));
    console.log('  可见目录：', JSON.stringify(visible));
    proofs.push({ name: drainScenario.name, ok: true });
  } finally {
    await source.close();
    await rm(stateDir, { recursive: true, force: true });
  }
}

// ---- 证明 2：REFUSED_STREAM 可重取，不升级为整批重跑 ----
{
  const run2 = await execute('refused');
  try {
    const { result, source } = run2;
    console.log('\n【证明 2】拒流（REFUSED_STREAM）流级恢复');
    assert.equal(result.complete, true);
    assert.deepEqual(result.ids, ['refused']);
    const attempts = result.attempts.refused;
    assert.equal(attempts[0].reason, 'refused-stream');
    assert.equal(attempts.at(-1).outcome, 'accepted');
    const refusedRequests = source.history.filter(entry => entry.path === '/items/refused');
    assert.equal(refusedRequests.length, 2, '只重取被拒的这一条流');
    console.log('  尝试序列：', JSON.stringify(attempts.map(a => ({ reason: a.reason ?? null, outcome: a.outcome, stream: a.streamId ?? null, generation: a.generation ?? null }))));
    console.log('  实际来源访问：', JSON.stringify(summarizeHistory(source.history)));
    proofs.push({ name: run2.scenario.name, ok: true });
  } finally {
    await run2.source.close();
    await rm(run2.stateDir, { recursive: true, force: true });
  }
}

// ---- 证明 3：截断正文绝不报告完成，半个文件不进可见目录 ----
{
  const run3 = await execute('broken');
  try {
    const { result, source, visible } = run3;
    console.log('\n【证明 3】截断正文（RST_STREAM 中断 DATA）不发布');
    assert.equal(result.complete, false);
    assert.deepEqual(result.ids, []);
    assert.deepEqual(result.unresolved, ['broken']);
    assert.deepEqual(visible, [], '半个文件不得进入可见目录');
    assert.ok(result.attempts.broken.length >= 2);
    assert.ok(result.attempts.broken.every(a => a.reason === 'truncated-body'));
    console.log('  尝试次数：', result.attempts.broken.length, '（全部记录为 truncated-body）');
    console.log('  实际来源访问：', JSON.stringify(summarizeHistory(source.history)));
    console.log('  可见目录：', JSON.stringify(visible), '；unresolved：', JSON.stringify(result.unresolved));
    proofs.push({ name: run3.scenario.name, ok: true });
  } finally {
    await run3.source.close();
    await rm(run3.stateDir, { recursive: true, force: true });
  }
}

// ---- 附：领取失联只查收据不重发（证明不重复领取）----
{
  const run4 = await execute('claim');
  try {
    const { result, source } = run4;
    console.log('\n【附证】POST 领取失联后的收据判定');
    assert.equal(result.complete, true);
    const posts = source.history.filter(entry => entry.method === 'POST');
    assert.equal(posts.length, 1, 'POST 只发一次');
    console.log('  最终收据：', JSON.stringify(result.receipts));
    console.log('  实际来源访问：', JSON.stringify(summarizeHistory(source.history)));
    proofs.push({ name: run4.scenario.name, ok: true });
  } finally {
    await run4.source.close();
    await rm(run4.stateDir, { recursive: true, force: true });
  }
}

console.log('\n每份资料的最终收据与实际传输尝试：');
for (const [index, entry] of proofs.entries()) {
  console.log(`  ${index + 1}. ${entry.name}: ${entry.ok ? '通过' : '失败'}`);
}
assert.ok(proofs.every(entry => entry.ok));
console.log('\n短运行证明全部通过：连接换代无重复领取，截断正文未被报告完成。');

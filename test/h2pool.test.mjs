import test from 'node:test';
import assert from 'node:assert/strict';
import http2 from 'node:http2';
import { createHash } from 'node:crypto';
import { H2Pool, H2RequestError, Reason } from '../src/h2pool.mjs';

function digestOf(payload) {
  return createHash('sha256').update(payload).digest('hex');
}

// 可编程 h2c 网关：支持按路径注入 GOAWAY、REFUSED_STREAM、截断与伪造尾部
function startProgrammaticServer(handlers) {
  const server = http2.createServer();
  const sessions = new Set();
  const history = [];
  server.on('session', session => {
    sessions.add(session);
    session.on('close', () => sessions.delete(session));
    session.on('error', () => {});
  });
  server.on('stream', (stream, headers) => {
    stream.on('error', () => {});
    history.push({ path: headers[':path'], method: headers[':method'], streamId: stream.id });
    const handler = handlers[headers[':path']];
    if (!handler) { stream.respond({ ':status': 404 }); stream.end(); return; }
    handler(stream, headers);
  });
  return new Promise(resolve => {
    server.listen(0, '127.0.0.1', () => resolve({
      url: `http://127.0.0.1:${server.address().port}`,
      history,
      async close() {
        for (const session of sessions) session.destroy();
        await new Promise(done => server.close(done));
      },
    }));
  });
}

test('GOAWAY 后排空连接不接新工作，在途流仍完成，新工作走换代连接', async () => {
  const server = await startProgrammaticServer({
    '/go': stream => {
      const payload = Buffer.from('payload-go');
      stream.respond({ ':status': 200, 'content-length': String(payload.length), 'x-content-sha256': digestOf(payload) });
      stream.session.goaway(http2.constants.NGHTTP2_NO_ERROR, stream.id);
      stream.end(payload);
    },
    '/next': stream => {
      const payload = Buffer.from('payload-next');
      stream.respond({ ':status': 200, 'content-length': String(payload.length), 'x-content-sha256': digestOf(payload) });
      stream.end(payload);
    },
  });
  const pool = new H2Pool(server.url);
  try {
    const response = await pool.request({ ':method': 'GET', ':path': '/go' });
    assert.equal(response.generation, 1);
    const body = await response.readBody();
    assert.ok(body.drainInfo);
    assert.equal(body.drainInfo.lastStreamId, response.streamId);
    assert.equal(body.drainInfo.accepted, true);
    assert.equal(body.buffer.toString(), 'payload-go');
    await pool.idle();
    const second = await pool.request({ ':method': 'GET', ':path': '/next' });
    assert.equal(second.generation, 2, '排空后新请求必须落在换代连接');
    const secondBody = await second.readBody();
    assert.equal(secondBody.buffer.toString(), 'payload-next');
    assert.equal(secondBody.drainInfo, null);
  } finally {
    await pool.close();
    await server.close();
  }
});

test('交叠期间旧连接上的有效流完成回调带 GOAWAY 边界，且不被重试', async () => {
  const server = await startProgrammaticServer({
    '/go': stream => {
      const payload = Buffer.from('x'.repeat(16));
      stream.respond({ ':status': 200 });
      stream.session.goaway(http2.constants.NGHTTP2_NO_ERROR, stream.id);
      stream.end(payload);
    },
    '/quiet': stream => {
      stream.respond({ ':status': 200 });
      stream.end(Buffer.from('quiet'));
    },
  });
  const pool = new H2Pool(server.url);
  try {
    const [a, b] = await Promise.all([
      pool.request({ ':method': 'GET', ':path': '/quiet' }),
      pool.request({ ':method': 'GET', ':path': '/go' }),
    ]);
    assert.equal(a.generation, 1);
    assert.equal(b.generation, 1);
    const [ba, bb] = await Promise.all([a.readBody(), b.readBody()]);
    assert.equal(ba.buffer.toString(), 'quiet');
    assert.equal(bb.buffer.toString(), 'x'.repeat(16));
    assert.ok(ba.drainInfo, '交叠完成回调应回填排空边界');
    assert.equal(ba.drainInfo.accepted, a.streamId <= ba.drainInfo.lastStreamId);
    assert.ok(bb.drainInfo.accepted);
    assert.equal(server.history.length, 2, '在途流成功完成，不应产生重试请求');
  } finally {
    await pool.close();
    await server.close();
  }
});

test('REFUSED_STREAM 与正文截断分类为不同原因', async () => {
  const server = await startProgrammaticServer({
    '/refuse': stream => stream.close(http2.constants.NGHTTP2_REFUSED_STREAM),
    '/cut': stream => {
      stream.respond({ ':status': 200, 'content-length': '32' });
      stream.write(Buffer.from('only-three'));
      stream.close(http2.constants.NGHTTP2_INTERNAL_ERROR);
    },
  });
  const pool = new H2Pool(server.url);
  try {
    await assert.rejects(
      pool.request({ ':method': 'GET', ':path': '/refuse' }).then(r => r.readBody()),
      error => error instanceof H2RequestError && error.reason === Reason.REFUSED,
    );
    const cut = pool.request({ ':method': 'GET', ':path': '/cut' });
    const response = await cut;
    await assert.rejects(
      response.readBody(),
      error => error instanceof H2RequestError && error.reason === Reason.TRUNCATED,
    );
  } finally {
    await pool.close();
    await server.close();
  }
});

test('GOAWAY 窗口外（streamId 大于 lastStreamID）的在途流按未处理分类，可安全重发', async () => {
  const server = await startProgrammaticServer({
    '/go': stream => {
      stream.respond({ ':status': 200 });
      // 模拟服务端在处理更高 id 的流之前就下线：不承诺处理 stream 3
      stream.session.goaway(http2.constants.NGHTTP2_NO_ERROR, 1);
      stream.end(Buffer.from('ok'));
    },
    '/late': stream => {
      stream.close(http2.constants.NGHTTP2_REFUSED_STREAM);
    },
  });
  const pool = new H2Pool(server.url);
  try {
    // 先用 stream 1 建立会话（该流被拒，分类为 REFUSED 而非会话失败）
    await assert.rejects(
      (async () => {
        const response = await pool.request({ ':method': 'GET', ':path': '/late' });
        await response.readBody();
      })(),
      error => error.reason === Reason.REFUSED,
    );
    // stream 3 超出 GOAWAY(lastStreamID=1) 窗口：即使先收到响应头也按未处理分类
    await assert.rejects(
      (async () => {
        const response = await pool.request({ ':method': 'GET', ':path': '/go' });
        assert.equal(response.streamId, 3);
        await response.readBody();
      })(),
      error => error instanceof H2RequestError
        && error.reason === Reason.GOAWAY_REJECTED
        && error.streamId === 3
        && error.lastStreamId === 1,
    );
  } finally {
    await pool.close();
    await server.close();
  }
});

test('伪造响应尾部导致摘要不一致时正文判定失败', async () => {
  const server = await startProgrammaticServer({
    '/lying-trailer': stream => {
      const payload = Buffer.from('real-body');
      stream.respond({ ':status': 200, 'content-length': String(payload.length) }, { waitForTrailers: true });
      stream.end(payload);
      stream.on('wantTrailers', () => {
        stream.sendTrailers({ 'x-content-sha256': digestOf(Buffer.from('different-body')) });
      });
    },
  });
  const pool = new H2Pool(server.url);
  try {
    const response = await pool.request({ ':method': 'GET', ':path': '/lying-trailer' });
    const body = await response.readBody();
    assert.equal(body.buffer.toString(), 'real-body');
    const claimed = String(body.trailers['x-content-sha256']).toLowerCase();
    assert.notEqual(claimed, digestOf(body.buffer));
  } finally {
    await pool.close();
    await server.close();
  }
});

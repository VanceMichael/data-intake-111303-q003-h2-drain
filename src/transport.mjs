import http2 from 'node:http2';
import { createHash } from 'node:crypto';

export async function createH2Source(scenario) {
  const history = [];
  const sessions = new Set();
  const counts = new Map();
  const claims = new Map();
  const server = http2.createServer();
  server.on('session', session => {
    sessions.add(session);
    session.on('error', () => {});
    session.on('close', () => sessions.delete(session));
  });
  server.on('stream', (stream, headers) => {
    stream.on('error', () => {});
    const path = headers[':path'];
    const count = counts.get(path) ?? 0;
    counts.set(path, count + 1);
    history.push({ path, method: headers[':method'], streamId: stream.id });
    if (path === '/about') { stream.respond({ ':status': 200 }); stream.end(JSON.stringify({ name: scenario.name, kind: 'h2' })); return; }
    if (path.startsWith('/receipts/')) {
      const id = path.split('/').at(-1);
      stream.respond({ ':status': claims.has(id) ? 200 : 404 });
      stream.end(JSON.stringify(claims.get(id) ?? { state: 'unknown' })); return;
    }
    if (path.startsWith('/claim/')) {
      const id = path.split('/').at(-1);
      claims.set(id, { state: 'claimed', location: '/items/' + id, receipt: 'receipt-' + id });
      if (count === 0) { stream.close(http2.constants.NGHTTP2_INTERNAL_ERROR); return; }
      stream.respond({ ':status': 409 }); stream.end('已经领取'); return;
    }
    const id = path.split('/').at(-1);
    if (id === 'refused' && count === 0) { stream.close(http2.constants.NGHTTP2_REFUSED_STREAM); return; }
    const payload = Buffer.from('本地资料:' + id);
    const digest = createHash('sha256').update(payload).digest('hex');
    stream.respond({ ':status': 200, 'content-length': String(payload.length), 'x-content-sha256': digest });
    if (id === 'drain' && count === 0) stream.session.goaway(http2.constants.NGHTTP2_NO_ERROR, stream.id);
    if (id === 'broken') {
      stream.write(payload.subarray(0, 3));
      stream.close(http2.constants.NGHTTP2_INTERNAL_ERROR); return;
    }
    stream.end(payload);
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  return {
    url: `http://127.0.0.1:${server.address().port}`, history,
    async close() { for (const session of sessions) session.destroy(); await new Promise(resolve => server.close(resolve)); },
  };
}

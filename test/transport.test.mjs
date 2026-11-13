import test from 'node:test';
import assert from 'node:assert/strict';
import http2 from 'node:http2';
import { createH2Source } from '../src/transport.mjs';

test('网关实际发出排空边界且流可完成', async () => {
  const source = await createH2Source({ name: '排空核对' });
  const client = http2.connect(source.url);
  try {
    const boundary = new Promise(resolve => client.once('goaway', (code, last) => resolve({ code, last })));
    const request = client.request({ ':path': '/items/drain' });
    const chunks = []; for await (const chunk of request) chunks.push(chunk);
    const drained = await boundary;
    assert.equal(drained.code, 0); assert.equal(drained.last, request.id);
    assert.ok(Buffer.concat(chunks).length > 0);
  } finally { client.destroy(); await source.close(); }
});
test('领取收据先落地而响应可以失联', async () => {
  const source = await createH2Source({ name: '领取核对' });
  const client = http2.connect(source.url);
  client.on('error', () => {});
  try {
    const claim = client.request({ ':method': 'POST', ':path': '/claim/paid' });
    const failed = new Promise(resolve => claim.once('error', resolve)); claim.end(); await failed;
    const receipt = client.request({ ':path': '/receipts/paid' });
    const chunks = []; for await (const chunk of receipt) chunks.push(chunk);
    assert.equal(JSON.parse(Buffer.concat(chunks)).state, 'claimed');
  } finally { client.destroy(); await source.close(); }
});

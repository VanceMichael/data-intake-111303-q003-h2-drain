import http2 from 'node:http2';

// 流级错误原因分类：池只负责分类与换代决策，重试策略由上层按 GET/POST 区分
export const Reason = Object.freeze({
  REFUSED: 'refused-stream',          // RST_STREAM(REFUSED_STREAM)：流未被处理，可重取
  RESET: 'stream-reset',              // 其他 RST_STREAM：正文可能截断，GET 可重取
  TRUNCATED: 'truncated-body',        // 流异常结束且没收到完整 END_STREAM
  GOAWAY_REJECTED: 'goaway-rejected', // 流 id 超过 GOAWAY lastStreamID，服务端未处理
  SESSION: 'session-failed',          // 连接级失败，请求是否执行无法判断
  TIMEOUT: 'attempt-timeout',         // 单次尝试超时，请求是否执行无法判断
});

export class H2RequestError extends Error {
  constructor(reason, details = {}) {
    super('HTTP2 请求失败：' + reason + (details.code ? '（' + details.code + '）' : ''));
    this.name = 'H2RequestError';
    this.reason = reason;
    this.code = details.code;
    this.rstCode = details.rstCode;
    this.streamId = details.streamId;
    this.lastStreamId = details.lastStreamId;
  }
}

function classifyRst(rstCode) {
  if (rstCode === http2.constants.NGHTTP2_REFUSED_STREAM) return Reason.REFUSED;
  return Reason.RESET;
}

/**
 * 分层管理连接额度：
 * - 一个 origin 同时只维护一个“在岗”连接接新工作；
 * - 收到 GOAWAY 的连接立即标记排空，只保留已在途的流，不再接新工作；
 * - 新工作落到换代连接，旧连接上仍有效的流可以自然完成，
 *   完成回调通过 streamId 与 GOAWAY lastStreamID 判定归属。
 */
export class H2Pool {
  constructor(origin, options = {}) {
    this.origin = origin;
    this.options = options;
    this.active = null;         // 在岗连接
    this.draining = new Set();  // 排空中的连接
    this.closed = false;
    this.generation = 0;
  }

  #createSlot() {
    if (this.closed) throw new H2RequestError(Reason.SESSION, { code: 'POOL_CLOSED' });
    const generation = ++this.generation;
    const slot = {
      generation,
      session: null,
      draining: false,
      goaway: null, // { code, lastStreamId }
      inflight: new Set(),
      settled: false,
      whenSettled: null,
      markSettled: null,
    };
    slot.whenSettled = new Promise(resolve => { slot.markSettled = resolve; });
    let session;
    try {
      session = http2.connect(this.origin, this.options.connect ?? {});
    } catch (error) {
      this.#retire(slot);
      throw new H2RequestError(Reason.SESSION, { code: error.code });
    }
    slot.session = session;
    session.on('goaway', (code, lastStreamId) => {
      // lastStreamID：服务端承诺 <= 该 id 的流都已被处理；更大的流可能根本没执行
      slot.draining = true;
      slot.goaway = { code, lastStreamId };
      this.draining.add(slot);
      if (this.active === slot) this.active = null; // 排空连接不再接新工作
    });
    session.on('error', () => {
      // 连接级错误：在途流各自收到自己的 error 事件，这里只负责换代
      if (this.active === slot) this.active = null;
    });
    session.on('close', () => this.#retire(slot));
    return slot;
  }

  #retire(slot) {
    if (slot.settled) return;
    slot.settled = true;
    this.draining.delete(slot);
    if (this.active === slot) this.active = null;
    slot.markSettled();
  }

  #pickSlot() {
    if (this.closed) throw new H2RequestError(Reason.SESSION, { code: 'POOL_CLOSED' });
    const candidate = this.active;
    if (candidate && !candidate.draining && !candidate.session.closed && !candidate.session.destroyed) {
      return candidate;
    }
    const slot = this.#createSlot();
    this.active = slot;
    return slot;
  }

  /**
   * 发起一次流级请求。成功时返回 { status, headers, streamId, generation, drainInfo, readBody }。
   * readBody() 必须被调用并读完，以便排空连接能在流结束后真正收口。
   * 任何失败都以 H2RequestError 拒绝，reason 已分类。
   */
  async request(headers, { body, timeoutMs = 8000 } = {}) {
    const slot = this.#pickSlot();
    const session = slot.session;
    let stream;
    try {
      stream = session.request(headers);
    } catch (error) {
      // ERR_HTTP2_GOAWAY_SESSION / INVALID_SESSION：连接恰好在选槽后排空，交给换代
      if (this.active === slot) this.active = null;
      throw new H2RequestError(Reason.GOAWAY_REJECTED, { code: error.code });
    }
    slot.inflight.add(stream);
    // 流 id 在请求发出后才分配，极早期错误下可能仍为 undefined，统一用函数读取
    const streamIdOf = () => (typeof stream.id === 'number' ? stream.id : undefined);

    let responseHeaders = null;
    let streamError = null;
    let timedOut = false;
    let finished = false;

    const attemptTimer = timeoutMs
      ? setTimeout(() => {
          timedOut = true;
          stream.destroy(new Error('单次尝试超时'));
        }, timeoutMs)
      : null;
    attemptTimer?.unref?.();

    const cleanup = () => {
      if (attemptTimer) clearTimeout(attemptTimer);
      slot.inflight.delete(stream);
      if (slot.draining && slot.inflight.size === 0 && (slot.session.closed || slot.session.destroyed)) {
        this.#retire(slot);
      }
    };

    const toError = () => {
      const streamId = streamIdOf();
      if (timedOut) return new H2RequestError(Reason.TIMEOUT, { streamId });
      // GOAWAY 边界优先：客户端会自行把 lastStreamID 之外的流以 REFUSED_STREAM 重置，
      // 但语义上这些流从未被服务端处理，必须与真实的 REFUSED_STREAM 区分开
      if (slot.goaway && streamId !== undefined && streamId > slot.goaway.lastStreamId) {
        return new H2RequestError(Reason.GOAWAY_REJECTED, {
          rstCode: stream.rstCode, code: 'ERR_HTTP2_STREAM_ERROR',
          streamId, lastStreamId: slot.goaway.lastStreamId,
        });
      }
      const rstCode = stream.rstCode;
      if (rstCode !== undefined) {
        // 响应头之后的 RST/中断一律按正文截断处理
        const reason = responseHeaders === null ? classifyRst(rstCode) : Reason.TRUNCATED;
        return new H2RequestError(reason, {
          rstCode, code: 'ERR_HTTP2_STREAM_ERROR', streamId,
          lastStreamId: slot.goaway?.lastStreamId,
        });
      }
      return new H2RequestError(Reason.SESSION, { code: streamError?.code, streamId });
    };

    // 全程只挂一个 error 监听，响应前与正文阶段共用同一份错误状态
    stream.on('error', error => { streamError = error; });

    if (body !== undefined) stream.end(body);

    // 完成回调在新旧连接交叠期间触发时，以回调时刻的 GOAWAY 边界为准回填
    const drainInfoOf = id => (slot.goaway
      ? { ...slot.goaway, streamId: id, accepted: id !== undefined && id <= slot.goaway.lastStreamId }
      : null);

    return new Promise((resolve, reject) => {
      stream.once('response', incoming => {
        responseHeaders = incoming;
        const streamId = streamIdOf();
        resolve({
          status: Number(incoming[':status']),
          headers: incoming,
          streamId,
          generation: slot.generation,
          drainInfo: drainInfoOf(streamId),
          readBody: () => new Promise((resolveBody, rejectBody) => {
            const chunks = [];
            let trailers = {};
            stream.on('data', chunk => chunks.push(chunk));
            stream.on('trailers', frame => { trailers = frame; });
            stream.on('aborted', () => {
              if (finished) return;
              finished = true;
              cleanup();
              rejectBody(new H2RequestError(Reason.TRUNCATED, { code: 'ABORTED', streamId: streamIdOf() }));
            });
            stream.on('end', () => {
              if (finished) return;
              finished = true;
              cleanup();
              const id = streamIdOf();
              resolveBody({ buffer: Buffer.concat(chunks), trailers, drainInfo: drainInfoOf(id) });
            });
            stream.on('error', () => {
              if (finished) return;
              finished = true;
              cleanup();
              rejectBody(toError());
            });
          }),
        });
      });
      stream.once('error', () => {
        if (responseHeaders !== null || finished) return; // 正文阶段归 readBody
        finished = true;
        cleanup();
        reject(toError());
      });
    });
  }

  /** 等待所有排空连接的在途流结束（用于优雅停机证明） */
  async idle() {
    while (this.draining.size > 0) {
      await Promise.all([...this.draining].map(slot => slot.whenSettled));
    }
  }

  async close() {
    this.closed = true;
    const slots = new Set([...this.draining]);
    if (this.active) slots.add(this.active);
    this.active = null;
    this.draining.clear();
    for (const slot of slots) {
      try { slot.session.destroy(); } catch { /* 已关闭 */ }
      slot.markSettled();
    }
  }
}

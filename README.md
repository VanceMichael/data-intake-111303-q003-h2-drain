# 资料网关优雅下线时的 HTTP2 采集续接

这是 h2-drain 的 Node.js 项目。接收工作流位于 `src/workflow.mjs`，连接池与流级错误分类位于 `src/h2pool.mjs`，直接对接本地 h2c 网关，不回退到逐条 HTTP/1 请求。

## 连接排空与流级恢复

- **连接额度分层管理**：一个 origin 同时只有一条在岗连接承接新工作；收到 GOAWAY 的连接立即转入排空集合，不再接新流，已在途且 streamId 不大于 lastStreamID 的流允许自然完成，新工作落到换代连接。
- **区分处置的失败原因**：`REFUSED_STREAM`（流未被处理，可重取）、其他 RST/会话失败（GET 可重取）、正文截断（END_STREAM 前中断）、GOAWAY 窗口外拒绝（lastStreamID 之外，按未处理分类）、单次尝试超时。
- **GET 与 POST 分开决策**：GET 资料项可安全重取；带领取凭据的 POST 在响应失联时绝不从“没读到响应”推断未执行，而是先查 `GET /receipts/:id`，只有供方明确无收据且失败原因属于“未处理”类时才重发。
- **完整性门控**：文件先写 `staging/`，content-length 与 x-content-sha256（响应头或响应尾部，头尾部必须互相一致）全部通过后才原子移动到 `visible/`；半个文件不会被报告完成。
- **状态可恢复**：每次尝试先落 `state.json`，结果落 `result.json`；进程重启后只凭 stateDir 继续，已完成项不重复采集、已领取项不重复 POST。

## 环境

使用 Node.js 22，最低 22.20.0；Docker 基础镜像按摘要固定，依赖已在 package-lock.json 固定。资料源仅使用本地接口或本地合成设备进程。不需要真实账户、新闻站点连接或生产数据。

开发环境首次安装执行 `npm ci --ignore-scripts --no-audit --no-fund`。构建镜像执行 `docker build -t h2-drain:local .`，依赖准备只在这一步进行；正式运行不下载包、浏览器或数据。

## 常用操作

```sh
npm test
npm run build
npm run inspect
npm run source
```

`npm test` 检查已有资料源与接口读取。`npm run build` 做全部 JavaScript 源码语法检查，不生成运行时依赖。`npm run source` 启动回环服务并打印地址，`SOURCE_CASE=1 npm run source` 可切换异常场景。

## 工作流接入

业务入口为 `src/workflow.mjs` 导出的异步 `run({ source, stateDir, input })`。source 是本地资料源地址，input 形如 `{ transport: 'h2', items: [...], claims: [...] }`，stateDir 是跨次执行保留的状态目录。调用结束返回结构化业务结果（ids、complete、unresolved、receipts、attempts），并将相同结果持久化至 stateDir/result.json；逐次尝试证据与 GOAWAY 边界写入 stateDir/state.json，不依赖调用方持有的内存。

来源是 h2c 本地网关；GET /items/:id 读取字节，x-content-sha256 与 content-length 校验（响应头与响应尾部均可携带，二者必须一致）。POST /claim/:id 领取，GET /receipts/:id 查询状态和 location。/items/drain 首次连接收到 GOAWAY；/items/refused 首次流被 REFUSED_STREAM；/items/broken 永远截断。每次尝试都留证据，截断与校验失败的正文只留在 staging 并最终清理，不进入 visible。

完整来源约定、输出示例与状态判定见 `docs/source-contract.md`。fixtures 下的资料只用于本地演练，代表不同供方响应或门户状态，不是资料接收器的实现。

## 业务核对与短演示

```sh
npm run acceptance
npm run demo
```

`npm run acceptance` 跑四个契约场景并核对结果、落盘内容和实际来源访问。`npm run demo` 是短运行证明，从干净临时状态启动本地服务，实际执行优雅排空（含换代连接承接新工作）、REFUSED_STREAM 流级重取、截断正文不发布，以及 POST 失联后凭收据恢复，输出每份资料的最终收据、GOAWAY 边界证据与逐次传输尝试，不回放记录。每种情形应在 15 秒内完成，整次演示在 75 秒超时后报失败退出。

容器内执行基线检查：`docker run --rm --network none h2-drain:local npm test`。工作流核对：`docker run --rm --network none h2-drain:local npm run demo`。容器内回环端口可用；正式核对过程不连接外部网络。

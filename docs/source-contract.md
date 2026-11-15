# 来源与接收约定

来源是 h2c 本地网关；GET /items/:id 读取字节，x-content-sha256 与 content-length 校验。POST /claim/:id 领取，GET /receipts/:id 查询状态和 location。/items/drain 首次连接收到 GOAWAY；/items/refused 首次流被 REFUSED_STREAM；/items/broken 永远截断。输出 ids、complete、unresolved，每次尝试留证据。

## 边界与操作

来源地址由启动函数动态分配，不固定端口。所有时间值只按各自字段的业务含义使用；不以请求抵达顺序推导资料版本。可见结果应附真实原始响应、页面或运行证据，不能仅根据调用参数生成结果。

来源演练用例位于 fixtures/cases.json，name 为当前情形名称，routes 为实际供方响应。工作流只通过 source 暴露的接口或页面访问来源；不得导入模拟源内部状态、读取它的文件来代替真实采集。浏览器门户内部动作接口只服务页面，不是机器人可直接调用的业务 API。

读取失败不等于不存在；部分资料可以保留，但需要完整性承诺的结果不能伪称完成。重启时只能复用已落盘且能追溯依据的进度。输出可扩展更多证据字段，但不能省略下列业务状态。

## 失败原因与恢复判定

| 现象 | 分类 reason | GET 资料项 | POST 领取 |
| --- | --- | --- | --- |
| RST_STREAM(REFUSED_STREAM)，响应头前 | `refused-stream` | 直接重取 | 查无收据后可重发（流未被处理） |
| GOAWAY 且 streamId 大于 lastStreamID | `goaway-rejected` | 换代连接重取 | 查无收据后可重发 |
| 响应头后 RST/连接中断，未收到 END_STREAM | `truncated-body` | 重取，已收字节作废 | 不重发，转查收据 |
| 连接失败/单次超时，执行情况不明 | `session-failed` / `attempt-timeout` | 重取 | 不重发，转查收据 |
| 响应尾部摘要或长度与正文不一致 | `trailer-check-failed` | 重取，不进 visible | 不适用 |

POST 的恢复顺序固定为：先 `GET /receipts/:id`；收据状态为 claimed 即据此完成；收据查询失联时继续查而不是发 POST；只有供方明确返回无收据，且上一次 POST 的失败原因属于“未处理”类（`refused-stream`、`goaway-rejected`）时才重发。不能从“没有读到响应”推断请求未执行。

## 状态落盘布局

- `state.json`：items/claims 两份条目，每条含 phase、credential（领取）、逐次 attempts（含 method/path/status/streamId/generation/goaway 边界/reason/integrity）、receipt、error；每次尝试后原子覆写。
- `staging/<id>.part`：校验中的正文片段；任何校验失败或截断都不得移出该目录。
- `visible/<id>`：仅当 content-length、x-content-sha256（头部或尾部，头尾部须一致）与实际字节全部满足后，由 staging 原子改名进入。
- `result.json`：最终结构化结果，字段含 ids、complete、unresolved、receipts、attempts。

## 业务输入与结果

### 优雅排空后继续领取

业务输入：
```json
{
  "transport": "h2",
  "items": [
    "plain",
    "drain"
  ]
}
```
结果必须包含：
```json
{
  "ids": [
    "drain",
    "plain"
  ],
  "complete": true
}
```

### 拒流可重取

业务输入：
```json
{
  "transport": "h2",
  "items": [
    "refused"
  ]
}
```
结果必须包含：
```json
{
  "ids": [
    "refused"
  ],
  "complete": true
}
```

### 截断正文不得发布

业务输入：
```json
{
  "transport": "h2",
  "items": [
    "broken"
  ]
}
```
结果必须包含：
```json
{
  "ids": [],
  "complete": false,
  "unresolved": [
    "broken"
  ]
}
```

### 领取回复失联先查收据

业务输入：
```json
{
  "transport": "h2",
  "claims": [
    "paid"
  ]
}
```
结果必须包含：
```json
{
  "ids": [
    "paid"
  ],
  "complete": true
}
```

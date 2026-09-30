# 服务入口网关

Go 实现的服务入口网关，聚焦版本化路由、请求转发、**可审查的配置候选发布与回退**、上游状态、请求记录和优雅关停。

## 快速开始

```bash
go run ./cmd/local-service -addr 127.0.0.1:18081 -name v1
go run ./cmd/local-service -addr 127.0.0.1:18082 -name v2
go run ./cmd/gateway -config configs/gateway.json
```

示例请求：

```bash
curl -i http://127.0.0.1:8080/v1/health
curl -i http://127.0.0.1:8080/v2/echo/ping
curl -i http://127.0.0.1:8080/v1/flaky
```

## 路由与转发

- 配置中的 `routes` 按顺序匹配带边界的路径前缀，并携带显式 `version`。
- 路由命中后记录 `X-Gateway-Route`、`X-Gateway-Version` 和递增的 `X-Gateway-Attempt`。
- `strip_prefix` 可将网关前缀去掉后再拼到上游 `base_url`。
- 请求头会复制到上游，Hop-by-hop 头会被移除。
- 上游超时只限制连接和等待响应头；首字节返回后按流转发，适配长响应。

## 重试边界

- 只有方法出现在路由 `retry.methods` 中的请求允许重试。
- 可重试请求体会在内存中缓冲，以便安全重放；不可重试请求体保持流式。
- 连接失败和配置的 `retry_on_status` 只在网关向客户端写出响应头前重试。
- 一旦成功响应头或任意响应体开始返回，不再发起第二次上游请求，因此不会拼接两份内容。
- 客户端取消在途响应时记录 `499`、`canceled=true` 和已写字节数。

## 配置候选发布与回退

配置发布是一条可审查的流水线，每个阶段都是一条带 ID 和 revision 的发布记录，失败配置不会被抹掉：

1. 候选 candidate：整份新配置先做独立校验（反序列化加完整校验），无效配置直接拒绝，服务快照不变。
2. 有限请求演练 canary：对候选开启有请求数上限的演练窗口。只有显式携带 X-Gateway-Canary-Revision 头的请求才走候选路由，普通流量仍走当前版本；超出预算的演练请求得到 409。每个演练请求记录状态码，任一失败或零演练都拒绝候选。
3. 整体切换 promote：演练通过后整份快照原子替换。每个请求在入口固定一次快照，因此切换时在途长响应继续使用原路由、原上游，绝不会把两条上游响应拼到一起；新请求才使用新版本。
4. 回退 rollback：回退不是删除失败记录，而是基于指定历史发布的配置生成一条全新的、阶段为 rolled_back 的发布记录，parent_id 指向被回退发布，历史中完整保留候选、演练结果和失败配置。

响应头标明每个请求归属的版本：X-Gateway-Release、X-Gateway-Revision，演练请求另有 X-Gateway-Canary: true。请求记录同样带 release_id、revision、canary。

管理接口（admin_path 前缀，默认 /__gateway）：

- POST /releases/propose：提交候选，请求体为 {"config": ...} 或 {"config_path": "...", "reason": "..."}，做独立校验。
- POST /releases/{id}/canary/start：请求体 {"max_requests": N}，开启有限演练窗口。
- 演练请求：curl -H 'X-Gateway-Canary-Revision: rev-2' http://127.0.0.1:8080/api/long。
- POST /releases/{id}/canary/end：结束演练，全部成功才可确认；失败或零请求会拒绝候选。
- POST /releases/{id}/promote：确认后整体切换。
- POST /releases/{id}/rollback：基于历史发布生成全新可追溯的回退发布记录。
- POST /releases/{id}/reject：拒绝候选但保留记录。
- GET /releases、GET /releases/current、GET /inflight：审查发布历史、当前版本和每个在途请求归属的 release/revision。

旧的文件整份热更仍然支持（SIGHUP 或 POST /__gateway/reload），内部同样生成一条新的发布记录而不是原地覆盖；新配置无效时拒绝发布，当前快照继续服务。

## 优雅关停与终态

服务收到 SIGTERM/SIGINT 后停止接受新请求，并在 drain_timeout 内让在途请求按约定收尾：

- 关停后到达的新请求立即得到明确的 503 gateway is shutting down，不会排队悬挂。
- 在途长响应在窗口内继续转发直到完整结束。
- 超过等待范围仍未结束的请求被取消，得到明确的 503 gateway shutdown: drain timeout exceeded 终态，网关随后强制关闭残留连接；请求记录保留状态、已写字节和错误原因。

## 状态与记录

路由、上游状态和请求记录拆分为独立包和存储：

```bash
curl http://127.0.0.1:8080/__gateway/upstreams
curl http://127.0.0.1:8080/__gateway/requests
curl http://127.0.0.1:8080/__gateway/config
```

上游状态包含活跃请求、累计成功/失败、连续错误、重试次数和最近时间；请求记录包含路由版本、上游、状态、尝试次数、字节数、耗时、取消和错误。

## 验证

```bash
go test -race ./...
```

测试覆盖：允许的 GET 在 503 后重试且无重复内容、POST 失败不重试、首字节后取消不拼接响应；候选/有限演练/整体切换/回退的完整发布审计；真实不同本地路由上的完整长响应在切换时不拼接、中途取消记录 499；优雅关停时在途请求在窗口内收尾、超出等待范围得到明确 503 终态。集成测试通过内存 net.Pipe 承载完整 HTTP 栈（无需绑定端口）。

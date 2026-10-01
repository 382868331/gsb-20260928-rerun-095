# 服务入口网关

Go 实现的起步版服务入口网关，当前聚焦版本化路由、请求转发、整份配置原子更新、上游状态和请求记录。

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

## 候选发布与回退

配置更新走可审查的候选发布流程，有限演练是真正的发布条件，每次激活都留下不可篡改的发布记录：

```bash
# 1. 修改配置文件后，把新配置登记为候选（独立校验，失败不影响现役配置）
curl -X POST http://127.0.0.1:8080/__gateway/candidates
# 或 kill -HUP <gateway-pid>

# 2. 有限请求演练：只有带 X-Gateway-Release 头的请求走候选快照
curl -H 'X-Gateway-Release: cand-1' http://127.0.0.1:8080/v1/health
# 至少完成 release.min_successful_rehearsals 次 2xx/3xx 演练（默认 1，示例配置为 2）
curl http://127.0.0.1:8080/__gateway/candidates

# 2b. 在发布或放弃前，对比现役配置与候选在同一组请求上的差异
curl -X POST 'http://127.0.0.1:8080/__gateway/candidates/cand-1/diff'                 # 整份配置差异
curl -X POST 'http://127.0.0.1:8080/__gateway/candidates/cand-1/diff?path=/v1/health'  # 同一请求的路由/限流决策差异

# 3. 确认后整体切换（演练未完成或未达标会被拒绝，返回 409 和明确状态）
curl -X POST http://127.0.0.1:8080/__gateway/candidates/cand-1/confirm

# 3b. 修改配置后把候选修订到新内容；路由或限制一旦改变，旧演练成功记录立即失效，需要重新演练
curl -X POST http://127.0.0.1:8080/__gateway/candidates/cand-1/revise

# 3c. 放弃候选（不影响现役配置；候选与演练历史仍可在归档中追溯）
curl -X POST http://127.0.0.1:8080/__gateway/candidates/cand-1/discard

# 4. 回退：生成一条新的 rollback 发布记录，失败配置保留在历史中
curl -X POST http://127.0.0.1:8080/__gateway/rollback
curl -X POST 'http://127.0.0.1:8080/__gateway/rollback?release=rel-1'
```

- 演练门槛有四种明确状态：`not_rehearsed`（完全没演练）、`in_progress`（有演练请求尚未结束）、`incomplete_results`（全部结束但成功数/结果不达标）、`ready`（可以发布）。取消（499）、中止、5xx、无路由都不算成功演练。
- 候选配置指纹覆盖监听地址、上游、路由顺序/匹配/去前缀/重试策略；`revise` 发现指纹变化时修订号加一并把旧证据归档到 `invalidated_evidence`，新修订必须重新演练。
- 每个请求在入口取得一次快照并与当时采用的版本（发布 ID 或候选 ID + 修订号）绑定；确认、回退、放弃、关停都不会改变在途请求的版本，在途请求继续用原路由直到结束，不会把两条上游响应拼到一起。
- 请求记录中的 `release_id` 标明请求属于哪个发布（`rel-N`）或哪个候选（`cand-N`），`candidate_revision` 标明候选修订号；上游也会收到同样的 `X-Gateway-Release` 头。
- `GET /__gateway/releases` 返回完整发布历史（initial / promote / rollback）及当前现役发布；promote 记录内嵌发布时的演练证据汇总。
- `GET /__gateway/candidates` 同时返回在役候选（含每次演练结果明细）和归档（已发布 / 已放弃 / 各失效修订的证据汇总），历史演练与发布记录始终可追溯。

## 优雅关停

服务收到 `SIGTERM` 后停止接受新请求（新请求得到 `503 gateway draining`），在途请求在 `drain_timeout` 内按约定收尾；超出等待范围的请求被取消并记录 `aborted=true`、`error="shutdown deadline exceeded"` 的明确终态。

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

内存集成测试覆盖：允许的 GET 在 503 后重试且无重复内容、POST 失败不重试、首字节后取消不拼接响应、在途请求固定使用旧快照而新请求使用新快照；候选在未演练/演练在途/演练失败时确认被拒（409）、修订后旧演练证据失效、同请求差异对比，以及放弃与发布归档可追溯。

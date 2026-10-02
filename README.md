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

配置更新走可审查的候选发布流程，有限演练是发布的强制条件，每次激活都留下不可篡改的发布记录：

```bash
# 1. 修改配置文件后，把新配置登记为候选（独立校验，失败不影响现役配置）
curl -X POST http://127.0.0.1:8080/__gateway/candidates
# 或 kill -HUP <gateway-pid>

# 2. 演练开始前确认按业务类别定义的覆盖计划（必须类别、每类最低有效样本、高风险类别）
curl -X POST 'http://127.0.0.1:8080/__gateway/candidates/cand-1/coverage/acknowledge?source=runbook'

# 3. 有限请求演练：只有带 X-Gateway-Release 头的请求走候选快照；未确认计划时返回 409
curl -H 'X-Gateway-Release: cand-1' http://127.0.0.1:8080/v1/health
curl -H 'X-Gateway-Release: cand-1' -H 'X-Gateway-Sample-ID: sample-echo-1' http://127.0.0.1:8080/v2/echo/ping
curl http://127.0.0.1:8080/__gateway/candidates          # 查看演练计数与 readiness 状态
curl http://127.0.0.1:8080/__gateway/candidates/cand-1/coverage  # 分类覆盖报告

# 4. 对尚未覆盖的类别发起可追溯的补充批次（保留来源与每个请求的结果）
curl -X POST http://127.0.0.1:8080/__gateway/candidates/cand-1/supplements \
  -d '{"source":"traffic-replay","category_ids":["pay-traffic"]}'
curl -H 'X-Gateway-Release: cand-1' -H 'X-Gateway-Supplement: cand-1-sup-1' \
  -H 'X-Biz: pay' -X POST http://127.0.0.1:8080/v1/echo/pay
curl -X POST http://127.0.0.1:8080/__gateway/supplements/cand-1-sup-1/end

# 5. 在同一组请求上对比现役配置与候选，再决定发布或放弃
curl 'http://127.0.0.1:8080/__gateway/candidates/cand-1/diff?path=/v1/health&path=/v2/echo/ping'
curl -X POST http://127.0.0.1:8080/__gateway/candidates/cand-1/discard

# 6. 演练达标后确认，整体切换；发布记录冻结当时的分类证据集合
curl -X POST http://127.0.0.1:8080/__gateway/candidates/cand-1/confirm

# 7. 回退：生成一条新的 rollback 发布记录，失败配置保留在历史中
curl -X POST http://127.0.0.1:8080/__gateway/rollback
curl -X POST 'http://127.0.0.1:8080/__gateway/rollback?release=rel-1'
```

- 候选创建后处于 `staged`（尚未演练）状态，不能直接确认；演练中为 `rehearsing`（有请求未结束），演练结束但失败数未清零或成功数不足 `release.min_successful_rehearsals`（默认 1）时为 `insufficient`（未达到要求），只有 `ready` 才能确认。状态不是简单计数：在途演练必须先收尾，失败、取消、超时中止都计入证据。
- 定义了 `release.coverage` 计划的候选先进入 `plan_pending`：操作者必须在演练开始前通过 `/coverage/acknowledge` 确认必须覆盖的请求类别、每类 `min_valid_samples` 最低有效样本以及 `high_risk` 高风险类别；确认前所有演练请求以 `409` 拒绝，不产生候选证据。
- 类别匹配支持 `methods`、`path_prefixes`（带边界前缀）和 `header_name`/`header_value`（`header_value` 留空表示仅要求请求头存在），一个请求可同时属于多个类别：各类别独立累计，但总体统计对同一请求只去重计一次；同一请求可用 `X-Gateway-Sample-ID` 指定稳定样本标识，重复提交被标记为 `duplicate`，不会虚增任何覆盖。
- 只有成功且命中至少一个已定义类别的请求才算有效覆盖；失败（5xx/错误）、客户端取消（499）、关停中止都保留为证据并在报告中显示，但永远不计入有效样本。高风险类别独立判定：即使总体平均值健康，该类别缺样本或出现失败/取消/中止都会在 readiness 原因和 `409` 错误中点名该类别，候选不得发布。
- `/coverage` 报告展示每个类别的计划、匹配数、有效/失败/取消/中止数、缺失样本数、不满足条件的具体原因、该类别代表请求的基线（现役）与候选路由差异，以及整体去重统计、全部证据明细和补充批次记录。
- 对仍未覆盖的类别可通过 `/supplements` 开启补充批次（声明来源与目标类别），批次 ID 经 `X-Gateway-Supplement` 头随请求进入候选；`/supplements/<id>/end` 关闭后该 ID 不再能驱动候选流量。演练中途停止后继续时，最终发布判断使用的是同一份可追溯证据集合，重复样本去重，也无法只挑选有利结果让候选通过。
- 调整类别规则或覆盖要求后重新登记，会让路由/上游/限制变化或覆盖计划变化的在演候选立即变为 `superseded`（记录 `superseded_by` 与具体失效原因）；旧报告仍可通过 `/coverage` 查看，但带有 `invalid` 标记和失效原因，promote 记录也只携带当时冻结的证据，已失效依据不能用于新发布。
- 候选被修订（再次 `Stage` 且路由、上游目标、超时或重试限制发生变化）后，原候选立即变为 `superseded`，旧演练成功记录不再可用于发布，但记录保留可查；配置内容未变化时重复登记复用同一候选及其证据。
- 放弃候选走 `/discard`，候选与演练证据以 `discarded` 状态保留，不会被删除。
- 每个请求在入口取得一次快照并绑定；确认、回退、关闭与在途请求交错时，在途请求继续用原采用版本直到结束，不会把两条上游响应拼到一起；请求记录中的发布归属保持不变。
- 请求记录中的 `release_id` 标明请求属于哪个发布（`rel-N`）或哪个候选（`cand-N`），上游也会收到同样的 `X-Gateway-Release` 头。
- `GET /__gateway/releases` 返回完整发布历史（initial / promote / rollback）及当前现役发布；promote 记录携带当时的演练证据（成功/失败/取消/中止计数）。
- 确认未达标候选返回 `409` 并说明原因，未知候选返回 `404`。

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

内存集成测试覆盖：允许的 GET 在 503 后重试且无重复内容、POST 失败不重试、首字节后取消不拼接响应、在途请求固定使用旧快照而新请求使用新快照；候选未演练/演练在途/演练失败均不能发布、修订候选导致旧证据失效、放弃与差异对比、回退交错时在途请求绑定原发布且历史可追溯；覆盖计划未确认时演练被 409 拒绝、多类别归属与整体去重、失败/取消/中止不计有效覆盖、高风险类别不被总体平均值掩盖、补充批次来源与结果可追溯、覆盖规则变更后旧报告明确标记为失效依据。

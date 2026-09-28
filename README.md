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

## 配置更新

配置从 JSON 文件整份读取并校验，校验通过后通过原子指针替换快照：

```bash
kill -HUP <gateway-pid>
curl -X POST http://127.0.0.1:8080/__gateway/reload
```

每个请求在入口取得一次快照；后续配置更新不会改变该请求的路由、上游和重试策略。新配置无效时保留旧快照继续服务，可用于配置回退。服务收到 `SIGTERM` 后使用 `drain_timeout` 优雅关停，让旧连接正常收尾。

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

内存集成测试覆盖：允许的 GET 在 503 后重试且无重复内容、POST 失败不重试、首字节后取消不拼接响应、在途请求固定使用旧快照而新请求使用新快照。

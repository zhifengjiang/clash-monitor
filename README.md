# Clash Verge ChatGPT Monitor

一个用于在终端持续监控本地 Clash Verge / Mihomo 节点 ChatGPT 连通性的 Python 脚本。它会检查当前路由、隔离测试订阅节点，并在连续失败后尝试恢复连接。

节点优先级固定为：

1. 当前配置中已加载的可信节点。
2. 其他自有订阅节点。
3. 未列入可信白名单的本地订阅（仅在配置白名单时存在）。
4. 免费节点，仅作为最后兜底。

延迟阈值只影响排序，不会淘汰仍可连通的可信慢节点。

## 功能

- 自动发现 Clash Verge / Mihomo 控制接口。
- 检查 ChatGPT、OpenAI API、SSE 和 WebSocket 端点连通性。
- 测试当前配置或全部订阅中的节点延迟。
- 连续多轮失败后才切换，避免瞬时网络抖动。
- 只切换 ChatGPT 当前路由上的 `select` 策略组。
- 正在使用免费备用或白名单外节点时，先尝试当前配置里已加载的可信节点，再检查其他可信订阅；两者都不可用才继续保留备用节点。
- 跨订阅切换以本地基础配置为模板，只注入已验证的单个节点；订阅中的规则、DNS、TUN、端口、控制接口和策略组不会覆盖本地配置。
- 提交前会再次核对 API 可见的规则、端口、节点名称及全部策略组结构，并确认包含 DNS/TUN/节点定义的本地回滚参考文件没有变化；任一检查不一致都会拒绝重载。
- 切换失败会恢复提交前的配置及各 Selector 原选择；候选测试或回滚期间检测到手动切换时会停止且不覆盖，回滚失败会立即停止本轮后续切换。
- 支持免费备用订阅缓存和可用性统计。
- 支持持续监控、单次检查、清屏刷新和日志输出。

## 环境要求

- Python 3.9 或更高版本。
- 本机正在运行 Clash Verge Rev、Mihomo、Clash Meta 或兼容核心。
- 可访问 Clash 外部控制接口，或提供 Unix socket。
- 可选：安装 `PyYAML`。如果未安装，脚本会使用系统 Ruby 的安全 YAML 解析模式；不会反序列化 YAML 对象。

## 首次升级注意

旧版本曾把整份免费订阅配置加载到主 Clash。若当前 Clash 仍显示 `免费源...` 策略组，先在 Clash Verge 中手动重新加载你平时使用的正常配置，再启动新版监控器。修改脚本本身不会触碰当前运行中的 Clash。

## 快速开始

```bash
./monitor_clash_verge.py
```

只检查一次：

```bash
./monitor_clash_verge.py --once
```

只检查当前已加载配置：

```bash
./monitor_clash_verge.py --current-only --once
```

指定控制接口和密钥：

```bash
CLASH_API=http://127.0.0.1:9097 CLASH_SECRET=your-secret ./monitor_clash_verge.py
```

通过 Unix socket 连接：

```bash
./monitor_clash_verge.py --unix-socket /tmp/verge/verge-mihomo.sock
```

禁用自动切换：

```bash
./monitor_clash_verge.py --no-auto-switch
```

指定要切换的策略组：

```bash
./monitor_clash_verge.py --switch-group 大哥云
```

先验证流程但不修改主 Clash：

```bash
./monitor_clash_verge.py --dry-run-switch --once
```

## 常用参数

- `--interval`：正常监控间隔，默认 60 秒。
- `--retry-interval`：没有可用节点时的快速复查间隔，默认 30 秒。
- `--timeout`：节点延迟测试超时时间，默认 5000 毫秒。
- `--route-retries`：每轮综合路由探测次数，默认 3。
- `--failure-threshold`：连续失败多少轮后才允许切换，默认 2。
- `--slow`：超过该延迟标记为偏慢，默认 1200 毫秒。
- `--deep-probe-fast-ms`：候选优先延迟，不是硬淘汰线。
- `--deep-probe-max-candidates`：限制免费候选的综合探测数量；不限制可信候选。
- `--base-config`：用于安全注入节点的本地基础配置，默认是 Clash Verge 的 `clash-verge.yaml`。
- `--trusted-profile-uid`：可重复指定可信订阅 UID；设置后，`profiles.yaml` 中未列入的订阅只按未知备用处理。
- `--show-nodes`：显示节点明细。
- `--show-errors`：显示失败错误明细。
- `--clear`：每次刷新前清屏。
- `--no-log-file`：不写入日志文件。
- `--no-free-backup`：禁用所有免费备用节点和免费订阅。

## 运行产物

脚本运行时可能生成以下文件或目录，这些内容包含本机状态、订阅缓存或监控日志，不应提交到公开仓库：

- `clash-monitor-*.log`
- `clash-monitor-background.out`
- `clash-free-stats.json`
- `.clash-monitor-cache/`
- `__pycache__/`

成功跨订阅切换时，还会在 Clash Verge 数据目录交替生成两个私有运行时配置：

- `clash-monitor-runtime-a.yaml`
- `clash-monitor-runtime-b.yaml`

两个槽位用于事务回滚，权限会设置为仅当前用户可读写。它们始终由本地基础配置派生，不会采用免费订阅的顶层配置。

这些文件已在 `.gitignore` 中排除。

## 说明

脚本默认会排除香港节点，并跳过名称中类似“剩余流量”“套餐到期”的订阅信息节点。可以通过 `--exclude-regex` 和 `--skip-candidate-regex` 调整匹配规则。

默认把 Clash Verge `profiles.yaml` 中由你添加的远程订阅视为自有/可信，把内置免费源和 `--free-url` 视为免费备用。若 `profiles.yaml` 里也混有公共订阅，请使用 `--trusted-profile-uid` 明确可信 UID；所有可信候选失败后，才会考虑未列入白名单的订阅和免费源。

没有 `mixed-port` 时，监控器无法验证切换后的实际 ChatGPT 路由，因此不会执行自动切换。控制接口只接受本机回环地址或本地 Unix socket。

启用 `--strict-stream-probes` 时会读取真实 API Key；为避免凭据被转发到自定义端点，严格 SSE/WebSocket 探测只允许连接 `api.openai.com:443`。

# Clash Verge Codex Monitor

一个用于在终端持续监控本地 Clash Verge / Mihomo 节点 Codex 连通性的 Python 脚本。默认在后端执行真实鉴权、模型列表读取和 WebSocket ping/pong，不发送模型生成请求；也可显式启用 SSE/WS 生成验证。它检查当前路由、隔离测试订阅节点，并在连续网络失败后尝试恢复连接。运行时需要同目录的 `codex_probe.py`，无需打开或操作桌面 App。

节点优先级固定为：

1. 当前配置中已加载的可信节点。
2. 其他自有订阅节点。
3. 未列入可信白名单的本地订阅（仅在配置白名单时存在）。
4. 免费节点，仅作为最后兜底。

延迟阈值只影响排序，不会淘汰仍可连通的可信慢节点。

## 功能

- 自动发现 Clash Verge / Mihomo 控制接口。
- 默认 `network` 模式：真实鉴权、模型列表 HTTP 200、WebSocket 101 握手校验和随机 ping/pong 往返。只发送网络请求和控制帧，不触发模型推理，不使用生成 token。
- 可选 `generation` 模式：SSE、WebSocket 都要收到正确文本及 `response.completed` 才通过。生成模型必须显式选择，不自动继承桌面端模型。
- 400/401、只有首事件或只有 101 握手，均不算对应模式的完整验证成功。
- 默认读取本机 Codex 的 ChatGPT 登录；API Key 登录时自动改用 OpenAI API 后端。
- 鉴权、模型/参数配置、额度/限流和服务端错误单独报告“受阻”，不会因此遍历切换节点。
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

已有 Codex 登录（`$CODEX_HOME/auth.json`，默认 `~/.codex/auth.json`）时直接运行，无需配置探测模型：

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

只验证当前路由、不执行自动切换：

```bash
./monitor_clash_verge.py --current-only --no-auto-switch --once
```

使用 API Key 登录方式时，先在环境中设置 `OPENAI_API_KEY`，再运行：

```bash
./monitor_clash_verge.py --codex-auth-mode api-key --current-only --no-auto-switch --once
```

默认每 60 秒执行一次网络/协议检查，可用 `--interval` 调整。重试、候选验证和切换复查沿用所选模式，网络模式下也不会自动触发生成。

需要验证实际模型输出或 SSE 时，显式选择生成模式和账号支持的探测模型，例如：

```bash
./monitor_clash_verge.py --probe-mode generation --openai-stream-model gpt-5.6-luna --current-only --no-auto-switch --once
```

生成模式每轮执行一次 SSE 和一次 WebSocket 生成，会使用所选模型对应的账号额度；重试、候选验证和切换复查会增加调用。便宜模型可以验证共同的网络与生成传输链路，但不能证明另一模型的权限、额度或服务状态。默认网络模式不会执行这些生成请求。

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

- `--codex-auth-mode`：`auto`（默认）、`chatgpt` 或 `api-key`；默认优先使用 Codex 登录文件，缺少登录时读取 API Key。
- `--codex-auth-file`：指定 Codex `auth.json`；只读，不自动刷新、修改或复制登录文件。
- `--probe-mode`：`network`（默认，无模型生成）或 `generation`（SSE 和 WebSocket 完整生成）。
- `--openai-stream-model`：生成模式必须显式指定模型，也可通过 `OPENAI_STREAM_TEST_MODEL` 设置；网络模式不使用模型。
- `--stream-timeout`：每项真实 API/SSE/WS 探测的总时限，默认 45 秒，与节点延迟测试的 `--timeout` 分开。
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
- `--log-file`：自定义日志文件名或绝对路径；相对路径统一放在脚本所在目录的 `logs/` 下。
- `--no-log-file`：不写入日志文件。
- `--no-free-backup`：禁用所有免费备用节点和免费订阅。

## 运行产物

脚本运行时可能生成以下文件或目录，这些内容包含本机状态、订阅缓存或监控日志，不应提交到公开仓库：

- `logs/`：监控日志、轮转备份和历史归档。
- `clash-free-stats.json`
- `.clash-monitor-cache/`
- `__pycache__/`

日志默认写入脚本所在目录的 `logs/clash-monitor.log`，与启动命令的工作目录无关。终端输出同步写入文件并去掉颜色转义，每次启动追加会话记录。单文件达到约 5 MiB 时轮转，最多保留 5 份备份（`.1` 最新，`.5` 最旧）；轮转只管理当前日志及其备份。文件权限为 `0600`。`--no-log-file` 可关闭文件日志。

例如，`--log-file validation.log` 会写入 `logs/validation.log`，同样按大小轮转；绝对路径则按指定位置写入。旧的根目录日志集中保存在 `logs/archive/`，不参与自动清理。不要再使用根目录的日期日志名或个人名称日志。

后台运行时可仅保留内置日志，避免生成 `nohup.out` 或重复的终端日志：

```bash
nohup ./monitor_clash_verge.py > /dev/null 2>&1 &
```

并行启动多个监控进程时，请用不同的 `--log-file` 文件名，避免争用同一份轮转日志。

修改脚本后需要重启已经运行的监控进程。历史日志中的旧“预检/未验证”记录会保留，新版本不再生成这类成功记录。

成功跨订阅切换时，还会在 Clash Verge 数据目录交替生成两个私有运行时配置：

- `clash-monitor-runtime-a.yaml`
- `clash-monitor-runtime-b.yaml`

两个槽位用于事务回滚，权限会设置为仅当前用户可读写。它们始终由本地基础配置派生，不会采用免费订阅的顶层配置。

这些文件已在 `.gitignore` 中排除。

## 说明

脚本默认会排除香港节点，并跳过名称中类似“剩余流量”“套餐到期”的订阅信息节点。可以通过 `--exclude-regex` 和 `--skip-candidate-regex` 调整匹配规则。

默认把 Clash Verge `profiles.yaml` 中由你添加的远程订阅视为自有/可信，把内置免费源和 `--free-url` 视为免费备用。若 `profiles.yaml` 里也混有公共订阅，请使用 `--trusted-profile-uid` 明确可信 UID；所有可信候选失败后，才会考虑未列入白名单的订阅和免费源。

没有 `mixed-port` 时，监控器无法验证切换后的实际 ChatGPT 路由，因此不会执行自动切换。控制接口只接受本机回环地址或本地 Unix socket。

所有业务探测都通过显式 `127.0.0.1:mixed-port` CONNECT 隧道建立 TLS，不受 `NO_PROXY` 绕过影响，也不跟随 HTTP 重定向。ChatGPT 登录凭据仅发送到 `chatgpt.com:443/backend-api/codex/`；API Key 仅发送到 `api.openai.com:443/v1/`。支持 Codex 的 `CODEX_CA_CERTIFICATE` / `SSL_CERT_FILE` CA 配置，仍然校验 TLS 证书。

ChatGPT 登录使用 Codex 的 `/models` 和 WSS `/responses`，API Key 登录使用 `/v1/models` 和 WSS `/v1/responses`。网络模式只做认证后的接口读取、WebSocket 握手和控制帧往返；不发送 `response.create` 或模型输入。日志的“网络/协议验证通过”只表示这个范围通过，不代表 SSE 生成流、某个模型的权限/额度或完整生成已经通过。

生成模式额外调用 HTTPS `/responses`（API Key 为 `/v1/responses`），并通过 WebSocket 发送 `response.create`。每次生成使用随机校验口令、空工具列表及 `store: false`，只发送探测文本，不读取工程内容或创建桌面任务。WebSocket 不会自动回退到 SSE。

两种模式都验证 `Sec-WebSocket-Accept`。网络模式要求 pong 内容与发送的随机 ping 完全一致，只有握手、错误 pong、提前断开或超时均不能通过。生成模式还要求 SSE/WS 正确文本增量与正常完成事件，处理数据分片和 ping/pong；错误事件、无有效输出或未完成就断流均失败。Codex 网关可能省略 SSE Content-Type 或完成事件中已发送过的 output，此时仍须通过事件格式、文本校验及完成状态检查。

日志示例：

```text
Codex 网络/协议验证：通过 - 认证=chatgpt，模式=网络（鉴权/API/WS 往返，不执行模型生成或 SSE 生成流测试）；API：HTTP 200，真实鉴权成功；WebSocket：101 握手校验通过，随机 ping/pong 往返通过（未发送生成请求）
Codex 生成验证：通过 - 认证=chatgpt，模式=生成，模型=...；API：HTTP 200，真实鉴权成功；SSE：真实文本校验通过，response.completed；WebSocket：101 握手校验通过，真实文本校验通过，response.completed
Codex 网络/协议验证：受阻 - API：鉴权失败，HTTP 401（请求失败）；本轮停止节点切换
```

`--strict-stream-probes` 保留为 `--probe-mode generation` 的兼容参数，需同时指定探测模型。`--quick-route-only` 和三个 `--skip-*-probe` 参数已停用，应通过 `--probe-mode` 明确选择验证范围。不存在可读登录文件、凭据过期、生成模式未指定模型或不支持的配置都会报告受阻。仅存于系统钥匙串的凭据需要先通过受支持的 Codex 登录方式提供登录文件，或显式选择 API Key 模式。

`--once` 退出码：0 表示所选验证范围通过，1 表示网络/协议验证失败，2 表示控制接口错误，3 表示订阅/配置检查错误，4 表示日志文件错误，5 表示鉴权、额度、配置或其他原因导致验证受阻。

测试：`python3 -m unittest discover -s tests -v`。单元测试使用模拟的网络响应；实际联机验证使用上面的 `--current-only --no-auto-switch --once` 命令。后端探测验证指定代理的真实业务链路，不覆盖桌面 UI、其他插件和长时间空闲后的重连行为。

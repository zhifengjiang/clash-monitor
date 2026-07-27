# Clash Verge ChatGPT Monitor

一个用于在终端持续监控本地 Clash Verge / Mihomo 节点 ChatGPT 连通性的 Python 脚本。它会检查当前路由、测试订阅节点延迟，并可在当前 ChatGPT 路由不可用时尝试自动切换到可用节点。

## 功能

- 自动发现 Clash Verge / Mihomo 控制接口。
- 检查 ChatGPT、OpenAI API、SSE 和 WebSocket 端点连通性。
- 测试当前配置或全部订阅中的节点延迟。
- 支持自动切换 selector 策略组。
- 支持免费备用订阅缓存和可用性统计。
- 支持持续监控、单次检查、清屏刷新和日志输出。

## 环境要求

- Python 3.10 或更高版本。
- 本机正在运行 Clash Verge Rev、Mihomo、Clash Meta 或兼容核心。
- 可访问 Clash 外部控制接口，或提供 Unix socket。
- 可选：安装 `PyYAML`。如果未安装，脚本会尝试使用系统 Ruby 解析 YAML。

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

## 常用参数

- `--interval`：正常监控间隔，默认 60 秒。
- `--retry-interval`：没有可用节点时的快速复查间隔，默认 30 秒。
- `--timeout`：节点延迟测试超时时间，默认 5000 毫秒。
- `--slow`：超过该延迟标记为偏慢，默认 1200 毫秒。
- `--show-nodes`：显示节点明细。
- `--show-errors`：显示失败错误明细。
- `--clear`：每次刷新前清屏。
- `--no-log-file`：不写入日志文件。
- `--no-free-backup`：禁用免费备用订阅。

## 运行产物

脚本运行时可能生成以下文件或目录，这些内容包含本机状态、订阅缓存或监控日志，不应提交到公开仓库：

- `clash-monitor-*.log`
- `clash-monitor-background.out`
- `clash-free-stats.json`
- `.clash-monitor-cache/`
- `__pycache__/`

这些文件已在 `.gitignore` 中排除。

## 说明

脚本默认会排除香港节点，并跳过名称中类似“剩余流量”“套餐到期”的订阅信息节点。可以通过 `--exclude-regex` 和 `--skip-candidate-regex` 调整匹配规则。

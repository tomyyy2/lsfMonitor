# bmon 采样守护服务说明（方案 A）

## 1. 目标
通过 `bmon sample daemon ...` 提供统一的采样守护服务管理入口，替代手工 crontab 环境拼接，保证采样长期稳定运行。

## 2. 命令入口
```bash
./monitor/bin/bmon sample daemon install [--interval 5m]
./monitor/bin/bmon sample daemon start [--interval 5m]
./monitor/bin/bmon sample daemon stop
./monitor/bin/bmon sample daemon status
./monitor/bin/bmon sample daemon uninstall
```

- `--interval` 支持：
  - 秒：`300`
  - 分钟：`5m`
  - 小时：`1h`
- 默认 `5m`，且最小限制 `60s`。

## 3. 执行内容
每个周期内按顺序执行：
1) `bsample -q -l -U`
2) `bsample -u`

每个命令失败时自动重试（最多 2 次重试，合计 3 次尝试），重试间隔递增。

## 4. 运行模式（Linux 优先）
### 4.1 systemd user 模式（优先）
- 生成服务文件：`~/.lsfMonitor/daemon/lsfmonitor-sample-daemon.service`
- 生成环境文件：`~/.lsfMonitor/daemon/sample-daemon.env`
- 日志文件：`~/.lsfMonitor/logs/sample-daemon.log`

`install/start` 时会更新环境文件并执行 `systemctl --user daemon-reload`。

### 4.2 fallback 模式（无 systemd 时）
自动降级为 `nohup + pidfile` 风格后台运行：
- pid 文件：`~/.lsfMonitor/daemon/sample-daemon.pid`
- 日志文件同上：`~/.lsfMonitor/logs/sample-daemon.log`

## 5. 健壮性行为
- 重复 `start`：
  - systemd 模式由 systemd 保证单实例；
  - fallback 模式检测 pidfile，已运行则提示 `already running`，不重复拉起。
- `stop/uninstall`：
  - 未安装或未运行时给出友好提示（如 `not running` / `service not installed`）。

## 6. 建议操作流程
```bash
# 1) 先在当前 shell 加载 LSF 环境
source /path/to/lsf/profile

# 2) 安装并启动
./monitor/bin/bmon sample daemon install --interval 5m
./monitor/bin/bmon sample daemon start

# 3) 查看状态与日志
./monitor/bin/bmon sample daemon status
tail -f ~/.lsfMonitor/logs/sample-daemon.log
```

## 7. 卸载步骤
```bash
./monitor/bin/bmon sample daemon stop
./monitor/bin/bmon sample daemon uninstall
```

卸载后会清理 service/env/pid 文件；日志文件保留在 `~/.lsfMonitor/logs/` 便于审计。

# v3 M1 测试报告（质量子代理C）

日期：2026-03-03  
范围：最近核心改动（tab 切换、采样 join、连接关闭、新 CLI 参数解析）

## 1) 本次补充的测试

### 自动化单测（pytest）
- `tests/test_runtime_utils.py`
  - 覆盖 `resolve_monitor_tab` 与 `resolve_switch_tab`
  - 验证 LICENSE tab 不可用时回退到 JOBS
- `tests/test_bsample_runtime_behavior.py`
  - 验证采样子进程全部 `start + join`
  - 验证任一子进程失败时主流程 `SystemExit(1)`
  - 验证 `get_utilization_day_info()` 会关闭 DB 连接
- `tests/test_cli_args_parsing.py`（新增）
  - `bsample.read_args()`：`-UD` 解析正确、无参数时正确退出
  - `bmonitor.read_args()`：显式 `-t` 优先级正确、feature 驱动默认 tab 解析正确

### 手工/无 pytest 环境可执行验证脚本
- `tests/manual_verify_recent_changes.py`
  - 覆盖上述核心行为（tab、join、close、CLI 解析）
  - 仅依赖 Python 标准库与 stub，不依赖 pytest/PyQt5

## 2) CI 守门调整

已更新 `.github/workflows/ci-cd.yml`：
- 保留静态语法检查：`python -m compileall monitor memPrediction`
- 在 `build-verify` 增加无依赖 dry-run：
  - `python tests/manual_verify_recent_changes.py`
- `test` 阶段改为“pytest 优先，失败时 fallback 手工验证脚本”
  - 安装 pytest 成功：执行 `pytest -q`
  - 安装 pytest 失败：执行 `python tests/manual_verify_recent_changes.py`
- 保留 `deploy-dry-run` 制品打包流程

## 3) 本地执行情况

当前子代理运行环境缺少可用 Python 解释器（`python`/`py` 均不可用），因此**未能在本机直接跑 pytest**。  
已通过：
- 补齐/审阅测试代码逻辑
- 提供无 pytest 依赖验证脚本
- 在 CI 中加入兜底路径，保证至少静态检查 + runtime dry-run 可执行

## 4) 结论

本轮已完成针对 v3 M1 关键回归点的测试补强与 CI 守门加固。  
建议后续在具备 Python 运行时的开发机执行一次：

```bash
python -m pytest -q
python tests/manual_verify_recent_changes.py
```

用于与 CI 结果做本地一致性确认。

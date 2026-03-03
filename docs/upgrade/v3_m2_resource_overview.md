# v3 M2 资源总览增强说明

日期：2026-03-03  
范围：`lsfmon mgmt overview --range`

## 本次增强

在管理员总览输出中新增核心指标：

1. 作业成功率（DONE / 总完成作业）
2. 内存浪费率（基于 `rusage_mem` 与 `max_mem`）
3. 队列平均等待时长（基于 `submitted_time` 与 `started_time`）
4. slot/cpu/mem 利用率（范围内平均值）

## 无数据/缺字段降级策略

- 当数据库中没有任何可用数据：保持原有友好提示，不报错。
- 当 `job` 表缺少可选列（如 `rusage_mem`、`submitted_time`）：
  - 保留可计算指标（例如成功率）
  - 不可计算指标显示为 `N/A`
  - 命令整体返回成功

## 测试补充

新增/补充用例（`tests/test_lsfmon_cli_mvp.py`）：

- `test_mgmt_overview_prints_core_metrics`
  - 验证核心指标输出与预期数值
- `test_mgmt_overview_degrades_core_metrics_when_optional_columns_missing`
  - 验证缺少可选字段时优雅降级（`N/A`）
- `test_mgmt_overview_wait_time_can_show_seconds`
  - 验证短等待场景按秒展示（`sec`）
- `test_mgmt_overview_memory_waste_rate_clamps_negative_values`
  - 验证当 `max_mem > rusage_mem` 时浪费率被钳制为 `0.0%`

> 说明：若当前运行环境缺少 Python 解释器，可在具备 Python 的开发机执行 `pytest -q` 完成验证。

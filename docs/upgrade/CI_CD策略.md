# lsfMonitor CI/CD 策略（V1.0）

## 1. 关键原则（重点）
**每一次代码提交都必须经过：编码质量检查 → 构建验证 → 自动化测试 → 部署验证（至少 Dry-Run）**。

这是项目升级期的强约束，作为发布门禁长期保留。

---

## 2. 流水线目标
1. 任何提交都可追溯（提交即触发流水线）。
2. 任何变更都有质量证据（编译/测试结果）。
3. 任何发布都有可重复步骤（构建产物、部署验证）。
4. 将“手工经验”转为“自动门禁”。

---

## 3. 流水线分层

### Stage A：Code Quality
- 语法与可导入性检查（compileall）
- 代码风格检查（后续接入 ruff/black）

### Stage B：Build
- 安装脚本可执行性验证
- 关键入口帮助命令可运行（install/bsample/bmonitor）

### Stage C：Test
- 单元测试（pytest）
- 核心回归集（MVP 用例自动化子集）
- GUI 冒烟（后续接入 pytest-qt）

### Stage D：Deploy Verify
- 主分支执行部署演练（Dry-Run）
- 产物打包/归档（artifact）
- 发布前再执行一次全量回归（手工 + 自动）

---

## 4. 触发策略
- `pull_request`：执行 A/B/C（阻断式）
- `push` 到开发分支：执行 A/B/C
- `push` 到主分支：执行 A/B/C/D

---

## 5. 门禁规则（建议）
1. 任一 Stage 失败则不可合并。
2. 主分支只允许通过 PR 合并，不允许直推。
3. 发布版本必须带测试报告与构建产物。
4. 紧急修复也必须保留最小流水线（A+C）。

---

## 6. 与项目约束对齐
- 主环境 CentOS 7.9，CI 先在 GitHub Actions 做通用验证；
- 关键版本发布前，补充一轮 CentOS 7.9 真实环境回归；
- 管理员 GUI 测试采用“自动冒烟 + 人工探索”双轨制。

---

## 7. 里程碑
- M1：接入基础 CI（compile + pytest + build help check）
- M2：接入 GUI 冒烟自动化（pytest-qt）
- M3：接入发布流水线（artifact + release note + deploy verify）

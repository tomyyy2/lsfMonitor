# lsfMonitor v3 安装与部署指南（研发环境）

## 1. 目标
面向研发环境快速部署 `lsfMonitor`（黑风山分支），并完成最小可用验证。

## 2. 环境要求
- OS: Linux (CentOS 7.9 / Rocky 8.10)
- Python: 3.12.12+
- 已配置 LSF 运行环境（`lsid` 可用）

## 3. 获取代码
```bash
git clone https://github.com/tomyyy2/lsfMonitor.git
cd lsfMonitor
git checkout 黑风山
```

## 4. 安装依赖
```bash
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

> 若部分依赖在当前环境不可用，可先最小安装：
```bash
python3 -m pip install pandas pyyaml tabulate pytest pytest-cov
```

## 5. 安装 lsfMonitor
```bash
python3 install.py --prefix /path/to/lsfMonitor
```

安装后关键入口：
- `bmonitor`
- `bsample`
- `lsfmon`

## 6. 配置
优先配置路径（推荐）：
- `$HOME/.lsfMonitor/conf/config.py`

示例：
```python
db_path = "/path/to/lsfMonitor/db"
license_administrators = "all"
lmstat_path = ""
lmstat_bsub_command = ""
excluded_license_servers = ""
```

## 7. 采样（最小启动）
```bash
bsample -q -l -U
bsample -u
```

## 8. 功能验证清单
### 8.1 CLI
```bash
lsfmon my jobs
lsfmon my mem --days 7
lsfmon mgmt overview --range 7d
lsfmon mgmt trend --range 30d --export csv
lsfmon report weekly --range 7d --export csv,md
```

### 8.2 GUI
```bash
bmonitor
```
验证：
- File 菜单可见 report export center（管理员）
- 可导出 weekly csv+md
- 可查看导出历史（SUCCESS/FAILED/NO_EXPORT）

## 9. 常见问题
1) `ImportError: cannot import name 'config' from 'conf'`
- 原因：缺少 `$HOME/.lsfMonitor/conf/config.py`
- 处理：按第 6 节创建配置

2) `No LSF/Volclava/Openlava environment detected`
- 原因：LSF 环境变量未加载
- 处理：先在当前 shell 加载 LSF 环境后再执行

3) 导出显示无新文件
- 含义：命令执行成功，但本次未生成新导出（通常是无数据）

## 10. 回归测试（建议）
```bash
pytest -q
python3 tests/manual_verify_recent_changes.py
```

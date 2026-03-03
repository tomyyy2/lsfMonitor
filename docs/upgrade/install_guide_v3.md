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

> 若部分依赖在当前环境不可用，可先最小安装（仅用于 CLI/测试，不含 GUI 依赖）：
```bash
python3 -m pip install pandas pyyaml tabulate pytest pytest-cov
```

> 若需要 GUI（`bmonitor`），请额外安装：
```bash
python3 -m pip install PyQt5 qdarkstyle
```

## 5. 安装 lsfMonitor
推荐在仓库根目录直接执行：
```bash
python3 install.py
```

如需显式指定 `--prefix`，该路径应指向当前代码根目录（用于写入 `LSFMONITOR_INSTALL_PATH`），不是独立安装目录：
```bash
python3 install.py --prefix $(pwd)
```

安装后入口脚本默认生成在仓库内 `./monitor/bin/`，请使用：
- `./monitor/bin/bmonitor`
- `./monitor/bin/bsample`
- `./monitor/bin/lsfmon`

如需直接用命令名调用，可先加入 PATH：
```bash
export PATH="$(pwd)/monitor/bin:$PATH"
```

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
./monitor/bin/bsample -q -l -U
./monitor/bin/bsample -u
```

## 8. 功能验证清单
### 8.1 CLI
```bash
./monitor/bin/lsfmon my jobs
./monitor/bin/lsfmon my mem --days 7
./monitor/bin/lsfmon mgmt overview --range 7d
./monitor/bin/lsfmon mgmt trend --range 30d --export csv
./monitor/bin/lsfmon report weekly --range 7d --export csv,md
```

### 8.2 GUI
```bash
./monitor/bin/bmonitor
```
验证：
- File 菜单可见 report export center（管理员）
- 可导出 weekly csv+md
- 可查看导出历史（SUCCESS/FAILED/NO_EXPORT）

## 9. 常见问题
1) `ImportError: cannot import name 'config' from 'conf'`
- 常见原因（按顺序排查）：
  - 未执行 `install.py`，导致 `monitor/conf/config.py` 未生成
  - 未使用安装入口脚本（应使用 `./monitor/bin/*` 或确保 PATH 指向 `monitor/bin`）
  - 用户覆盖配置缺失（`$HOME/.lsfMonitor/conf/config.py`）
- 处理：先执行第 5 节安装，再按第 6 节补齐用户配置

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

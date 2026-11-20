# 多源数据采集与RPA自动化平台

面向网页抓取、API接入、增量采集、RPA流程编排、数据校验与采集监控的数据工程平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 数据源配置 | `datasource` | 数据源 | 源端编号、连接类型、接入地址 |
| 网页采集规则 | `crawler` | 采集规则 | 规则编号、目标网址、页面选择器 |
| API接入管理 | `apiclient` | API客户端 | 客户端编号、接口名称、请求方法 |
| 增量采集 | `incremental` | 增量游标 | 游标编号、来源标识、增量字段 |
| RPA任务编排 | `robot_task` | RPA任务 | 任务编号、任务名称、页面定位器 |
| 数据接入通道 | `ingestion` | 接入通道 | 通道编号、通道名称、输入格式 |
| 格式解析器 | `parser` | 解析配置 | 解析编号、输入格式、输出格式 |
| 采集结果校验 | `validator` | 校验规则 | 规则编号、校验字段、校验类型 |
| 采集去重 | `dedup` | 去重策略 | 策略编号、输入来源、去重键 |
| 采集调度 | `schedule` | 调度计划 | 计划编号、关联任务、调度周期 |
| 采集监控 | `monitor` | 监控面板 | 面板编号、采集源总数、今日采集量 |
| 采集日志 | `log_viewer` | 日志条目 | 日志编号、关联任务、日志级别 |
| 凭证管理 | `auth` | 认证凭证 | 凭证编号、凭证类型、关联数据源 |
| 采集转换管道 | `transform` | 转换节点 | 节点编号、输入来源、输出目标 |
| 数据流转编排 | `flow` | 流转管道 | 管道编号、管道名称、起始节点 |
| 数据血缘追踪 | `lineage` | 血缘关系 | 关系编号、上游节点、下游节点 |
| 采集报告 | `report` | 采集报告 | 报告编号、采集源、统计周期 |

### 增量批次包

增量采集页的「增量批次包」（`/incremental/batches`）把指定游标区间的源端变更切成分片，
打包成可离线回放的批次包。后端在 `app/services/batch_package.py`，接口在
`app/routers/batch_package.py`，包文件默认落在 `backend/var/packages/<批次包编号>/`。

- `POST /api/incremental/batches`：切分打包。必填 `来源标识`、`起始游标`、`结束游标`；
  可选 `分片条数`、`游标编号`、`模拟写入失败于第几片`（本地演练用）。
- `POST /api/incremental/batches/{编号}/retry`：追加一次尝试；上一次的中间片保留供核对。
- `GET /api/incremental/batches/{编号}/index`：只返回已交付且逐片校验通过的索引。
- `GET /api/incremental/batches/{编号}/coverage?start=&end=&attempt=`：逐片列区间并核对覆盖。
- `GET /api/incremental/batches/{编号}/replay?cursor=`：按整片回放包含该游标位置的分片。
- `GET /api/incremental/batches/{编号}/download`：下载 zip；未交付或分片不齐备时拒绝。

关键口径：

- **索引最后发布**：所有分片写完并且记录数、游标区间、校验和都对得上，才原子写入
  `index.json`；没有索引的包不能下载、不能回放，避免把缺失游标误认为「没有变更」。
- **每次尝试独立目录**：`attempts/attempt-000N/` 下的中间片即使存在，也不被本次索引引用。
- **一次切分只用一个口径**：切分途中分片条数被改动就判本次尝试失败，不会产出混排索引；
  重试按新口径整包重切。
- **采集日志按尝试登记**：每条尝试记录来源、切分口径、`覆盖游标区间`、`实际覆盖段` 和
  逐片货架区间；在采集日志页按 `批次包编号` 可筛出，也可通过批次包详情的 `logs` 读取。

复现「游标 180-209 货架变更、前十条已写入、现场改为每片六条」的完整口径：

```bash
cd backend
python3 scripts/replay_scenario.py
```

回放人员据此可以看到：第 1 次切分只实际覆盖 `[180, 199]` 且未交付；重试后索引为 5 片、
每片 6 条；游标 188 落在分片 2 `[186, 191]`，整片重放货架 `SH-0186`~`SH-0191`；
第 1 次切分只能从采集日志核对，不能作为回放输入。


## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。
- 增量批次包的回归测试在 `backend/tests/`，运行：`cd backend && python3 -m pytest tests -q`。

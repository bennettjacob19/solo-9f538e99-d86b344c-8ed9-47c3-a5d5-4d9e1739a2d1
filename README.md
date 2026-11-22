# cgroup v2 SIGKILL 归因诊断 API

批处理子进程收到 SIGKILL 时，值班员用它分清是 **OOM** 还是 **主动强停**。

## 原理

- 每轮作业跑在**独立 slot cgroup**（`/sys/fs/cgroup/gsb/slotN`），各自设 `memory.max`。
- 起作业前、作业回收后各读一次本轮 `memory.events` 的 `oom_kill` 计数。
- 结合三样证据归因：**进程退出状态**、**oom_kill 前后差值**、**本轮是否登记了强停请求**。

| 退出状态 | oom_kill 差值 | 有强停请求 | 结论 |
|---|---|---|---|
| 0 / 非零自行退出 | 0 或缺失 | 任意 | `normal_exit` |
| 自行退出 | >0 | 任意 | `unknown`（证据冲突） |
| -9 (SIGKILL) | >0 | 无 | `oom_kill` |
| -9 (SIGKILL) | 0 | 有 | `force_stopped` |
| -9 (SIGKILL) | >0 | 有 | `unknown`（证据冲突） |
| -9 (SIGKILL) | 0 | 无 | `unknown`（可能被外部杀死） |
| -9 (SIGKILL) | 缺失 | 任意 | `unknown`（证据缺失） |
| 其他信号 / 退出状态缺失 | 任意 | 任意 | `unknown` |

## 规则实现

- **上限生效才接作业**：启动时检测本容器 cgroup v2 的 `memory.max` 为具体数值、
  且能创建子 cgroup 并启用 memory 控制器，否则 `POST /jobs` 一律 503，`GET /status` 给出原因。
- **两轮并行**：槽位数默认 2（`GSB_SLOTS`）。第三轮在槽位占满时收到 409。
- **结论后才交还**：槽位只在归因结论（含 `unknown`）落定后交还，供下一轮使用。

## 启动（docker compose）

要求：cgroup v2 宿主机 + Docker ≥ 20.10（默认 private cgroup 命名空间，容器内
`/sys/fs/cgroup` 可写；不依赖 privileged）。

```bash
docker compose up --build
# 打开 http://127.0.0.1:8000/docs

# 无 cgroup 条件的机器：切到模拟后端（接口与归因流程不变，OOM 由样例作业自查复现）
GSB_SIMULATE=1 docker compose up --build
```

compose 里 `mem_limit: 384m`（等值 `memswap_limit` 禁 swap）即“内存上限生效”。
单轮上限 = (384 − 96 预留) / 2 = **144MiB**，见 `GET /status`。
`GSB_SIMULATE=1` 时 `/status` 的 `mode` 为 `simulation`，仍正常接作业。
`GET /docs` 里可直接调用全部接口（含 `GET /jobs` 的逐条响应）。

## 用法（/docs 里点，或 curl）

```bash
# 1. 看上限是否生效、单轮上限多少
curl -s localhost:8000/status

# 2. 起一轮：目标 200MiB > 单轮上限 144MiB -> 预期 OOM
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' \
     -d '{"memory_mib": 200, "hold_seconds": 120}'

# 3. 再起一轮小的，然后凭作业号强停 -> 预期 force_stopped
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' \
     -d '{"memory_mib": 32, "hold_seconds": 120}'
curl -s -X POST localhost:8000/jobs/<job_id>/stop

# 4. 查归因
curl -s localhost:8000/jobs/<job_id>

# 5. 拉整晚全部记录（新的在前，逐条发送）
curl -s localhost:8000/jobs
```

返回字段：`exit_status`（subprocess 约定，-9 = SIGKILL）、`exit_signal`、
`oom_kill_before/after/delta`、`stop_requested`、`verdict`、`verdict_reason`、
`output_tail`。

> 注意：Python 解释器自身开销（约 20–40MiB）也计入本轮 cgroup，选目标 MiB 时留余量。

## `GET /jobs`：逐条发送的完整列表

响应体仍是**原字段组成的 JSON 数组**，但服务不再等整份 JSON 拼完才发：

- **按接单先后从新到旧**逐条编码、逐条写出（chunked 传输），首条记录无需
  先扫完历史即可到达客户端。
- **名单与证据以受理这次查询时为准**：受理即固定接单链头与各运行中作业的
  记录快照；受理之后才接单的作业留到下次查询；发送期间才结束的作业仍返回
  查询开始时的状态、归因与日志尾部（单条记录内部也不会半新半旧）。
- **额外内存不随历史条数增长**：遍历只持有一个作业号，编码缓冲只放当前一条，
  受理时的快照至多等于槽位数（默认 2）；不复制、不排序、不物化整份历史。
- **与其他请求隔离**：慢读的客户端只拖住自己那条响应，`POST /jobs` 照常接单、
  `POST /jobs/{id}/stop` 照常强停、槽位照常交还。
- **断开即回收**：客户端中途断开时响应生成器被取消/关闭，`finally` 立即释放
  本次查询的视图与快照；`GET /status` 的 `active_list_streams` 可观察在途查询数。

## 自测

```bash
# 真实 cgroup 环境（推荐）：一轮 OOM 与一轮强停交错并行 + 槽位交还 + 正常退出
docker compose run --rm selftest

# GET /jobs 逐条发送：长列表、慢读、中途断开、受理时刻语义
docker compose run --rm streamtest
#   -> 对已运行的服务：python3 scripts/streamtest.py --base-url http://127.0.0.1:8000

# 本机无特权环境：模拟后端跑通同一套流程（OOM 由样例作业按限制自查复现）
GSB_SIMULATE=1 python3 -m uvicorn app.main:app &
python3 scripts/selftest.py
python3 scripts/streamtest.py            # 自起 in-process 服务 + 长历史

# 纯逻辑单测（归因真值表 + 逐条发送/受理时刻/内存/释放）
python3 -m unittest tests.test_verdict tests.test_stream -v
```

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `GSB_SLOTS` | 2 | 并行槽位（轮）数 |
| `GSB_RESERVE_MIB` | 96 | 给 API 进程预留的内存 MiB |
| `GSB_SIMULATE` | 0 | 1 = 模拟后端（不碰 /sys/fs/cgroup） |
| `GSB_SIM_LIMIT_MIB` | 256 | 模拟后端的“容器上限” |

## 已知边界

- 作业记录保存在进程内存，重启即失；单 worker 运行。
- `GET /jobs` 的名单在一次查询内固定：查询受理之后才接单的作业，要等下一次
  查询才会出现。
- 两轮同时都打满内存时，容器级 `memory.max` 可能先于 slot 级触发，
  此时 slot 计数不增、归因落 `unknown` —— 属“证据不足按不明处理”的预期行为。

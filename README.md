# PowerMem / memory-powermem 压测说明

本目录是独立压测项目，用于验证：
- powermem-server 的 API 性能上限与延迟拐点
- OpenClaw 端到端链路（OpenClaw -> memory-powermem 插件 -> powermem-server）的吞吐与稳定性

所有脚本均基于 .venv 环境运行，输出 JSON 结果用于汇总分析。

---

## 为什么要这样测试
1) **分层定位瓶颈**
   - 直连 powermem-server 可直接定位后端存储与检索性能
   - 端到端链路可验证 OpenClaw + 插件的整体开销与失败率
2) **找到 SLA 拐点**
   - 通过并发梯度拉升，观察 p95/p99 延迟与错误率突增点
3) **验证插件调用路径**
   - OpenClaw 端到端压测通过 prompt 强制触发 memory_store / memory_recall
   - 若插件配置正确，实际走的是 memory-powermem 插件链路

---

## 环境准备
1) 创建虚拟环境并安装依赖
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2) 生成 mock 数据
```
python scripts/generate_mock_data.py --count 2000
```

3) 最小可运行流程（快速上手）
- 先完成上面的 `.venv` 与 mock 数据准备
- 然后执行：
  - 直连 powermem-server（v2 API）
  - OpenClaw LTM 端到端链路

### 快速上手命令（直连 v2 API）
```
python scripts/bench_powermem_api.py \
  --base-url http://localhost:8000 \
  --mode mix \
  --concurrency 200 \
  --duration 300 \
  --user-count 200 \
  --agent-count 50 \
  --warmup-creates 10 \
  --cleanup-before-run \
  --output results_powermem_mix.json
```

### 快速上手命令（OpenClaw LTM，推荐）
```
python scripts/bench_openclaw_e2e.py \
  --transport ltm \
  --openclaw-path openclaw \
  --mode mix \
  --concurrency 20 \
  --duration 120 \
  --output results_openclaw_ltm_mix.json
```

补充说明：
- powermem API 默认使用 `/api/v2` 接口
- `bench_powermem_api.py` 默认使用 sticky worker 上下文：每个 worker 固定 `user_id/agent_id`
- 默认会先执行预热写入（`--warmup-creates`），再进入正式压测循环
- 历史记忆默认不会自动清空；需要显式传入 `--cleanup-before-run`
- 压测专用环境可使用 `--cleanup-global`，只调用一次 `delete-all` 全量清理
- OpenClaw 端到端场景需启用 `memory-powermem` 插件并正确绑定 memory slot
- 高并发场景建议多机分片运行，避免单机资源成为瓶颈

---

## powermem-server 直连压测
脚本：`scripts/bench_powermem_api.py`  
默认走 v2 接口 `/api/v2`，支持 `X-API-Key`。

基础命令见上方“快速上手命令（直连 v2 API）”。  
需要更保守的超时设置时，可追加：`--timeout 240`。

常用参数：
- `--mode write|read|mix`：读写比例
- `--infer`：开启智能记忆（默认关闭，减少模型外部开销）
- `--api-key`：powermem-server 开启鉴权时填写
- `--timeout`：请求超时（默认 180s）
- `--think-time-ms`：请求间隔（模拟人类节奏）
- `--max-connections`：限制并发连接数（避免句柄耗尽）
- `--max-keepalive`：限制 keepalive 连接数
- `--warmup-creates`：每个 worker 正式压测前的预热写入次数（默认 5）
- `--warmup-search-ratio`：每次预热写入后附加一次查询的概率（默认 0.2）
- `--cleanup-before-run`：压测开始前清理本次 worker 上下文的历史记忆（默认关闭）
- `--cleanup-global`：压测前调用一次 `delete-all` 全量清理（默认关闭，谨慎使用）
- `--cleanup-page-size`：清理时每轮拉取数量（默认 200）
- `--cleanup-max-rounds`：每个上下文的最大清理轮数（默认 200）
- `--graceful-stop-timeout`：首次中断后等待收敛的秒数，超时会强制取消 worker（默认 5）

---

## OpenClaw 端到端压测（验证 memory-powermem 插件）
脚本：`scripts/bench_openclaw_e2e.py`

### LTM 模式（推荐）
直接调用 `openclaw ltm add/search`。

```
python scripts/bench_openclaw_e2e.py \
  --transport ltm \
  --mode mix \
  --concurrency 20 \
  --duration 120
```

### 插件路径确认（关键）
确保 OpenClaw 配置将 memory slot 绑定到 `memory-powermem`，否则测试不是插件链路。
```
"plugins": {
  "slots": { "memory": "memory-powermem" },
  "entries": {
    "memory-powermem": {
      "enabled": true,
      "config": {
        "mode": "http",
        "baseUrl": "http://<powermem-server>:8000",
        "httpApiVersion": "v2"
      }
    }
  }
}
```

---

## 结果解读
脚本输出 JSON，核心字段：
```
{
  "metrics": {
    "create": {
      "count": 1000,
      "errors": 5,
      "error_rate": 0.005,
      "p50_ms": 120,
      "p90_ms": 260,
      "p95_ms": 410,
      "p99_ms": 900,
      "status": { "200": 995, "500": 5 }
    }
  }
}
```

解读建议：
- **p95/p99**：衡量尾延迟，决定 SLA 是否达标
- **error_rate**：错误率上升点就是容量上限边界
- **status**：定位 4xx/5xx 来源

---

## 常见问题
### OSError: Too many open files
说明服务或压测端的文件描述符不足，常见于高并发场景。

处理方式：
- 降低 `--concurrency`，或设置 `--max-connections` / `--max-keepalive`
- 在启动 powermem-server 前提高文件句柄上限，例如：
```
ulimit -n 65535
```
- 若是在 macOS，需要时可用 `launchctl` 提升系统级上限后重启终端

---

## 指标含义与可得结论
1) **延迟分位数**
   - p95 上升说明系统进入拥塞区
   - p99 突增通常意味着数据库或外部模型瓶颈
2) **错误率**
   - 5xx 上升代表服务资源不足或依赖失败
   - 4xx 上升代表请求格式或鉴权配置问题
3) **端到端 vs 直连对比**
   - 端到端延迟 - 直连延迟 = OpenClaw 调度层与插件链路的额外开销
   - 若差值过大，需重点优化插件链路或 OpenClaw 并发配置

基于以上指标，重点关注：
- **powermem-server 是否达到目标 QPS 与 SLA**
- **插件链路是否稳定，是否存在 OpenClaw/插件瓶颈**
- **多 user/agent 并发下是否出现明显抖动**
- **是否需要拆分多实例进行水平扩展**

---

## 大并发建议
并发 5000 建议分片运行：
- 10~20 个 OpenClaw 实例
- 每实例 250~500 并发
否则单机 CPU/内存资源会导致数据不具代表性

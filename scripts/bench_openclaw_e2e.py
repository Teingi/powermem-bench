#!/usr/bin/env python3
import argparse
import asyncio
import json
import random
from typing import Optional

import httpx

from common import Metrics, load_jsonl, load_lines, now_ms, write_json


OP_WEIGHTS = {
    "write": [("store", 0.8), ("search", 0.2)],
    "read": [("search", 0.8), ("store", 0.2)],
    "mix": [("store", 0.5), ("search", 0.5)],
}


def pick_op(mode: str) -> str:
    choices = OP_WEIGHTS[mode]
    r = random.random()
    acc = 0.0
    for op, w in choices:
        acc += w
        if r <= acc:
            return op
    return choices[-1][0]


def pick_user(user_count: int) -> str:
    return f"user-{random.randint(1, user_count)}"


def pick_agent(agent_count: int) -> str:
    return f"agent-{random.randint(1, agent_count)}"


async def run_gateway_request(
    client: httpx.AsyncClient,
    url: str,
    token: str,
    model: str,
    user_id: str,
    prompt: str,
    timeout: float,
) -> tuple[bool, Optional[int]]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    body = {
        "model": model,
        "input": prompt,
        "user": user_id,
    }
    resp = await client.post(url, headers=headers, json=body, timeout=timeout)
    return resp.status_code < 400, resp.status_code


async def run_ltm_cmd(cmd: list[str]) -> bool:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    return (await proc.wait()) == 0


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--transport", choices=["gateway", "ltm"], default="gateway")
    parser.add_argument("--gateway-url", default="http://127.0.0.1:18789")
    parser.add_argument("--gateway-token", default="")
    parser.add_argument("--model", default="openclaw")
    parser.add_argument("--openclaw-path", default="openclaw")
    parser.add_argument("--mode", choices=list(OP_WEIGHTS.keys()), default="mix")
    parser.add_argument("--duration", type=int, default=300)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--user-count", type=int, default=50)
    parser.add_argument("--agent-count", type=int, default=10)
    parser.add_argument("--dataset", default="data/mock_memories.jsonl")
    parser.add_argument("--queries", default="data/mock_queries.txt")
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--max-connections", type=int, default=256)
    parser.add_argument("--max-keepalive", type=int, default=64)
    parser.add_argument("--think-time-ms", type=int, default=0)
    parser.add_argument("--store-template", default="Please use memory_store to remember: {content}")
    parser.add_argument("--search-template", default="Please use memory_recall to search: {query}")
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    records = load_jsonl(args.dataset)
    queries = load_lines(args.queries)
    if not records:
        raise SystemExit("dataset is empty")
    if not queries:
        raise SystemExit("queries is empty")

    metrics = Metrics()
    gateway_url = args.gateway_url.rstrip("/") + "/v1/responses"

    async def worker_gateway(client: httpx.AsyncClient, stop_at: float) -> None:
        while now_ms() < stop_at:
            op = pick_op(args.mode)
            user_id = pick_user(args.user_count)
            agent_id = pick_agent(args.agent_count)
            if op == "store":
                content = random.choice(records)["content"]
                prompt = args.store_template.format(content=content, user_id=user_id, agent_id=agent_id)
            else:
                query = random.choice(queries)
                prompt = args.search_template.format(query=query, user_id=user_id, agent_id=agent_id)
            t0 = now_ms()
            try:
                ok, status = await run_gateway_request(
                    client, gateway_url, args.gateway_token, args.model, user_id, prompt, args.timeout
                )
                latency = now_ms() - t0
                metrics.record(op, latency, ok, status)
            except Exception:
                latency = now_ms() - t0
                metrics.record(op, latency, False, None)
            if args.think_time_ms > 0:
                await asyncio.sleep(args.think_time_ms / 1000.0)

    async def worker_ltm(stop_at: float) -> None:
        while now_ms() < stop_at:
            op = pick_op(args.mode)
            if op == "store":
                content = random.choice(records)["content"]
                cmd = [args.openclaw_path, "ltm", "add", content]
            else:
                query = random.choice(queries)
                cmd = [args.openclaw_path, "ltm", "search", query]
            t0 = now_ms()
            try:
                ok = await run_ltm_cmd(cmd)
                latency = now_ms() - t0
                metrics.record(op, latency, ok, None)
            except Exception:
                latency = now_ms() - t0
                metrics.record(op, latency, False, None)
            if args.think_time_ms > 0:
                await asyncio.sleep(args.think_time_ms / 1000.0)

    if args.transport == "gateway" and not args.gateway_token:
        raise SystemExit("gateway-token is required for transport=gateway")

    stop_at = now_ms() + args.duration * 1000.0
    if args.transport == "gateway":
        max_connections = max(1, min(args.max_connections, args.concurrency))
        max_keepalive = max(1, min(args.max_keepalive, max_connections))
        limits = httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive,
        )
        async with httpx.AsyncClient(limits=limits) as client:
            tasks = [asyncio.create_task(worker_gateway(client, stop_at)) for _ in range(args.concurrency)]
            await asyncio.gather(*tasks)
    else:
        tasks = [asyncio.create_task(worker_ltm(stop_at)) for _ in range(args.concurrency)]
        await asyncio.gather(*tasks)

    summary = {
        "transport": args.transport,
        "mode": args.mode,
        "duration_s": args.duration,
        "concurrency": args.concurrency,
        "metrics": metrics.summary(),
    }
    if args.output:
        write_json(args.output, summary)
    print(json.dumps(summary, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    asyncio.run(main())

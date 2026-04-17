#!/usr/bin/env python3
import argparse
import asyncio
import json
import random
import signal
from collections import deque
from typing import Deque, Dict, Optional
from urllib.parse import quote

import httpx

from common import Metrics, load_jsonl, load_lines, now_ms, write_json


OP_WEIGHTS = {
    "write": [("create", 0.8), ("search", 0.2)],
    "read": [("search", 0.8), ("create", 0.2)],
    "mix": [("create", 0.5), ("search", 0.4), ("update", 0.05), ("delete", 0.05)],
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


def worker_user(worker_idx: int, user_count: int) -> str:
    return f"user-{(worker_idx % max(1, user_count)) + 1}"


def worker_agent(worker_idx: int, agent_count: int) -> str:
    return f"agent-{(worker_idx % max(1, agent_count)) + 1}"


def build_headers(api_key: Optional[str]) -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-API-Key"] = api_key
    return headers


def extract_memory_id(payload: dict) -> Optional[str]:
    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("memory_id", "id"):
            if key in data:
                return str(data[key])
    return None


def classify_error(exc: Exception) -> str:
    if isinstance(exc, httpx.TimeoutException):
        return "timeout"
    if isinstance(exc, httpx.ConnectError):
        return "connect_error"
    if isinstance(exc, httpx.ReadError):
        return "read_error"
    if isinstance(exc, httpx.RemoteProtocolError):
        return "protocol_error"
    return type(exc).__name__


def extract_agent_memories(payload: dict) -> list[dict]:
    data = payload.get("data")
    if isinstance(data, dict):
        memories = data.get("memories")
        if isinstance(memories, list):
            return [row for row in memories if isinstance(row, dict)]
    return []


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--api-key", default="")
    parser.add_argument("--mode", choices=list(OP_WEIGHTS.keys()), default="mix")
    parser.add_argument("--duration", type=int, default=300)
    parser.add_argument("--concurrency", type=int, default=200)
    parser.add_argument("--user-count", type=int, default=200)
    parser.add_argument("--agent-count", type=int, default=50)
    parser.add_argument("--dataset", default="data/mock_memories.jsonl")
    parser.add_argument("--queries", default="data/mock_queries.txt")
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--max-connections", type=int, default=512)
    parser.add_argument("--max-keepalive", type=int, default=128)
    parser.add_argument("--infer", action="store_true", default=False)
    parser.add_argument("--config-file", default="")
    parser.add_argument("--think-time-ms", type=int, default=0)
    parser.add_argument(
        "--warmup-creates",
        type=int,
        default=5,
        help="per-worker preheat creates before entering mixed/read loop",
    )
    parser.add_argument(
        "--warmup-search-ratio",
        type=float,
        default=0.2,
        help="probability of appending one search after each warmup create",
    )
    parser.add_argument(
        "--cleanup-before-run",
        action="store_true",
        default=False,
        help="cleanup existing memories for each worker context before benchmark",
    )
    parser.add_argument(
        "--cleanup-global",
        action="store_true",
        default=False,
        help="cleanup all memories once via delete-all before benchmark",
    )
    parser.add_argument(
        "--cleanup-page-size",
        type=int,
        default=200,
        help="page size used when listing memories during cleanup",
    )
    parser.add_argument(
        "--cleanup-max-rounds",
        type=int,
        default=200,
        help="max cleanup rounds per context to prevent infinite loops",
    )
    parser.add_argument(
        "--cleanup-strategy",
        choices=["auto", "delete-all", "list-delete"],
        default="auto",
        help="cleanup strategy: auto tries delete-all first, then list-delete fallback",
    )
    parser.add_argument(
        "--graceful-stop-timeout",
        type=float,
        default=5.0,
        help="seconds to wait after first interrupt before forcing worker cancellation",
    )
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    records = load_jsonl(args.dataset)
    queries = load_lines(args.queries)
    if not records:
        raise SystemExit("dataset is empty")
    if not queries:
        raise SystemExit("queries is empty")

    config_payload = None
    if args.config_file:
        with open(args.config_file, "r", encoding="utf-8") as f:
            config_payload = json.load(f)

    headers = build_headers(args.api_key)
    metrics = Metrics()
    stop_event = asyncio.Event()
    warmup_search_ratio = max(0.0, min(1.0, args.warmup_search_ratio))

    async def request_create(
        client: httpx.AsyncClient,
        user_id: str,
        agent_id: str,
        memory_ids: Deque[str],
    ) -> None:
        record = random.choice(records)
        body = {
            "content": record["content"],
            "metadata": record.get("metadata"),
            "user_id": user_id,
            "agent_id": agent_id,
            "infer": args.infer,
        }
        if config_payload:
            body["config"] = config_payload
        t0 = now_ms()
        try:
            resp = await client.post("/api/v2/memories", json=body, headers=headers, timeout=args.timeout)
            latency = now_ms() - t0
            ok = resp.status_code < 400
            payload = resp.json() if resp.content else {}
            if ok:
                mem_id = extract_memory_id(payload)
                if mem_id:
                    memory_ids.append(mem_id)
            error_type = None if ok else f"status_{resp.status_code}"
            metrics.record("create", latency, ok, resp.status_code, error_type)
        except Exception as exc:
            latency = now_ms() - t0
            metrics.record("create", latency, False, None, classify_error(exc))

    async def request_search(client: httpx.AsyncClient, user_id: str, agent_id: str) -> None:
        query = random.choice(queries)
        body = {
            "query": query,
            "user_id": user_id,
            "agent_id": agent_id,
            "limit": 3,
        }
        if config_payload:
            body["config"] = config_payload
        t0 = now_ms()
        try:
            resp = await client.post("/api/v2/memories/search", json=body, headers=headers, timeout=args.timeout)
            latency = now_ms() - t0
            ok = resp.status_code < 400
            error_type = None if ok else f"status_{resp.status_code}"
            metrics.record("search", latency, ok, resp.status_code, error_type)
        except Exception as exc:
            latency = now_ms() - t0
            metrics.record("search", latency, False, None, classify_error(exc))

    async def request_update(
        client: httpx.AsyncClient,
        user_id: str,
        agent_id: str,
        memory_ids: Deque[str],
    ) -> None:
        mem_id = memory_ids[0] if memory_ids else None
        if not mem_id:
            await request_create(client, user_id, agent_id, memory_ids)
            return
        body = {
            "content": random.choice(records)["content"],
            "metadata": {"updated": True},
            "user_id": user_id,
            "agent_id": agent_id,
        }
        if config_payload:
            body["config"] = config_payload
        t0 = now_ms()
        try:
            resp = await client.post(f"/api/v2/memories/update/{mem_id}", json=body, headers=headers, timeout=args.timeout)
            latency = now_ms() - t0
            ok = resp.status_code < 400
            error_type = None if ok else f"status_{resp.status_code}"
            metrics.record("update", latency, ok, resp.status_code, error_type)
        except Exception as exc:
            latency = now_ms() - t0
            metrics.record("update", latency, False, None, classify_error(exc))

    async def request_delete(
        client: httpx.AsyncClient,
        user_id: str,
        agent_id: str,
        memory_ids: Deque[str],
    ) -> None:
        mem_id = memory_ids.popleft() if memory_ids else None
        if not mem_id:
            await request_create(client, user_id, agent_id, memory_ids)
            return
        body = {
            "user_id": user_id,
            "agent_id": agent_id,
        }
        if config_payload:
            body["config"] = config_payload
        t0 = now_ms()
        try:
            resp = await client.post(f"/api/v2/memories/delete/{mem_id}", json=body, headers=headers, timeout=args.timeout)
            latency = now_ms() - t0
            ok = resp.status_code < 400
            error_type = None if ok else f"status_{resp.status_code}"
            metrics.record("delete", latency, ok, resp.status_code, error_type)
        except Exception as exc:
            latency = now_ms() - t0
            metrics.record("delete", latency, False, None, classify_error(exc))

    async def cleanup_context_memories_delete_all(
        client: httpx.AsyncClient,
        user_id: str,
        agent_id: str,
    ) -> tuple[bool, int, int]:
        body = {
            "user_id": user_id,
            "agent_id": agent_id,
        }
        if config_payload:
            body["config"] = config_payload

        try:
            resp = await client.post(
                "/api/v2/system/delete-all-memories",
                json=body,
                headers=headers,
                timeout=args.timeout,
            )
        except Exception:
            return False, 0, 1

        if resp.status_code >= 400:
            return False, 0, 1

        payload = resp.json() if resp.content else {}
        data = payload.get("data") if isinstance(payload, dict) else None
        deleted = 0
        if isinstance(data, dict):
            try:
                deleted = int(data.get("deleted", 0))
            except Exception:
                deleted = 0
        return True, deleted, 0

    async def cleanup_global_memories_delete_all(
        client: httpx.AsyncClient,
    ) -> tuple[bool, int, int]:
        body: dict = {}
        if config_payload:
            body["config"] = config_payload
        try:
            resp = await client.post(
                "/api/v2/system/delete-all-memories",
                json=body,
                headers=headers,
                timeout=args.timeout,
            )
        except Exception:
            return False, 0, 1

        if resp.status_code >= 400:
            return False, 0, 1

        payload = resp.json() if resp.content else {}
        data = payload.get("data") if isinstance(payload, dict) else None
        deleted = 0
        if isinstance(data, dict):
            try:
                deleted = int(data.get("deleted", 0))
            except Exception:
                deleted = 0
        return True, deleted, 0

    async def cleanup_context_memories_list_delete(
        client: httpx.AsyncClient,
        user_id: str,
        agent_id: str,
    ) -> tuple[int, int]:
        deleted = 0
        failed = 0
        page_size = max(1, args.cleanup_page_size)
        max_rounds = max(1, args.cleanup_max_rounds)
        list_path = f"/api/v2/agents/{quote(agent_id, safe='')}/memories/list"

        for _ in range(max_rounds):
            if stop_event.is_set():
                break
            list_body = {"limit": page_size, "offset": 0}
            if config_payload:
                list_body["config"] = config_payload
            try:
                resp = await client.post(list_path, json=list_body, headers=headers, timeout=args.timeout)
            except Exception:
                failed += 1
                break

            if resp.status_code >= 400:
                failed += 1
                break

            payload = resp.json() if resp.content else {}
            rows = extract_agent_memories(payload)
            if not rows:
                break

            deleted_this_round = 0
            for row in rows:
                if stop_event.is_set():
                    break
                mem_id = row.get("memory_id") or row.get("id")
                if mem_id is None:
                    continue
                delete_body = {"user_id": user_id, "agent_id": agent_id}
                if config_payload:
                    delete_body["config"] = config_payload
                try:
                    del_resp = await client.post(
                        f"/api/v2/memories/delete/{quote(str(mem_id), safe='')}",
                        json=delete_body,
                        headers=headers,
                        timeout=args.timeout,
                    )
                    if del_resp.status_code < 400:
                        deleted += 1
                        deleted_this_round += 1
                    else:
                        failed += 1
                except Exception:
                    failed += 1

            if deleted_this_round == 0:
                break

        return deleted, failed

    max_connections = max(1, min(args.max_connections, args.concurrency))
    max_keepalive = max(1, min(args.max_keepalive, max_connections))
    limits = httpx.Limits(
        max_connections=max_connections,
        max_keepalive_connections=max_keepalive,
    )

    async def worker(
        client: httpx.AsyncClient,
        stop_at: float,
        user_id: str,
        agent_id: str,
    ) -> None:
        memory_ids: Deque[str] = deque()

        for _ in range(max(0, args.warmup_creates)):
            if now_ms() >= stop_at or stop_event.is_set():
                return
            await request_create(client, user_id, agent_id, memory_ids)
            if (
                warmup_search_ratio > 0
                and not stop_event.is_set()
                and now_ms() < stop_at
                and random.random() < warmup_search_ratio
            ):
                await request_search(client, user_id, agent_id)
            if args.think_time_ms > 0:
                await asyncio.sleep(args.think_time_ms / 1000.0)

        while now_ms() < stop_at and not stop_event.is_set():
            op = pick_op(args.mode)
            if op == "create":
                await request_create(client, user_id, agent_id, memory_ids)
            elif op == "search":
                await request_search(client, user_id, agent_id)
            elif op == "update":
                await request_update(client, user_id, agent_id, memory_ids)
            else:
                await request_delete(client, user_id, agent_id, memory_ids)
            if args.think_time_ms > 0:
                await asyncio.sleep(args.think_time_ms / 1000.0)

    worker_contexts = [
        (worker_user(idx, args.user_count), worker_agent(idx, args.agent_count))
        for idx in range(args.concurrency)
    ]
    unique_contexts = list(dict.fromkeys(worker_contexts))

    async with httpx.AsyncClient(base_url=args.base_url, limits=limits) as client:
        loop = asyncio.get_running_loop()
        signal_state = {"count": 0}
        worker_tasks: list[asyncio.Task[None]] = []
        force_cancel_task: Optional[asyncio.Task[None]] = None

        async def force_cancel_after_timeout() -> None:
            await asyncio.sleep(max(0.0, args.graceful_stop_timeout))
            pending = [task for task in worker_tasks if not task.done()]
            if pending:
                print(
                    f"[signal] graceful timeout reached ({args.graceful_stop_timeout}s), "
                    f"force cancelling {len(pending)} workers"
                )
                for task in pending:
                    task.cancel()

        def handle_signal(sig_name: str) -> None:
            nonlocal force_cancel_task
            signal_state["count"] += 1
            if signal_state["count"] == 1:
                print(f"[signal] {sig_name} received, stop scheduling new requests...")
                stop_event.set()
                if args.graceful_stop_timeout > 0:
                    force_cancel_task = asyncio.create_task(force_cancel_after_timeout())
            else:
                pending = [task for task in worker_tasks if not task.done()]
                print(
                    f"[signal] {sig_name} received again, force cancelling "
                    f"{len(pending)} workers now"
                )
                for task in pending:
                    task.cancel()

        registered_signals: list[signal.Signals] = []
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, handle_signal, sig.name)
                registered_signals.append(sig)
            except NotImplementedError:
                pass

        if args.cleanup_before_run:
            if args.cleanup_global:
                ok, deleted, failed = await cleanup_global_memories_delete_all(client)
                print(
                    "[cleanup-before-run] "
                    f"global={args.cleanup_global} strategy=delete-all "
                    f"deleted={deleted} failed={failed} ok={ok}"
                )
            else:
                total_deleted = 0
                total_failed = 0
                for user_id, agent_id in unique_contexts:
                    if stop_event.is_set():
                        break
                    if args.cleanup_strategy == "list-delete":
                        deleted, failed = await cleanup_context_memories_list_delete(client, user_id, agent_id)
                        total_deleted += deleted
                        total_failed += failed
                        continue

                    used_delete_all = args.cleanup_strategy in ("auto", "delete-all")
                    if used_delete_all:
                        ok, deleted, failed = await cleanup_context_memories_delete_all(client, user_id, agent_id)
                        total_deleted += deleted
                        total_failed += failed
                        if ok or args.cleanup_strategy == "delete-all":
                            continue

                    deleted, failed = await cleanup_context_memories_list_delete(client, user_id, agent_id)
                    total_deleted += deleted
                    total_failed += failed
                print(
                    f"[cleanup-before-run] contexts={len(unique_contexts)} "
                    f"global={args.cleanup_global} "
                    f"strategy={args.cleanup_strategy} "
                    f"deleted={total_deleted} failed={total_failed}"
                )

        if not stop_event.is_set():
            stop_at = now_ms() + args.duration * 1000.0
            worker_tasks = [
                asyncio.create_task(worker(client, stop_at, user_id, agent_id))
                for user_id, agent_id in worker_contexts
            ]
            await asyncio.gather(*worker_tasks, return_exceptions=True)

        if force_cancel_task and not force_cancel_task.done():
            force_cancel_task.cancel()
            await asyncio.gather(force_cancel_task, return_exceptions=True)

        for sig in registered_signals:
            loop.remove_signal_handler(sig)

    summary = {
        "mode": args.mode,
        "duration_s": args.duration,
        "concurrency": args.concurrency,
        "warmup_creates": args.warmup_creates,
        "warmup_search_ratio": warmup_search_ratio,
        "cleanup_before_run": args.cleanup_before_run,
        "cleanup_global": args.cleanup_global,
        "cleanup_strategy": args.cleanup_strategy,
        "metrics": metrics.summary(),
    }
    if args.output:
        write_json(args.output, summary)
    print(json.dumps(summary, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
import argparse
import asyncio
import json
import math
import random
import re
import signal
from collections import deque
from contextlib import AsyncExitStack
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


def parse_prometheus_samples(metrics_text: str, metric_name: str) -> list[tuple[dict[str, str], float]]:
    samples: list[tuple[dict[str, str], float]] = []
    for raw_line in metrics_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or not line.startswith(metric_name):
            continue

        labels: dict[str, str] = {}
        value_part = ""
        remainder = line[len(metric_name):]
        if remainder.startswith("{"):
            end_idx = remainder.find("}")
            if end_idx == -1:
                continue
            labels_part = remainder[1:end_idx]
            value_part = remainder[end_idx + 1 :].strip()
            for match in re.finditer(r'([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"', labels_part):
                labels[match.group(1)] = match.group(2)
        else:
            value_part = remainder.strip()

        try:
            value = float(value_part)
        except ValueError:
            continue
        samples.append((labels, value))
    return samples


def parse_duration_snapshot(metrics_text: str) -> dict[tuple[str, str], dict[str, float]]:
    snapshot: dict[tuple[str, str], dict[str, float]] = {}

    for labels, value in parse_prometheus_samples(metrics_text, "powermem_api_request_duration_seconds_count"):
        method = labels.get("method", "")
        endpoint = labels.get("endpoint", "")
        if not method or not endpoint:
            continue
        snapshot.setdefault((method, endpoint), {"count": 0.0, "sum": 0.0})["count"] = value

    for labels, value in parse_prometheus_samples(metrics_text, "powermem_api_request_duration_seconds_sum"):
        method = labels.get("method", "")
        endpoint = labels.get("endpoint", "")
        if not method or not endpoint:
            continue
        snapshot.setdefault((method, endpoint), {"count": 0.0, "sum": 0.0})["sum"] = value

    return snapshot


def diff_duration_snapshot(
    before: dict[tuple[str, str], dict[str, float]],
    after: dict[tuple[str, str], dict[str, float]],
) -> dict[tuple[str, str], dict[str, float]]:
    result: dict[tuple[str, str], dict[str, float]] = {}
    for key in set(before) | set(after):
        prev = before.get(key, {"count": 0.0, "sum": 0.0})
        curr = after.get(key, {"count": 0.0, "sum": 0.0})
        count_delta = curr.get("count", 0.0) - prev.get("count", 0.0)
        sum_delta = curr.get("sum", 0.0) - prev.get("sum", 0.0)
        if count_delta <= 0:
            continue
        if sum_delta < 0:
            sum_delta = 0.0
        result[key] = {"count": count_delta, "sum": sum_delta}
    return result


def endpoint_to_operation_by_version(method: str, endpoint: str, api_version: str) -> Optional[str]:
    prefix = f"/api/{api_version}/memories"
    if method == "POST" and endpoint == prefix:
        return "create"
    if method == "POST" and endpoint == f"{prefix}/search":
        return "search"

    if api_version == "v2":
        if method == "POST" and endpoint.startswith(f"{prefix}/update"):
            return "update"
        if method == "POST" and endpoint.startswith(f"{prefix}/delete"):
            return "delete"
    else:
        if method == "PUT" and endpoint.startswith(f"{prefix}/"):
            return "update"
        if method == "DELETE" and endpoint.startswith(f"{prefix}/"):
            return "delete"
    return None


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="")
    parser.add_argument(
        "--base-urls",
        default="",
        help="comma-separated powermem-server base URLs for routing, e.g. http://127.0.0.1:8000,http://127.0.0.1:8001",
    )
    parser.add_argument("--api-key", default="")
    parser.add_argument(
        "--api-version",
        choices=["v1", "v2"],
        default="v2",
        help="API version to benchmark",
    )
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
    parser.add_argument(
        "--route-strategy",
        choices=["sticky-worker", "round-robin", "random"],
        default="sticky-worker",
        help="backend routing strategy when multiple base URLs are provided",
    )
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
    parser.add_argument(
        "--collect-server-metrics",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="collect /api/v2/system/metrics before and after benchmark",
    )
    args = parser.parse_args()

    backend_urls: list[str] = []
    if args.base_urls:
        backend_urls.extend([item.strip() for item in args.base_urls.split(",") if item.strip()])
    if args.base_url.strip():
        backend_urls.append(args.base_url.strip())
    backend_urls = [item.rstrip("/") for item in backend_urls if item.rstrip("/")]
    backend_urls = list(dict.fromkeys(backend_urls))
    if not backend_urls:
        raise SystemExit("one of --base-url or --base-urls is required")

    api_prefix = f"/api/{args.api_version}"
    create_endpoint = f"{api_prefix}/memories"
    search_endpoint = f"{api_prefix}/memories/search"
    metrics_endpoint = f"{api_prefix}/system/metrics"

    if args.api_version == "v2":
        update_endpoint = f"{api_prefix}/memories/update"
        delete_endpoint = f"{api_prefix}/memories/delete"
        delete_all_endpoint = f"{api_prefix}/system/delete-all-memories"
    else:
        update_endpoint = f"{api_prefix}/memories"
        delete_endpoint = f"{api_prefix}/memories"
        delete_all_endpoint = f"{api_prefix}/system/delete-all-memories"

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
    if args.api_version == "v1" and config_payload is not None:
        print("[warn] --config-file is ignored for --api-version v1")
        config_payload = None

    headers = build_headers(args.api_key)
    metrics = Metrics()
    stop_event = asyncio.Event()
    warmup_search_ratio = max(0.0, min(1.0, args.warmup_search_ratio))
    backend_count = len(backend_urls)
    round_robin_cursor = 0

    def pick_backend(preferred_idx: int) -> int:
        nonlocal round_robin_cursor
        if backend_count == 1:
            return 0
        if args.route_strategy == "sticky-worker":
            return preferred_idx % backend_count
        if args.route_strategy == "random":
            return random.randrange(backend_count)
        idx = round_robin_cursor % backend_count
        round_robin_cursor += 1
        return idx

    def worker_backend(worker_idx: int) -> int:
        return worker_idx % backend_count

    async def request_create(
        client: httpx.AsyncClient,
        user_id: str,
        agent_id: str,
        memory_ids: Deque[tuple[str, int]],
        backend_idx: int,
    ) -> None:
        record = random.choice(records)
        body = {
            "content": record["content"],
            "metadata": record.get("metadata"),
            "user_id": user_id,
            "agent_id": agent_id,
            "infer": args.infer,
        }
        if config_payload and args.api_version == "v2":
            body["config"] = config_payload
        t0 = now_ms()
        try:
            resp = await client.post(create_endpoint, json=body, headers=headers, timeout=args.timeout)
            latency = now_ms() - t0
            ok = resp.status_code < 400
            payload = resp.json() if resp.content else {}
            if ok:
                mem_id = extract_memory_id(payload)
                if mem_id:
                    memory_ids.append((mem_id, backend_idx))
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
        if config_payload and args.api_version == "v2":
            body["config"] = config_payload
        t0 = now_ms()
        try:
            resp = await client.post(search_endpoint, json=body, headers=headers, timeout=args.timeout)
            latency = now_ms() - t0
            ok = resp.status_code < 400
            error_type = None if ok else f"status_{resp.status_code}"
            metrics.record("search", latency, ok, resp.status_code, error_type)
        except Exception as exc:
            latency = now_ms() - t0
            metrics.record("search", latency, False, None, classify_error(exc))

    async def request_update(
        clients: list[httpx.AsyncClient],
        user_id: str,
        agent_id: str,
        memory_ids: Deque[tuple[str, int]],
        default_backend_idx: int,
    ) -> None:
        mem_ref = memory_ids[0] if memory_ids else None
        if not mem_ref:
            backend_idx = pick_backend(default_backend_idx)
            await request_create(clients[backend_idx], user_id, agent_id, memory_ids, backend_idx)
            return
        mem_id, backend_idx = mem_ref
        body = {
            "content": random.choice(records)["content"],
            "metadata": {"updated": True},
            "user_id": user_id,
            "agent_id": agent_id,
        }
        if config_payload and args.api_version == "v2":
            body["config"] = config_payload
        t0 = now_ms()
        client = clients[backend_idx]
        try:
            if args.api_version == "v2":
                resp = await client.post(
                    f"{update_endpoint}/{mem_id}",
                    json=body,
                    headers=headers,
                    timeout=args.timeout,
                )
            else:
                resp = await client.put(
                    f"{update_endpoint}/{mem_id}",
                    params={"user_id": user_id, "agent_id": agent_id},
                    json=body,
                    headers=headers,
                    timeout=args.timeout,
                )
            latency = now_ms() - t0
            ok = resp.status_code < 400
            error_type = None if ok else f"status_{resp.status_code}"
            metrics.record("update", latency, ok, resp.status_code, error_type)
        except Exception as exc:
            latency = now_ms() - t0
            metrics.record("update", latency, False, None, classify_error(exc))

    async def request_delete(
        clients: list[httpx.AsyncClient],
        user_id: str,
        agent_id: str,
        memory_ids: Deque[tuple[str, int]],
        default_backend_idx: int,
    ) -> None:
        mem_ref = memory_ids.popleft() if memory_ids else None
        if not mem_ref:
            backend_idx = pick_backend(default_backend_idx)
            await request_create(clients[backend_idx], user_id, agent_id, memory_ids, backend_idx)
            return
        mem_id, backend_idx = mem_ref
        body = {"user_id": user_id, "agent_id": agent_id}
        if config_payload and args.api_version == "v2":
            body["config"] = config_payload
        t0 = now_ms()
        client = clients[backend_idx]
        try:
            if args.api_version == "v2":
                resp = await client.post(
                    f"{delete_endpoint}/{mem_id}",
                    json=body,
                    headers=headers,
                    timeout=args.timeout,
                )
            else:
                resp = await client.delete(
                    f"{delete_endpoint}/{mem_id}",
                    params={"user_id": user_id, "agent_id": agent_id},
                    headers=headers,
                    timeout=args.timeout,
                )
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
        try:
            if args.api_version == "v2":
                body = {"user_id": user_id, "agent_id": agent_id}
                if config_payload:
                    body["config"] = config_payload
                resp = await client.post(
                    delete_all_endpoint,
                    json=body,
                    headers=headers,
                    timeout=args.timeout,
                )
            else:
                resp = await client.delete(
                    delete_all_endpoint,
                    params={"user_id": user_id, "agent_id": agent_id},
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
        try:
            if args.api_version == "v2":
                body: dict = {}
                if config_payload:
                    body["config"] = config_payload
                resp = await client.post(
                    delete_all_endpoint,
                    json=body,
                    headers=headers,
                    timeout=args.timeout,
                )
            else:
                resp = await client.delete(
                    delete_all_endpoint,
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
        if args.api_version == "v2":
            list_path = f"{api_prefix}/agents/{quote(agent_id, safe='')}/memories/list"
        else:
            list_path = f"{api_prefix}/agents/{quote(agent_id, safe='')}/memories"

        for _ in range(max_rounds):
            if stop_event.is_set():
                break
            try:
                if args.api_version == "v2":
                    list_body = {"limit": page_size, "offset": 0}
                    if config_payload:
                        list_body["config"] = config_payload
                    resp = await client.post(list_path, json=list_body, headers=headers, timeout=args.timeout)
                else:
                    resp = await client.get(
                        list_path,
                        params={"limit": page_size, "offset": 0},
                        headers=headers,
                        timeout=args.timeout,
                    )
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
                try:
                    if args.api_version == "v2":
                        delete_body = {"user_id": user_id, "agent_id": agent_id}
                        if config_payload:
                            delete_body["config"] = config_payload
                        del_resp = await client.post(
                            f"{delete_endpoint}/{quote(str(mem_id), safe='')}",
                            json=delete_body,
                            headers=headers,
                            timeout=args.timeout,
                        )
                    else:
                        del_resp = await client.delete(
                            f"{delete_endpoint}/{quote(str(mem_id), safe='')}",
                            params={"user_id": user_id, "agent_id": agent_id},
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

    async def fetch_server_metrics_snapshot(client: httpx.AsyncClient) -> Optional[dict[tuple[str, str], dict[str, float]]]:
        try:
            response = await client.get(
                metrics_endpoint,
                headers=headers,
                timeout=min(args.timeout, 30.0),
            )
            if response.status_code >= 400:
                return None
            return parse_duration_snapshot(response.text)
        except Exception:
            return None

    max_connections = max(1, min(args.max_connections, args.concurrency))
    max_keepalive = max(1, min(args.max_keepalive, max_connections))
    per_client_connections = max(1, math.ceil(max_connections / backend_count))
    per_client_keepalive = max(1, min(math.ceil(max_keepalive / backend_count), per_client_connections))
    limits = httpx.Limits(
        max_connections=per_client_connections,
        max_keepalive_connections=per_client_keepalive,
    )

    async def worker(
        clients: list[httpx.AsyncClient],
        stop_at: float,
        user_id: str,
        agent_id: str,
        preferred_backend_idx: int,
    ) -> None:
        memory_ids: Deque[tuple[str, int]] = deque()

        for _ in range(max(0, args.warmup_creates)):
            if now_ms() >= stop_at or stop_event.is_set():
                return
            backend_idx = pick_backend(preferred_backend_idx)
            await request_create(clients[backend_idx], user_id, agent_id, memory_ids, backend_idx)
            if (
                warmup_search_ratio > 0
                and not stop_event.is_set()
                and now_ms() < stop_at
                and random.random() < warmup_search_ratio
            ):
                search_backend = pick_backend(preferred_backend_idx)
                await request_search(clients[search_backend], user_id, agent_id)
            if args.think_time_ms > 0:
                await asyncio.sleep(args.think_time_ms / 1000.0)

        while now_ms() < stop_at and not stop_event.is_set():
            op = pick_op(args.mode)
            if op == "create":
                backend_idx = pick_backend(preferred_backend_idx)
                await request_create(clients[backend_idx], user_id, agent_id, memory_ids, backend_idx)
            elif op == "search":
                search_backend = pick_backend(preferred_backend_idx)
                await request_search(clients[search_backend], user_id, agent_id)
            elif op == "update":
                await request_update(clients, user_id, agent_id, memory_ids, preferred_backend_idx)
            else:
                await request_delete(clients, user_id, agent_id, memory_ids, preferred_backend_idx)
            if args.think_time_ms > 0:
                await asyncio.sleep(args.think_time_ms / 1000.0)

    worker_contexts = [
        (
            idx,
            worker_user(idx, args.user_count),
            worker_agent(idx, args.agent_count),
            worker_backend(idx),
        )
        for idx in range(args.concurrency)
    ]
    unique_contexts = list(dict.fromkeys((user_id, agent_id) for _, user_id, agent_id, _ in worker_contexts))

    async with AsyncExitStack() as stack:
        clients: list[httpx.AsyncClient] = []
        for base_url in backend_urls:
            client = await stack.enter_async_context(httpx.AsyncClient(base_url=base_url, limits=limits))
            clients.append(client)
        metrics_snapshot_before: list[Optional[dict[tuple[str, str], dict[str, float]]]] = [None] * backend_count
        metrics_snapshot_after: list[Optional[dict[tuple[str, str], dict[str, float]]]] = [None] * backend_count
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
                ok = True
                deleted = 0
                failed = 0
                for client in clients:
                    c_ok, c_deleted, c_failed = await cleanup_global_memories_delete_all(client)
                    ok = ok and c_ok
                    deleted += c_deleted
                    failed += c_failed
                print(
                    "[cleanup-before-run] "
                    f"global={args.cleanup_global} strategy=delete-all backends={backend_count} "
                    f"deleted={deleted} failed={failed} ok={ok}"
                )
            else:
                total_deleted = 0
                total_failed = 0
                for user_id, agent_id in unique_contexts:
                    if stop_event.is_set():
                        break
                    if args.cleanup_strategy == "list-delete":
                        for client in clients:
                            deleted, failed = await cleanup_context_memories_list_delete(client, user_id, agent_id)
                            total_deleted += deleted
                            total_failed += failed
                        continue

                    used_delete_all = args.cleanup_strategy in ("auto", "delete-all")
                    if used_delete_all:
                        all_backends_ok = True
                        for client in clients:
                            ok, deleted, failed = await cleanup_context_memories_delete_all(client, user_id, agent_id)
                            total_deleted += deleted
                            total_failed += failed
                            all_backends_ok = all_backends_ok and ok
                        if all_backends_ok or args.cleanup_strategy == "delete-all":
                            continue

                    for client in clients:
                        deleted, failed = await cleanup_context_memories_list_delete(client, user_id, agent_id)
                        total_deleted += deleted
                        total_failed += failed
                print(
                    f"[cleanup-before-run] contexts={len(unique_contexts)} "
                    f"global={args.cleanup_global} "
                    f"strategy={args.cleanup_strategy} "
                    f"backends={backend_count} "
                    f"deleted={total_deleted} failed={total_failed}"
                )

        if args.collect_server_metrics:
            for idx, client in enumerate(clients):
                metrics_snapshot_before[idx] = await fetch_server_metrics_snapshot(client)
                if metrics_snapshot_before[idx] is None:
                    print(f"[metrics] failed to collect pre-run snapshot from backend[{idx}] {backend_urls[idx]}")

        if not stop_event.is_set():
            stop_at = now_ms() + args.duration * 1000.0
            worker_tasks = [
                asyncio.create_task(worker(clients, stop_at, user_id, agent_id, backend_idx))
                for _, user_id, agent_id, backend_idx in worker_contexts
            ]
            await asyncio.gather(*worker_tasks, return_exceptions=True)

        if args.collect_server_metrics:
            for idx, client in enumerate(clients):
                metrics_snapshot_after[idx] = await fetch_server_metrics_snapshot(client)
                if metrics_snapshot_after[idx] is None:
                    print(f"[metrics] failed to collect post-run snapshot from backend[{idx}] {backend_urls[idx]}")

        if force_cancel_task and not force_cancel_task.done():
            force_cancel_task.cancel()
            await asyncio.gather(force_cancel_task, return_exceptions=True)

        for sig in registered_signals:
            loop.remove_signal_handler(sig)

    summary = {
        "api_version": args.api_version,
        "mode": args.mode,
        "duration_s": args.duration,
        "concurrency": args.concurrency,
        "backend_count": backend_count,
        "base_urls": backend_urls,
        "route_strategy": args.route_strategy,
        "warmup_creates": args.warmup_creates,
        "warmup_search_ratio": warmup_search_ratio,
        "cleanup_before_run": args.cleanup_before_run,
        "cleanup_global": args.cleanup_global,
        "cleanup_strategy": args.cleanup_strategy,
        "metrics": metrics.summary(),
    }

    if args.collect_server_metrics:
        server_metrics_backends = []
        for idx, base_url in enumerate(backend_urls):
            before_snapshot = metrics_snapshot_before[idx]
            after_snapshot = metrics_snapshot_after[idx]
            if before_snapshot is None or after_snapshot is None:
                server_metrics_backends.append(
                    {
                        "base_url": base_url,
                        "collected": False,
                        "error": "failed to fetch pre/post /api/v2/system/metrics snapshot",
                    }
                )
                continue

            diff = diff_duration_snapshot(before_snapshot, after_snapshot)
            operation_rollup: dict[str, dict[str, float]] = {}
            for (method, endpoint), values in diff.items():
                operation = endpoint_to_operation_by_version(method, endpoint, args.api_version)
                if not operation:
                    continue
                row = operation_rollup.setdefault(operation, {"count": 0.0, "duration_s_sum": 0.0})
                row["count"] += values.get("count", 0.0)
                row["duration_s_sum"] += values.get("sum", 0.0)

            operations_summary = {}
            total_count = 0.0
            total_duration_s = 0.0
            for operation, row in operation_rollup.items():
                count = row["count"]
                duration_s_sum = row["duration_s_sum"]
                total_count += count
                total_duration_s += duration_s_sum
                operations_summary[operation] = {
                    "count": int(count),
                    "avg_ms": (duration_s_sum * 1000.0 / count) if count > 0 else None,
                    "qps": (count / args.duration) if args.duration > 0 else None,
                    "duration_s_sum": duration_s_sum,
                }

            server_metrics_backends.append(
                {
                    "base_url": base_url,
                    "collected": True,
                    "operations": operations_summary,
                    "total": {
                        "count": int(total_count),
                        "avg_ms": (total_duration_s * 1000.0 / total_count) if total_count > 0 else None,
                        "qps": (total_count / args.duration) if args.duration > 0 else None,
                        "duration_s_sum": total_duration_s,
                    },
                }
            )

        # Intentionally keep server metrics out of output summary for now.

    if args.output:
        write_json(args.output, summary)
    print(json.dumps(summary, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    asyncio.run(main())

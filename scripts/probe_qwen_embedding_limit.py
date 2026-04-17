#!/usr/bin/env python3
import argparse
import json
import os
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Tuple


def load_env_file(path: str) -> Dict[str, str]:
    values: Dict[str, str] = {}
    p = Path(path)
    if not p.exists():
        return values

    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def pick_value(name: str, env_file_values: Dict[str, str], default: str = "") -> str:
    return os.getenv(name) or env_file_values.get(name, default)


def percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    idx = int((len(sorted_values) - 1) * p)
    return round(sorted_values[idx], 2)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Probe Qwen/DashScope embedding rate-limit and latency under concurrency.",
    )
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--api-key", default="")
    parser.add_argument("--model", default="")
    parser.add_argument("--dims", type=int, default=0)
    parser.add_argument("--total", type=int, default=60)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--text", default="powermem benchmark embedding probe text")
    parser.add_argument("--text-type", choices=["document", "query"], default="document")
    parser.add_argument("--output", default="")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    env_file_values = load_env_file(args.env_file)

    api_key = (
        args.api_key
        or pick_value("EMBEDDING_API_KEY", env_file_values)
        or pick_value("QWEN_API_KEY", env_file_values)
        or pick_value("DASHSCOPE_API_KEY", env_file_values)
    )
    if not api_key:
        raise SystemExit("missing API key: set --api-key or EMBEDDING_API_KEY/QWEN_API_KEY/DASHSCOPE_API_KEY")

    model = args.model or pick_value("EMBEDDING_MODEL", env_file_values, "text-embedding-v4")
    dims_text = str(args.dims) if args.dims > 0 else pick_value("EMBEDDING_DIMS", env_file_values, "1536")
    dims = int(dims_text)

    try:
        from dashscope import TextEmbedding
    except ImportError as exc:
        raise SystemExit("dashscope sdk not installed, run: pip install dashscope") from exc

    status_counter: Counter[str] = Counter()
    message_counter: Counter[str] = Counter()
    latencies: list[float] = []

    def one_request(i: int) -> Tuple[str, str, float]:
        t0 = time.time()
        try:
            resp = TextEmbedding.call(
                api_key=api_key,
                model=model,
                input=f"{args.text} #{i}",
                dimension=dims,
                text_type=args.text_type,
            )
            dt = (time.time() - t0) * 1000
            status = str(getattr(resp, "status_code", "unknown"))
            msg = (getattr(resp, "message", "") or "").strip()
            return status, msg[:200], dt
        except Exception as exc:  # pragma: no cover - runtime probe script
            dt = (time.time() - t0) * 1000
            return "EXC", str(exc)[:200], dt

    started_at = time.time()
    with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
        futures = [pool.submit(one_request, i) for i in range(max(1, args.total))]
        for fut in as_completed(futures):
            status, message, latency_ms = fut.result()
            status_counter[status] += 1
            if message:
                message_counter[message] += 1
            latencies.append(latency_ms)

    latencies.sort()
    elapsed = round(time.time() - started_at, 2)
    total = len(latencies)
    success = status_counter.get("200", 0)
    errors = total - success

    summary = {
        "probe_total": total,
        "probe_concurrency": args.concurrency,
        "model": model,
        "dims": dims,
        "text_type": args.text_type,
        "elapsed_s": elapsed,
        "metrics": {
            "success": success,
            "errors": errors,
            "error_rate": round(errors / total, 6) if total > 0 else 0.0,
            "status_counts": dict(status_counter),
            "latency_ms": {
                "p50": percentile(latencies, 0.50),
                "p90": percentile(latencies, 0.90),
                "p95": percentile(latencies, 0.95),
                "p99": percentile(latencies, 0.99),
            },
            "top_messages": message_counter.most_common(5),
        },
    }

    text = json.dumps(summary, indent=2, ensure_ascii=True)
    print(text)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()

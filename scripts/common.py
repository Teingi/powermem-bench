import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional


def load_jsonl(path: str) -> List[dict]:
    items = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            items.append(json.loads(line))
    return items


def load_lines(path: str) -> List[str]:
    items = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                items.append(line)
    return items


def percentile(values: List[float], pct: float) -> Optional[float]:
    if not values:
        return None
    if pct <= 0:
        return min(values)
    if pct >= 100:
        return max(values)
    values_sorted = sorted(values)
    k = (len(values_sorted) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(values_sorted) - 1)
    if f == c:
        return values_sorted[f]
    return values_sorted[f] + (values_sorted[c] - values_sorted[f]) * (k - f)


@dataclass
class Metrics:
    latencies_ms: Dict[str, List[float]] = field(default_factory=lambda: defaultdict(list))
    counts: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    errors: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    status_counts: Dict[str, Dict[str, int]] = field(default_factory=lambda: defaultdict(lambda: defaultdict(int)))
    error_types: Dict[str, Dict[str, int]] = field(default_factory=lambda: defaultdict(lambda: defaultdict(int)))

    def record(
        self,
        op: str,
        latency_ms: float,
        ok: bool,
        status: Optional[int] = None,
        error_type: Optional[str] = None,
    ) -> None:
        self.latencies_ms[op].append(latency_ms)
        self.counts[op] += 1
        if not ok:
            self.errors[op] += 1
            if error_type:
                self.error_types[op][error_type] += 1
        if status is not None:
            self.status_counts[op][str(status)] += 1

    def summary(self) -> dict:
        result = {}
        for op, latencies in self.latencies_ms.items():
            p50 = percentile(latencies, 50)
            p90 = percentile(latencies, 90)
            p95 = percentile(latencies, 95)
            p99 = percentile(latencies, 99)
            count = self.counts.get(op, 0)
            err = self.errors.get(op, 0)
            result[op] = {
                "count": count,
                "errors": err,
                "error_rate": (err / count) if count else 0.0,
                "p50_ms": p50,
                "p90_ms": p90,
                "p95_ms": p95,
                "p99_ms": p99,
                "status": dict(self.status_counts.get(op, {})),
                "error_types": dict(self.error_types.get(op, {})),
            }
        return result


def write_json(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=True)


def now_ms() -> float:
    return time.monotonic() * 1000.0

#!/usr/bin/env python3
import argparse
import json
import random
from pathlib import Path


WORDS = [
    "user", "prefers", "dark", "mode", "likes", "coffee", "meeting", "project",
    "deadline", "budget", "travel", "tokyo", "backend", "frontend", "database",
    "python", "rust", "editor", "cursor", "neovim", "cat", "health", "allergy",
    "schedule", "report", "analysis", "design", "planning", "system", "memory",
    "search", "vector", "fulltext", "agent", "profile", "experience", "review",
]

TOPICS = [
    "preference", "profile", "project", "schedule", "tooling",
    "health", "org", "travel", "experience", "note",
]


def random_text(min_words: int, max_words: int) -> str:
    size = random.randint(min_words, max_words)
    return " ".join(random.choice(WORDS) for _ in range(size))


def build_record(idx: int, min_words: int, max_words: int) -> dict:
    content = random_text(min_words, max_words)
    metadata = {
        "topic": random.choice(TOPICS),
        "source": "mock",
        "index": idx,
        "len": len(content),
    }
    return {"content": content, "metadata": metadata}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=2000)
    parser.add_argument("--min-words", type=int, default=8)
    parser.add_argument("--max-words", type=int, default=40)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out-memories", default="data/mock_memories.jsonl")
    parser.add_argument("--out-queries", default="data/mock_queries.txt")
    args = parser.parse_args()

    random.seed(args.seed)

    out_memories = Path(args.out_memories)
    out_memories.parent.mkdir(parents=True, exist_ok=True)
    with out_memories.open("w", encoding="utf-8") as f:
        for i in range(args.count):
            record = build_record(i, args.min_words, args.max_words)
            f.write(json.dumps(record, ensure_ascii=True) + "\n")

    out_queries = Path(args.out_queries)
    out_queries.parent.mkdir(parents=True, exist_ok=True)
    with out_queries.open("w", encoding="utf-8") as f:
        for topic in TOPICS:
            f.write(topic + "\n")
        for word in WORDS[:20]:
            f.write(word + "\n")


if __name__ == "__main__":
    main()

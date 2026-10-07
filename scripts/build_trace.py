#!/usr/bin/env python3
"""把各服务的 spans_*.jsonl 按 trace_id 聚合，按 parent_span_id 拼树，打印每跳耗时。

用法：python3 build_trace.py <目录或文件...>
- 传目录：聚合该目录下所有 spans_*.jsonl（各服务独立文件）。
- 传文件/glob：直接读。

拼树只靠 parent_span_id（跨进程单调钟不可比）；wall_us 仅用于同一 trace 内粗略排序。
"""
import glob
import json
import os
import sys
from collections import defaultdict


def load_spans(paths):
    spans = []
    for p in paths:
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    spans.append(json.loads(line))
    return spans


def print_tree(node, children, indent):
    dur = int(node.get("end_us", 0)) - int(node.get("start_us", 0))
    label = "%s.%s" % (node.get("service", "?"), node.get("method", "?"))
    print("%s%s (%dus, %s)" % ("  " * indent, label, dur, node.get("status", "?")))
    kids = sorted(children.get(node["span_id"], []),
                  key=lambda x: int(x.get("wall_us", 0)))
    for c in kids:
        print_tree(c, children, indent + 1)


def main():
    if len(sys.argv) < 2:
        print("usage: build_trace.py <dir|spans_*.jsonl> [more ...]", file=sys.stderr)
        sys.exit(1)

    paths = []
    for a in sys.argv[1:]:
        if os.path.isdir(a):
            paths.extend(sorted(glob.glob(os.path.join(a, "spans_*.jsonl"))))
        elif "*" in a or "?" in a:
            paths.extend(glob.glob(a))
        else:
            paths.append(a)

    spans = load_spans(paths)
    by_trace = defaultdict(list)
    for s in spans:
        by_trace[s.get("trace_id", "")].append(s)

    for tid, group in by_trace.items():
        nodes = {s["span_id"]: s for s in group}
        children = defaultdict(list)
        roots = []
        for s in group:
            pid = s.get("parent_span_id") or ""
            if pid and pid in nodes:
                children[pid].append(s)
            else:
                roots.append(s)
        print("=== trace %s (%d spans) ===" % (tid, len(group)))
        for r in sorted(roots, key=lambda x: int(x.get("wall_us", 0))):
            print_tree(r, children, 0)
        print()


if __name__ == "__main__":
    main()

#!/usr/bin/env bash
# tests/run_all.sh — 一键：编译 + 运行全部测试（单元 → 业务 → 多节点 → 压测）。
# 前置：etcd / Redis 7.x / MySQL 需就绪（业务/多节点测试需要）。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "==================== 单元测试 ===================="
"$ROOT/tests/unit/run.sh"

echo "==================== 业务测试 ===================="
"$ROOT/tests/business/run.sh"

echo "==================== 多节点测试 ===================="
"$ROOT/tests/multi-node/run.sh"

echo "==================== 压测基准 ===================="
"$ROOT/tests/bench/run.sh"

echo
echo "全部完成。"

#!/usr/bin/env bash
# tests/unit/run.sh — 编译并运行框架/单元测试（不含 IM 业务/多节点 E2E）。
# 多数无外部依赖；少数（*etcd* / async_lb / gateway_tls）需要 etcd 在跑。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

BUILD_DIR="${BUILD_DIR:-build}"
JOBS="$(nproc 2>/dev/null || echo 4)"

echo "==> 编译"
cmake --build "$BUILD_DIR" -j"$JOBS"

echo "==> 运行单元测试（排除 3 个 IM E2E）"
ctest --test-dir "$BUILD_DIR" --output-on-failure \
  -E 'test_im_e2e|test_im_business|test_im_multi_node'

echo "==> 完成"

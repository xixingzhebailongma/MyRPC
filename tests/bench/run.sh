#!/usr/bin/env bash
# tests/bench/run.sh — 一键编译并运行 AsyncRpcClient 压测 + 相关功能回归。
#
# 用法：
#   ./tests/bench/run.sh                  # 默认：Release 构建 + 3 档压测 + 功能回归
#   DURATION=5 ./tests/bench/run.sh       # 自定义每档压测时长（秒）
#   RUN_CTEST=0 ./tests/bench/run.sh      # 只跑压测，跳过功能回归
#
# 可用环境变量：
#   BUILD_DIR    构建目录（默认 build-bench，独立于日常 Debug 的 build/）
#   BUILD_TYPE   构建类型（默认 Release；压测务必 Release，Debug 数字无参考意义）
#   DURATION     每档压测时长，秒（默认 3）
#   RUN_CTEST    是否跑功能回归（默认 1）

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

BUILD_DIR="${BUILD_DIR:-build-bench}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
DURATION="${DURATION:-3}"
RUN_CTEST="${RUN_CTEST:-1}"
JOBS="$(nproc 2>/dev/null || echo 4)"

# 1) 配置 + 编译 bench 目标
if [ ! -f "$BUILD_DIR/CMakeCache.txt" ]; then
  echo "==> cmake 配置 ($BUILD_DIR, $BUILD_TYPE)"
  cmake -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE="$BUILD_TYPE"
else
  echo "==> 复用已有构建目录 $BUILD_DIR（类型 $(grep '^CMAKE_BUILD_TYPE' "$BUILD_DIR/CMakeCache.txt" | cut -d= -f2)）"
fi

echo "==> 编译 bench_async_rpc"
cmake --build "$BUILD_DIR" --target bench_async_rpc -j"$JOBS"

BIN="$BUILD_DIR/tests/bench/bench_async_rpc"

# 2) 三档压测（与 bench/README.md 实测配置一致）
run_bench() { # label servers concurrency threads payload
  local label="$1"
  echo
  echo "================ $label ================"
  "$BIN" --servers="$2" --concurrency="$3" --threads="$4" \
         --duration="$DURATION" --payload="$5"
}

run_bench "单并发（纯延迟下限）" 4 1  4  64
run_bench "8 并发"              4 8  4  64
run_bench "32 并发"             8 32 8  64

# 3) 功能回归：心跳淘汰 / 连接池回收 / 熔断恢复
if [ "$RUN_CTEST" = "1" ]; then
  echo
  echo "================ 功能回归 ================"
  cmake --build "$BUILD_DIR" \
        --target test_rpc_channel_heartbeat \
                 test_rpc_channel_pool_reap \
                 test_rpc_channel_circuit_breaker -j"$JOBS"
  ctest --test-dir "$BUILD_DIR" --output-on-failure \
        -R 'test_rpc_channel_heartbeat|test_rpc_channel_pool_reap|test_rpc_channel_circuit_breaker'
fi

echo
echo "全部完成。"

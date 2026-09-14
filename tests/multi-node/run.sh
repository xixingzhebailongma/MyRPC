#!/usr/bin/env bash
# tests/multi-node/run.sh — 一键编译并运行 IM 多节点一致性测试（2 个 IM 节点 + 去 pin）。
#
# 前置：etcd(:2379) / Redis 7.x(:6379) / MySQL(:3306, 库 myrpc_im) 已在跑。
# 注意：不要先跑 scripts/start_im_services.sh（会抢 etcd 里同名的 "ImService"）。
#
# 用法：
#   ./tests/multi-node/run.sh
#   KEEP_BIZ_LOGS=1 ./tests/multi-node/run.sh   # 保留 /tmp/myrpc_multi_* 日志排查
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

BUILD_DIR="${BUILD_DIR:-build}"
JOBS="$(nproc 2>/dev/null || echo 4)"

check_tcp() {
  local host="$1" port="$2" name="$3"
  if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
    echo "  ✅ $name ($host:$port) 就绪"
  else
    echo "  ❌ $name ($host:$port) 未监听"
    return 1
  fi
}

echo "==> 前置检查"
fail=0
check_tcp 127.0.0.1 2379 etcd || fail=1
check_tcp 127.0.0.1 6379 redis || fail=1
check_tcp 127.0.0.1 3306 mysql || fail=1
[ "$fail" = "0" ] || { echo "请先起 etcd / Redis 7.x / MySQL（库 myrpc_im）。"; exit 2; }

if command -v redis-cli >/dev/null 2>&1; then
  rv="$(redis-cli -p 6379 INFO server 2>/dev/null | sed -n 's/^redis_version://p' | tr -d '[:space:]')"
  if [ -n "$rv" ] && [ "${rv%%.*}" -lt 7 ]; then
    echo "  ❌ Redis 版本 $rv < 7.x，请换 7.x"
    exit 2
  fi
fi

# 清掉上次运行遗留的投递流（含消费组），避免跨次串扰导致偶发「收不到推送」。
redis-cli -p 6379 DEL im:delivery >/dev/null 2>&1 || true

if [ ! -f "$BUILD_DIR/CMakeCache.txt" ]; then
  cmake -B "$BUILD_DIR"
fi

echo "==> 编译 + 运行 test_im_multi_node"
cmake --build "$BUILD_DIR" --target test_im_multi_node -j"$JOBS"
"$BUILD_DIR/tests/test_im_multi_node"

echo "==> 完成"

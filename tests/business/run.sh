#!/usr/bin/env bash
# tests/business/run.sh — 一键编译并运行 IM 业务端到端测试。
#   默认跑 test_im_business（完整业务场景）；E2E=1 时顺带跑 test_im_e2e（happy path）。
#
# 前置：etcd(:2379) / Redis 7.x(:6379) / MySQL(:3306, 库 myrpc_im) 已在跑。
# 注意：不要先跑 scripts/start_im_services.sh —— 测试内部自己 fork 服务进程，
#       先起服务会往 etcd 注册同名 "ImService" 与测试冲突。
#
# 用法：
#   ./tests/business/run.sh
#   E2E=1 ./tests/business/run.sh
#   KEEP_BIZ_LOGS=1 ./tests/business/run.sh   # 保留 /tmp/myrpc_biz_* 日志排查
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

BUILD_DIR="${BUILD_DIR:-build}"
E2E="${E2E:-0}"
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

echo "==> 编译 + 运行 test_im_business"
cmake --build "$BUILD_DIR" --target test_im_business -j"$JOBS"
"$BUILD_DIR/tests/test_im_business"

if [ "$E2E" = "1" ]; then
  echo "==> 编译 + 运行 test_im_e2e（happy path）"
  cmake --build "$BUILD_DIR" --target test_im_e2e -j"$JOBS"
  "$BUILD_DIR/tests/test_im_e2e"
fi

echo "==> 完成"

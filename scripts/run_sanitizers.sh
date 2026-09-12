#!/usr/bin/env bash
# Sanitizer 回归：独立 build 目录 + 跑全量 CTest。
# 用法：./scripts/run_sanitizers.sh <asan|tsan|ubsan> [ctest 额外参数]
#
# 注意：
#   - ASAN / TSAN 互斥，脚本每次只启用一个。
#   - TSAN 在 WSL2 上有地址映射问题：若检测到 WSL 且存在 setarch，会用
#     `setarch $(uname -m) -R` 关闭 ASLR 再跑；真 Linux/CI 上无需此绕过。
set -euo pipefail

SAN="${1:?usage: run_sanitizers.sh <asan|tsan|ubsan> [ctest args...]}"
shift || true

case "$SAN" in
  asan)   FLAG="-DENABLE_ASAN=ON";;
  tsan)   FLAG="-DENABLE_TSAN=ON";;
  ubsan)  FLAG="-DENABLE_UBSAN=ON";;
  *) echo "未知 sanitizer: $SAN（可选 asan/tsan/ubsan）"; exit 2;;
esac

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD="$ROOT/build-$SAN"

echo "==> 配置 $SAN 到 $BUILD"
cmake -S "$ROOT" -B "$BUILD" "$FLAG" -DCMAKE_BUILD_TYPE=Debug

echo "==> 编译"
cmake --build "$BUILD" -j"$(nproc)"

echo "==> 运行 CTest"
cd "$BUILD"

if [ "$SAN" = "tsan" ] && grep -qi microsoft /proc/version && command -v setarch >/dev/null 2>&1; then
  echo "==> 检测到 WSL，使用 setarch -R 关闭 ASLR 绕过 TSAN 地址映射问题"
  # setarch -R 关闭地址随机化后，TSAN 才能稳定映射内存。
  exec setarch "$(uname -m)" -R ctest --test-dir "$BUILD" --output-on-failure "$@"
fi

ctest --test-dir "$BUILD" --output-on-failure "$@"

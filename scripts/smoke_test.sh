#!/usr/bin/env bash
# scripts/smoke_test.sh — 容器化端到端 smoke test（默认 profile：etcd/redis/mysql/route）。
#
# auth/im/gateway/deliver 在 "full" profile 里，因为 fresh 构建的 etcd-cpp-apiv3 会让
# auth 启动即 SIGSEGV（known issue，见 docs/containerization.md）。所以默认只验证
# 基础设施 + route：route 能起来、连上 etcd/redis、注册进 etcd。
#
# 用法：./scripts/smoke_test.sh          # 默认 profile（基础设施 + route）
#       PROFILE=full ./scripts/smoke_test.sh   # full profile（含 auth，预期崩溃）
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
PROFILE="${PROFILE:-}"

PROFILE_ARGS=()
if [ -n "$PROFILE" ]; then
  PROFILE_ARGS=(--profile "$PROFILE")
fi

cleanup() {
  echo "==> 清理：docker compose down -v"
  docker compose "${PROFILE_ARGS[@]}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

# 1. 构建 + 启动
echo "==> docker compose ${PROFILE_ARGS[*]} up -d --build"
docker compose "${PROFILE_ARGS[@]}" up -d --build

# 2. 等待 route healthy（超时 180s）
echo "==> 等待 route healthy"
deadline=$((SECONDS + 180))
cid=""
while (( SECONDS < deadline )); do
  cid=$(docker compose ps -q route 2>/dev/null || true)
  if [ -n "$cid" ] && [ "$(docker inspect --format '{{.State.Health.Status}}' "$cid" 2>/dev/null)" = "healthy" ]; then
    echo "==> route healthy"
    break
  fi
  sleep 5
done

if [ -z "$cid" ] || [ "$(docker inspect --format '{{.State.Health.Status}}' "$cid" 2>/dev/null)" != "healthy" ]; then
  echo "!! route 未在超时内 healthy" >&2
  docker compose "${PROFILE_ARGS[@]}" ps
  docker compose "${PROFILE_ARGS[@]}" logs --no-color
  exit 1
fi

# 3. 验证 route 已注册进 etcd（真实链路：route → etcd 注册）
echo "==> 验证 route 已注册进 etcd"
reg=$(docker compose exec -T etcd etcdctl get --prefix /myrpc/services/RouteService/ 2>/dev/null | grep -c 'RouteService' || true)
if [ "${reg:-0}" -lt 1 ]; then
  echo "!! route 未在 etcd 注册" >&2
  docker compose "${PROFILE_ARGS[@]}" logs route --no-color
  exit 1
fi

echo "==> smoke test 通过（基础设施 + route 正常；auth 为 known issue）"

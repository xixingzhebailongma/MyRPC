#!/usr/bin/env bash
# scripts/smoke_test.sh — 容器化端到端 smoke test。
# 一条命令：构建镜像 → 拉起五个服务 + etcd/redis/mysql → 等 healthy → 跑真实链路
# (register/login/send) → 失败自动 dump compose logs → 非零退出 → 清理。
# 用法：./scripts/smoke_test.sh
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

SERVICES=(route auth im gateway deliver)
TOTAL=${#SERVICES[@]}

cleanup() {
  echo "==> 清理：docker compose down -v"
  docker compose down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

# 1. 构建 + 启动
echo "==> docker compose up -d --build"
docker compose up -d --build

# 2. 等待五个服务全部 healthy（超时 300s）
echo "==> 等待 ${TOTAL} 个服务 healthy"
deadline=$((SECONDS + 300))
healthy=0
while (( SECONDS < deadline )); do
  healthy=0
  for svc in "${SERVICES[@]}"; do
    cid=$(docker compose ps -q "$svc" 2>/dev/null || true)
    [ -n "$cid" ] || continue
    if [ "$(docker inspect --format '{{.State.Health.Status}}' "$cid" 2>/dev/null)" = "healthy" ]; then
      healthy=$((healthy + 1))
    fi
  done
  if (( healthy >= TOTAL )); then
    echo "==> ${TOTAL} 个服务全部 healthy"
    break
  fi
  sleep 5
done

if (( healthy < TOTAL )); then
  echo "!! 服务未在超时内全部 healthy（healthy=${healthy}/${TOTAL}）" >&2
  docker compose ps
  docker compose logs --no-color
  exit 1
fi

# 3. 构建 im_test_client 镜像（build 阶段与五个服务共享缓存，很快）
echo "==> 构建 im_test_client 镜像"
docker build --build-arg SERVICE_NAME=im_test_client -t myrpc-client:test -f docker/Dockerfile . >/dev/null

# 4. 跑真实链路：register ta1/tb1 → login ta1 → send tb1
echo "==> 跑真实链路 smoke（register/login/send）"
output=$(docker run --rm --network myrpc-net --entrypoint sh myrpc-client:test -c '
  /usr/local/bin/app register ta1 pass123 --server.ip=gateway --server.port=9000 &&
  /usr/local/bin/app register tb1 pass123 --server.ip=gateway --server.port=9000 &&
  /usr/local/bin/app login ta1 pass123 --server.ip=gateway --server.port=9000 &&
  /usr/local/bin/app send tb1 "hello-smoke" --server.ip=gateway --server.port=9000
')

echo "$output"

# 5. 校验 send 成功
if ! echo "$output" | grep -qE 'send: success=(1|true)'; then
  echo "!! smoke test 失败：send 未成功" >&2
  docker compose logs --no-color
  exit 1
fi

echo "==> smoke test 通过"

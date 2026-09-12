#!/usr/bin/env bash
# 启动 IM 服务进程（依赖 etcd/redis/mysql 已就绪）。
# 用法：./scripts/start_im_services.sh [logdir]
# 服务写入 logdir 下的 <name>.log（Logger 文件日志），stdout/stderr 重定向到 <name>.console.log。
set -euo pipefail

BIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../build/apps/im" && pwd)"
LOG_DIR="${1:-/tmp/e2e}"
# Gateway↔IM 共享的 HMAC 密钥，两侧必须一致。
SECRET="${2:-devsecret123}"
mkdir -p "$LOG_DIR"

start() {
  local name="$1"; shift
  nohup "$BIN_DIR/$name" "$@" > "$LOG_DIR/$name.console.log" 2>&1 &
  echo $! > "$LOG_DIR/$name.pid"
  echo "started $name pid=$(cat "$LOG_DIR/$name.pid")"
}

# 启动顺序：route/auth（先注册） → im（依赖 route/auth） → gateway（依赖 im/auth） → deliver（投递 worker）
start route_server
sleep 1
start auth_server
sleep 1
start im_server --shared.secret="$SECRET"
sleep 1
start gateway_server --shared.secret="$SECRET"
sleep 1
start deliver_server

echo "all started; logs in $LOG_DIR"

#!/usr/bin/env bash
# MyRPC 依赖安装：版本钉死，本地（Ubuntu 22.04）与 CI（ubuntu-22.04 runner）共用。
#
# 版本策略：
#   - ABI 敏感的 apt 包用 `=` 精确钉住（数值 = 开发机 dpkg 输出，jammy 与 CI 同源）。
#   - etcd-cpp-apiv3 无可靠 apt 包 → 源码编译，钉 git tag v0.15.3。
#   - etcd 二进制 → 下载官方 release，钉 v3.5.16（给 test_watch_incremental 自 spawn 用）。
# 其余（gRPC/cpprestsdk/boost/zlib）是传递依赖，靠「锁 OS」钉住，不再逐一 `=`。
set -euo pipefail

# ---- 版本常量 ----
PROTOBUF_VER="3.12.4-1ubuntu7.22.04.6"
HIREDIS_VER="0.14.1-2"
MYSQLCPPCONN_VER="1.1.12-4ubuntu2"   # 注意是 1.1.x legacy API，不是 8.x X DevAPI
# libmysqlclient-dev / libssl-dev 不 `=` 钉：它们随 jammy 安全更新频繁 bump（runner 的 apt
# 快照往往比本机旧一两个补丁），且 ABI 稳定、补丁版本对构建无影响，靠「锁 ubuntu-22.04」钉住即可。
GRPC_PLUGIN_VER="1.30.2-3build6"
ETCD_CPP_API_TAG="v0.15.3"
ETCD_VERSION="v3.5.16"

# ---- 1. apt 依赖 ----
sudo apt-get update
sudo apt-get install -y --no-install-recommends \
  build-essential cmake git pkg-config curl ca-certificates \
  protobuf-compiler="${PROTOBUF_VER}" \
  libprotobuf-dev="${PROTOBUF_VER}" \
  libgrpc-dev libgrpc++-dev protobuf-compiler-grpc="${GRPC_PLUGIN_VER}" \
  libhiredis-dev="${HIREDIS_VER}" \
  libmysqlclient-dev \
  libmysqlcppconn-dev="${MYSQLCPPCONN_VER}" \
  libssl-dev \
  zlib1g-dev libcpprest-dev libboost-all-dev \
  redis-tools default-mysql-client

# ---- 2. etcd-cpp-apiv3（源码编译，钉 v0.15.3）----
ETCD_CPP_API_DIR="/tmp/myrpc-ci-deps/etcd-cpp-apiv3"
if [ ! -f "${ETCD_CPP_API_DIR}/.built" ]; then
  rm -rf "${ETCD_CPP_API_DIR}"
  git clone --depth 1 --branch "${ETCD_CPP_API_TAG}" \
    https://github.com/etcd-cpp-apiv3/etcd-cpp-apiv3.git "${ETCD_CPP_API_DIR}"
  cmake -S "${ETCD_CPP_API_DIR}" -B "${ETCD_CPP_API_DIR}/build" \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_ETCD_TESTS=OFF \
    -DETCD_W_STRICT=OFF
  cmake --build "${ETCD_CPP_API_DIR}/build" -j"$(nproc)"
  sudo cmake --install "${ETCD_CPP_API_DIR}/build"
  touch "${ETCD_CPP_API_DIR}/.built"
fi

# ---- 3. etcd 二进制（钉版本，watch 测试自 spawn 单节点）----
ETCD_BIN_DIR="/tmp/myrpc-ci-etcd"
if [ ! -x "${ETCD_BIN_DIR}/etcd" ]; then
  mkdir -p "${ETCD_BIN_DIR}"
  curl -sSL "https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-linux-amd64.tar.gz" \
    -o "${ETCD_BIN_DIR}/etcd.tar.gz"
  tar -xzf "${ETCD_BIN_DIR}/etcd.tar.gz" -C "${ETCD_BIN_DIR}" --strip-components=1
  rm -f "${ETCD_BIN_DIR}/etcd.tar.gz"
fi

echo "依赖安装完成：etcd/etcdctl → ${ETCD_BIN_DIR}"

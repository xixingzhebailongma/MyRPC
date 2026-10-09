# 统一 Dockerfile：构建五个 IM 服务 + im_test_client（用 --build-arg SERVICE_NAME 区分）。
# 用法：docker build --build-arg SERVICE_NAME=route_server -t myrpc/route_server -f docker/Dockerfile .
# 关键点：build 阶段一次编出全部六个二进制（--target 列表固定），该层在六个镜像间共享缓存，
#        只有 runtime 阶段按 SERVICE_NAME 拷贝不同二进制。

FROM ubuntu:22.04 AS build
ARG SERVICE_NAME
ENV DEBIAN_FRONTEND=noninteractive

# sudo 供 install_deps.sh 使用（Docker 内是 root，但脚本里写死了 sudo）
RUN apt-get update && apt-get install -y --no-install-recommends sudo ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 依赖（版本钉死，复用 install_deps.sh；先拷整个 scripts/ 再跑，保持 patches/ 相对结构，
# 充分利用层缓存）
COPY scripts/ /tmp/scripts/
RUN bash /tmp/scripts/install_deps.sh

# 源码 + 编译（一次编出六个二进制；此 RUN 不引用 SERVICE_NAME，跨镜像共享缓存）
WORKDIR /src
COPY . .
RUN cmake -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build -j"$(nproc)" \
       --target route_server auth_server im_server gateway_server deliver_server im_test_client

# ---- runtime：只装运行时库 + 拷二进制 + 非 root ----
FROM ubuntu:22.04 AS runtime
ARG SERVICE_NAME
ENV DEBIAN_FRONTEND=noninteractive

# 运行时库（非 -dev；abseil/brotli/boost 等作为 grpc/cpprest 的依赖自动带入）
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgrpc++1 libcpprest2.10 libprotobuf23 libhiredis0.14 \
        libmysqlclient21 libmysqlcppconn7v5 libssl3 zlib1g libc-ares2 \
        curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 二进制 + 源码构建的 libetcd-cpp-api.so（apt 里没有）
COPY --from=build "/src/build/apps/im/${SERVICE_NAME}" /usr/local/bin/app
COPY --from=build /usr/local/lib/libetcd-cpp-api.so* /usr/local/lib/
RUN ldconfig

# 非 root + 可写 CWD（服务向 CWD 写 *_server.log / spans_*.jsonl）
RUN useradd -r -u 1000 -m myrpc
WORKDIR /app
RUN chown myrpc:myrpc /app
USER myrpc

ENTRYPOINT ["/usr/local/bin/app"]

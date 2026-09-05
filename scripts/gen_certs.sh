 #!/usr/bin/env bash
  # 生成自签名开发证书
  set -euo pipefail
  mkdir -p apps/im/certs
  openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout apps/im/certs/server.key \
    -out apps/im/certs/server.crt \
    -days 365 \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
  echo "生成完成：apps/im/certs/server.crt + server.key"
// Gateway TLS 握手端到端验证：每个场景单独起一个真实 gateway_server
// （客户端侧开启 TLS），用 ImClientConn 验证：
//   (a) 携带正确 CA（insecure=false）握手成功；
//   (b) insecure=true 跳过校验握手成功；
//   (c) 错误信任锚（不信任服务端证书）握手失败。
// 覆盖「ImClientConn(SSL_connect) ↔ Gateway(muduo Connection SSL_accept)」链路。
//
// 注：每场景独立 spawn gateway，是因为当前 muduo Connection 的 TLS teardown
// 存在一个 bug——TLS 客户端断开后 acceptor 不再 accept（见 /TODO 记录），
// 同一 gateway 上「连接→断开→再连」会触发。独立 spawn 可绕开该缺陷，
// 把测试聚焦在 TLS 握手本身。
#include "im_client_conn.h"
#include "test_helpers.h"

#include <chrono>
#include <cstdlib>
#include <cstdio>
#include <string>
#include <vector>

namespace {

std::string envStr(const char *name, const std::string &def) {
  const char *v = std::getenv(name);
  return (v && *v) ? std::string(v) : def;
}

// 生成自签证书（server + 一个「错误信任锚」other），返回临时目录；失败返回空。
std::string generateCerts() {
  char tmpl[] = "/tmp/myrpc_tls_XXXXXX";
  char *dir = ::mkdtemp(tmpl);
  if (!dir)
    return "";
  std::string d(dir);
  auto run = [](const std::string &cmd) { return ::system(cmd.c_str()) == 0; };
  std::string srv = "openssl req -x509 -newkey rsa:2048 -nodes -keyout " + d +
                    "/server.key -out " + d + "/server.crt -days 1 "
                    "-subj /CN=localhost -addext "
                    "subjectAltName=DNS:localhost,IP:127.0.0.1 "
                    ">/dev/null 2>&1";
  std::string other = "openssl req -x509 -newkey rsa:2048 -nodes -keyout " + d +
                      "/other.key -out " + d + "/other.crt -days 1 "
                      "-subj /CN=untrusted >/dev/null 2>&1";
  if (!run(srv) || !run(other))
    return "";
  return d;
}

// 起一个 TLS gateway，用 ImClientConn 尝试握手，返回是否成功。
// 每场景独立 spawn，避免触发「TLS 断开后不再 accept」的框架 bug。
bool runHandshake(const std::string &certDir, const std::string &etcd,
                  const std::string &ca, bool insecure, uint16_t *outPort) {
  const uint16_t clientPort = testutil::pickFreePort();
  const uint16_t rpcPort = testutil::pickFreePort();
  if (clientPort == 0 || rpcPort == 0)
    return false;
  if (outPort)
    *outPort = clientPort;

  const std::string ts = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  std::vector<std::string> argv = {
      GATEWAY_SERVER_BIN,
      "--client.ip=127.0.0.1",
      "--client.port=" + std::to_string(clientPort),
      "--rpc.ip=127.0.0.1",
      "--rpc.port=" + std::to_string(rpcPort),
      "--gateway.id=GW-TLS-" + ts,
      "--etcd.endpoints=" + etcd,
      "--im.service=ImService." + ts,
      "--auth.service=AuthService." + ts,
      "--shared.secret=devsecret",
      "--tls.cert=" + certDir + "/server.crt",
      "--tls.key=" + certDir + "/server.key",
  };

  pid_t gw = testutil::spawn(argv, certDir + "/gateway.console.log");
  if (gw <= 0)
    return false;
  if (!testutil::waitForPort("127.0.0.1", clientPort)) {
    testutil::stopProcess(gw);
    return false;
  }

  ImClientConn c;
  c.enableTls(ca, insecure);
  bool ok = false;
  for (int i = 0; i < 20 && !ok; ++i)
    ok = c.connect("127.0.0.1", clientPort);
  if (ok)
    c.close();

  testutil::stopProcess(gw);
  return ok;
}

int fail(const char *msg) {
  std::fprintf(stderr, "FAILED: %s\n", msg);
  return 1;
}

} // namespace

int main() {
  std::string certDir = generateCerts();
  if (certDir.empty()) {
    std::printf("test_gateway_tls: SKIP (openssl unavailable)\n");
    return 0;
  }
  const std::string etcd = envStr("RPC_TEST_ETCD", "http://127.0.0.1:2379");
  if (!testutil::etcdReachable(etcd)) {
    std::printf("test_gateway_tls: SKIP (no etcd at %s)\n", etcd.c_str());
    return 0;
  }

  // (a) 正确 CA + 校验：握手成功
  if (!runHandshake(certDir, etcd, certDir + "/server.crt", false, nullptr)) {
    ::system(("rm -rf " + certDir).c_str());
    return fail("TLS handshake with valid CA failed");
  }

  // (b) insecure：跳过校验，握手成功
  if (!runHandshake(certDir, etcd, "", true, nullptr)) {
    ::system(("rm -rf " + certDir).c_str());
    return fail("TLS handshake insecure failed");
  }

  // (c) 错误信任锚：校验失败，握手应失败
  if (runHandshake(certDir, etcd, certDir + "/other.crt", false, nullptr)) {
    ::system(("rm -rf " + certDir).c_str());
    return fail("TLS handshake with wrong CA unexpectedly succeeded");
  }

  ::system(("rm -rf " + certDir).c_str());
  std::printf("PASSED\n");
  return 0;
}

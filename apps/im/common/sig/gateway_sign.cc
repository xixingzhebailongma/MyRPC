#include "gateway_sign.h"
#include <cstring>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>

namespace gateway_sign {

static std::string toHex(const unsigned char *data, size_t len) {
  static const char *hex = "0123456789abcdef";
  std::string out;
  out.reserve(len * 2);
  for (size_t i = 0; i < len; ++i) {
    out.push_back(hex[data[i] >> 4]);
    out.push_back(hex[data[i] & 0x0f]);
  }
  return out;
}

std::string hmacSha256Hex(const std::string &secret, const std::string &data) {
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int dlen = 0;
  HMAC(EVP_sha256(), secret.data(), static_cast<int>(secret.size()),
       reinterpret_cast<const unsigned char *>(data.data()), data.size(),
       digest, &dlen);
  return toHex(digest, dlen);
}

std::string buildCanonical(const RpcHeader &hdr) {
  std::string c;
  c += hdr.identity().user_id();
  c += '\n';
  c += hdr.identity().session_id();
  c += '\n';
  c += std::to_string(hdr.conn_id());
  c += '\n';
  c += std::to_string(hdr.timestamp());
  c += '\n';
  c += hdr.nonce();
  c += '\n';
  c += hdr.identity().device_id();
  c += '\n';
  c += hdr.identity().username();
  c += '\n';
  c += std::to_string(hdr.identity().authenticated_at());
  return c;
}

std::string sign(const std::string &secret, const RpcHeader &hdr) {
  return hmacSha256Hex(secret, buildCanonical(hdr));
}

bool verify(const std::string &secret, const RpcHeader &hdr) {
  if (!hdr.has_identity() || hdr.signature().empty()) {
    return false;
  }
  std::string expect = sign(secret, hdr);
  if (expect.size() != hdr.signature().size()) {
    return false;
  }
  // 常量时间比较，避免时序侧信道
  return CRYPTO_memcmp(expect.data(), hdr.signature().data(), expect.size()) ==
         0;
}

} // namespace gateway_sign
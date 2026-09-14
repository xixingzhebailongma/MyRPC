#include "Buffer.h"
#include "rpc_protocol.h"
#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>

int main() {
  // 1. Buffer 基础（仍测 muduo 的 Buffer）
  {
    Buffer buf;
    assert(buf.readableBytes() == 0);
    buf.append("hello", 5);
    assert(buf.readableBytes() == 5);
    assert(std::string(buf.peek(), 5) == "hello");
    std::string s = buf.retrieveAsString(2);
    assert(s == "he");
    assert(buf.readableBytes() == 3);
    assert(std::string(buf.peek(), 3) == "llo");
    buf.retrieveAll();
    assert(buf.readableBytes() == 0);
  }

  // 2. protocol 编解码往返：encodeMessage -> tryDecodeFrame -> decodeMessage
  {
    RpcMessage req = buildRequest("Svc", "Method", 42, "hello body");
    std::string frame = encodeMessage(req);
    assert(frame.size() == kHeaderLen + req.ByteSizeLong());

    std::string body;
    size_t frame_len = 0;
    assert(tryDecodeFrame(frame.data(), frame.size(), &frame_len, &body) ==
           FrameDecode::kOk);
    assert(frame_len == frame.size());

    RpcMessage parsed;
    assert(decodeMessage(body, parsed));
    assert(parsed.header().service_name() == "Svc");
    assert(parsed.header().method_name() == "Method");
    assert(parsed.header().sequence_id() == 42);
    assert(parsed.body() == "hello body");
  }

  // 3. 半帧：数据不完整返回 kNeedMore，且不消费字节
  {
    std::string body = "abcdef";
    uint32_t len = 6;
    char full[10];
    std::memcpy(full, &len, 4);
    std::memcpy(full + 4, body.data(), 6);

    std::string out;
    size_t fl = 0;
    assert(tryDecodeFrame(full, 4, &fl, &out) ==
           FrameDecode::kNeedMore); // 只有头
    assert(tryDecodeFrame(full, 7, &fl, &out) ==
           FrameDecode::kNeedMore); // 头+部分
    assert(tryDecodeFrame(full, 10, &fl, &out) == FrameDecode::kOk); // 补齐
    assert(out == body);
    assert(fl == 10);
  }

  // 4. 坏帧：非法长度（0 或 >64MB）返回 kError，不崩溃
  {
    uint32_t bad = 0xFFFFFFFFu; // 4GB，超过 64MB 上限
    char hdr[4];
    std::memcpy(hdr, &bad, 4);
    std::string out;
    size_t fl = 0;
    assert(tryDecodeFrame(hdr, 4, &fl, &out) == FrameDecode::kError);

    uint32_t zero = 0;
    char zhdr[4];
    std::memcpy(zhdr, &zero, 4);
    assert(tryDecodeFrame(zhdr, 4, &fl, &out) == FrameDecode::kError);
  }

  std::printf("all tests passed\n");
  return 0;
}
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
    char full[10];
    writeLenBE(full, 6); // 大端长度 6
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
    char hdr[4];
    writeLenBE(hdr, 0xFFFFFFFFu); // 4GB，超过 64MB 上限
    std::string out;
    size_t fl = 0;
    assert(tryDecodeFrame(hdr, 4, &fl, &out) == FrameDecode::kError);

    char zhdr[4];
    writeLenBE(zhdr, 0);
    assert(tryDecodeFrame(zhdr, 4, &fl, &out) == FrameDecode::kError);
  }

  // 5. 长度前缀是大端：锁死字节布局，防止回退成主机序
  {
    RpcMessage req = buildRequest("Svc", "Method", 1, std::string(300, 'x'));
    std::string frame = encodeMessage(req);
    uint32_t expected = static_cast<uint32_t>(req.ByteSizeLong());
    assert(frame.size() == kHeaderLen + expected);
    // 逐字节核对大端布局（expected > 255，MSB/LSB 字节不同，能暴露 LE 回退）
    assert(static_cast<unsigned char>(frame[0]) ==
           static_cast<unsigned char>((expected >> 24) & 0xff));
    assert(static_cast<unsigned char>(frame[1]) ==
           static_cast<unsigned char>((expected >> 16) & 0xff));
    assert(static_cast<unsigned char>(frame[2]) ==
           static_cast<unsigned char>((expected >> 8) & 0xff));
    assert(static_cast<unsigned char>(frame[3]) ==
           static_cast<unsigned char>(expected & 0xff));
    assert(readLenBE(frame.data()) == expected);
  }

  std::printf("all tests passed\n");
  return 0;
}
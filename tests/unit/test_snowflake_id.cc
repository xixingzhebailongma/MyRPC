#include "snowflake_id.h"
#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>

// 从十进制 ID 反解低位字段（仅测试用）
static uint64_t seqOf(const std::string &id) { return std::stoull(id) & 0xFFF; }
static uint64_t workerOf(const std::string &id) {
  return (std::stoull(id) >> 12) & 0x3FF;
}

int main() {
  // (a) 正常：新毫秒落盘、同毫秒序列自增且不重复、推进到下一毫秒再次落盘
  {
    SnowflakeIdGenerator gen(1);
    uint64_t wall = 1000000, steady = 0;
    gen.SetClock([&] { return wall; }, [&] { return steady; },
                 [&](uint64_t ms) {
                   steady += ms;
                   wall += ms;
                 });
    int persist_calls = 0;
    gen.SetPersistFn([&](uint64_t) {
      ++persist_calls;
      return true;
    });

    std::string a = gen.Next(); // 新毫秒 → 落盘一次
    assert(!a.empty());
    assert(persist_calls == 1);
    assert(workerOf(a) == 1);

    std::string b = gen.Next(); // 同毫秒 → 序列自增，不落盘
    assert(!b.empty());
    assert(persist_calls == 1);
    assert(seqOf(b) == seqOf(a) + 1);

    wall = 1000001;             // 推进到下一毫秒
    std::string c = gen.Next(); // 再次落盘
    assert(!c.empty());
    assert(persist_calls == 2);

    assert(a != b && b != c && a != c);
  }

  // (b) 微小回拨（<=spin_threshold）：有界自旋后追平，不产生故障
  {
    SnowflakeIdGenerator gen(1);
    uint64_t wall = 1000;
    gen.SetClock([&] { return wall++; }, // 每次读推进 1ms，模拟时钟追上
                 [&] { return static_cast<uint64_t>(1000000); },
                 [&](uint64_t) {});
    gen.SetPersistFn([&](uint64_t) { return true; });

    gen.SetLastTimestampMs(1002); // 回拨 2ms
    std::string id = gen.Next();
    assert(!id.empty());
    assert(!gen.InFault());
  }

  // (c) 中度回拨（>spin_threshold 且 <=severe）：小步睡眠等待后追平
  {
    SnowflakeIdGenerator gen(1);
    uint64_t wall = 1000, steady = 0;
    gen.SetClock([&] { return wall; }, [&] { return steady; },
                 [&](uint64_t ms) {
                   steady += ms;
                   wall += ms;
                 });
    gen.SetPersistFn([&](uint64_t) { return true; });

    gen.SetLastTimestampMs(1050); // 回拨 50ms
    std::string id = gen.Next();
    assert(!id.empty());
    assert(!gen.InFault());
  }

  // (d)+(e) 严重回拨：置故障返回空串；故障期快速失败；时钟追平后自动恢复
  {
    SnowflakeIdGenerator gen(1, SnowflakeConfig{5, 20, 100});
    uint64_t wall = 1000, steady = 0;
    gen.SetClock([&] { return wall; }, [&] { return steady; },
                 [&](uint64_t ms) { steady += ms; /* wall 卡住不回拨 */ });
    gen.SetPersistFn([&](uint64_t) { return true; });

    gen.SetLastTimestampMs(2000); // 回拨 1000ms > severe 20ms
    assert(gen.Next().empty());
    assert(gen.InFault());
    assert(gen.Next().empty()); // 故障期：仍落后 → 快速失败，不重新自旋

    wall = 2000;              // 时钟追平
    std::string id = gen.Next();
    assert(!id.empty());
    assert(!gen.InFault());
  }

  // (f) 持久化节奏：每个新毫秒恰好一次，同毫秒不再调用
  {
    SnowflakeIdGenerator gen(3);
    uint64_t wall = 5000, steady = 0;
    gen.SetClock([&] { return wall; }, [&] { return steady; },
                 [&](uint64_t ms) {
                   steady += ms;
                   wall += ms;
                 });
    int calls = 0;
    gen.SetPersistFn([&](uint64_t) {
      ++calls;
      return true;
    });

    gen.Next();       // 新毫秒：calls=1
    gen.Next();       // 同毫秒
    gen.Next();       // 同毫秒
    assert(calls == 1);
    wall = 5001;      // 新毫秒
    gen.Next();       // calls=2
    gen.Next();       // 同毫秒
    assert(calls == 2);
  }

  // (g) 持久化失败：置故障返回空串；恢复后重新落盘发号
  {
    SnowflakeIdGenerator gen(1);
    uint64_t wall = 1000, steady = 0;
    gen.SetClock([&] { return wall; }, [&] { return steady; },
                 [&](uint64_t ms) {
                   steady += ms;
                   wall += ms;
                 });
    bool fail = true;
    int calls = 0;
    gen.SetPersistFn([&](uint64_t) {
      ++calls;
      return !fail;
    });

    assert(gen.Next().empty()); // 新毫秒落盘失败 → 空串 + 故障
    assert(gen.InFault());
    assert(calls == 1);

    fail = false;
    std::string id = gen.Next(); // 时钟未回拨 → 自动恢复并重新落盘
    assert(!id.empty());
    assert(!gen.InFault());
    assert(calls == 2);
  }

  // (h) SetLastTimestampMs：sequence 归零 + needs_persist 触发首个毫秒落盘
  {
    SnowflakeIdGenerator gen(7);
    uint64_t wall = 1000, steady = 0;
    gen.SetClock([&] { return wall; }, [&] { return steady; },
                 [&](uint64_t ms) {
                   steady += ms;
                   wall += ms;
                 });
    int calls = 0;
    gen.SetPersistFn([&](uint64_t) {
      ++calls;
      return true;
    });

    gen.SetLastTimestampMs(1000); // seed = 当前毫秒（无回拨）
    assert(gen.LastTimestampMs() == 1000);

    std::string a = gen.Next(); // now==last 但 needs_persist → 仍落盘，seq=0
    assert(!a.empty());
    assert(calls == 1);
    assert(workerOf(a) == 7);
    assert(seqOf(a) == 0);

    std::string b = gen.Next(); // 同毫秒：seq=1，不落盘
    assert(!b.empty());
    assert(calls == 1);
    assert(seqOf(b) == 1);
  }

  std::cout << "test_snowflake_id: all assertions passed" << std::endl;
  return 0;
}

#pragma once
#include <atomic>
#include <cstdint>
#include <memory>

// 有界无锁 MPSC 队列（多生产者 -> 单消费者），Vyukov 经典实现
// 要求 T 可默认构造 + 可移动构造/移动赋值（std::string 满足）
template <typename T> class BoundedMpscQueue {
  struct Cell {
    std::atomic<size_t> seq; // 槽位"代数"，用来判定 空闲/就绪/已满
    T data;                  // 实际数据
  };
  std::unique_ptr<Cell[]> buffer_;    //环形数组
  size_t mask_;                       // capacity-1(capacity是2的幂)
  std::atomic<size_t> enqueuePos_{0}; //多生产者CAS争用
  std::atomic<size_t> dequeuePos_{0}; //仅消费者读写

public:
  explicit BoundedMpscQueue(size_t capacity) {
    size_t cap = 1;
    while (cap < capacity)
      cap <<= 1; // 向上取到 2 的幂
    buffer_ = std::make_unique<Cell[]>(cap);
    for (size_t i = 0; i < cap; ++i)
      buffer_[i].seq.store(i, std::memory_order_relaxed); //关键：初始seq = i
    mask_ = cap - 1;
  }

  // 生产者：入队。满时返回 false，且【不会消费
  // v】，调用方的数据保持完整，方便重试
  bool tryEnqueue(T &&v) noexcept {
    size_t pos = enqueuePos_.load(std::memory_order_relaxed);
    for (;;) {
      Cell &cell = buffer_[pos & mask_];
      size_t seq = cell.seq.load(std::memory_order_acquire);
      intptr_t dif = (intptr_t)seq - (intptr_t)pos;
      if (dif == 0) {
        // 槽位空闲，抢全局位置
        if (enqueuePos_.compare_exchange_weak(pos, pos + 1,
                                              std::memory_order_relaxed))
          break;
      } else if (dif < 0) {
        return false; // 队列满，v 原封不动
      } else {
        pos = enqueuePos_.load(std::memory_order_relaxed); // pos 过期，重读
      }
    }
    buffer_[pos & mask_].data = std::move(v); // 只有抢到位置后才真正消费 v
    buffer_[pos & mask_].seq.store(pos + 1,
                                   std::memory_order_release); // 发布"数据就绪"
    return true;
  }
  // 消费者：出队。空时返回 false。仅单消费者调用
  bool tryDequeue(T &out) noexcept {
    size_t pos = dequeuePos_.load(std::memory_order_relaxed);
    Cell &cell = buffer_[pos & mask_];
    size_t seq = cell.seq.load(std::memory_order_acquire);
    intptr_t dif = (intptr_t)seq - (intptr_t)(pos + 1);
    if (dif < 0)
      return false; // 队列空
    out = std::move(cell.data);
    cell.data.clear(); // 释放掉旧堆内存
    cell.seq.store(pos + mask_ + 1,
                   std::memory_order_release); // 槽位还给生产者
    dequeuePos_.store(pos + 1, std::memory_order_relaxed);
    return true;
  }
  // 以下都是近似值（relaxed），只给条件变量谓词用，正确性以
  // tryEnqueue/tryDequeue 为准
  bool empty() const {
    return enqueuePos_.load(std::memory_order_relaxed) ==
           dequeuePos_.load(std::memory_order_relaxed);
  }
  bool full() const {
    return enqueuePos_.load(std::memory_order_relaxed) -
               dequeuePos_.load(std::memory_order_relaxed) >=
           (mask_ + 1);
  }
  size_t size() const {
    return enqueuePos_.load(std::memory_order_relaxed) -
           dequeuePos_.load(std::memory_order_relaxed);
  }
};
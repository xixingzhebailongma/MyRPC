#include "Logger.h"
#include "message_store.h"
#include <cassert>
#include <iostream>
#include <string>

// 简单断言宏
#define TEST(name) std::cout << "[TEST] " << name << " ... "
#define PASS() std::cout << "PASSED" << std::endl
#define FAIL(msg)                                                              \
  do {                                                                         \
    std::cerr << "FAILED: " << msg << std::endl;                               \
    return 1;                                                                  \
  } while (0)

int main() {
  Logger::instance().init(LogLevel::DEBUG, "test_message_store.log", true);

  // 1. 创建 + 连接
  TEST("connect to Redis");
  MessageStore store("TEST_SERVER");
  if (!store.connect("127.0.0.1", 6379))
    FAIL("cannot connect to Redis (is redis-server running?)");
  PASS();

  // 2. 去重测试
  TEST("dedup - first call returns true");
  if (!store.tryMarkProcessing("test_msg_dedup_001", 60))
    FAIL("first tryMarkProcessing should return true");
  PASS();

  TEST("dedup - second call returns false");
  if (store.tryMarkProcessing("test_msg_dedup_001", 60))
    FAIL("second tryMarkProcessing should return false");
  PASS();

  // 3. 离线消息测试
  TEST("offline - store 3 messages");
  for (int i = 0; i < 3; i++) {
    im::ChatMessage msg;
    msg.set_msg_id("offline_test_" + std::to_string(i));
    msg.set_from_user_id("alice");
    msg.set_to_user_id("bob");
    msg.set_content("Hello " + std::to_string(i));
    msg.set_timestamp(1000 + i);
    msg.set_chat_type(0); // 单聊
    msg.set_status(im::MessageStatus::SENT);

    if (!store.storeOfflineMessage("bob", msg))
      FAIL("storeOfflineMessage failed at msg " + std::to_string(i));
  }
  PASS();

  TEST("offline - fetch should return 3 messages in FIFO order");
  auto msgs = store.fetchOfflineMessages("bob");
  if (msgs.size() != 3)
    FAIL("expected 3 messages, got " + std::to_string(msgs.size()));
  // LPUSH 是后进先出，fetchOfflineMessages 已反转，应该按发送顺序
  for (int i = 0; i < 3; i++) {
    if (msgs[i].content() != "Hello " + std::to_string(i))
      FAIL("FIFO order broken at index " + std::to_string(i) +
           ", got: " + msgs[i].content());
  }
  PASS();

  TEST("offline - clear then fetch should return 0");
  store.clearOfflineMessages("bob");
  auto empty = store.fetchOfflineMessages("bob");
  if (!empty.empty())
    FAIL("expected 0 messages after clear, got " +
         std::to_string(empty.size()));
  PASS();

  // 4. 状态追踪测试
  TEST("status - mark DELIVERED then read back");
  store.markStatus("test_status_001", im::MessageStatus::DELIVERED);
  auto status = store.getStatus("test_status_001");
  if (status != im::MessageStatus::DELIVERED)
    FAIL("expected DELIVERED(2), got " +
         std::to_string(static_cast<int>(status)));
  PASS();

  TEST("status - unknown msg_id returns SENDING");
  auto unknown = store.getStatus("non_existent_msg_id");
  if (unknown != im::MessageStatus::SENDING)
    FAIL("expected SENDING(0) for unknown, got " +
         std::to_string(static_cast<int>(unknown)));
  PASS();

  // 5. ID 生成测试
  TEST("generateMsgId - should contain server_id");
  std::string id1 = store.generateMsgId();
  std::string id2 = store.generateMsgId();
  if (id1.empty() || id2.empty())
    FAIL("generateMsgId returned empty string");
  if (id1.find("TEST_SERVER_") != 0)
    FAIL("msg_id should start with TEST_SERVER_, got: " + id1);
  if (id1 == id2)
    FAIL("two generated IDs should be different, got: " + id1);
  PASS();

  std::cout << "\n=== All tests passed! ===" << std::endl;
  return 0;
}
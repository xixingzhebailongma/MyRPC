#pragma once
#include <cstdint>
// 消息队列共享常量：生产端(im_server) 与消费端(deliver_server) 必须一致。
namespace immq {
inline constexpr const char *kDeliveryStream = "im:delivery";
inline constexpr const char *kDeliveryGroup = "im-delivery-workers";

// ===== 事件广播 Stream（替代 Redis Pub/Sub，保证广播不丢） =====
// 每条广播 = 一个 Stream + 每节点一个专属消费组（组名含节点 ip:port）。
inline constexpr const char *kRouteEventsStream = "im:route:events:stream";
inline constexpr const char *kUserEventsStream  = "im:user:events:stream";
// Stream 消息体字段名（与 stream_producer/consumer 保持一致）
inline constexpr const char *kBodyField     = "body";
// 事件流长度上限（XADD MAXLEN ~），防无限增长；超出窗口的旧事件会被裁剪。
inline constexpr int64_t      kEventsMaxLen = 10000;
} // namespace immq
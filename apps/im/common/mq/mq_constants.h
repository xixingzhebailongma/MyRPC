#pragma once
// 消息队列共享常量：生产端(im_server) 与消费端(deliver_server) 必须一致。
namespace immq {
inline constexpr const char *kDeliveryStream = "im:delivery";
inline constexpr const char *kDeliveryGroup = "im-delivery-workers";
} // namespace immq
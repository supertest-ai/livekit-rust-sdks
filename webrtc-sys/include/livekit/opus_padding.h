/*
 * Copyright 2026 LiveKit, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <stdint.h>

#include <map>
#include <memory>
#include <optional>
#include <vector>

#include "api/frame_transformer_interface.h"
#include "api/scoped_refptr.h"
#include "rtc_base/synchronization/mutex.h"

namespace livekit_ffi {

// Writes caller-supplied bytes verbatim as the Opus padding (RFC 6716,
// 3.2.5) of encoded frames. Padding is keyed by the capture time of its 10 ms
// chunk; a frame stamped C holds the chunks in [C, C + duration).
class OpusPaddingTransformer : public webrtc::FrameTransformerInterface {
 public:
  bool IsAttached();
  void AddPadding(int64_t chunk_capture_ms, std::vector<uint8_t> padding);

  void Transform(
      std::unique_ptr<webrtc::TransformableFrameInterface> frame) override;
  void RegisterTransformedFrameCallback(
      webrtc::scoped_refptr<webrtc::TransformedFrameCallback> callback) override;
  void RegisterTransformedFrameSinkCallback(
      webrtc::scoped_refptr<webrtc::TransformedFrameCallback> callback,
      uint32_t ssrc) override;
  void UnregisterTransformedFrameCallback() override;
  void UnregisterTransformedFrameSinkCallback(uint32_t ssrc) override;

 private:
  webrtc::Mutex mutex_;
  webrtc::scoped_refptr<webrtc::TransformedFrameCallback> callback_
      RTC_GUARDED_BY(mutex_);
  std::map<int64_t, std::vector<uint8_t>> padding_ RTC_GUARDED_BY(mutex_);
  // Large frames are sent as plain Opus even with RED.
  std::optional<uint8_t> red_payload_type_ RTC_GUARDED_BY(mutex_);
};

}  // namespace livekit_ffi

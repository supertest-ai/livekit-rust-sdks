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
#include <span>
#include <vector>

#include "api/frame_transformer_interface.h"
#include "api/scoped_refptr.h"
#include "rtc_base/synchronization/mutex.h"

namespace livekit_ffi {

// Sender-side transformer that appends caller-supplied tags to encoded audio
// frames as a packet trailer.
//
// The audio source stamps every 10 ms chunk it hands to WebRTC with a capture
// time that advances by exactly 10 ms per chunk, and registers each tag under
// the capture time of the chunk holding the tagged sample. The audio coding
// module labels each encoded frame with the capture time of its first chunk,
// so a frame with capture time C contains the chunks C, C + 10, ... and the
// tag's position in the frame follows from its chunk's capture time.
//
// The trailer uses the packet trailer format (see packet_trailer.h) with two
// TLVs: user data (the tag) and the tagged sample's offset from the start of
// the frame, in 48 kHz samples. Frames without a tag are forwarded unchanged.
class AudioFrameTagTransformer : public webrtc::FrameTransformerInterface {
 public:
  // Registers `tag` for the sample `offset_samples_48k` into the chunk
  // stamped `chunk_capture_ms`.
  void AddTag(int64_t chunk_capture_ms,
              uint16_t offset_samples_48k,
              std::vector<uint8_t> tag);

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
  struct Tag {
    uint16_t offset_samples_48k;
    std::vector<uint8_t> data;
  };

  webrtc::Mutex mutex_;
  webrtc::scoped_refptr<webrtc::TransformedFrameCallback> callback_
      RTC_GUARDED_BY(mutex_);
  // Keyed by chunk capture time (ms).
  std::map<int64_t, Tag> tags_ RTC_GUARDED_BY(mutex_);
  int64_t last_frame_capture_ms_ RTC_GUARDED_BY(mutex_) = -1;
};

}  // namespace livekit_ffi

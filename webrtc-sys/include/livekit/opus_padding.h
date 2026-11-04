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

// Sender-side transformer that writes caller-supplied bytes into the Opus
// padding (RFC 6716, section 3.2.5) of encoded audio frames. The bytes are
// written verbatim; the audio data is untouched, so any Opus decoder decodes
// the frame exactly as it would without the padding. Frames without padding
// to write are forwarded unchanged.
//
// While attached, the audio source stamps every 10 ms chunk it hands to
// WebRTC with a capture time at least 10 ms after the previous chunk's, and
// registers padding under the capture time of the chunk holding the frame's
// first sample. The audio coding module labels each encoded frame with the
// capture time of its first chunk, and the frame's duration is read from its
// Opus TOC byte, so a frame stamped C holds the chunks in [C, C + duration).
class OpusPaddingTransformer : public webrtc::FrameTransformerInterface {
 public:
  // True while attached to a sender, i.e. while padding reaches the wire.
  bool IsAttached();

  // Registers `padding` for the chunk stamped `chunk_capture_ms`. `padding`
  // must not be empty.
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
  // Keyed by chunk capture time (ms).
  std::map<int64_t, std::vector<uint8_t>> padding_ RTC_GUARDED_BY(mutex_);
  // The RED encoder sends a frame too large for RED as plain Opus under the
  // Opus payload type; the RED payload type tells the two apart. Learned from
  // the first frame that can only be RED.
  std::optional<uint8_t> red_payload_type_ RTC_GUARDED_BY(mutex_);
};

}  // namespace livekit_ffi

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

#include "livekit/audio_frame_tags.h"

#include <optional>

#include "livekit/packet_trailer.h"
#include "rtc_base/logging.h"

namespace livekit_ffi {

namespace {

// Used until two consecutive frames reveal the encoder's frame duration.
constexpr int64_t kDefaultFrameMs = 20;
// Opus frames are at most 60 ms; a larger gap between frames means the
// encoder skipped frames (DTX), not that frames got longer.
constexpr int64_t kMaxFrameMs = 60;

constexpr size_t kOffsetTlvSize = 4;  // tag + len + 2-byte value
constexpr size_t kMaxTagSize = kPacketTrailerMaxTotal - kTrailerEnvelopeSize -
                               kOffsetTlvSize - kUserDataTlvHeaderSize;

std::vector<uint8_t> BuildTrailer(const std::vector<uint8_t>& tag,
                                  uint16_t offset_samples_48k) {
  const size_t trailer_len = kUserDataTlvHeaderSize + tag.size() +
                             kOffsetTlvSize + kTrailerEnvelopeSize;
  std::vector<uint8_t> trailer;
  trailer.reserve(trailer_len);
  trailer.push_back(kTagUserData ^ 0xFF);
  trailer.push_back(static_cast<uint8_t>(tag.size()) ^ 0xFF);
  for (uint8_t byte : tag) {
    trailer.push_back(byte ^ 0xFF);
  }
  trailer.push_back(kTagSampleOffset ^ 0xFF);
  trailer.push_back(2 ^ 0xFF);
  trailer.push_back(static_cast<uint8_t>(offset_samples_48k >> 8) ^ 0xFF);
  trailer.push_back(static_cast<uint8_t>(offset_samples_48k & 0xFF) ^ 0xFF);
  trailer.push_back(static_cast<uint8_t>(trailer_len) ^ 0xFF);
  trailer.insert(trailer.end(), std::begin(kPacketTrailerMagic),
                 std::end(kPacketTrailerMagic));
  return trailer;
}

}  // namespace

void AudioFrameTagTransformer::AddTag(int64_t chunk_capture_ms,
                                      uint16_t offset_samples_48k,
                                      std::vector<uint8_t> tag) {
  if (tag.size() > kMaxTagSize) {
    RTC_LOG(LS_WARNING) << "AudioFrameTagTransformer dropping " << tag.size()
                        << "-byte tag, the limit is " << kMaxTagSize;
    return;
  }
  webrtc::MutexLock lock(&mutex_);
  // Not attached to a sender: no frame would ever consume the tag.
  if (!callback_) {
    return;
  }
  tags_.emplace(chunk_capture_ms, Tag{offset_samples_48k, std::move(tag)});
}

void AudioFrameTagTransformer::Transform(
    std::unique_ptr<webrtc::TransformableFrameInterface> frame) {
  webrtc::scoped_refptr<webrtc::TransformedFrameCallback> callback;
  std::optional<Tag> tag;
  {
    webrtc::MutexLock lock(&mutex_);
    callback = callback_;
    auto capture_time = frame->CaptureTime();
    if (capture_time.has_value()) {
      const int64_t capture_ms = capture_time->ms();
      const int64_t since_last = capture_ms - last_frame_capture_ms_;
      const int64_t frame_ms =
          last_frame_capture_ms_ >= 0 && since_last > 0 &&
                  since_last <= kMaxFrameMs
              ? since_last
              : kDefaultFrameMs;
      last_frame_capture_ms_ = capture_ms;

      // Tags before this frame belong to frames the encoder never emitted.
      tags_.erase(tags_.begin(), tags_.lower_bound(capture_ms));
      auto end = tags_.lower_bound(capture_ms + frame_ms);
      if (tags_.begin() != end) {
        // One tag per frame: the first wins.
        const int64_t first_ms = tags_.begin()->first;
        tag = std::move(tags_.begin()->second);
        tag->offset_samples_48k = static_cast<uint16_t>(
            (first_ms - capture_ms) * 48 + tag->offset_samples_48k);
        tags_.erase(tags_.begin(), end);
      }
    }
  }

  if (tag.has_value()) {
    auto data = frame->GetData();
    std::vector<uint8_t> tagged(data.begin(), data.end());
    auto trailer = BuildTrailer(tag->data, tag->offset_samples_48k);
    tagged.insert(tagged.end(), trailer.begin(), trailer.end());
    frame->SetData(tagged);
  }

  if (callback) {
    callback->OnTransformedFrame(std::move(frame));
  }
}

void AudioFrameTagTransformer::RegisterTransformedFrameCallback(
    webrtc::scoped_refptr<webrtc::TransformedFrameCallback> callback) {
  webrtc::MutexLock lock(&mutex_);
  callback_ = callback;
}

void AudioFrameTagTransformer::RegisterTransformedFrameSinkCallback(
    webrtc::scoped_refptr<webrtc::TransformedFrameCallback> callback,
    uint32_t ssrc) {
  RegisterTransformedFrameCallback(callback);
}

void AudioFrameTagTransformer::UnregisterTransformedFrameCallback() {
  webrtc::MutexLock lock(&mutex_);
  callback_ = nullptr;
  tags_.clear();
}

void AudioFrameTagTransformer::UnregisterTransformedFrameSinkCallback(
    uint32_t ssrc) {
  UnregisterTransformedFrameCallback();
}

}  // namespace livekit_ffi

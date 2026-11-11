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

#include "livekit/opus_padding.h"

#include <algorithm>
#include <span>
#include <string>

#include "absl/strings/match.h"
#include "rtc_base/logging.h"

namespace livekit_ffi {

namespace {

// AudioEncoderCopyRed sends larger packets as plain Opus.
constexpr size_t kRedMaxPrimarySize = 1024;

constexpr int64_t kChunkMs = 10;

// Bounds the chunks kept while no frames arrive, e.g. before negotiation.
constexpr size_t kMaxChunks = 1000;

// Samples per Opus frame at 48 kHz, from the TOC byte (RFC 6716, 3.1).
int OpusFrameSamples(uint8_t toc) {
  const int config = toc >> 3;
  if (config < 12) {  // SILK: 10, 20, 40, 60 ms
    static constexpr int kSamples[] = {480, 960, 1920, 2880};
    return kSamples[config & 3];
  }
  if (config < 16) {  // Hybrid: 10, 20 ms
    return (config & 1) ? 960 : 480;
  }
  static constexpr int kSamples[] = {120, 240, 480, 960};  // CELT
  return kSamples[config & 3];
}

std::optional<int64_t> OpusPacketDurationMs(std::span<const uint8_t> packet) {
  if (packet.empty()) {
    return std::nullopt;
  }
  int frames = 0;
  switch (packet[0] & 3) {
    case 0:
      frames = 1;
      break;
    case 1:
    case 2:
      frames = 2;
      break;
    case 3:
      if (packet.size() < 2 || (packet[1] & 0x3F) == 0) {
        return std::nullopt;
      }
      frames = packet[1] & 0x3F;
      break;
  }
  const int64_t samples = int64_t{frames} * OpusFrameSamples(packet[0]);
  return (samples + 47) / 48;
}

// Offset of the primary block in a RED payload (RFC 2198).
std::optional<size_t> RedPrimaryOffset(std::span<const uint8_t> red) {
  size_t pos = 0;
  size_t redundant_bytes = 0;
  while (true) {
    if (pos >= red.size()) {
      return std::nullopt;
    }
    if ((red[pos] & 0x80) == 0) {  // Last header: F = 0, payload type only.
      pos += 1;
      break;
    }
    if (pos + 4 > red.size()) {
      return std::nullopt;
    }
    redundant_bytes += ((red[pos + 2] & 0x03) << 8) | red[pos + 3];
    pos += 4;
  }
  const size_t primary = pos + redundant_bytes;
  if (primary >= red.size()) {
    return std::nullopt;
  }
  return primary;
}

// Rewrites `packet` to code 3 with `padding`. False if it already has padding.
bool AppendPaddedOpusPacket(std::span<const uint8_t> packet,
                            const std::vector<uint8_t>& padding,
                            std::vector<uint8_t>* out) {
  const uint8_t toc = packet[0];
  const int code = toc & 3;
  uint8_t count_byte = 0;
  std::span<const uint8_t> rest;
  if (code == 3) {
    if (packet[1] & 0x40) {
      return false;
    }
    count_byte = packet[1] | 0x40;
    rest = packet.subspan(2);
  } else {
    // Code 2 already leads with the first frame's length, as code 3 VBR does.
    count_byte = (code == 0 ? 1 : 2) | (code == 2 ? 0x80 : 0) | 0x40;
    rest = packet.subspan(1);
  }

  out->push_back(toc | 3);
  out->push_back(count_byte);
  size_t remaining = padding.size();
  while (remaining >= 255) {
    out->push_back(255);  // 254 bytes of padding, and another length byte.
    remaining -= 254;
  }
  out->push_back(static_cast<uint8_t>(remaining));
  out->insert(out->end(), rest.begin(), rest.end());
  out->insert(out->end(), padding.begin(), padding.end());
  return true;
}

}  // namespace

bool OpusPaddingTransformer::IsActive() {
  webrtc::MutexLock lock(&mutex_);
  return !unregistered_;
}

void OpusPaddingTransformer::AddChunk(int64_t capture_ms,
                                      std::vector<uint8_t> padding) {
  bool dropped = false;
  {
    webrtc::MutexLock lock(&mutex_);
    if (chunks_.size() >= kMaxChunks) {
      dropped = !chunks_.front().padding.empty();
      chunks_.pop_front();
    }
    chunks_.push_back({capture_ms, std::move(padding)});
  }
  if (dropped) {
    RTC_LOG(LS_WARNING) << "Dropping Opus padding: no frame was encoded";
  }
}

void OpusPaddingTransformer::Transform(
    std::unique_ptr<webrtc::TransformableFrameInterface> frame) {
  webrtc::scoped_refptr<webrtc::TransformedFrameCallback> callback;
  std::vector<uint8_t> padding;
  int unsent = 0;
  int superseded = 0;
  const char* skip_reason = nullptr;
  size_t opus_offset = 0;
  const std::span<const uint8_t> data = frame->GetData();
  const std::string mime = frame->GetMimeType();
  const auto capture_time = frame->CaptureTime();
  {
    webrtc::MutexLock lock(&mutex_);
    callback = callback_;
    if (capture_time.has_value() && !chunks_.empty()) {
      if (absl::EqualsIgnoreCase(mime, "audio/red")) {
        if (!red_payload_type_ && data.size() < kRedMaxPrimarySize) {
          red_payload_type_ = frame->GetPayloadType();
        }
        if (!red_payload_type_) {
          skip_reason = "cannot tell a RED payload from plain Opus yet";
        } else if (frame->GetPayloadType() == *red_payload_type_) {
          auto offset = RedPrimaryOffset(data);
          if (offset) {
            opus_offset = *offset;
          } else {
            skip_reason = "malformed RED payload";
          }
        }
      } else if (!absl::EqualsIgnoreCase(mime, "audio/opus")) {
        skip_reason = "the codec is not Opus";
      }

      std::optional<int64_t> duration_ms;
      if (!skip_reason) {
        duration_ms = OpusPacketDurationMs(data.subspan(opus_offset));
        if (!duration_ms) {
          skip_reason = "malformed Opus packet";
        }
      }

      // Chunks before this frame's first one were not sent (e.g. DTX).
      const int64_t capture_ms = capture_time->ms();
      while (!chunks_.empty() && chunks_.front().capture_ms < capture_ms) {
        unsent += !chunks_.front().padding.empty();
        chunks_.pop_front();
      }
      if (!chunks_.empty() && chunks_.front().capture_ms == capture_ms) {
        const int64_t n =
            std::max<int64_t>(1, (duration_ms.value_or(kChunkMs) + kChunkMs - 1) /
                                     kChunkMs);
        for (int64_t i = 0; i < n && !chunks_.empty(); ++i) {
          std::vector<uint8_t>& p = chunks_.front().padding;
          if (padding.empty()) {
            padding = std::move(p);
          } else {
            superseded += !p.empty();
          }
          chunks_.pop_front();
        }
      }
    }
  }

  if (unsent) {
    RTC_LOG(LS_WARNING) << "Dropping Opus padding of " << unsent
                        << " chunk(s): no sent frame matched them";
  }
  if (superseded) {
    RTC_LOG(LS_WARNING) << "Dropping Opus padding of " << superseded
                        << " chunk(s): an earlier chunk's fills their frame";
  }
  if (!padding.empty()) {
    std::vector<uint8_t> padded;
    if (!skip_reason) {
      padded.reserve(data.size() + padding.size() + 3 + padding.size() / 254);
      padded.assign(data.begin(), data.begin() + opus_offset);
      if (!AppendPaddedOpusPacket(data.subspan(opus_offset), padding,
                                  &padded)) {
        skip_reason = "the Opus packet already carries padding";
      }
    }
    if (skip_reason) {
      RTC_LOG(LS_WARNING) << "Dropping Opus padding: " << skip_reason;
    } else {
      frame->SetData(padded);
    }
  }

  if (callback) {
    callback->OnTransformedFrame(std::move(frame));
  }
}

void OpusPaddingTransformer::RegisterTransformedFrameCallback(
    webrtc::scoped_refptr<webrtc::TransformedFrameCallback> callback) {
  webrtc::MutexLock lock(&mutex_);
  callback_ = callback;
  unregistered_ = false;
  red_payload_type_.reset();
}

void OpusPaddingTransformer::RegisterTransformedFrameSinkCallback(
    webrtc::scoped_refptr<webrtc::TransformedFrameCallback> callback,
    uint32_t ssrc) {
  RegisterTransformedFrameCallback(callback);
}

void OpusPaddingTransformer::UnregisterTransformedFrameCallback() {
  webrtc::MutexLock lock(&mutex_);
  callback_ = nullptr;
  unregistered_ = true;
  chunks_.clear();
  red_payload_type_.reset();
}

void OpusPaddingTransformer::UnregisterTransformedFrameSinkCallback(
    uint32_t ssrc) {
  UnregisterTransformedFrameCallback();
}

}  // namespace livekit_ffi

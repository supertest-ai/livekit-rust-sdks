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

#include <cctype>
#include <span>
#include <string>

#include "rtc_base/checks.h"
#include "rtc_base/logging.h"

namespace livekit_ffi {

namespace {

// AudioEncoderCopyRed sends larger packets as plain Opus.
constexpr size_t kRedMaxPrimarySize = 1024;

// Window used to drop padding for a frame whose duration is unknown.
constexpr int64_t kChunkMs = 10;

bool MimeIs(const std::string& mime, const char* expected) {
  size_t i = 0;
  for (; expected[i] != '\0'; ++i) {
    if (i >= mime.size() || std::tolower(static_cast<unsigned char>(mime[i])) !=
                                expected[i]) {
      return false;
    }
  }
  return i == mime.size();
}

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
    default:
      if (packet.size() < 2 || (packet[1] & 0x3F) == 0) {
        return std::nullopt;
      }
      frames = packet[1] & 0x3F;
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

bool OpusPaddingTransformer::IsAttached() {
  webrtc::MutexLock lock(&mutex_);
  return callback_ != nullptr;
}

void OpusPaddingTransformer::AddPadding(int64_t chunk_capture_ms,
                                        std::vector<uint8_t> padding) {
  RTC_DCHECK(!padding.empty());
  webrtc::MutexLock lock(&mutex_);
  if (!callback_) {
    return;
  }
  padding_.emplace(chunk_capture_ms, std::move(padding));
}

void OpusPaddingTransformer::Transform(
    std::unique_ptr<webrtc::TransformableFrameInterface> frame) {
  webrtc::scoped_refptr<webrtc::TransformedFrameCallback> callback;
  std::optional<std::vector<uint8_t>> padding;
  const char* skip_reason = nullptr;
  size_t opus_offset = 0;
  const std::span<const uint8_t> data = frame->GetData();
  const std::string mime = frame->GetMimeType();
  const auto capture_time = frame->CaptureTime();
  {
    webrtc::MutexLock lock(&mutex_);
    callback = callback_;
    if (capture_time.has_value()) {
      if (MimeIs(mime, "audio/red")) {
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
      } else if (!MimeIs(mime, "audio/opus")) {
        skip_reason = "the codec is not Opus";
      }

      std::optional<int64_t> duration_ms;
      if (!skip_reason) {
        duration_ms = OpusPacketDurationMs(data.subspan(opus_offset));
        if (!duration_ms) {
          skip_reason = "malformed Opus packet";
        }
      }

      const int64_t capture_ms = capture_time->ms();
      // Earlier padding belongs to frames never sent (e.g. DTX).
      padding_.erase(padding_.begin(), padding_.lower_bound(capture_ms));
      auto end = padding_.lower_bound(capture_ms + duration_ms.value_or(kChunkMs));
      if (padding_.begin() != end) {
        // One padding per frame: the first wins.
        padding = std::move(padding_.begin()->second);
        padding_.erase(padding_.begin(), end);
      }
    }
  }

  if (padding.has_value()) {
    std::vector<uint8_t> padded;
    if (!skip_reason) {
      padded.reserve(data.size() + padding->size() + 3 +
                     padding->size() / 254);
      padded.assign(data.begin(), data.begin() + opus_offset);
      if (!AppendPaddedOpusPacket(data.subspan(opus_offset), *padding,
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
}

void OpusPaddingTransformer::RegisterTransformedFrameSinkCallback(
    webrtc::scoped_refptr<webrtc::TransformedFrameCallback> callback,
    uint32_t ssrc) {
  RegisterTransformedFrameCallback(callback);
}

void OpusPaddingTransformer::UnregisterTransformedFrameCallback() {
  webrtc::MutexLock lock(&mutex_);
  callback_ = nullptr;
  padding_.clear();
}

void OpusPaddingTransformer::UnregisterTransformedFrameSinkCallback(
    uint32_t ssrc) {
  UnregisterTransformedFrameCallback();
}

}  // namespace livekit_ffi

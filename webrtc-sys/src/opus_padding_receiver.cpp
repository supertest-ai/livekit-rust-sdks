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

#include "livekit/opus_padding_receiver.h"

#include <algorithm>
#include <span>
#include <string>

#include "absl/strings/match.h"
#include "api/audio/audio_frame.h"
#include "livekit/audio_track.h"
#include "livekit/peer_connection_factory.h"
#include "livekit/rtp_receiver.h"
#include "rtc_base/logging.h"

namespace livekit_ffi {

namespace {

// Opus always uses a 48 kHz RTP clock (RFC 7587, 4.1).
constexpr int64_t kOpusClockRate = 48000;

// Bounds the paddings kept for sinks that are slow or absent: about 10 s of
// 20 ms packets, and nothing older than 10 s.
constexpr size_t kMaxPaddings = 500;
constexpr int64_t kMaxAge = 10 * kOpusClockRate;

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

// The padding of a code 3 Opus packet (RFC 6716, 3.2.5), or empty.
std::span<const uint8_t> OpusPadding(std::span<const uint8_t> packet) {
  if (packet.size() < 2 || (packet[0] & 3) != 3 || (packet[1] & 0x40) == 0) {
    return {};
  }
  size_t pos = 2;
  size_t padding = 0;
  while (true) {
    if (pos >= packet.size()) {
      return {};
    }
    const uint8_t n = packet[pos++];
    if (n != 255) {
      padding += n;
      break;
    }
    padding += 254;  // 254 bytes of padding, and another length byte.
  }
  if (padding > packet.size() - pos) {
    return {};
  }
  return packet.subspan(packet.size() - padding);
}

std::span<const uint8_t> FramePadding(
    const webrtc::TransformableFrameInterface& frame) {
  const std::span<const uint8_t> data = frame.GetData();
  const std::string mime = frame.GetMimeType();
  if (absl::EqualsIgnoreCase(mime, "audio/opus")) {
    return OpusPadding(data);
  }
  if (absl::EqualsIgnoreCase(mime, "audio/red")) {
    const auto offset = RedPrimaryOffset(data);
    return offset ? OpusPadding(data.subspan(*offset))
                  : std::span<const uint8_t>();
  }
  return {};
}

struct DeferredChunk {
  std::weak_ptr<NativeAudioSink> sink;
  std::vector<int16_t> data;
  int sample_rate;
  size_t number_of_channels;
  size_t number_of_frames;
};

// The chunks track sinks received during the current source call, if it has
// a store.
thread_local std::vector<DeferredChunk>* deferred_chunks = nullptr;

}  // namespace

void OpusPaddingStore::Add(uint32_t rtp_timestamp,
                           std::vector<uint8_t> padding) {
  webrtc::MutexLock lock(&mutex_);
  const int64_t timestamp = unwrapper_.Unwrap(rtp_timestamp);
  paddings_.emplace(timestamp, std::move(padding));
  const int64_t newest = paddings_.rbegin()->first;
  while (paddings_.size() > kMaxPaddings ||
         paddings_.begin()->first < newest - kMaxAge) {
    paddings_.erase(paddings_.begin());
  }
}

void OpusPaddingStore::Take(Cursor* cursor,
                            uint32_t rtp_timestamp,
                            uint32_t duration,
                            std::vector<std::vector<uint8_t>>* out) {
  webrtc::MutexLock lock(&mutex_);
  const int64_t start = unwrapper_.Unwrap(rtp_timestamp);
  const int64_t end = start + duration;
  // A new sink starts at its first chunk, not at earlier packets.
  const int64_t next = cursor->next.value_or(start);
  for (auto it = paddings_.lower_bound(next);
       it != paddings_.end() && it->first < end; ++it) {
    out->push_back(it->second);
  }
  // NetEq can repeat or skip timestamps; never deliver a padding twice.
  cursor->next = std::max(next, end);
}

void OpusPaddingRegistry::Register(
    uint32_t ssrc,
    const std::shared_ptr<OpusPaddingStore>& store) {
  webrtc::MutexLock lock(&mutex_);
  auto& stores = stores_[ssrc];
  std::erase_if(stores, [&](const std::weak_ptr<OpusPaddingStore>& s) {
    auto live = s.lock();
    return !live || live == store;
  });
  stores.push_back(store);
  if (stores.size() > 1) {
    RTC_LOG(LS_WARNING) << "Not delivering Opus padding: " << stores.size()
                        << " receive streams have SSRC " << ssrc;
  }
}

void OpusPaddingRegistry::Unregister(uint32_t ssrc,
                                     const OpusPaddingStore* store) {
  webrtc::MutexLock lock(&mutex_);
  auto it = stores_.find(ssrc);
  if (it == stores_.end()) {
    return;
  }
  std::erase_if(it->second, [&](const std::weak_ptr<OpusPaddingStore>& s) {
    auto live = s.lock();
    return !live || live.get() == store;
  });
  if (it->second.empty()) {
    stores_.erase(it);
  }
}

std::shared_ptr<OpusPaddingStore> OpusPaddingRegistry::Find(uint32_t ssrc) {
  webrtc::MutexLock lock(&mutex_);
  auto it = stores_.find(ssrc);
  if (it == stores_.end() || it->second.size() != 1) {
    return nullptr;
  }
  return it->second.front().lock();
}

OpusPaddingReader::OpusPaddingReader(
    std::shared_ptr<OpusPaddingRegistry> registry)
    : registry_(std::move(registry)) {}

OpusPaddingReader::~OpusPaddingReader() {
  webrtc::MutexLock lock(&mutex_);
  UnregisterStore();
}

void OpusPaddingReader::Transform(
    std::unique_ptr<webrtc::TransformableFrameInterface> frame) {
  const uint32_t ssrc = frame->GetSsrc();
  const std::span<const uint8_t> padding = FramePadding(*frame);
  if (!padding.empty()) {
    store_->Add(frame->GetTimestamp(),
                std::vector<uint8_t>(padding.begin(), padding.end()));
  }

  webrtc::scoped_refptr<webrtc::TransformedFrameCallback> callback;
  {
    webrtc::MutexLock lock(&mutex_);
    if (ssrc_ != ssrc) {
      UnregisterStore();
      registry_->Register(ssrc, store_);
      ssrc_ = ssrc;
    }
    auto it = sink_callbacks_.find(ssrc);
    callback = it != sink_callbacks_.end() ? it->second : callback_;
  }
  if (callback) {
    callback->OnTransformedFrame(std::move(frame));
  }
}

void OpusPaddingReader::RegisterTransformedFrameCallback(
    webrtc::scoped_refptr<webrtc::TransformedFrameCallback> callback) {
  webrtc::MutexLock lock(&mutex_);
  callback_ = std::move(callback);
}

void OpusPaddingReader::RegisterTransformedFrameSinkCallback(
    webrtc::scoped_refptr<webrtc::TransformedFrameCallback> callback,
    uint32_t ssrc) {
  webrtc::MutexLock lock(&mutex_);
  sink_callbacks_[ssrc] = std::move(callback);
}

void OpusPaddingReader::UnregisterTransformedFrameCallback() {
  webrtc::MutexLock lock(&mutex_);
  callback_ = nullptr;
  UnregisterStore();
}

void OpusPaddingReader::UnregisterTransformedFrameSinkCallback(uint32_t ssrc) {
  webrtc::MutexLock lock(&mutex_);
  sink_callbacks_.erase(ssrc);
  if (ssrc_ == ssrc) {
    UnregisterStore();
  }
}

void OpusPaddingReader::UnregisterStore() {
  if (ssrc_) {
    registry_->Unregister(*ssrc_, store_.get());
    ssrc_.reset();
  }
}

class OpusPaddingAudioMixer::SourceWrapper : public webrtc::AudioMixer::Source {
 public:
  SourceWrapper(Source* inner, std::shared_ptr<OpusPaddingRegistry> registry)
      : inner_(inner), registry_(std::move(registry)) {}

  AudioFrameInfo GetAudioFrameWithInfo(int sample_rate_hz,
                                       webrtc::AudioFrame* frame) override {
    const std::shared_ptr<OpusPaddingStore> store =
        registry_->Find(static_cast<uint32_t>(inner_->Ssrc()));
    if (!store) {
      return inner_->GetAudioFrameWithInfo(sample_rate_hz, frame);
    }

    // The track sinks receive the chunk inside this call, before its RTP
    // timestamp is set on `frame`.
    std::vector<DeferredChunk> chunks;
    std::vector<DeferredChunk>* const outer = deferred_chunks;
    deferred_chunks = &chunks;
    const AudioFrameInfo info =
        inner_->GetAudioFrameWithInfo(sample_rate_hz, frame);
    deferred_chunks = outer;

    // Before NetEq has decoded a packet, `timestamp_` is not an RTP
    // timestamp, and no packet contributed to the chunk.
    const bool from_packet = !frame->packet_infos_.empty();
    std::vector<std::vector<uint8_t>> padding;
    for (const DeferredChunk& chunk : chunks) {
      const std::shared_ptr<NativeAudioSink> sink = chunk.sink.lock();
      if (!sink) {
        continue;
      }
      padding.clear();
      OpusPaddingStore::Cursor& cursor = sink->opus_padding_cursor();
      if (cursor.store.lock() != store) {
        cursor = {store, std::nullopt};
      }
      if (cursor.next || from_packet) {
        const auto duration = static_cast<uint32_t>(
            chunk.number_of_frames * kOpusClockRate / chunk.sample_rate);
        store->Take(&cursor, frame->timestamp_, duration, &padding);
      }
      sink->Deliver(chunk.data.data(), chunk.sample_rate,
                    chunk.number_of_channels, chunk.number_of_frames, padding);
    }
    return info;
  }

  int Ssrc() const override { return inner_->Ssrc(); }

  int PreferredSampleRate() const override {
    return inner_->PreferredSampleRate();
  }

 private:
  Source* const inner_;
  const std::shared_ptr<OpusPaddingRegistry> registry_;
};

OpusPaddingAudioMixer::OpusPaddingAudioMixer(
    webrtc::scoped_refptr<webrtc::AudioMixer> inner,
    std::shared_ptr<OpusPaddingRegistry> registry)
    : inner_(std::move(inner)), registry_(std::move(registry)) {}

OpusPaddingAudioMixer::~OpusPaddingAudioMixer() = default;

bool OpusPaddingAudioMixer::AddSource(Source* source) {
  webrtc::MutexLock lock(&mutex_);
  auto wrapper = std::make_unique<SourceWrapper>(source, registry_);
  if (!inner_->AddSource(wrapper.get())) {
    return false;
  }
  wrappers_[source] = std::move(wrapper);
  return true;
}

void OpusPaddingAudioMixer::RemoveSource(Source* source) {
  webrtc::MutexLock lock(&mutex_);
  auto it = wrappers_.find(source);
  if (it == wrappers_.end()) {
    return;
  }
  inner_->RemoveSource(it->second.get());
  wrappers_.erase(it);
}

void OpusPaddingAudioMixer::Mix(size_t number_of_channels,
                                webrtc::AudioFrame* audio_frame_for_mixing) {
  inner_->Mix(number_of_channels, audio_frame_for_mixing);
}

bool DeferSinkChunk(NativeAudioSink* sink,
                    const int16_t* data,
                    int sample_rate,
                    size_t number_of_channels,
                    size_t number_of_frames) {
  if (!deferred_chunks) {
    return false;
  }
  deferred_chunks->push_back(
      {sink->weak_from_this(),
       std::vector<int16_t>(data, data + number_of_channels * number_of_frames),
       sample_rate, number_of_channels, number_of_frames});
  return true;
}

void attach_opus_padding_reader(std::shared_ptr<PeerConnectionFactory> factory,
                                std::shared_ptr<RtpReceiver> receiver) {
  receiver->rtc_receiver()->SetDepacketizerToDecoderFrameTransformer(
      webrtc::make_ref_counted<OpusPaddingReader>(
          factory->opus_padding_registry()));
}

}  // namespace livekit_ffi

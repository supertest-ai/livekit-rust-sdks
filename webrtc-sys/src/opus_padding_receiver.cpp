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
#include "livekit/opus_padding.h"
#include "livekit/peer_connection_factory.h"
#include "livekit/rtp_receiver.h"
#include "rtc_base/logging.h"

namespace livekit_ffi {

namespace {

// Opus always uses a 48 kHz RTP clock (RFC 7587, 4.1).
constexpr int64_t kOpusClockRate = 48000;

// Bounds the paddings kept for streams that are slow or not played: about
// 10 s of 20 ms packets, and nothing older than 10 s.
constexpr size_t kMaxPaddings = 500;
constexpr int64_t kMaxAge = 10 * kOpusClockRate;

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

// The chunks track sinks received during the current source call, if its
// stream has padding.
thread_local std::vector<DeferredChunk>* deferred_chunks = nullptr;

}  // namespace

void OpusPaddingRegistry::Add(uint32_t ssrc,
                              uint32_t rtp_timestamp,
                              std::vector<uint8_t> padding) {
  webrtc::MutexLock lock(&mutex_);
  Stream& stream = streams_[ssrc];
  stream.paddings.emplace(stream.unwrapper.Unwrap(rtp_timestamp),
                          std::move(padding));
  const int64_t newest = stream.paddings.rbegin()->first;
  while (stream.paddings.size() > kMaxPaddings ||
         stream.paddings.begin()->first < newest - kMaxAge) {
    stream.paddings.erase(stream.paddings.begin());
  }
}

void OpusPaddingRegistry::Remove(uint32_t ssrc) {
  webrtc::MutexLock lock(&mutex_);
  streams_.erase(ssrc);
}

bool OpusPaddingRegistry::Has(uint32_t ssrc) {
  webrtc::MutexLock lock(&mutex_);
  return streams_.contains(ssrc);
}

std::vector<std::vector<uint8_t>> OpusPaddingRegistry::Take(
    uint32_t ssrc,
    uint32_t rtp_timestamp,
    uint32_t duration,
    bool from_packet) {
  webrtc::MutexLock lock(&mutex_);
  std::vector<std::vector<uint8_t>> out;
  auto stream = streams_.find(ssrc);
  if (stream == streams_.end() || (!stream->second.next && !from_packet)) {
    return out;
  }
  Stream& s = stream->second;
  const int64_t start = s.unwrapper.Unwrap(rtp_timestamp);
  const int64_t end = start + duration;
  // Delivery starts at the first chunk played, not at earlier packets.
  const int64_t next = s.next.value_or(start);
  for (auto it = s.paddings.lower_bound(next);
       it != s.paddings.end() && it->first < end;) {
    out.push_back(std::move(it->second));
    it = s.paddings.erase(it);
  }
  // NetEq can repeat or skip timestamps; never deliver a padding twice.
  s.next = std::max(next, end);
  return out;
}

OpusPaddingReader::OpusPaddingReader(
    std::shared_ptr<OpusPaddingRegistry> registry)
    : registry_(std::move(registry)) {}

OpusPaddingReader::~OpusPaddingReader() {
  webrtc::MutexLock lock(&mutex_);
  RemoveStream();
}

void OpusPaddingReader::Transform(
    std::unique_ptr<webrtc::TransformableFrameInterface> frame) {
  const uint32_t ssrc = frame->GetSsrc();
  webrtc::scoped_refptr<webrtc::TransformedFrameCallback> callback;
  {
    webrtc::MutexLock lock(&mutex_);
    if (ssrc_ != ssrc) {
      RemoveStream();
      ssrc_ = ssrc;
    }
    const std::span<const uint8_t> padding = FramePadding(*frame);
    if (!padding.empty()) {
      registry_->Add(ssrc, frame->GetTimestamp(),
                     std::vector<uint8_t>(padding.begin(), padding.end()));
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
  RemoveStream();
}

void OpusPaddingReader::UnregisterTransformedFrameSinkCallback(uint32_t ssrc) {
  webrtc::MutexLock lock(&mutex_);
  sink_callbacks_.erase(ssrc);
  if (ssrc_ == ssrc) {
    RemoveStream();
  }
}

void OpusPaddingReader::RemoveStream() {
  if (ssrc_) {
    registry_->Remove(*ssrc_);
    ssrc_.reset();
  }
}

class OpusPaddingAudioMixer::SourceWrapper : public webrtc::AudioMixer::Source {
 public:
  SourceWrapper(Source* inner, std::shared_ptr<OpusPaddingRegistry> registry)
      : inner_(inner), registry_(std::move(registry)) {}

  AudioFrameInfo GetAudioFrameWithInfo(int sample_rate_hz,
                                       webrtc::AudioFrame* frame) override {
    const auto ssrc = static_cast<uint32_t>(inner_->Ssrc());
    if (!registry_->Has(ssrc)) {
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
    if (chunks.empty()) {
      return info;
    }

    // Before NetEq has decoded a packet, `timestamp_` is not an RTP
    // timestamp, and no packet contributed to the chunk.
    const DeferredChunk& first = chunks.front();
    const std::vector<std::vector<uint8_t>> padding = registry_->Take(
        ssrc, frame->timestamp_,
        static_cast<uint32_t>(first.number_of_frames * kOpusClockRate /
                              first.sample_rate),
        !frame->packet_infos_.empty());
    for (const DeferredChunk& chunk : chunks) {
      if (const std::shared_ptr<NativeAudioSink> sink = chunk.sink.lock()) {
        sink->Deliver(chunk.data.data(), chunk.sample_rate,
                      chunk.number_of_channels, chunk.number_of_frames,
                      padding);
      }
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

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

#include "api/audio/audio_mixer.h"
#include "api/frame_transformer_interface.h"
#include "api/scoped_refptr.h"
#include "rtc_base/numerics/sequence_number_unwrapper.h"
#include "rtc_base/synchronization/mutex.h"
#include "rtc_base/thread_annotations.h"

// Receive side of Opus padding (RFC 6716, 3.2.5).
//
// A reader on each audio receiver records the padding of every received Opus
// packet by RTP timestamp. A mixer decorator learns the RTP timestamp of each
// 10 ms chunk a receive stream plays out, and hands the chunk's track sinks
// the paddings of the packets that start before the chunk ends.

namespace livekit_ffi {

class NativeAudioSink;
class PeerConnectionFactory;
class RtpReceiver;

// The paddings one receiver has read, keyed by RTP timestamp.
class OpusPaddingStore {
 public:
  // A sink's position in the store.
  struct Cursor {
    std::weak_ptr<OpusPaddingStore> store;
    // First RTP timestamp not yet delivered, unwrapped.
    std::optional<int64_t> next;
  };

  void Add(uint32_t rtp_timestamp, std::vector<uint8_t> padding);

  // Appends to `out` the paddings of packets that start before the chunk
  // [rtp_timestamp, rtp_timestamp + duration) ends and were not yet taken
  // through `cursor`.
  void Take(Cursor* cursor,
            uint32_t rtp_timestamp,
            uint32_t duration,
            std::vector<std::vector<uint8_t>>* out);

 private:
  webrtc::Mutex mutex_;
  webrtc::RtpTimestampUnwrapper unwrapper_ RTC_GUARDED_BY(mutex_);
  std::map<int64_t, std::vector<uint8_t>> paddings_ RTC_GUARDED_BY(mutex_);
};

// Finds the store of the receive stream with a given SSRC. One per factory.
class OpusPaddingRegistry {
 public:
  void Register(uint32_t ssrc, const std::shared_ptr<OpusPaddingStore>& store);
  void Unregister(uint32_t ssrc, const OpusPaddingStore* store);
  // Null unless exactly one live store has `ssrc`.
  std::shared_ptr<OpusPaddingStore> Find(uint32_t ssrc);

 private:
  webrtc::Mutex mutex_;
  std::map<uint32_t, std::vector<std::weak_ptr<OpusPaddingStore>>> stores_
      RTC_GUARDED_BY(mutex_);
};

// Reads the padding of each received frame into a store, and passes the
// frame on unchanged.
class OpusPaddingReader : public webrtc::FrameTransformerInterface {
 public:
  explicit OpusPaddingReader(std::shared_ptr<OpusPaddingRegistry> registry);
  ~OpusPaddingReader() override;

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
  void UnregisterStore() RTC_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  const std::shared_ptr<OpusPaddingRegistry> registry_;
  const std::shared_ptr<OpusPaddingStore> store_ =
      std::make_shared<OpusPaddingStore>();
  webrtc::Mutex mutex_;
  webrtc::scoped_refptr<webrtc::TransformedFrameCallback> callback_
      RTC_GUARDED_BY(mutex_);
  std::map<uint32_t, webrtc::scoped_refptr<webrtc::TransformedFrameCallback>>
      sink_callbacks_ RTC_GUARDED_BY(mutex_);
  std::optional<uint32_t> ssrc_ RTC_GUARDED_BY(mutex_);
};

// Wraps the factory's audio mixer. Each source call that has a store defers
// its track sinks' chunks until the call returns and the chunk's RTP
// timestamp is known.
class OpusPaddingAudioMixer : public webrtc::AudioMixer {
 public:
  OpusPaddingAudioMixer(webrtc::scoped_refptr<webrtc::AudioMixer> inner,
                        std::shared_ptr<OpusPaddingRegistry> registry);
  ~OpusPaddingAudioMixer() override;

  bool AddSource(Source* source) override;
  void RemoveSource(Source* source) override;
  void Mix(size_t number_of_channels,
           webrtc::AudioFrame* audio_frame_for_mixing) override;

 private:
  class SourceWrapper;

  const webrtc::scoped_refptr<webrtc::AudioMixer> inner_;
  const std::shared_ptr<OpusPaddingRegistry> registry_;
  webrtc::Mutex mutex_;
  std::map<Source*, std::unique_ptr<SourceWrapper>> wrappers_
      RTC_GUARDED_BY(mutex_);
};

// Called by NativeAudioSink::OnData. True if the chunk was copied, to be
// delivered with its paddings once the current source call returns.
bool DeferSinkChunk(NativeAudioSink* sink,
                    const int16_t* data,
                    int sample_rate,
                    size_t number_of_channels,
                    size_t number_of_frames);

// Reads Opus padding on `receiver`. For E2EE receivers, the frame cryptor
// reads it after decryption instead.
void attach_opus_padding_reader(std::shared_ptr<PeerConnectionFactory> factory,
                                std::shared_ptr<RtpReceiver> receiver);

}  // namespace livekit_ffi

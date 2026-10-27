/*
 * Copyright 2025 LiveKit, Inc.
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

#include "livekit/audio_track.h"

#include <algorithm>
#include <iostream>
#include <iterator>
#include <memory>

#include "api/audio_options.h"
#include "api/audio/audio_frame.h"
#include "api/media_stream_interface.h"
#include "api/task_queue/task_queue_base.h"
#include "audio/remix_resample.h"
#include "common_audio/include/audio_util.h"
#include "livekit/global_task_queue.h"
#include "livekit/rtp_sender.h"
#include "rtc_base/checks.h"
#include "rtc_base/logging.h"
#include "rtc_base/ref_counted_object.h"
#include "rtc_base/synchronization/mutex.h"
#include "rtc_base/time_utils.h"
#include "rust/cxx.h"
#include "webrtc-sys/src/audio_track.rs.h"

namespace livekit_ffi {

inline webrtc::AudioOptions to_native_audio_options(
    const AudioSourceOptions& options) {
  webrtc::AudioOptions rtc_options{};
  rtc_options.echo_cancellation = options.echo_cancellation;
  rtc_options.noise_suppression = options.noise_suppression;
  rtc_options.auto_gain_control = options.auto_gain_control;
  return rtc_options;
}

inline AudioSourceOptions to_rust_audio_options(
    const webrtc::AudioOptions& rtc_options) {
  AudioSourceOptions options{};
  options.echo_cancellation = rtc_options.echo_cancellation.value_or(false);
  options.noise_suppression = rtc_options.noise_suppression.value_or(false);
  options.auto_gain_control = rtc_options.auto_gain_control.value_or(false);
  return options;
}

AudioTrack::AudioTrack(std::shared_ptr<RtcRuntime> rtc_runtime,
                       webrtc::scoped_refptr<webrtc::AudioTrackInterface> track)
    : MediaStreamTrack(rtc_runtime, std::move(track)) {}

AudioTrack::~AudioTrack() {
  webrtc::MutexLock lock(&mutex_);
  for (auto& sink : sinks_) {
    track()->RemoveSink(sink.get());
  }
}

void AudioTrack::add_sink(const std::shared_ptr<NativeAudioSink>& sink) const {
  webrtc::MutexLock lock(&mutex_);
  track()->AddSink(sink.get());
  sinks_.push_back(sink);
}

void AudioTrack::remove_sink(
    const std::shared_ptr<NativeAudioSink>& sink) const {
  webrtc::MutexLock lock(&mutex_);
  track()->RemoveSink(sink.get());
  sinks_.erase(std::remove(sinks_.begin(), sinks_.end(), sink), sinks_.end());
}

NativeAudioSink::NativeAudioSink(rust::Box<AudioSinkWrapper> observer,
                                 int sample_rate,
                                 int num_channels)
    : observer_(std::move(observer)),
      sample_rate_(sample_rate),
      num_channels_(num_channels) {
  frame_.sample_rate_hz_ = sample_rate;
  frame_.num_channels_ = num_channels;
  frame_.samples_per_channel_ = webrtc::SampleRateToDefaultChannelSize(sample_rate);
}

void NativeAudioSink::OnData(const void* audio_data,
                             int bits_per_sample,
                             int sample_rate,
                             size_t number_of_channels,
                             size_t number_of_frames) {
  RTC_CHECK_EQ(16, bits_per_sample);

  const int16_t* data = static_cast<const int16_t*>(audio_data);

  if (sample_rate_ != sample_rate || num_channels_ != number_of_channels) {
    webrtc::InterleavedView<const int16_t> source(data,
                                         number_of_frames,
                                         number_of_channels);
    // resample/remix before capturing
    webrtc::voe::RemixAndResample(source, sample_rate, &resampler_, &frame_);

    rust::Slice<const int16_t> rust_slice(
        frame_.data(), frame_.num_channels() * frame_.samples_per_channel());

    observer_->on_data(rust_slice, frame_.sample_rate_hz(),
                       frame_.num_channels(), frame_.samples_per_channel());

  } else {
    rust::Slice<const int16_t> rust_slice(
        data, number_of_channels * number_of_frames);

    observer_->on_data(rust_slice, sample_rate, number_of_channels,
                       number_of_frames);
  }
}

std::shared_ptr<NativeAudioSink> new_native_audio_sink(
    rust::Box<AudioSinkWrapper> observer,
    int sample_rate,
    int num_channels) {
  return std::make_shared<NativeAudioSink>(std::move(observer), sample_rate,
                                           num_channels);
}

AudioTrackSource::InternalSource::InternalSource(
    const webrtc::AudioOptions& options,
    int sample_rate,
    int num_channels,
    int queue_size_ms,  // must be a multiple of 10ms
    webrtc::TaskQueueFactory* task_queue_factory)
    : options_(options),
      sample_rate_(sample_rate),
      num_channels_(num_channels),
      capture_userdata_(nullptr),
      on_complete_(nullptr),
      tag_transformer_(webrtc::make_ref_counted<AudioFrameTagTransformer>()) {
  if (!queue_size_ms) {
    // Set queue_size_samples_ to 0 so that capture_frame() will get to the fast path.
    queue_size_samples_ = 0;
    return;  // no audio queue
  }

  int samples10ms = sample_rate / 100 * num_channels;

  silence_buffer_.assign(samples10ms, 0);
  padded_buffer_.assign(samples10ms, 0);
  queue_size_samples_ = queue_size_ms / 10 * samples10ms;
  notify_threshold_samples_ = queue_size_samples_;  // TODO: this is currently
                                                    // using x2 the queue size
  buffer_.reserve(queue_size_samples_ + notify_threshold_samples_);

  audio_queue_ =
      task_queue_factory->CreateTaskQueue(
          "AudioSourceCapture", webrtc::TaskQueueFactory::Priority::NORMAL);

  audio_task_ = webrtc::RepeatingTaskHandle::Start(
      audio_queue_.get(),
      [this, samples10ms]() {
        webrtc::MutexLock lock(&mutex_);

        if (buffer_.size() >= samples10ms) {
          SendChunk(buffer_.data(), samples10ms / num_channels_,
                    samples10ms / num_channels_);
          buffer_.erase(buffer_.begin(), buffer_.begin() + samples10ms);
        } else if (!buffer_.empty()) {
          // Underrun mid-stream: send what is left padded with silence rather
          // than holding it back, so audio queued before the gap also plays
          // before it.
          std::copy(buffer_.begin(), buffer_.end(), padded_buffer_.begin());
          std::fill(padded_buffer_.begin() + buffer_.size(),
                    padded_buffer_.end(), 0);
          SendChunk(padded_buffer_.data(), samples10ms / num_channels_,
                    buffer_.size() / num_channels_);
          buffer_.clear();
        } else {
          // Always provide a 10ms frame to avoid playout underruns.
          SendChunk(nullptr, samples10ms / num_channels_, 0);
        }

        if (on_complete_ && buffer_.size() <= notify_threshold_samples_) {
          on_complete_(capture_userdata_);
          on_complete_ = nullptr;
          capture_userdata_ = nullptr;
        }

        return webrtc::TimeDelta::Millis(10);
      },
      webrtc::TaskQueueBase::DelayPrecision::kHigh);
}

AudioTrackSource::InternalSource::~InternalSource() {
}

// `data` is null for a chunk of inserted silence. Only queued samples carry
// tags and advance sent_samples_.
void AudioTrackSource::InternalSource::SendChunk(const int16_t* data,
                                                 size_t samples_per_channel,
                                                 size_t queued_samples) {
  if (next_capture_ms_ < 0) {
    next_capture_ms_ = webrtc::TimeMillis();
  }
  const int64_t capture_ms = next_capture_ms_;
  next_capture_ms_ += 10;

  if (queued_samples > 0) {
    const int64_t chunk_end = sent_samples_ + queued_samples;
    while (!pending_tags_.empty() && pending_tags_.front().sample < chunk_end) {
      PendingTag& tag = pending_tags_.front();
      const int64_t offset = (tag.sample - sent_samples_) * 48000 / sample_rate_;
      tag_transformer_->AddTag(capture_ms, static_cast<uint16_t>(offset),
                               std::move(tag.data));
      pending_tags_.pop_front();
    }
    sent_samples_ = chunk_end;
  }

  constexpr int kBitsPerSample = sizeof(int16_t) * 8;
  for (auto sink : sinks_)
    sink->OnData(data ? data : silence_buffer_.data(), kBitsPerSample,
                 sample_rate_, num_channels_, samples_per_channel, capture_ms);
}

bool AudioTrackSource::InternalSource::capture_frame(
    rust::Slice<const int16_t> data,
    uint32_t sample_rate,
    uint32_t number_of_channels,
    size_t number_of_frames,
    rust::Slice<const uint8_t> tag,
    const SourceContext* ctx,
    void (*on_complete)(const SourceContext*)) {
  webrtc::MutexLock lock(&mutex_);

  if (!tag.empty()) {
    pending_tags_.push_back(
        {sent_samples_ + static_cast<int64_t>(buffer_.size()) / num_channels_,
         std::vector<uint8_t>(tag.begin(), tag.end())});
  }

  if (queue_size_samples_) {
    int available =
        (queue_size_samples_ + notify_threshold_samples_) - buffer_.size();
    if (available < data.size() || on_complete_ || capture_userdata_) {
      if (!tag.empty())
        pending_tags_.pop_back();
      return false;
    }

    buffer_.insert(buffer_.end(), data.begin(), data.end());

    if (buffer_.size() <= notify_threshold_samples_) {
      on_complete(ctx);  // complete directly
    } else {
      on_complete_ = on_complete;
      capture_userdata_ = ctx;
    }

  } else {
    // Fast path: capture directly when the queue buffer is 0 (frame size must be 10ms)
    SendChunk(data.data(), number_of_frames, number_of_frames);
  }

  return true;
}

void AudioTrackSource::InternalSource::clear_buffer() {
  webrtc::MutexLock lock(&mutex_);
  buffer_.clear();
  pending_tags_.clear();
}

webrtc::MediaSourceInterface::SourceState
AudioTrackSource::InternalSource::state() const {
  return webrtc::MediaSourceInterface::SourceState::kLive;
}

bool AudioTrackSource::InternalSource::remote() const {
  return false;
}

const webrtc::AudioOptions AudioTrackSource::InternalSource::options() const {
  webrtc::MutexLock lock(&mutex_);
  return options_;
}

void AudioTrackSource::InternalSource::set_options(
    const webrtc::AudioOptions& options) {
  webrtc::MutexLock lock(&mutex_);
  options_ = options;
}

void AudioTrackSource::InternalSource::AddSink(
    webrtc::AudioTrackSinkInterface* sink) {
  webrtc::MutexLock lock(&mutex_);
  sinks_.push_back(sink);
}

void AudioTrackSource::InternalSource::RemoveSink(
    webrtc::AudioTrackSinkInterface* sink) {
  webrtc::MutexLock lock(&mutex_);
  sinks_.erase(std::remove(sinks_.begin(), sinks_.end(), sink), sinks_.end());
}

AudioTrackSource::AudioTrackSource(AudioSourceOptions options,
                                   int sample_rate,
                                   int num_channels,
                                   int queue_size_ms,
                                   webrtc::TaskQueueFactory* task_queue_factory)
    : source_(webrtc::make_ref_counted<InternalSource>(
          to_native_audio_options(options),
          sample_rate,
          num_channels,
          queue_size_ms,
          task_queue_factory)) {}

AudioSourceOptions AudioTrackSource::audio_options() const {
  return to_rust_audio_options(source_->options());
}

void AudioTrackSource::set_audio_options(
    const AudioSourceOptions& options) const {
  source_->set_options(to_native_audio_options(options));
}

bool AudioTrackSource::capture_frame(
    rust::Slice<const int16_t> audio_data,
    uint32_t sample_rate,
    uint32_t number_of_channels,
    size_t number_of_frames,
    rust::Slice<const uint8_t> tag,
    const SourceContext* ctx,
    void (*on_complete)(const SourceContext*)) const {
  return source_->capture_frame(audio_data, sample_rate, number_of_channels,
                                number_of_frames, tag, ctx, on_complete);
}

void AudioTrackSource::clear_buffer() const {
  source_->clear_buffer();
}

void AudioTrackSource::attach_frame_tags(
    std::shared_ptr<RtpSender> sender) const {
  sender->rtc_sender()->SetEncoderToPacketizerFrameTransformer(
      source_->tag_transformer());
}

std::shared_ptr<AudioTrackSource> new_audio_track_source(
    AudioSourceOptions options,
    int sample_rate,
    int num_channels,
    int queue_size_ms) {
  return std::make_shared<AudioTrackSource>(options, sample_rate, num_channels,
                                            queue_size_ms,
                                            GetGlobalTaskQueueFactory());
}

webrtc::scoped_refptr<AudioTrackSource::InternalSource> AudioTrackSource::get()
    const {
  return source_;
}

}  // namespace livekit_ffi

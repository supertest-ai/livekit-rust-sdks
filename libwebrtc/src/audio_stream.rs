// Copyright 2025 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::imp::audio_stream as stream_imp;

#[cfg(not(target_arch = "wasm32"))]
pub mod native {
    use std::{
        fmt::{Debug, Formatter},
        pin::Pin,
        task::{Context, Poll},
    };

    use tokio_stream::Stream;

    use super::stream_imp;
    use crate::{
        audio_frame::AudioFrame, audio_track::RtcAudioTrack,
        peer_connection_factory::PeerConnectionFactory, rtp_receiver::RtpReceiver,
    };

    const DEFAULT_QUEUE_SIZE_FRAMES: usize = 10;

    #[derive(Clone, Debug, Default)]
    pub struct NativeAudioStreamOptions {
        /// Maximum number of queued WebRTC sink frames after the audio callback.
        ///
        /// Each queued frame corresponds to roughly 10 ms of decoded PCM audio
        /// on the WebRTC sink path.
        ///
        /// `None` uses the default bounded queue size of 10 frames. `Some(0)`
        /// opts into unbounded buffering. Positive values bound the queue, and
        /// the stream drops the oldest queued frames on overflow so latency
        /// stays bounded.
        ///
        /// If your application consumes both audio and video, keep the queue
        /// sizing strategy coordinated across both streams. Using a much larger
        /// queue, or unbounded buffering, for only one of them can increase
        /// end-to-end latency for that stream and cause audio/video drift.
        pub queue_size_frames: Option<usize>,
    }

    pub struct NativeAudioStream {
        pub(crate) handle: stream_imp::NativeAudioStream,
    }

    impl Debug for NativeAudioStream {
        fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
            f.debug_struct("NativeAudioStream").field("track", &self.track()).finish()
        }
    }

    impl NativeAudioStream {
        pub fn new(audio_track: RtcAudioTrack, sample_rate: i32, num_channels: i32) -> Self {
            Self {
                handle: stream_imp::NativeAudioStream::new(
                    audio_track,
                    sample_rate,
                    num_channels,
                    Some(DEFAULT_QUEUE_SIZE_FRAMES),
                ),
            }
        }

        pub fn with_options(
            audio_track: RtcAudioTrack,
            sample_rate: i32,
            num_channels: i32,
            options: NativeAudioStreamOptions,
        ) -> Self {
            Self {
                handle: stream_imp::NativeAudioStream::new(
                    audio_track,
                    sample_rate,
                    num_channels,
                    normalize_queue_size_frames(options.queue_size_frames),
                ),
            }
        }

        pub fn track(&self) -> RtcAudioTrack {
            self.handle.track()
        }

        pub fn close(&mut self) {
            self.handle.close()
        }
    }

    impl Stream for NativeAudioStream {
        type Item = AudioFrame<'static>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.get_mut().handle).poll_next(cx).map(|f| f.map(|f| f.frame))
        }
    }

    /// Decoded audio and the Opus padding (RFC 6716, 3.2.5) of the received
    /// packets whose first sample falls in it.
    #[derive(Debug)]
    pub struct ReceivedAudioFrame {
        pub frame: AudioFrame<'static>,
        /// The padding of each such packet that had padding, in order and
        /// byte for byte.
        pub opus_padding: Vec<Vec<u8>>,
    }

    /// Like [`NativeAudioStream`], and each frame carries the Opus padding of
    /// the received packets whose first sample falls in it.
    ///
    /// Padding is read on audio tracks subscribed in a room with
    /// `receive_opus_padding`; on other tracks it is always empty.
    pub struct NativeReceivedAudioStream {
        handle: stream_imp::NativeAudioStream,
    }

    impl Debug for NativeReceivedAudioStream {
        fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
            f.debug_struct("NativeReceivedAudioStream").field("track", &self.track()).finish()
        }
    }

    impl NativeReceivedAudioStream {
        pub fn new(audio_track: RtcAudioTrack, sample_rate: i32, num_channels: i32) -> Self {
            NativeAudioStream::new(audio_track, sample_rate, num_channels).into()
        }

        pub fn with_options(
            audio_track: RtcAudioTrack,
            sample_rate: i32,
            num_channels: i32,
            options: NativeAudioStreamOptions,
        ) -> Self {
            NativeAudioStream::with_options(audio_track, sample_rate, num_channels, options).into()
        }

        pub fn track(&self) -> RtcAudioTrack {
            self.handle.track()
        }

        pub fn close(&mut self) {
            self.handle.close()
        }
    }

    impl From<NativeAudioStream> for NativeReceivedAudioStream {
        fn from(stream: NativeAudioStream) -> Self {
            Self { handle: stream.handle }
        }
    }

    impl Stream for NativeReceivedAudioStream {
        type Item = ReceivedAudioFrame;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.get_mut().handle).poll_next(cx)
        }
    }

    /// Reads the Opus padding of the audio `receiver` receives, for
    /// [`NativeReceivedAudioStream`]. For E2EE receivers, use
    /// [`FrameCryptor::attach_opus_padding_reader`] instead, which reads it
    /// after decryption.
    ///
    /// [`FrameCryptor::attach_opus_padding_reader`]: crate::native::frame_cryptor::FrameCryptor::attach_opus_padding_reader
    pub fn attach_opus_padding_reader(factory: &PeerConnectionFactory, receiver: &RtpReceiver) {
        stream_imp::attach_opus_padding_reader(factory, receiver)
    }

    fn normalize_queue_size_frames(queue_size_frames: Option<usize>) -> Option<usize> {
        match queue_size_frames {
            None => Some(DEFAULT_QUEUE_SIZE_FRAMES),
            Some(0) => None,
            Some(value) => Some(value),
        }
    }
}

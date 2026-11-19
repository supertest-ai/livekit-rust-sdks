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

    /// Re-chunks received audio to a fixed number of samples. Each Opus
    /// padding goes with the chunk that holds the first sample of the frame it
    /// came with.
    #[doc(hidden)]
    pub struct ReceivedAudioRechunker {
        size: usize,
        buf: Vec<i16>,
        // Paddings, each at the `buf` index where its frame starts.
        opus_padding: Vec<(usize, Vec<u8>)>,
    }

    impl ReceivedAudioRechunker {
        pub fn new(size: usize) -> Self {
            Self { size, buf: Vec::new(), opus_padding: Vec::new() }
        }

        pub fn push(&mut self, data: &[i16], opus_padding: Vec<Vec<u8>>) {
            let start = self.buf.len();
            self.opus_padding.extend(opus_padding.into_iter().map(|p| (start, p)));
            self.buf.extend_from_slice(data);
        }

        /// The next full chunk, and its paddings.
        pub fn pop(&mut self) -> Option<(Vec<i16>, Vec<Vec<u8>>)> {
            if self.buf.len() < self.size {
                return None;
            }
            let chunk = self.buf.drain(..self.size).collect();
            let later = self.opus_padding.partition_point(|(i, _)| *i < self.size);
            let rest = self.opus_padding.split_off(later);
            let taken = std::mem::replace(
                &mut self.opus_padding,
                rest.into_iter().map(|(i, p)| (i - self.size, p)).collect(),
            );
            Some((chunk, taken.into_iter().map(|(_, p)| p).collect()))
        }
    }

    fn normalize_queue_size_frames(queue_size_frames: Option<usize>) -> Option<usize> {
        match queue_size_frames {
            None => Some(DEFAULT_QUEUE_SIZE_FRAMES),
            Some(0) => None,
            Some(value) => Some(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::native::ReceivedAudioRechunker;

    #[test]
    fn rechunker_keeps_padding_with_its_first_sample() {
        let mut r = ReceivedAudioRechunker::new(4);
        r.push(&[0, 1], vec![b"a".to_vec()]);
        assert_eq!(r.pop(), None);
        // "b" starts at index 2 of the first chunk; "c" at index 0 of the second.
        r.push(&[2, 3, 4, 5], vec![b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(
            r.pop(),
            Some((vec![0, 1, 2, 3], vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]))
        );
        // "d" starts at index 2 of the second chunk.
        r.push(&[6, 7, 8, 9, 10, 11], vec![b"d".to_vec()]);
        assert_eq!(r.pop(), Some((vec![4, 5, 6, 7], vec![b"d".to_vec()])));
        assert_eq!(r.pop(), Some((vec![8, 9, 10, 11], vec![])));
        assert_eq!(r.pop(), None);
    }
}

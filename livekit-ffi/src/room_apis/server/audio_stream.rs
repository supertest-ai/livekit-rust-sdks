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

use std::borrow::Cow;
use std::time::Duration;

use futures_util::StreamExt;
use livekit::track::Track;
use livekit::webrtc::{
    audio_stream::native::{NativeAudioStream, NativeAudioStreamOptions, ReceivedAudioFrame},
    prelude::*,
};
use livekit::{registered_audio_filter_plugin, AudioFilterAudioStream, AudioFilterStreamInfo};
use tokio::sync::{broadcast, mpsc, oneshot};

use super::audio_plugin::AudioStreamKind;
use super::room::FfiRoom;
use super::{room::FfiTrack, FfiHandle};
use crate::server::utils;
use crate::{proto, server, FfiError, FfiHandleId, FfiResult};

pub struct FfiAudioStream {
    pub handle_id: FfiHandleId,
    pub stream_type: proto::AudioStreamType,

    #[allow(dead_code)]
    self_dropped_tx: oneshot::Sender<()>, // Close the stream on drop
}

impl FfiHandle for FfiAudioStream {}

impl FfiAudioStream {
    /// Setup a new AudioStream and forward the audio data to the client/the foreign
    /// language.
    ///
    /// When FfiAudioStream is dropped (When the corresponding handle_id is dropped), the task
    /// is being closed.
    ///
    /// It is possible that the client receives an AudioFrame after the task is closed. The client
    /// musts ignore it.
    pub fn from_track(
        server: &'static server::FfiServer,
        new_stream: proto::NewAudioStreamRequest,
    ) -> FfiResult<proto::OwnedAudioStream> {
        let ffi_track = server.retrieve_handle::<FfiTrack>(new_stream.track_handle)?.clone();
        let rtc_track = ffi_track.track.rtc_track();
        let (self_dropped_tx, self_dropped_rx) = oneshot::channel();

        let MediaStreamTrack::Audio(rtc_track) = rtc_track else {
            return Err(FfiError::InvalidRequest("not an audio track".into()));
        };

        let (audio_filter, info) = match &new_stream.audio_filter_module_id {
            Some(module_id) => {
                let Some(room_handle) = ffi_track.room_handle else {
                    return Err(FfiError::InvalidRequest(
                        "this track has no room information".into(),
                    ));
                };
                let room = server.retrieve_handle::<FfiRoom>(room_handle)?.clone();
                let Some(filter) = registered_audio_filter_plugin(module_id) else {
                    return Err(FfiError::InvalidRequest("the audio filter is not found".into()));
                };

                let stream_info = AudioFilterStreamInfo {
                    url: room.inner.url(),
                    room_id: room
                        .inner
                        .room
                        .maybe_sid()
                        .map(|sid| sid.to_string())
                        .unwrap_or("".into()),
                    room_name: room.inner.room.name(),
                    participant_identity: room.inner.room.local_participant().identity().into(),
                    participant_id: room.inner.room.local_participant().name(),
                    track_id: rtc_track.id(),
                };

                (Some(filter), Some(AudioFilterInfo { stream_info, room_handle }))
            }
            None => (None, None),
        };

        let stream_type = new_stream.r#type();
        let handle_id = server.next_id();
        let audio_stream = match stream_type {
            #[cfg(not(target_arch = "wasm32"))]
            proto::AudioStreamType::AudioStreamNative => {
                let audio_stream = Self { handle_id, stream_type, self_dropped_tx };
                let output_sample_rate = new_stream.sample_rate.unwrap_or(48000);
                let num_channels = new_stream.num_channels.unwrap_or(1);
                let options = NativeAudioStreamOptions {
                    queue_size_frames: new_stream
                        .queue_size_frames
                        .map(|capacity| capacity as usize),
                };

                // When the audio filter supports separate rates (v2), it
                // converts from the codec's native rate to the requested
                // output rate, so the WebRTC sink runs at the codec rate.
                let input_sample_rate =
                    if audio_filter.as_ref().is_some_and(|f| f.supports_separate_rates()) {
                        ffi_track.track.codec_clock_rate().unwrap_or(48000)
                    } else {
                        output_sample_rate
                    };

                // Create the filter session (if requested) before the sink,
                // so the sink's rate can depend on whether the filter is
                // actually available.
                let audio_filter_options = new_stream.audio_filter_options.unwrap_or_default();
                let filter_session = match &audio_filter {
                    Some(audio_filter) => {
                        let session = audio_filter.clone().new_session(
                            input_sample_rate,
                            output_sample_rate,
                            &audio_filter_options,
                            info.as_ref().map(|i| i.stream_info.clone()).unwrap(),
                        );
                        if session.is_none() {
                            log::error!("failed to initialize the audio filter. it will not be enabled for this session.");
                        }
                        session
                    }
                    None => None,
                };

                // Without a live filter session (none requested, or creation
                // failed) the sink must run at the requested output rate
                // directly — otherwise codec-rate audio would be forwarded
                // mislabeled as the output rate, dilating it downstream.
                let sink_rate =
                    if filter_session.is_some() { input_sample_rate } else { output_sample_rate };
                let native_stream = NativeAudioStream::with_options(
                    rtc_track,
                    sink_rate as i32,
                    num_channels as i32,
                    options,
                );

                let stream = match filter_session {
                    Some(session) => AudioStreamKind::Filtered(AudioFilterAudioStream::new(
                        native_stream,
                        session,
                        Duration::from_millis(10),
                        input_sample_rate,
                        output_sample_rate,
                        num_channels,
                    )),
                    None => AudioStreamKind::Native(native_stream.into()),
                };

                let handle = server.async_runtime.spawn(Self::native_audio_stream_task(
                    server,
                    handle_id,
                    stream,
                    self_dropped_rx,
                    server.watch_handle_dropped(new_stream.track_handle),
                    true,
                    info,
                    new_stream.frame_size_ms,
                    output_sample_rate.try_into().unwrap(),
                    num_channels.try_into().unwrap(),
                ));
                server.watch_panic(handle);
                Ok::<FfiAudioStream, FfiError>(audio_stream)
            }
            _ => return Err(FfiError::InvalidRequest("unsupported audio stream type".into())),
        }?;

        // Store AudioStreamInfothe new audio stream and return the info
        let info = proto::AudioStreamInfo::from(&audio_stream);
        server.store_handle(handle_id, audio_stream);

        Ok(proto::OwnedAudioStream { handle: proto::FfiOwnedHandle { id: handle_id }, info })
    }

    pub fn from_participant(
        server: &'static server::FfiServer,
        request: proto::AudioStreamFromParticipantRequest,
    ) -> FfiResult<proto::OwnedAudioStream> {
        let (self_dropped_tx, self_dropped_rx) = oneshot::channel();
        let handle_id = server.next_id();
        let stream_type = request.r#type();

        let audio_stream = match stream_type {
            #[cfg(not(target_arch = "wasm32"))]
            proto::AudioStreamType::AudioStreamNative => {
                let audio_stream = Self { handle_id, stream_type, self_dropped_tx };

                let handle = server.async_runtime.spawn(Self::participant_audio_stream_task(
                    server,
                    request,
                    handle_id,
                    self_dropped_rx,
                ));
                server.watch_panic(handle);
                Ok::<FfiAudioStream, FfiError>(audio_stream)
            }
            _ => return Err(FfiError::InvalidRequest("unsupported audio stream type".into())),
        }?;

        // Store the new audio stream and return the info
        let info = proto::AudioStreamInfo::from(&audio_stream);
        server.store_handle(handle_id, audio_stream);

        Ok(proto::OwnedAudioStream { handle: proto::FfiOwnedHandle { id: handle_id }, info })
    }

    async fn participant_audio_stream_task(
        server: &'static server::FfiServer,
        request: proto::AudioStreamFromParticipantRequest,
        stream_handle: FfiHandleId,
        mut self_dropped_rx: oneshot::Receiver<()>,
    ) {
        let ffi_participant =
            utils::ffi_participant_from_handle(server, request.participant_handle);
        let ffi_participant = match ffi_participant {
            Ok(ffi_participant) => ffi_participant,
            Err(err) => {
                log::error!("failed to get participant: {}", err);
                return;
            }
        };

        let track_source = request.track_source();

        let (track_tx, mut track_rx) = mpsc::channel::<Track>(1);
        let (track_finished_tx, _) = broadcast::channel::<Track>(1);
        server.async_runtime.spawn(utils::track_changed_trigger(
            ffi_participant.clone(),
            track_source.into(),
            track_tx,
            track_finished_tx.clone(),
        ));
        // track_tx is no longer held, so the track_rx will be closed when track_changed_trigger is done

        let url = ffi_participant.room.url();
        let room_sid =
            ffi_participant.room.room.maybe_sid().map(|id| id.to_string()).unwrap_or("".into());
        let room_name = ffi_participant.room.room.name();
        let participant_identity = ffi_participant.participant.identity();
        let participant_id = ffi_participant.participant.sid();
        let filter = match &request.audio_filter_module_id {
            Some(module_id) => registered_audio_filter_plugin(module_id),
            None => None,
        };

        loop {
            let track = tokio::select! {
                track = track_rx.recv() => track,
                _ = &mut self_dropped_rx => {
                    break;
                }
            };

            if let Some(track) = track {
                let rtc_track = track.rtc_track();
                let MediaStreamTrack::Audio(rtc_track) = rtc_track else {
                    continue;
                };

                let (c_tx, c_rx) = oneshot::channel::<()>();
                let (handle_dropped_tx, handle_dropped_rx) = oneshot::channel::<()>();
                let (done_tx, mut done_rx) = oneshot::channel::<()>();
                let output_sample_rate = request.sample_rate.unwrap_or(48000) as i32;
                let num_channels = request.num_channels.unwrap_or(1) as i32;
                let track_sid = track.sid();
                let options = NativeAudioStreamOptions {
                    queue_size_frames: request.queue_size_frames.map(|capacity| capacity as usize),
                };

                let mut track_finished_rx = track_finished_tx.subscribe();
                server.async_runtime.spawn(async move {
                    tokio::select! {
                            t = track_finished_rx.recv() => {
                            let Ok(t) = t else {
                                return
                            };
                            if t.sid() == track_sid {
                                handle_dropped_tx.send(()).ok();
                                return
                            }
                        }
                    }
                });

                let input_sample_rate =
                    if filter.as_ref().is_some_and(|f| f.supports_separate_rates()) {
                        track.codec_clock_rate().unwrap_or(48000) as i32
                    } else {
                        output_sample_rate
                    };

                let (mut audio_filter_session, info) = match &filter {
                    Some(filter) => match &request.audio_filter_options {
                        Some(options) => {
                            let stream_info = AudioFilterStreamInfo {
                                url: url.clone(),
                                room_id: room_sid.clone().into(),
                                room_name: room_name.clone(),
                                participant_identity: participant_identity.clone().into(),
                                participant_id: participant_id.clone().into(),
                                track_id: track.sid().into(),
                            };

                            let info = AudioFilterInfo {
                                stream_info,
                                room_handle: ffi_participant.room.handle_id,
                            };

                            let session = filter.clone().new_session(
                                input_sample_rate as u32,
                                output_sample_rate as u32,
                                &options,
                                info.stream_info.clone(),
                            );
                            if session.is_none() {
                                log::error!("failed to initialize the audio filter. it will not be enabled for this session.");
                            }
                            (session, Some(info))
                        }
                        None => (None, None),
                    },
                    None => (None, None),
                };

                // Without a live filter session (none requested, or creation
                // failed) the sink must run at the requested output rate
                // directly — otherwise codec-rate audio would be forwarded
                // mislabeled as the output rate, dilating it downstream.
                let sink_rate = if audio_filter_session.is_some() {
                    input_sample_rate
                } else {
                    output_sample_rate
                };
                let native_stream =
                    NativeAudioStream::with_options(rtc_track, sink_rate, num_channels, options);

                let stream = if let Some(session) = audio_filter_session.take() {
                    let stream = AudioFilterAudioStream::new(
                        native_stream,
                        session,
                        Duration::from_millis(10),
                        input_sample_rate as u32,
                        output_sample_rate as u32,
                        num_channels as u32,
                    );
                    AudioStreamKind::Filtered(stream)
                } else {
                    AudioStreamKind::Native(native_stream.into())
                };

                server.async_runtime.spawn(async move {
                    Self::native_audio_stream_task(
                        server,
                        stream_handle,
                        stream,
                        c_rx,
                        handle_dropped_rx,
                        false,
                        info,
                        request.frame_size_ms,
                        output_sample_rate.try_into().unwrap(),
                        num_channels.try_into().unwrap(),
                    )
                    .await;
                    let _ = done_tx.send(());
                });
                tokio::select! {
                    _ = &mut self_dropped_rx => {
                        let _ = c_tx.send(());
                        break
                    }
                    _ = &mut done_rx => {
                        continue
                    }
                }
            } else {
                // when tracks are done (i.e. the participant leaves the room), we are done
                break;
            }
        }
        if let Err(err) = server.send_event(
            proto::AudioStreamEvent {
                stream_handle: stream_handle,
                message: Some(proto::AudioStreamEos {}.into()),
            }
            .into(),
        ) {
            log::warn!("failed to send audio eos: {}", err);
        }
    }

    async fn native_audio_stream_task(
        server: &'static server::FfiServer,
        stream_handle_id: FfiHandleId,
        mut native_stream: AudioStreamKind,
        mut self_dropped_rx: oneshot::Receiver<()>,
        mut handle_dropped_rx: oneshot::Receiver<()>,
        send_eos: bool,
        mut filter_info: Option<AudioFilterInfo>,
        frame_size_ms: Option<u32>,
        sample_rate: u32,
        num_channels: u32,
    ) {
        let mut rechunker = frame_size_ms.map(|ms| {
            Rechunker::new(sample_rate as usize * ms as usize / 1000 * num_channels as usize)
        });

        loop {
            tokio::select! {
                _ = &mut self_dropped_rx => {
                    break;
                }
                _ = &mut handle_dropped_rx => {
                    break;
                }
                frame = native_stream.next() => {
                    let Some(frame) = frame else {
                        break;
                    };

                    if let Some(ref mut info) = filter_info {
                        if info.stream_info.room_id == "" {
                            // check if room_id is updated
                            if info.update_room_id(server) {
                                if info.stream_info.room_id != "" {
                                    if let AudioStreamKind::Filtered(ref mut filter) = native_stream {
                                        filter.update_stream_info(info.stream_info.clone());
                                    }
                                    // room_id is updated, this check is no longer needed.
                                    filter_info = None;
                                }
                            }
                        }
                    }

                    let ReceivedAudioFrame { frame, opus_padding } = frame;
                    let Some(rechunker) = rechunker.as_mut() else {
                        Self::send_frame(server, stream_handle_id, frame, opus_padding);
                        continue;
                    };

                    for (data, opus_padding) in rechunker.push(&frame.data, opus_padding) {
                        let new_frame = AudioFrame {
                            samples_per_channel: data.len() as u32 / num_channels,
                            data: Cow::Owned(data),
                            sample_rate,
                            num_channels,
                        };
                        Self::send_frame(server, stream_handle_id, new_frame, opus_padding);
                    }
                }
            }
        }
        if send_eos {
            if let Err(err) = server.send_event(
                proto::AudioStreamEvent {
                    stream_handle: stream_handle_id,
                    message: Some(proto::AudioStreamEos {}.into()),
                }
                .into(),
            ) {
                log::warn!("failed to send audio eos: {}", err);
            }
        }
    }
}

impl FfiAudioStream {
    fn send_frame(
        server: &'static server::FfiServer,
        stream_handle_id: FfiHandleId,
        frame: AudioFrame<'static>,
        opus_padding: Vec<Vec<u8>>,
    ) {
        let handle_id = server.next_id();
        let buffer_info = proto::AudioFrameBufferInfo::from(&frame);
        server.store_handle(handle_id, frame);
        if let Err(err) = server.send_event(
            proto::AudioStreamEvent {
                stream_handle: stream_handle_id,
                message: Some(
                    proto::AudioFrameReceived {
                        frame: proto::OwnedAudioFrameBuffer {
                            handle: proto::FfiOwnedHandle { id: handle_id },
                            info: buffer_info,
                        },
                        opus_padding,
                    }
                    .into(),
                ),
            }
            .into(),
        ) {
            server.drop_handle(handle_id);
            log::warn!("failed to send audio frame: {}", err);
        }
    }
}

/// Re-chunks audio to a fixed size. Each Opus padding goes with the chunk
/// that holds the first sample of the input frame it came with.
struct Rechunker {
    size: usize,
    buf: Vec<i16>,
    // Paddings, each at the `buf` index where its input frame starts.
    opus_padding: Vec<(usize, Vec<u8>)>,
}

impl Rechunker {
    fn new(size: usize) -> Self {
        Self { size, buf: Vec::new(), opus_padding: Vec::new() }
    }

    fn push(&mut self, data: &[i16], opus_padding: Vec<Vec<u8>>) -> Vec<(Vec<i16>, Vec<Vec<u8>>)> {
        let start = self.buf.len();
        self.opus_padding.extend(opus_padding.into_iter().map(|p| (start, p)));
        self.buf.extend_from_slice(data);
        let mut out = Vec::new();
        while self.buf.len() >= self.size {
            let chunk = self.buf.drain(..self.size).collect();
            let later = self.opus_padding.partition_point(|(i, _)| *i < self.size);
            let rest = self.opus_padding.split_off(later);
            let taken = std::mem::replace(
                &mut self.opus_padding,
                rest.into_iter().map(|(i, p)| (i - self.size, p)).collect(),
            );
            out.push((chunk, taken.into_iter().map(|(_, p)| p).collect()));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::Rechunker;

    #[test]
    fn rechunker_keeps_padding_with_its_first_sample() {
        let mut r = Rechunker::new(4);
        assert!(r.push(&[0, 1], vec![b"a".to_vec()]).is_empty());
        // "b" starts at index 2 of the first chunk; "c" at index 0 of the second.
        let out = r.push(&[2, 3, 4, 5], vec![b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(
            out,
            vec![(vec![0, 1, 2, 3], vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])]
        );
        let out = r.push(&[6, 7, 8, 9, 10, 11], vec![b"d".to_vec()]);
        // "d" starts at index 2 of the second chunk.
        assert_eq!(
            out,
            vec![(vec![4, 5, 6, 7], vec![b"d".to_vec()]), (vec![8, 9, 10, 11], vec![])]
        );
    }
}

// Used to update audio filter session when the stream info is changed. (Mainly room_id
#[derive(Default)]
struct AudioFilterInfo {
    stream_info: AudioFilterStreamInfo,
    room_handle: FfiHandleId,
}

impl AudioFilterInfo {
    fn update_room_id(&mut self, server: &'static server::FfiServer) -> bool {
        let Ok(room) = server.retrieve_handle::<FfiRoom>(self.room_handle) else {
            return false;
        };
        let room_id = room.inner.room.maybe_sid().map(|id| id.to_string()).unwrap_or("".into());
        if room_id != "" {
            self.stream_info.room_id = room_id.into();
            return true;
        }
        false
    }
}

// Copyright 2026 LiveKit, Inc.
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

//! Opus padding from a native audio source to a subscriber's received frames.
//!
//! The publisher sends bursts of tone, each `TONE` chunks of 10 ms followed by
//! silence, one every `PERIOD` chunks. The first chunk of each burst carries
//! padding naming that chunk. The subscriber finds each burst's onset in the
//! decoded audio and checks that the burst's padding arrived just before it.

#[cfg(feature = "__lk-e2e-test")]
use {
    anyhow::{anyhow, ensure, Context, Result},
    common::{test_rooms, test_rooms_with_options},
    futures_util::StreamExt,
    libwebrtc::{
        audio_source::native::NativeAudioSource,
        audio_stream::native::{NativeReceivedAudioStream, ReceivedAudioFrame},
        prelude::{AudioFrame, AudioSourceOptions, RtcAudioSource},
    },
    livekit::{
        e2ee::{
            key_provider::{KeyProvider, KeyProviderOptions},
            EncryptionType,
        },
        options::TrackPublishOptions,
        prelude::*,
        E2eeOptions,
    },
    std::{collections::BTreeMap, time::Duration},
    tokio::{sync::mpsc::UnboundedReceiver, task::JoinHandle, time::timeout},
};

mod common;

#[cfg(feature = "__lk-e2e-test")]
const RATE: u32 = 48_000;
/// Samples in a 10 ms chunk.
#[cfg(feature = "__lk-e2e-test")]
const CHUNK: usize = (RATE / 100) as usize;
#[cfg(feature = "__lk-e2e-test")]
const PERIOD: u32 = 8;
#[cfg(feature = "__lk-e2e-test")]
const TONE: u32 = 4;
#[cfg(feature = "__lk-e2e-test")]
const CHUNKS: u32 = 50 * PERIOD;
/// Audio the subscriber may conceal while playout starts. Padding of
/// concealed audio is not delivered.
#[cfg(feature = "__lk-e2e-test")]
const STARTUP_CHUNKS: u32 = 7 * PERIOD;
/// Latest a burst's onset may follow the start of the chunk carrying its
/// padding: the chunk (10 ms), the start of the packet holding the burst's
/// first sample (up to 20 ms earlier), Opus's algorithmic delay (6.5 ms), and
/// slack for the jitter buffer stretching audio.
#[cfg(feature = "__lk-e2e-test")]
const MAX_ONSET_DELAY: usize = 40 * CHUNK / 10;
#[cfg(feature = "__lk-e2e-test")]
const LOUD: i16 = 4_000;

/// The padding of chunk `i`: an extension byte, then `i`.
#[cfg(feature = "__lk-e2e-test")]
fn padding(i: u32) -> Vec<u8> {
    let mut p = vec![0xF0];
    p.extend_from_slice(&i.to_be_bytes());
    p
}

#[cfg(feature = "__lk-e2e-test")]
fn chunk(i: u32) -> Vec<i16> {
    if i >= CHUNKS || i % PERIOD >= TONE {
        return vec![0; CHUNK];
    }
    (0..CHUNK)
        .map(|n| {
            let t = (i as usize * CHUNK + n) as f64 / RATE as f64;
            (f64::sin(2.0 * std::f64::consts::PI * 440.0 * t) * 16_000.0) as i16
        })
        .collect()
}

#[cfg(feature = "__lk-e2e-test")]
fn frame(data: &[i16]) -> AudioFrame<'_> {
    AudioFrame {
        data: data.into(),
        sample_rate: RATE,
        num_channels: 1,
        samples_per_channel: CHUNK as u32,
    }
}

/// A published track sending the bursts, then silence until unpublished.
#[cfg(feature = "__lk-e2e-test")]
struct Publication {
    source: NativeAudioSource,
    track: LocalAudioTrack,
    task: JoinHandle<Result<()>>,
}

#[cfg(feature = "__lk-e2e-test")]
impl Publication {
    /// Publishes the bursts, padding the first chunk of each if `pad`.
    async fn new(room: &Room, options: TrackPublishOptions, pad: bool) -> Result<Self> {
        let source = NativeAudioSource::new(AudioSourceOptions::default(), RATE, 1, 100);
        let track =
            LocalAudioTrack::create_audio_track("padded", RtcAudioSource::Native(source.clone()));
        room.local_participant().publish_track(LocalTrack::Audio(track.clone()), options).await?;
        let sender = source.clone();
        let task = tokio::spawn(async move {
            for i in 0.. {
                let data = chunk(i);
                let frame = frame(&data);
                if pad && i < CHUNKS && i % PERIOD == 0 {
                    sender.capture_frame_with_opus_padding(&frame, &padding(i)).await?;
                } else {
                    sender.capture_frame(&frame).await?;
                }
            }
            Ok(())
        });
        Ok(Self { source, track, task })
    }

    async fn unpublish(self, room: &Room) -> Result<()> {
        self.task.abort();
        room.local_participant().unpublish_track(&self.track.sid()).await?;
        Ok(())
    }
}

#[cfg(feature = "__lk-e2e-test")]
async fn subscribe(events: &mut UnboundedReceiver<RoomEvent>) -> Result<NativeReceivedAudioStream> {
    loop {
        let event = events.recv().await.ok_or(anyhow!("never received the track"))?;
        if let RoomEvent::TrackSubscribed { track: RemoteTrack::Audio(track), .. } = event {
            return Ok(NativeReceivedAudioStream::new(track.rtc_track(), RATE as i32, 1));
        }
    }
}

/// A received chunk.
#[cfg(feature = "__lk-e2e-test")]
struct Chunk {
    samples: Vec<i16>,
    opus_padding: Vec<Vec<u8>>,
}

/// Receives the bursts, plus slack for subscribing and the jitter buffer.
#[cfg(feature = "__lk-e2e-test")]
async fn receive(events: &mut UnboundedReceiver<RoomEvent>) -> Result<Vec<Chunk>> {
    let receive = async {
        let mut stream = subscribe(events).await?;
        let mut chunks = Vec::new();
        while chunks.len() < (CHUNKS + 200) as usize {
            let ReceivedAudioFrame { frame, opus_padding } =
                stream.next().await.context("stream ended")?;
            ensure!(frame.samples_per_channel as usize == CHUNK, "expected 10 ms frames");
            chunks.push(Chunk { samples: frame.data.into_owned(), opus_padding });
        }
        Ok(chunks)
    };
    timeout(Duration::from_secs(20), receive).await?
}

/// Checks that every padding arrived once, in order, unchanged, just before
/// its burst's onset, and that only startup padding is missing.
#[cfg(feature = "__lk-e2e-test")]
fn check(chunks: &[Chunk]) -> Result<()> {
    // Where a burst's audio starts: a loud sample after 20 ms without one.
    let loud: Vec<usize> = (chunks.iter().flat_map(|c| &c.samples).enumerate())
        .filter(|(_, s)| s.saturating_abs() > LOUD)
        .map(|(n, _)| n)
        .collect();
    let onsets: Vec<usize> = (loud.iter().enumerate())
        .filter(|&(k, &n)| k == 0 || n - loud[k - 1] > 2 * CHUNK)
        .map(|(_, &n)| n)
        .collect();

    let mut delivered = BTreeMap::new();
    let mut last = None;
    for (c, chunk) in chunks.iter().enumerate() {
        for p in &chunk.opus_padding {
            ensure!(p.len() == 5 && p[0] == 0xF0, "padding {p:02x?} was never sent");
            let i = u32::from_be_bytes(p[1..].try_into()?);
            ensure!(i < CHUNKS && i % PERIOD == 0, "padding for chunk {i} was never sent");
            ensure!(last < Some(i), "padding for chunk {i} after chunk {last:?}");
            last = Some(i);
            delivered.insert(i, c * CHUNK);
        }
    }

    for i in (STARTUP_CHUNKS..CHUNKS).step_by(PERIOD as usize) {
        let at = *delivered.get(&i).with_context(|| format!("padding for chunk {i} missing"))?;
        let onset = onsets.iter().find(|&&o| o >= at).context("no onset after padding")?;
        ensure!(
            onset - at < MAX_ONSET_DELAY,
            "padding for chunk {i} arrived {} ms before its burst",
            (onset - at) / (CHUNK / 10)
        );
    }
    Ok(())
}

#[cfg(feature = "__lk-e2e-test")]
async fn padding_arrives_with_its_audio(red: bool) -> Result<()> {
    let mut rooms = test_rooms(2).await?;
    let (pub_room, _) = rooms.pop().unwrap();
    let (_sub_room, mut sub_events) = rooms.pop().unwrap();
    let options = TrackPublishOptions { red, dtx: false, ..Default::default() };

    // The second track has a new SSRC, and is received after the first
    // receiver's teardown.
    for _ in 0..2 {
        let publication = Publication::new(&pub_room, options.clone(), true).await?;
        check(&receive(&mut sub_events).await?)?;
        publication.unpublish(&pub_room).await?;
    }
    Ok(())
}

#[cfg(feature = "__lk-e2e-test")]
#[test_log::test(tokio::test)]
async fn test_opus_padding() -> Result<()> {
    padding_arrives_with_its_audio(false).await
}

#[cfg(feature = "__lk-e2e-test")]
#[test_log::test(tokio::test)]
async fn test_opus_padding_red() -> Result<()> {
    padding_arrives_with_its_audio(true).await
}

/// With E2EE, the frame cryptor holds each side's only frame transformer:
/// padding can't be sent, and the encrypted audio still plays.
#[cfg(feature = "__lk-e2e-test")]
#[test_log::test(tokio::test)]
async fn test_opus_padding_e2ee() -> Result<()> {
    let options = || {
        let key_provider =
            KeyProvider::with_shared_key(KeyProviderOptions::default(), b"password".to_vec());
        let mut options = RoomOptions::default();
        options.encryption =
            Some(E2eeOptions { key_provider, encryption_type: EncryptionType::Gcm });
        options.into()
    };
    let mut rooms = test_rooms_with_options([options(), options()]).await?;
    let (pub_room, _) = rooms.pop().unwrap();
    let (sub_room, mut sub_events) = rooms.pop().unwrap();
    pub_room.e2ee_manager().set_enabled(true);
    sub_room.e2ee_manager().set_enabled(true);

    let publication = Publication::new(&pub_room, TrackPublishOptions::default(), false).await?;
    let silence = vec![0; CHUNK];
    let padded =
        publication.source.capture_frame_with_opus_padding(&frame(&silence), &padding(0)).await;
    ensure!(padded.is_err(), "padding was accepted on an E2EE track");

    let chunks = receive(&mut sub_events).await?;
    let loud = chunks.iter().flat_map(|c| &c.samples).any(|s| s.saturating_abs() > LOUD);
    ensure!(loud, "no audio received");
    ensure!(chunks.iter().all(|c| c.opus_padding.is_empty()), "padding received");
    publication.unpublish(&pub_room).await
}

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
//! Padding is opt-in on both sides, and best-effort: where no sender carries
//! it, capturing still succeeds and the padding is dropped.

#[cfg(feature = "__lk-e2e-test")]
use {
    anyhow::{anyhow, ensure, Context, Result},
    common::test_rooms_with_options,
    futures_util::{FutureExt, StreamExt},
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
        E2eeOptions, SimulateScenario,
    },
    std::{collections::BTreeMap, ops::Range, time::Duration},
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

/// Chunk `i` of `chunks`.
#[cfg(feature = "__lk-e2e-test")]
fn chunk(i: u32, chunks: u32) -> Vec<i16> {
    if i >= chunks || i % PERIOD >= TONE {
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
    track: LocalAudioTrack,
    task: JoinHandle<Result<()>>,
}

#[cfg(feature = "__lk-e2e-test")]
impl Publication {
    /// Publishes `chunks` chunks of bursts, padding the first chunk of each.
    async fn new(room: &Room, options: TrackPublishOptions, chunks: u32) -> Result<Self> {
        let source = NativeAudioSource::new(AudioSourceOptions::default(), RATE, 1, 100);
        let track =
            LocalAudioTrack::create_audio_track("padded", RtcAudioSource::Native(source.clone()));
        room.local_participant().publish_track(LocalTrack::Audio(track.clone()), options).await?;
        let task = tokio::spawn(async move {
            for i in 0.. {
                let data = chunk(i, chunks);
                let frame = frame(&data);
                if i < chunks && i % PERIOD == 0 {
                    source.capture_frame_with_opus_padding(&frame, &padding(i)).await?;
                } else {
                    source.capture_frame(&frame).await?;
                }
            }
            Ok(())
        });
        Ok(Self { track, task })
    }

    /// Fails if capturing failed.
    fn ensure_sending(&mut self) -> Result<()> {
        if let Some(result) = (&mut self.task).now_or_never() {
            result??;
        }
        Ok(())
    }

    async fn unpublish(mut self, room: &Room) -> Result<()> {
        self.ensure_sending()?;
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

/// Subscribes to the next track and receives `n` chunks of it.
#[cfg(feature = "__lk-e2e-test")]
async fn receive(events: &mut UnboundedReceiver<RoomEvent>, n: u32) -> Result<Vec<Chunk>> {
    let receive = async {
        let mut stream = subscribe(events).await?;
        let mut chunks = Vec::new();
        while chunks.len() < n as usize {
            let ReceivedAudioFrame { frame, opus_padding } =
                stream.next().await.context("stream ended")?;
            ensure!(frame.samples_per_channel as usize == CHUNK, "expected 10 ms frames");
            chunks.push(Chunk { samples: frame.data.into_owned(), opus_padding });
        }
        Ok(chunks)
    };
    timeout(Duration::from_secs(20), receive).await?
}

/// Where each padding arrived, by the chunk it names, checking that it was
/// sent as one of the first `sent` chunks, and arrived once, in order and
/// unchanged.
#[cfg(feature = "__lk-e2e-test")]
fn deliveries(chunks: &[Chunk], sent: u32) -> Result<BTreeMap<u32, usize>> {
    let mut delivered = BTreeMap::new();
    let mut last = None;
    for (c, chunk) in chunks.iter().enumerate() {
        for p in &chunk.opus_padding {
            ensure!(p.len() == 5 && p[0] == 0xF0, "padding {p:02x?} was never sent");
            let i = u32::from_be_bytes(p[1..].try_into()?);
            ensure!(i < sent && i % PERIOD == 0, "padding for chunk {i} was never sent");
            ensure!(last < Some(i), "padding for chunk {i} after chunk {last:?}");
            last = Some(i);
            delivered.insert(i, c * CHUNK);
        }
    }
    Ok(delivered)
}

/// Checks that the padding of every burst starting in `expected` arrived just
/// before the burst's onset.
#[cfg(feature = "__lk-e2e-test")]
fn check(chunks: &[Chunk], delivered: &BTreeMap<u32, usize>, expected: Range<u32>) -> Result<()> {
    // Where a burst's audio starts: a loud sample after 20 ms without one.
    let loud: Vec<usize> = (chunks.iter().flat_map(|c| &c.samples).enumerate())
        .filter(|(_, s)| s.saturating_abs() > LOUD)
        .map(|(n, _)| n)
        .collect();
    let onsets: Vec<usize> = (loud.iter().enumerate())
        .filter(|&(k, &n)| k == 0 || n - loud[k - 1] > 2 * CHUNK)
        .map(|(_, &n)| n)
        .collect();

    for i in expected.step_by(PERIOD as usize) {
        let at = *delivered.get(&i).with_context(|| {
            let near: Vec<_> =
                delivered.range(i.saturating_sub(4 * PERIOD)..i + 4 * PERIOD).collect();
            format!("padding for chunk {i} missing; chunk => sample received nearby: {near:?}")
        })?;
        let onset = onsets.iter().find(|&&o| o >= at).context("no onset after padding")?;
        ensure!(
            onset - at < MAX_ONSET_DELAY,
            "padding for chunk {i} arrived {} ms before its burst",
            (onset - at) / (CHUNK / 10)
        );
    }
    Ok(())
}

/// A publisher's room, then a subscriber's room and its events.
#[cfg(feature = "__lk-e2e-test")]
async fn rooms(
    receive_opus_padding: bool,
    e2ee: bool,
) -> Result<(Room, Room, UnboundedReceiver<RoomEvent>)> {
    let options = || {
        let mut options = RoomOptions::default();
        options.receive_opus_padding = receive_opus_padding;
        if e2ee {
            let key_provider =
                KeyProvider::with_shared_key(KeyProviderOptions::default(), b"password".to_vec());
            options.encryption =
                Some(E2eeOptions { key_provider, encryption_type: EncryptionType::Gcm });
        }
        options.into()
    };
    let mut rooms = test_rooms_with_options([options(), options()]).await?;
    let (pub_room, _) = rooms.pop().unwrap();
    let (sub_room, sub_events) = rooms.pop().unwrap();
    if e2ee {
        pub_room.e2ee_manager().set_enabled(true);
        sub_room.e2ee_manager().set_enabled(true);
    }
    Ok((pub_room, sub_room, sub_events))
}

#[cfg(feature = "__lk-e2e-test")]
fn padded(red: bool) -> TrackPublishOptions {
    TrackPublishOptions { red, dtx: false, opus_padding: true, ..Default::default() }
}

#[cfg(feature = "__lk-e2e-test")]
async fn padding_arrives_with_its_audio(red: bool, e2ee: bool) -> Result<()> {
    let (pub_room, _sub_room, mut sub_events) = rooms(true, e2ee).await?;

    // The second track has a new SSRC, and is received after the first
    // receiver's teardown.
    for _ in 0..2 {
        let publication = Publication::new(&pub_room, padded(red), CHUNKS).await?;
        // Slack for subscribing and the jitter buffer.
        let chunks = receive(&mut sub_events, CHUNKS + 200).await?;
        check(&chunks, &deliveries(&chunks, CHUNKS)?, STARTUP_CHUNKS..CHUNKS)?;
        publication.unpublish(&pub_room).await?;
    }
    Ok(())
}

#[cfg(feature = "__lk-e2e-test")]
#[test_log::test(tokio::test)]
async fn test_opus_padding() -> Result<()> {
    padding_arrives_with_its_audio(false, false).await
}

#[cfg(feature = "__lk-e2e-test")]
#[test_log::test(tokio::test)]
async fn test_opus_padding_red() -> Result<()> {
    padding_arrives_with_its_audio(true, false).await
}

/// With E2EE, frames are padded before encryption and read after decryption.
#[cfg(feature = "__lk-e2e-test")]
#[test_log::test(tokio::test)]
async fn test_opus_padding_e2ee() -> Result<()> {
    padding_arrives_with_its_audio(false, true).await
}

/// Padding is sent only on tracks published with `opus_padding`, and read only
/// in rooms with `receive_opus_padding`; the audio plays either way.
#[cfg(feature = "__lk-e2e-test")]
async fn padding_needs_both_options(publish: bool, read: bool) -> Result<()> {
    let (pub_room, _sub_room, mut sub_events) = rooms(read, false).await?;
    let options = TrackPublishOptions { opus_padding: publish, ..padded(false) };
    let publication = Publication::new(&pub_room, options, CHUNKS).await?;
    let chunks = receive(&mut sub_events, CHUNKS + 200).await?;
    let loud = chunks.iter().flat_map(|c| &c.samples).any(|s| s.saturating_abs() > LOUD);
    ensure!(loud, "no audio received");
    ensure!(chunks.iter().all(|c| c.opus_padding.is_empty()), "padding received");
    publication.unpublish(&pub_room).await
}

#[cfg(feature = "__lk-e2e-test")]
#[test_log::test(tokio::test)]
async fn test_opus_padding_off_when_publishing() -> Result<()> {
    padding_needs_both_options(false, true).await
}

#[cfg(feature = "__lk-e2e-test")]
#[test_log::test(tokio::test)]
async fn test_opus_padding_off_when_receiving() -> Result<()> {
    padding_needs_both_options(true, false).await
}

/// In a full reconnect, the publisher republishes its track, and the subscriber
/// subscribes anew, while the publisher keeps capturing padded audio. Padding
/// resumes on the new subscription.
#[cfg(feature = "__lk-e2e-test")]
async fn padding_resumes_after_full_reconnect(of_publisher: bool) -> Result<()> {
    let (pub_room, sub_room, mut sub_events) = rooms(true, false).await?;
    let mut publication = Publication::new(&pub_room, padded(true), u32::MAX).await?;
    receive(&mut sub_events, 100).await?;

    let room = if of_publisher { &pub_room } else { &sub_room };
    room.simulate_scenario(SimulateScenario::FullReconnect).await?;
    let chunks = receive(&mut sub_events, CHUNKS).await?;
    let delivered = deliveries(&chunks, u32::MAX)?;
    let (first, last) = match (delivered.first_key_value(), delivered.last_key_value()) {
        (Some((&first, _)), Some((&last, _))) => (first, last),
        _ => return Err(anyhow!("no padding after reconnecting")),
    };
    let expected = first + STARTUP_CHUNKS..last;
    ensure!(expected.len() >= (CHUNKS / 2) as usize, "padding resumed only for {expected:?}");
    check(&chunks, &delivered, expected)?;
    publication.ensure_sending()
}

#[cfg(feature = "__lk-e2e-test")]
#[test_log::test(tokio::test)]
async fn test_opus_padding_publisher_full_reconnect() -> Result<()> {
    padding_resumes_after_full_reconnect(true).await
}

#[cfg(feature = "__lk-e2e-test")]
#[test_log::test(tokio::test)]
async fn test_opus_padding_subscriber_full_reconnect() -> Result<()> {
    padding_resumes_after_full_reconnect(false).await
}

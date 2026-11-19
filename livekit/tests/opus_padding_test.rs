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
//! padding naming that chunk. The subscriber checks the first paddings it
//! receives: each is one that was sent, unchanged, in order, and arrives just
//! before its burst's audio. Like frame metadata on video, not every padding
//! has to arrive: audio the subscriber conceals loses its padding.

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
    std::time::Duration,
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
/// Paddings each test checks the placement of.
#[cfg(feature = "__lk-e2e-test")]
const PADDINGS: usize = 10;
/// Paddings a subscription receives while playout starts, whose bursts may not
/// play as sent: only checked for being sent, unchanged and in order.
#[cfg(feature = "__lk-e2e-test")]
const STARTUP_PADDINGS: usize = 7;
/// Latest a burst's onset may follow the start of the chunk carrying its
/// padding: the chunk (10 ms), the start of the packet holding the burst's
/// first sample (up to 20 ms earlier), Opus's algorithmic delay (6.5 ms), and
/// slack for the jitter buffer stretching audio. The padding and the onset
/// are decoded from the same packet, so system load doesn't change this.
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
    if i % PERIOD >= TONE {
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

/// A published track sending the bursts until unpublished.
#[cfg(feature = "__lk-e2e-test")]
struct Publication {
    track: LocalAudioTrack,
    task: JoinHandle<Result<()>>,
}

#[cfg(feature = "__lk-e2e-test")]
impl Publication {
    /// Publishes the bursts, padding the first chunk of each.
    async fn new(room: &Room, options: TrackPublishOptions) -> Result<Self> {
        let source = NativeAudioSource::new(AudioSourceOptions::default(), RATE, 1, 100);
        let track =
            LocalAudioTrack::create_audio_track("padded", RtcAudioSource::Native(source.clone()));
        room.local_participant().publish_track(LocalTrack::Audio(track.clone()), options).await?;
        let task = tokio::spawn(async move {
            for i in 0.. {
                let data = chunk(i);
                let frame = frame(&data);
                if i % PERIOD == 0 {
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

/// Waits for the next subscribed audio track.
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

/// Receives chunks of the next subscribed track until `done` holds for them.
#[cfg(feature = "__lk-e2e-test")]
async fn receive(
    events: &mut UnboundedReceiver<RoomEvent>,
    done: impl Fn(&[Chunk]) -> bool,
) -> Result<Vec<Chunk>> {
    let receive = async {
        let mut stream = subscribe(events).await?;
        let mut chunks = Vec::new();
        while !done(&chunks) {
            let ReceivedAudioFrame { frame, opus_padding } =
                stream.next().await.context("stream ended")?;
            ensure!(frame.samples_per_channel as usize == CHUNK, "expected 10 ms frames");
            chunks.push(Chunk { samples: frame.data.into_owned(), opus_padding });
        }
        Ok(chunks)
    };
    timeout(Duration::from_secs(20), receive).await?
}

/// Receives chunks until `PADDINGS` paddings after start-up have arrived, and
/// their bursts.
#[cfg(feature = "__lk-e2e-test")]
async fn receive_paddings(events: &mut UnboundedReceiver<RoomEvent>) -> Result<Vec<Chunk>> {
    receive(events, |chunks| {
        // The chunk where the last padding to check arrived.
        let mut paddings = 0;
        let nth = chunks.iter().position(|c| {
            paddings += c.opus_padding.len();
            paddings >= STARTUP_PADDINGS + PADDINGS
        });
        nth.is_some_and(|c| chunks.len() > c + PERIOD as usize)
    })
    .await
}

/// Checks that each padding is one that was sent, unchanged, that they are in
/// order, and that each after start-up arrived just before its burst's onset.
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

    let mut last = None;
    let mut received = 0;
    for (c, chunk) in chunks.iter().enumerate() {
        for p in &chunk.opus_padding {
            ensure!(p.len() == 5 && p[0] == 0xF0, "padding {p:02x?} was never sent");
            let i = u32::from_be_bytes(p[1..].try_into()?);
            ensure!(i % PERIOD == 0, "padding for chunk {i} was never sent");
            ensure!(last < Some(i), "padding for chunk {i} after chunk {last:?}");
            last = Some(i);

            // Skip start-up, and bursts not yet received.
            let n = received;
            received += 1;
            if n < STARTUP_PADDINGS || c + PERIOD as usize >= chunks.len() {
                continue;
            }
            let at = c * CHUNK;
            let onset = onsets.iter().find(|&&o| o >= at).context("no onset after padding")?;
            ensure!(
                onset - at < MAX_ONSET_DELAY,
                "padding for chunk {i} arrived {} ms before its burst",
                (onset - at) / (CHUNK / 10)
            );
        }
    }
    ensure!(received >= STARTUP_PADDINGS + PADDINGS, "only {received} paddings received");
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
        let publication = Publication::new(&pub_room, padded(red)).await?;
        check(&receive_paddings(&mut sub_events).await?)?;
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
    let publication = Publication::new(&pub_room, options).await?;
    let chunks = receive(&mut sub_events, |chunks| chunks.len() >= 200).await?;
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
    let mut publication = Publication::new(&pub_room, padded(true)).await?;
    check(&receive_paddings(&mut sub_events).await?)?;

    let room = if of_publisher { &pub_room } else { &sub_room };
    room.simulate_scenario(SimulateScenario::FullReconnect).await?;
    check(&receive_paddings(&mut sub_events).await?)?;
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

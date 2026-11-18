---
webrtc-sys: minor
libwebrtc: minor
livekit: minor
livekit-ffi: minor
---

Carry caller-supplied bytes in the Opus padding of audio frames: `NativeAudioSource::capture_frame_with_opus_padding` (`CaptureAudioFrameRequest.opus_padding` over FFI) writes them into the encoded frame, and `NativeReceivedAudioStream` yields each received packet's padding with the frame its first sample falls in (`AudioFrameReceived.opus_padding` over FFI). Opt-in on each side: `TrackPublishOptions::opus_padding` pads a track and `RoomOptions::receive_opus_padding` reads subscribed audio (both off by default); with E2EE, padding is written before encryption and read after decryption.

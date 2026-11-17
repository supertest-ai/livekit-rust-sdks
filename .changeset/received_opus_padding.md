---
webrtc-sys: minor
libwebrtc: minor
livekit: minor
livekit-ffi: minor
---

Expose the Opus padding of received audio: `NativeReceivedAudioStream` yields `ReceivedAudioFrame { frame, opus_padding }` with the padding of each packet whose first sample falls in the frame (`AudioFrameReceived.opus_padding` over FFI). Not read on E2EE tracks.

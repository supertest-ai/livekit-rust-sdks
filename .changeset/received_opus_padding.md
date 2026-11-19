---
webrtc-sys: minor
libwebrtc: minor
livekit: minor
livekit-ffi: minor
---

Expose the Opus padding of received audio: `NativeReceivedAudioStream` yields `ReceivedAudioFrame { frame, opus_padding }` with the padding of each packet whose first sample falls in the frame (`AudioFrameReceived.opus_padding` over FFI), in rooms with `RoomOptions::receive_opus_padding`, including on E2EE tracks, where it is read after decryption. Padding is written only on tracks published with `TrackPublishOptions::opus_padding`, before encryption on E2EE tracks, and capturing with padding no longer fails while no sender carries it.

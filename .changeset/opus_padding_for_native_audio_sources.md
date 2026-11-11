---
webrtc-sys: minor
libwebrtc: minor
livekit: patch
livekit-ffi: minor
---

Add `NativeAudioSource::capture_frame_with_opus_padding` (`CaptureAudioFrameRequest.opus_padding` over FFI), which writes caller-supplied bytes verbatim as the Opus padding of the encoded frame.

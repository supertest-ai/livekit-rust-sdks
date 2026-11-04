---
webrtc-sys: minor
libwebrtc: minor
livekit: patch
livekit-ffi: minor
---

`NativeAudioSource::capture_frame_with_opus_padding` writes caller-supplied bytes verbatim as the Opus padding (RFC 6716, section 3.2.5) of the encoded frame holding the captured frame's first sample, so receivers can read them in an encoded transform while every Opus decoder decodes the frame unchanged. FFI clients pass them as `CaptureAudioFrameRequest.opus_padding`. Published native audio tracks attach the padding transformer unless the room uses end-to-end encryption, and while attached, the source stamps its audio with capture times, which libwebrtc sends as `abs-capture-time`.

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

#[cxx::bridge(namespace = "livekit_ffi")]
pub mod ffi {
    extern "C++" {
        include!("livekit/peer_connection_factory.h");
        include!("livekit/rtp_receiver.h");

        type PeerConnectionFactory = crate::peer_connection_factory::ffi::PeerConnectionFactory;
        type RtpReceiver = crate::rtp_receiver::ffi::RtpReceiver;
    }

    unsafe extern "C++" {
        include!("livekit/opus_padding_receiver.h");

        /// Reads the Opus padding of the audio `receiver` receives. For E2EE
        /// receivers, the frame cryptor reads it after decryption instead.
        fn attach_opus_padding_reader(
            factory: SharedPtr<PeerConnectionFactory>,
            receiver: SharedPtr<RtpReceiver>,
        );
    }
}

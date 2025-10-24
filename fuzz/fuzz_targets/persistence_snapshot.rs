#![no_main]

use libfuzzer_sys::fuzz_target;
use rkvs::{namespace::Namespace, NamespaceSnapshot};

fuzz_target!(|data: &[u8]| {
    // The goal of this fuzzer is to test the deserialization and loading
    // of a NamespaceSnapshot. This is a critical security boundary.

    // 1. Attempt to deserialize the raw bytes into a NamespaceSnapshot.
    //    `bincode` is generally safe, but this ensures it doesn't panic on
    //    any byte sequence.
    if let Ok(snapshot) = bincode::deserialize::<NamespaceSnapshot>(data) {
        // 2. If deserialization succeeds, the `snapshot` struct is now populated
        //    with potentially garbage, but type-correct, data.
        //    Now, we test if the `Namespace::from_snapshot` constructor can
        //    safely handle this "valid but nonsensical" state without panicking.
        //    It should ideally return an Err or handle it gracefully.
        let _ = Namespace::from_snapshot(snapshot);
    }
});
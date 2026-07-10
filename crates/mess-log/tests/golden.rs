//! GOLDEN-FILE test: the encoder's byte output MUST match fixtures derived
//! **by hand from the spec tables** of `docs/spec/01-log-format.md` §4.
//!
//! The fixtures below were computed offset-by-offset from the spec tables, and
//! the two `batch_crc` fields were computed by an **independent** CRC32C
//! implementation (a reflected bit-serial Castagnoli reference, self-checked
//! against the canonical check value `CRC32C("123456789") == 0xE3069283`) —
//! NOT by the `crc32c` crate the encoder uses. So a match here confirms the
//! crate and the reference agree on the R4 split coverage
//! (`batch[0..68] ++ batch[72..total_len-4]`, D-FMT-8), not merely that the
//! encoder agrees with itself.
//!
//! Because D-FMT-8's split coverage differs bit-for-bit from the spikes'
//! copy-and-zero CRC, these fixtures cannot be, and are not, taken from any
//! spike test vector.

use mess_log::encode::{BatchEncoder, BatchInput, Subframe};
use mess_log::format::*;

/// Decode a compact hex string to bytes (no external dep).
fn hx(s: &str) -> Vec<u8> {
    let s: Vec<u8> = s.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    assert_eq!(s.len() % 2, 0, "odd hex length");
    s.chunks(2)
        .map(|c| {
            let hi = (c[0] as char).to_digit(16).expect("hex digit") as u8;
            let lo = (c[1] as char).to_digit(16).expect("hex digit") as u8;
            (hi << 4) | lo
        })
        .collect()
}

/// Fixture 1 — the §4.7 worked example: one uncompressed 12-byte subframe
/// (payload `00 01 .. 0b`), no crypto chain, `segment_epoch = 1`, everything
/// else `0`. `total_len = 128`.
///
/// Offset-by-offset (spec §4.2 / §4.3 / §4.5 tables):
/// ```text
/// [ 0.. 4) magic            = ad 4e 7c ba  (0xBA7C4EAD LE)
/// [ 4.. 6) format_version   = 03 00        (3)
/// [ 6.. 8) flags            = 00 00        (0, no crypto chain)
/// [ 8..12) frame_count      = 01 00 00 00  (1, A5 satisfied)
/// [12..20) batch_id         = 0            (per-segment, D-FMT-5)
/// [20..28) total_len        = 80 00 …      (128)
/// [28..36) first_global_pos = 0            (A1 seed)
/// [36..44) segment_epoch    = 01 00 …      (1, A9)
/// [44..52) stream_id        = 0
/// [52..60) category_id      = 0
/// [60..68) first_stream_ver = 0
/// [68..72) batch_crc        = a3 47 5c 29  (0x295C47A3, R4 split)
/// [72..76) event_type_id    = 11 00 00 00  (0x11)
/// [76..78) schema_version   = 00 00
/// [78..80) codec_id         = 00 00
/// [80]     compression_id   = 00
/// [81]     subframe_flags   = 00
/// [82..84) dict_id          = 00 00
/// [84..88) uncompressed_len = 0c 00 00 00  (12)
/// [88..92) compressed_len   = 0c 00 00 00  (12)
/// [92..96) metadata_len     = 00 00 00 00  (0)
/// [96..100) data_len        = 0c 00 00 00  (12)
/// [100..112) payload        = 00 01 .. 0b
/// [112..116) marker magic   = ed 17 aa c0  (0xC0AA17ED LE)
/// [116..124) total_len_echo = 80 00 …      (128, A3)
/// [124..128) batch_crc_echo = a3 47 5c 29  (== batch_crc, A3)
/// ```
const F1_HEX: &str = "ad4e7cba03000000010000000000000000000000800000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000a3475c291100000000000000000000000c0000000c000000000000000c000000000102030405060708090a0bed17aac08000000000000000a3475c29";

/// Fixture 2 — a **multi-frame** batch (2 subframes), exercising every stamped
/// field and a per-subframe metadata/data split. `segment_epoch = 7`,
/// `batch_id = 3`, `first_global_pos = 100`, `stream_id = 42`,
/// `category_id = 9`, `first_stream_version = 5`. Frame 0: `event_type_id
/// 0xAABBCCDD`, `schema_version 2`, `codec_id 1`, payload "hello" (5 bytes).
/// Frame 1: `event_type_id 0x01020304`, `schema_version 1`, `codec_id 1`,
/// `dict_id 3`, payload "world!!" (7 bytes) split `metadata_len 2` /
/// `data_len 5`. `total_len = 72 + (28+5) + (28+7) + 16 = 156`.
const F2_HEX: &str = "ad4e7cba030000000200000003000000000000009c00000000000000640000000000000007000000000000002a0000000000000009000000000000000500000000000000c9e5bf1fddccbbaa02000100000000000500000005000000000000000500000068656c6c6f04030201010001000000030007000000070000000200000005000000776f726c642121ed17aac09c00000000000000c9e5bf1f";

/// Fixture 3 — the **with-crypto_chain** layout (§4.4, §4.7 remark). `flags`
/// bit 0 set, 32 bytes of `0xC0` at `[72, 104)`, one 4-byte subframe
/// (`de ad be ef`) shifted to `[104, 132)+payload`. `segment_epoch = 2`,
/// `batch_id = 1`, `first_global_pos = 50`, `stream_id = 5`, `category_id = 1`,
/// `first_stream_version = 10`. `total_len = 72 + 32 + (28+4) + 16 = 152`.
/// The crypto chain bytes ARE inside the R4 CRC coverage (split point is 72,
/// right after the header `batch_crc`).
const F3_HEX: &str = "ad4e7cba03000100010000000100000000000000980000000000000032000000000000000200000000000000050000000000000001000000000000000a000000000000000a61d8adc0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c009000000000000000000000004000000040000000000000004000000deadbeefed17aac098000000000000000a61d8ad";

#[test]
fn golden_fixture_1_single_frame() {
    let payload: Vec<u8> = (0u8..12).collect();
    let sfs = [Subframe::plain(0x11, 0, 0, &payload)];
    let input = BatchInput {
        segment_epoch:        1,
        batch_id:             0,
        first_global_pos:     0,
        stream_id:            0,
        category_id:          0,
        first_stream_version: 0,
        crypto_chain:         None,
        subframes:            &sfs,
    };
    let mut enc = BatchEncoder::new();
    let got = enc.encode(&input).unwrap();
    let want = hx(F1_HEX);
    assert_eq!(want.len(), 128);
    assert_eq!(
        got,
        &want[..],
        "1-frame batch bytes must match the spec-derived golden"
    );

    // Spot-check the load-bearing offsets against the spec tables directly.
    assert_eq!(&got[0..4], &HEADER_MAGIC.to_le_bytes());
    assert_eq!(&got[4..6], &FORMAT_VERSION.to_le_bytes());
    assert_eq!(u64::from_le_bytes(got[20..28].try_into().unwrap()), 128); // total_len
    assert_eq!(u64::from_le_bytes(got[36..44].try_into().unwrap()), 1); // segment_epoch (A9)
    assert_eq!(&got[112..116], &MARKER_MAGIC.to_le_bytes());
    assert_eq!(u64::from_le_bytes(got[116..124].try_into().unwrap()), 128); // total_len_echo
    // A3: batch_crc == batch_crc_echo, and both are the R4 split value.
    assert_eq!(&got[68..72], &got[124..128]);
}

#[test]
fn golden_fixture_2_multi_frame() {
    let pa = b"hello";
    let pb = b"world!!";
    let sfs = [
        Subframe {
            event_type_id:    0xAABB_CCDD,
            schema_version:   2,
            codec_id:         1,
            compression_id:   0,
            dict_id:          0,
            uncompressed_len: 5,
            metadata_len:     0,
            data_len:         5,
            payload:          pa,
        },
        Subframe {
            event_type_id:    0x0102_0304,
            schema_version:   1,
            codec_id:         1,
            compression_id:   0,
            dict_id:          3,
            uncompressed_len: 7,
            metadata_len:     2,
            data_len:         5,
            payload:          pb,
        },
    ];
    let input = BatchInput {
        segment_epoch:        7,
        batch_id:             3,
        first_global_pos:     100,
        stream_id:            42,
        category_id:          9,
        first_stream_version: 5,
        crypto_chain:         None,
        subframes:            &sfs,
    };
    let mut enc = BatchEncoder::new();
    let got = enc.encode(&input).unwrap();
    let want = hx(F2_HEX);
    assert_eq!(want.len(), 156);
    assert_eq!(got, &want[..], "multi-frame batch bytes must match the golden");
    assert_eq!(u32::from_le_bytes(got[8..12].try_into().unwrap()), 2); // frame_count
}

#[test]
fn golden_fixture_3_with_crypto_chain() {
    let chain = [0xC0u8; CHAIN_LEN];
    let payload = [0xDEu8, 0xAD, 0xBE, 0xEF];
    let sfs = [Subframe::plain(9, 0, 0, &payload)];
    let input = BatchInput {
        segment_epoch:        2,
        batch_id:             1,
        first_global_pos:     50,
        stream_id:            5,
        category_id:          1,
        first_stream_version: 10,
        crypto_chain:         Some(&chain),
        subframes:            &sfs,
    };
    let mut enc = BatchEncoder::new();
    let got = enc.encode(&input).unwrap();
    let want = hx(F3_HEX);
    assert_eq!(want.len(), 152);
    assert_eq!(got, &want[..], "with-chain batch bytes must match the golden");

    // flags bit 0 set; chain occupies [72, 104); subframe starts at 104.
    assert_eq!(
        u16::from_le_bytes(got[6..8].try_into().unwrap()),
        FLAG_CRYPTO_CHAIN
    );
    assert_eq!(&got[72..104], &chain[..]);
}

/// The three fixtures were computed with copy-and-zero disabled; prove the
/// encoder's CRC would NOT match a copy-and-zero CRC (guards against a
/// regression that reintroduces the spike approach, D-FMT-8 / R4).
#[test]
fn golden_crc_is_not_the_copy_and_zero_value() {
    let got = hx(F1_HEX);
    let n = got.len();
    // copy-and-zero: zero both crc fields, hash the whole contiguous span.
    let mut zeroed = got.clone();
    zeroed[HEADER_CRC_OFF..HEADER_CRC_OFF + 4].fill(0);
    zeroed[n - 4..].fill(0);
    let copy_and_zero = crc32c::crc32c(&zeroed).to_le_bytes();
    assert_ne!(&got[HEADER_CRC_OFF..HEADER_CRC_OFF + 4], &copy_and_zero);
}

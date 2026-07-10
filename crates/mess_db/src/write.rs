use std::borrow::Cow;

use ident::Id;

use crate::{ExpectedVersion, StreamPos};

#[derive(Clone, Debug)]
pub struct WriteMessageOld<'a, D, M> {
    pub id:                       Id,
    pub stream_name:              Cow<'a, str>,
    pub message_type:             Cow<'a, str>,
    pub data:                     D,
    pub metadata:                 Option<M>,
    pub expected_stream_position: Option<StreamPos>,
}

/// A single-event append. Convenience wrapper over [`WriteMessages`] for the
/// common one-event case; converts into a one-event batch.
#[derive(Clone, Debug)]
pub struct WriteMessage<'a> {
    pub id:               Id,
    pub stream_name:      Cow<'a, str>,
    pub message_type:     Cow<'a, str>,
    pub data:             Cow<'a, [u8]>,
    pub metadata:         Cow<'a, [u8]>,
    pub expected_version: ExpectedVersion,
}

/// One event inside a [`WriteMessages`] batch. The stream and the
/// expected-version precondition are batch-level, so an event carries only its
/// own identity and payload.
#[derive(Clone, Debug)]
pub struct WriteEvent<'a> {
    pub id:           Id,
    pub message_type: Cow<'a, str>,
    pub data:         Cow<'a, [u8]>,
    pub metadata:     Cow<'a, [u8]>,
}

/// An atomic multi-event append to a single stream: ONE expected-version check,
/// N events, written in ONE `rocksdb::WriteBatch`. No partial/torn append is
/// possible — either every event lands or none does.
#[derive(Clone, Debug)]
pub struct WriteMessages<'a> {
    pub stream_name:      Cow<'a, str>,
    pub expected_version: ExpectedVersion,
    pub events:           Vec<WriteEvent<'a>>,
}

impl<'a> From<WriteMessage<'a>> for WriteMessages<'a> {
    fn from(msg: WriteMessage<'a>) -> Self {
        WriteMessages {
            stream_name:      msg.stream_name,
            expected_version: msg.expected_version,
            events:           vec![WriteEvent {
                id:           msg.id,
                message_type: msg.message_type,
                data:         msg.data,
                metadata:     msg.metadata,
            }],
        }
    }
}

/// Owned form of [`WriteEvent`], for the actor request that crosses the
/// channel into the actor task.
#[derive(Clone, Debug)]
pub struct OwnedWriteEvent {
    pub id:           Id,
    pub message_type: String,
    pub data:         Vec<u8>,
    pub metadata:     Vec<u8>,
}

/// Owned form of [`WriteMessages`], for the actor request.
#[derive(Clone, Debug)]
pub struct OwnedWriteMessages {
    pub stream_name:      String,
    pub expected_version: ExpectedVersion,
    pub events:           Vec<OwnedWriteEvent>,
}

impl From<WriteMessages<'_>> for OwnedWriteMessages {
    fn from(batch: WriteMessages<'_>) -> Self {
        OwnedWriteMessages {
            stream_name:      batch.stream_name.into_owned(),
            expected_version: batch.expected_version,
            events:           batch
                .events
                .into_iter()
                .map(|e| OwnedWriteEvent {
                    id:           e.id,
                    message_type: e.message_type.into_owned(),
                    data:         e.data.into_owned(),
                    metadata:     e.metadata.into_owned(),
                })
                .collect(),
        }
    }
}

impl From<OwnedWriteMessages> for WriteMessages<'_> {
    fn from(batch: OwnedWriteMessages) -> Self {
        WriteMessages {
            stream_name:      batch.stream_name.into(),
            expected_version: batch.expected_version,
            events:           batch
                .events
                .into_iter()
                .map(|e| WriteEvent {
                    id:           e.id,
                    message_type: e.message_type.into(),
                    data:         e.data.into(),
                    metadata:     e.metadata.into(),
                })
                .collect(),
        }
    }
}

//! The [`Event`] trait: the domain-event vocabulary.

use crate::error::CodecError;

/// A domain event: a stable name plus a wire codec.
///
/// This is the DX seam ported verbatim from the `dx_api` spike. It is
/// deliberately codec-agnostic: `name` is the durable message-type string a
/// backend stores alongside the payload, and `encode`/`decode` move the
/// payload to and from bytes using whatever serialization the implementer
/// chooses. The registry's interned `event_type_id` / `codec_id` (see
/// `docs/spec/04-registry.md`) are a *backend* concern; this trait is the
/// application-facing surface.
///
/// For the spike this is implemented by hand on an enum. The north star is
/// `#[derive(Event)]` (bn-hy7) generating exactly this impl — so the surface
/// is kept small and mechanical: one name per variant, one `encode`, one
/// `decode` that dispatches on the stored name.
pub trait Event: Sized + Send + Sync + 'static {
    /// Stable, unique name for this event, stored as the message type
    /// (e.g. `"account.opened"`).
    fn name(&self) -> &'static str;

    /// Serialize the event payload for storage.
    fn encode(&self) -> Result<Vec<u8>, CodecError>;

    /// Deserialize an event from its stored name and payload.
    fn decode(name: &str, data: &[u8]) -> Result<Self, CodecError>;
}

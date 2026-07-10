//! [`RegistryState`]: the pure in-memory state machine that `$registry`
//! replay folds into (`docs/spec/04-registry.md` §4-§7).
//!
//! This is deliberately backend-free: [`apply`](RegistryState::apply) takes
//! one already-decoded [`RegistryRecord`] and either advances the state or
//! returns a typed [`RegistryError`] — nothing here reads or writes bytes,
//! so it is trivial to drive from a live [`Backend`](crate::backend::Backend)
//! replay (see [`super::Registry`]) or directly from a hand-built list of
//! records in a test, which is what makes the acyclicity proof
//! (§7.1: "step 2 never reads its own in-progress table") checkable in code.

use std::collections::HashMap;

use super::codec::{
    RECORD_KIND_CATEGORY_REGISTERED, RECORD_KIND_DICT_REGISTERED,
    RECORD_KIND_EVENT_TYPE_REGISTERED, RECORD_KIND_STREAM_REGISTERED,
    RegistryRecord, TARGET_KIND_CATEGORY, TARGET_KIND_EVENT_TYPE,
    TARGET_KIND_STREAM,
};
use super::error::RegistryError;

/// The four reserved IDs (REG1), out-of-band of any log event.
pub const RESERVED_STREAM_ID: u64 = 0;
pub const RESERVED_CATEGORY_ID: u64 = 0;
pub const RESERVED_EVENT_TYPE_ID: u32 = 0;
/// `dict_id 0` means "no dictionary" (§3.8) — not a fourth reserved *object*
/// the way the other three are, but the same "never a `*Registered` record"
/// rule applies (there is no `DictRegistered { dict_id: 0, .. }`).
pub const RESERVED_DICT_ID: u16 = 0;

/// The reserved name for `stream_id 0` (REG1).
pub const RESERVED_STREAM_NAME: &str = "$registry";
/// The reserved name for `category_id 0` (REG1).
pub const RESERVED_CATEGORY_NAME: &str = "$system";
/// The reserved name for `event_type_id 0` (REG1).
pub const RESERVED_EVENT_TYPE_NAME: &str = "RegistryEventV1";

/// A bidirectional name<->id table for one namespace.
///
/// - `id -> name` is single-valued and always the *most recent* name (REG15): a
///   fresh registration sets it, and each `NameAliased` overwrites it.
/// - `name -> id` is permanent (REG16): once bound, a name is never rebound to
///   a different ID, even after the ID's current name has moved on.
#[derive(Debug, Clone, Default)]
struct NameTable<Id> {
    id_to_name: HashMap<Id, String>,
    name_to_id: HashMap<String, Id>,
}

impl<Id> NameTable<Id>
where
    Id: Copy + Eq + std::hash::Hash + Into<u64>,
{
    fn contains(&self, id: Id) -> bool { self.id_to_name.contains_key(&id) }

    fn current_name(&self, id: Id) -> Option<&str> {
        self.id_to_name.get(&id).map(String::as_str)
    }

    fn id_by_name(&self, name: &str) -> Option<Id> {
        self.name_to_id.get(name).copied()
    }

    /// First-ever registration of `id` under `name`. Caller has already
    /// checked `id` is not reserved and not already registered (REG14).
    fn register<E>(
        &mut self,
        namespace: &'static str,
        id: Id,
        name: String,
    ) -> Result<(), RegistryError<E>> {
        if let Some(bound_id) = self.name_to_id.get(&name)
            && *bound_id != id
        {
            return Err(RegistryError::NameAlreadyBound { namespace, name });
        }
        self.name_to_id.insert(name.clone(), id);
        self.id_to_name.insert(id, name);
        Ok(())
    }

    /// Rename via alias (REG15/REG16): `id` must already be registered,
    /// `new_name` must not already be bound to a *different* id.
    fn alias<E>(
        &mut self,
        namespace: &'static str,
        id: Id,
        new_name: String,
    ) -> Result<(), RegistryError<E>> {
        if !self.id_to_name.contains_key(&id) {
            return Err(RegistryError::UnregisteredReference {
                namespace,
                id: id.into(),
            });
        }
        if let Some(bound_id) = self.name_to_id.get(&new_name)
            && *bound_id != id
        {
            return Err(RegistryError::NameAlreadyBound {
                namespace,
                name: new_name,
            });
        }
        self.name_to_id.insert(new_name.clone(), id);
        self.id_to_name.insert(id, new_name);
        Ok(())
    }
}

/// A registered event type's metadata beyond its name (§3.5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventTypeMeta {
    /// The codec its frames are declared to use (`>= 1`; REG9).
    pub codec_id:           u16,
    /// Digest of `schema_version 1`'s shape (opaque to the registry).
    pub schema_fingerprint: [u8; 32],
}

/// A registered dictionary's metadata and bytes (§3.8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DictMeta {
    /// `TARGET_KIND_CATEGORY` or `TARGET_KIND_EVENT_TYPE`.
    pub scope_kind: u8,
    /// The `category_id` or `event_type_id` this dictionary applies to.
    pub scope_id:   u64,
    /// The codec whose byte shapes this dictionary was trained against
    /// (`>= 1`; REG20).
    pub codec_id:   u16,
    /// The trained dictionary bytes, opaque to the registry (D-REG-F).
    pub dict_bytes: Vec<u8>,
}

/// The materialized `$registry` state: id<->name maps for all three
/// interned-ID namespaces, the dictionary table, and the four per-namespace
/// high-water marks (§4.1).
///
/// Built by folding a sequence of decoded [`RegistryRecord`]s, in replay
/// order (§4.2), through [`apply`](Self::apply) starting from
/// [`RegistryState::new`] (the empty/bootstrap state — REG2/REG21's step-2
/// base case).
#[derive(Debug, Clone, Default)]
pub struct RegistryState {
    streams:         NameTable<u64>,
    /// `stream_id -> category_id`, populated alongside `streams`.
    stream_category: HashMap<u64, u64>,
    categories:      NameTable<u64>,
    event_types:     NameTable<u32>,
    event_type_meta: HashMap<u32, EventTypeMeta>,
    dicts:           HashMap<u16, DictMeta>,

    hwm_stream:     u64,
    hwm_category:   u64,
    hwm_event_type: u32,
    hwm_dict:       u16,
}

impl RegistryState {
    /// The empty state — no records replayed yet. Only the four reserved
    /// IDs (REG1) resolve; everything else is `$registry` replay away.
    #[must_use]
    pub fn new() -> Self { Self::default() }

    // -- high-water marks (§4.1) --------------------------------------

    #[must_use]
    pub fn stream_high_water_mark(&self) -> u64 { self.hwm_stream }

    #[must_use]
    pub fn category_high_water_mark(&self) -> u64 { self.hwm_category }

    #[must_use]
    pub fn event_type_high_water_mark(&self) -> u32 { self.hwm_event_type }

    #[must_use]
    pub fn dict_high_water_mark(&self) -> u16 { self.hwm_dict }

    // -- resolution (§5), reserved IDs baked in per REG2 ---------------

    /// `stream_id -> current name`, including the reserved `0 -> "$registry"`.
    #[must_use]
    pub fn stream_name(&self, id: u64) -> Option<&str> {
        if id == RESERVED_STREAM_ID {
            return Some(RESERVED_STREAM_NAME);
        }
        self.streams.current_name(id)
    }

    /// `name -> stream_id`, including the reserved name.
    #[must_use]
    pub fn stream_id(&self, name: &str) -> Option<u64> {
        if name == RESERVED_STREAM_NAME {
            return Some(RESERVED_STREAM_ID);
        }
        self.streams.id_by_name(name)
    }

    /// The category a registered stream belongs to (`None` for the reserved
    /// stream, which belongs to reserved category `0` by construction).
    #[must_use]
    pub fn stream_category(&self, id: u64) -> Option<u64> {
        if id == RESERVED_STREAM_ID {
            return Some(RESERVED_CATEGORY_ID);
        }
        self.stream_category.get(&id).copied()
    }

    #[must_use]
    pub fn category_name(&self, id: u64) -> Option<&str> {
        if id == RESERVED_CATEGORY_ID {
            return Some(RESERVED_CATEGORY_NAME);
        }
        self.categories.current_name(id)
    }

    #[must_use]
    pub fn category_id(&self, name: &str) -> Option<u64> {
        if name == RESERVED_CATEGORY_NAME {
            return Some(RESERVED_CATEGORY_ID);
        }
        self.categories.id_by_name(name)
    }

    #[must_use]
    pub fn event_type_name(&self, id: u32) -> Option<&str> {
        if id == RESERVED_EVENT_TYPE_ID {
            return Some(RESERVED_EVENT_TYPE_NAME);
        }
        self.event_types.current_name(id)
    }

    #[must_use]
    pub fn event_type_id(&self, name: &str) -> Option<u32> {
        if name == RESERVED_EVENT_TYPE_NAME {
            return Some(RESERVED_EVENT_TYPE_ID);
        }
        self.event_types.id_by_name(name)
    }

    #[must_use]
    pub fn event_type_meta(&self, id: u32) -> Option<&EventTypeMeta> {
        self.event_type_meta.get(&id)
    }

    #[must_use]
    pub fn dict(&self, id: u16) -> Option<&DictMeta> { self.dicts.get(&id) }

    // -- replay (§7.2) --------------------------------------------------

    /// Fold one decoded record into the state, enforcing every REG-rule this
    /// document defines (§4.2 dependency ordering, REG14 no-double-register,
    /// REG16/REG17 alias rules, REG9/REG20 codec rules). Records must be
    /// applied in replay order (§4.2): batch commit order, then subframe
    /// index within a batch.
    pub fn apply<E>(
        &mut self,
        record: RegistryRecord,
    ) -> Result<(), RegistryError<E>> {
        match record {
            RegistryRecord::CategoryRegistered { category_id, name } => {
                if category_id == RESERVED_CATEGORY_ID {
                    return Err(RegistryError::ReservedIdRegistered {
                        record_kind: RECORD_KIND_CATEGORY_REGISTERED,
                    });
                }
                if self.categories.contains(category_id) {
                    return Err(RegistryError::AlreadyRegistered {
                        namespace: "category",
                        id:        category_id,
                    });
                }
                self.categories.register("category", category_id, name)?;
                self.hwm_category = self.hwm_category.max(category_id);
                Ok(())
            }
            RegistryRecord::StreamRegistered {
                stream_id,
                category_id,
                name,
            } => {
                if stream_id == RESERVED_STREAM_ID {
                    return Err(RegistryError::ReservedIdRegistered {
                        record_kind: RECORD_KIND_STREAM_REGISTERED,
                    });
                }
                if self.streams.contains(stream_id) {
                    return Err(RegistryError::AlreadyRegistered {
                        namespace: "stream",
                        id:        stream_id,
                    });
                }
                // REG12: category_id must already be visible (0 = reserved
                // $system, always visible).
                if category_id != RESERVED_CATEGORY_ID
                    && !self.categories.contains(category_id)
                {
                    return Err(RegistryError::UnregisteredReference {
                        namespace: "category",
                        id:        category_id,
                    });
                }
                self.streams.register("stream", stream_id, name)?;
                self.stream_category.insert(stream_id, category_id);
                self.hwm_stream = self.hwm_stream.max(stream_id);
                Ok(())
            }
            RegistryRecord::EventTypeRegistered {
                event_type_id,
                codec_id,
                schema_fingerprint,
                name,
            } => {
                if event_type_id == RESERVED_EVENT_TYPE_ID {
                    return Err(RegistryError::ReservedIdRegistered {
                        record_kind: RECORD_KIND_EVENT_TYPE_REGISTERED,
                    });
                }
                if codec_id == 0 {
                    return Err(RegistryError::ReservedCodecId {
                        record_kind: RECORD_KIND_EVENT_TYPE_REGISTERED,
                    });
                }
                if self.event_types.contains(event_type_id) {
                    return Err(RegistryError::AlreadyRegistered {
                        namespace: "event_type",
                        id:        u64::from(event_type_id),
                    });
                }
                self.event_types.register("event_type", event_type_id, name)?;
                self.event_type_meta.insert(
                    event_type_id,
                    EventTypeMeta { codec_id, schema_fingerprint },
                );
                self.hwm_event_type = self.hwm_event_type.max(event_type_id);
                Ok(())
            }
            RegistryRecord::NameAliased {
                target_kind,
                target_id,
                new_name,
            } => self.apply_alias(target_kind, target_id, new_name),
            RegistryRecord::DictRegistered {
                dict_id,
                scope_kind,
                scope_id,
                codec_id,
                dict_bytes,
            } => {
                if dict_id == RESERVED_DICT_ID {
                    return Err(RegistryError::ReservedIdRegistered {
                        record_kind: RECORD_KIND_DICT_REGISTERED,
                    });
                }
                if codec_id == 0 {
                    return Err(RegistryError::ReservedCodecId {
                        record_kind: RECORD_KIND_DICT_REGISTERED,
                    });
                }
                if self.dicts.contains_key(&dict_id) {
                    return Err(RegistryError::AlreadyRegistered {
                        namespace: "dict",
                        id:        u64::from(dict_id),
                    });
                }
                // REG20: scope_id must already be registered, per scope_kind.
                match scope_kind {
                    TARGET_KIND_CATEGORY => {
                        if scope_id != RESERVED_CATEGORY_ID
                            && !self.categories.contains(scope_id)
                        {
                            return Err(RegistryError::UnregisteredReference {
                                namespace: "category",
                                id:        scope_id,
                            });
                        }
                    }
                    TARGET_KIND_EVENT_TYPE => {
                        if scope_id > u64::from(u32::MAX) {
                            return Err(RegistryError::ScopeIdNonZeroHighBits {
                                scope_id,
                            });
                        }
                        let scope_id_u32 = scope_id as u32;
                        if scope_id_u32 != RESERVED_EVENT_TYPE_ID
                            && !self.event_types.contains(scope_id_u32)
                        {
                            return Err(RegistryError::UnregisteredReference {
                                namespace: "event_type",
                                id:        scope_id,
                            });
                        }
                    }
                    other => {
                        return Err(RegistryError::InvalidScopeKind(other));
                    }
                }
                self.dicts.insert(
                    dict_id,
                    DictMeta { scope_kind, scope_id, codec_id, dict_bytes },
                );
                self.hwm_dict = self.hwm_dict.max(dict_id);
                Ok(())
            }
        }
    }

    fn apply_alias<E>(
        &mut self,
        target_kind: u8,
        target_id: u64,
        new_name: String,
    ) -> Result<(), RegistryError<E>> {
        match target_kind {
            TARGET_KIND_STREAM => {
                if target_id == RESERVED_STREAM_ID {
                    return Err(RegistryError::ReservedIdTargeted {
                        namespace: "stream",
                    });
                }
                self.streams.alias("stream", target_id, new_name)
            }
            TARGET_KIND_CATEGORY => {
                if target_id == RESERVED_CATEGORY_ID {
                    return Err(RegistryError::ReservedIdTargeted {
                        namespace: "category",
                    });
                }
                self.categories.alias("category", target_id, new_name)
            }
            TARGET_KIND_EVENT_TYPE => {
                // D-REG-E: event_type_id is zero-extended into the low 32
                // bits; a nonzero high half is corruption, not a big ID.
                if target_id > u64::from(u32::MAX) {
                    return Err(RegistryError::NonZeroHighBits { target_id });
                }
                let id = target_id as u32;
                if id == RESERVED_EVENT_TYPE_ID {
                    return Err(RegistryError::ReservedIdTargeted {
                        namespace: "event_type",
                    });
                }
                self.event_types.alias("event_type", id, new_name)
            }
            other => Err(RegistryError::InvalidTargetKind(other)),
        }
    }
}

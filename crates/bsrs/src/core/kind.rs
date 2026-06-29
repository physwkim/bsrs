//! `Kind` — ophyd's per-signal classification used by the bundler to decide where
//! a reading belongs (event data, configuration, hint, omitted).

/// How a signal contributes to documents.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum Kind {
    /// Default — readings appear in `Event.data`.
    #[default]
    Normal,
    /// Slow-changing — appears in `EventDescriptor.configuration`.
    Config,
    /// Like `Normal`, plus listed in `Hints`.
    Hinted,
    /// Excluded from documents; can still be subscribed to.
    Omitted,
}

impl Kind {
    /// Should this kind contribute a reading to the per-event data?
    pub fn in_event_data(self) -> bool {
        matches!(self, Kind::Normal | Kind::Hinted)
    }

    /// Should this kind contribute to descriptor configuration?
    pub fn in_configuration(self) -> bool {
        matches!(self, Kind::Config)
    }

    /// Should this kind appear in plot/visualization hints?
    pub fn is_hinted(self) -> bool {
        matches!(self, Kind::Hinted)
    }
}

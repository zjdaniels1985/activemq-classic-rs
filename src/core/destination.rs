use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DestinationKind {
    Queue,
    Topic,
    TempQueue,
    TempTopic,
}

impl DestinationKind {
    pub fn is_topic(self) -> bool {
        matches!(self, Self::Topic | Self::TempTopic)
    }

    pub fn is_temporary(self) -> bool {
        matches!(self, Self::TempQueue | Self::TempTopic)
    }

    pub fn scheme(self) -> &'static str {
        match self {
            Self::Queue => "queue",
            Self::Topic => "topic",
            Self::TempQueue => "temp-queue",
            Self::TempTopic => "temp-topic",
        }
    }
}

/// An ActiveMQ destination. Names use `.` separated segments; `*` matches one
/// segment and `>` matches all remaining segments (including none).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Destination {
    pub kind: DestinationKind,
    pub name: String,
}

pub const ADVISORY_PREFIX: &str = "ActiveMQ.Advisory.";
pub const SCHEDULER_MANAGEMENT_TOPIC: &str = "ActiveMQ.Scheduler.Management";

impl Destination {
    pub fn new(kind: DestinationKind, name: impl Into<String>) -> Self {
        Self {
            kind,
            name: name.into(),
        }
    }

    pub fn queue(name: impl Into<String>) -> Self {
        Self::new(DestinationKind::Queue, name)
    }

    pub fn topic(name: impl Into<String>) -> Self {
        Self::new(DestinationKind::Topic, name)
    }

    pub fn temp_queue(name: impl Into<String>) -> Self {
        Self::new(DestinationKind::TempQueue, name)
    }

    pub fn temp_topic(name: impl Into<String>) -> Self {
        Self::new(DestinationKind::TempTopic, name)
    }

    pub fn is_topic(&self) -> bool {
        self.kind.is_topic()
    }

    pub fn is_queue(&self) -> bool {
        !self.kind.is_topic()
    }

    pub fn is_temporary(&self) -> bool {
        self.kind.is_temporary()
    }

    pub fn is_wildcard(&self) -> bool {
        self.name
            .split(['.', ','])
            .any(|segment| segment == "*" || segment == ">")
    }

    pub fn is_composite(&self) -> bool {
        self.name.contains(',')
    }

    pub fn is_advisory(&self) -> bool {
        self.is_topic() && self.name.starts_with(ADVISORY_PREFIX)
    }

    /// Splits a composite destination (`a,b,topic://c`) into its components.
    pub fn components(&self) -> Vec<Destination> {
        if !self.is_composite() {
            return vec![self.clone()];
        }
        self.name
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(|part| {
                Destination::parse_uri(part)
                    .unwrap_or_else(|| Destination::new(self.kind, part.to_owned()))
            })
            .collect()
    }

    /// Returns true when `self`, treated as a (possibly wildcard or composite)
    /// pattern, matches the concrete destination `other`.
    pub fn matches(&self, other: &Destination) -> bool {
        self.components().iter().any(|pattern| {
            pattern.kind.is_topic() == other.kind.is_topic()
                && (pattern.kind.is_temporary() == other.kind.is_temporary()
                    || pattern.is_wildcard())
                && wildcard_match(&pattern.name, &other.name)
        })
    }

    /// Parses `queue://name`, `topic://name`, `temp-queue://name` and `temp-topic://name`.
    pub fn parse_uri(value: &str) -> Option<Self> {
        let (scheme, name) = value.split_once("://")?;
        let kind = match scheme.to_ascii_lowercase().as_str() {
            "queue" => DestinationKind::Queue,
            "topic" => DestinationKind::Topic,
            "temp-queue" => DestinationKind::TempQueue,
            "temp-topic" => DestinationKind::TempTopic,
            _ => return None,
        };
        if name.is_empty() {
            return None;
        }
        Some(Self::new(kind, name))
    }

    pub fn to_uri(&self) -> String {
        format!("{}://{}", self.kind.scheme(), self.name)
    }

    /// The advisory topic used to announce consumers on this destination.
    pub fn consumer_advisory_topic(&self) -> Destination {
        Destination::topic(format!(
            "{ADVISORY_PREFIX}Consumer.{}.{}",
            advisory_kind(self.kind),
            self.name
        ))
    }

    pub fn producer_advisory_topic(&self) -> Destination {
        Destination::topic(format!(
            "{ADVISORY_PREFIX}Producer.{}.{}",
            advisory_kind(self.kind),
            self.name
        ))
    }

    /// Advisory topic announcing creation/deletion of destinations of this kind.
    pub fn destination_advisory_topic(&self) -> Destination {
        Destination::topic(format!("{ADVISORY_PREFIX}{}", advisory_kind(self.kind)))
    }
}

fn advisory_kind(kind: DestinationKind) -> &'static str {
    match kind {
        DestinationKind::Queue => "Queue",
        DestinationKind::Topic => "Topic",
        DestinationKind::TempQueue => "TempQueue",
        DestinationKind::TempTopic => "TempTopic",
    }
}

impl fmt::Display for Destination {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_uri())
    }
}

/// ActiveMQ wildcard matching over `.` separated names.
pub fn wildcard_match(pattern: &str, name: &str) -> bool {
    let pattern = pattern.split('.').collect::<Vec<_>>();
    let name = name.split('.').collect::<Vec<_>>();
    match_segments(&pattern, &name)
}

fn match_segments(pattern: &[&str], name: &[&str]) -> bool {
    match pattern.split_first() {
        None => name.is_empty(),
        Some((&">", _)) => true,
        Some((&"*", rest)) => !name.is_empty() && match_segments(rest, &name[1..]),
        Some((segment, rest)) => {
            name.first().is_some_and(|first| first == segment) && match_segments(rest, &name[1..])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcards_follow_activemq_rules() {
        assert!(wildcard_match("PRICE.>", "PRICE.STOCK.NASDAQ"));
        assert!(wildcard_match("PRICE.>", "PRICE"));
        assert!(wildcard_match("PRICE.*.NASDAQ", "PRICE.STOCK.NASDAQ"));
        assert!(!wildcard_match("PRICE.*", "PRICE.STOCK.NASDAQ"));
        assert!(wildcard_match("orders", "orders"));
        assert!(!wildcard_match("orders", "orders.eu"));
    }

    #[test]
    fn composite_destinations_split_with_kind_overrides() {
        let composite = Destination::queue("a,topic://b,c");
        assert_eq!(
            composite.components(),
            vec![
                Destination::queue("a"),
                Destination::topic("b"),
                Destination::queue("c")
            ]
        );
        assert!(Destination::topic("x.>").matches(&Destination::topic("x.y")));
        assert!(!Destination::topic("x.>").matches(&Destination::queue("x.y")));
        assert!(composite.matches(&Destination::topic("b")));
    }

    #[test]
    fn parses_uris() {
        assert_eq!(
            Destination::parse_uri("temp-queue://abc"),
            Some(Destination::temp_queue("abc"))
        );
        assert_eq!(Destination::parse_uri("nope"), None);
    }
}

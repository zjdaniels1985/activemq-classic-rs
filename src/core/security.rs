//! Simple authentication and destination authorization, modelled after
//! ActiveMQ Classic's `simpleAuthenticationPlugin` and `authorizationPlugin`.

use super::destination::{Destination, DestinationKind};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fmt;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct SecurityConfig {
    /// When false, every connection is accepted and fully authorized.
    pub enabled: bool,
    /// Allow connections without credentials as `anonymous_user`.
    pub allow_anonymous: bool,
    pub anonymous_user: Option<String>,
    pub anonymous_groups: Vec<String>,
    pub users: Vec<UserConfig>,
    /// Destination authorization entries. When empty, every authenticated
    /// user may use every destination.
    pub authorization: Vec<AuthorizationEntry>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct UserConfig {
    pub username: String,
    /// Plain-text password (as in ActiveMQ's users.properties).
    pub password: Option<String>,
    /// Hex-encoded SHA-256 of the password; preferred over `password`.
    pub password_sha256: Option<String>,
    pub groups: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct AuthorizationEntry {
    /// Destination pattern, e.g. `queue://orders.>`, `topic://>` or `temp-queue://>`.
    pub destination: String,
    pub read: Vec<String>,
    pub write: Vec<String>,
    pub admin: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    pub username: String,
    pub groups: BTreeSet<String>,
    /// Internal principals (console, network bridges, scheduler) bypass authorization.
    pub system: bool,
}

impl Principal {
    pub fn system(name: &str) -> Self {
        Self {
            username: name.to_owned(),
            groups: BTreeSet::new(),
            system: true,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Permission {
    Read,
    Write,
    Admin,
}

impl fmt::Display for Permission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Admin => "admin",
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecurityError(pub String);

impl fmt::Display for SecurityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for SecurityError {}

struct CompiledEntry {
    pattern: Destination,
    read: BTreeSet<String>,
    write: BTreeSet<String>,
    admin: BTreeSet<String>,
}

pub struct SecurityManager {
    config: SecurityConfig,
    entries: Vec<CompiledEntry>,
}

impl SecurityManager {
    pub fn new(config: SecurityConfig) -> Result<Self, SecurityError> {
        let mut entries = Vec::new();
        for entry in &config.authorization {
            let pattern = Destination::parse_uri(&entry.destination).ok_or_else(|| {
                SecurityError(format!(
                    "invalid authorization destination '{}'; use queue://, topic://, temp-queue:// or temp-topic://",
                    entry.destination
                ))
            })?;
            entries.push(CompiledEntry {
                pattern,
                read: entry.read.iter().cloned().collect(),
                write: entry.write.iter().cloned().collect(),
                admin: entry.admin.iter().cloned().collect(),
            });
        }
        for user in &config.users {
            if user.username.is_empty() {
                return Err(SecurityError(
                    "security user with empty username".to_owned(),
                ));
            }
            if user.password.is_none() && user.password_sha256.is_none() {
                return Err(SecurityError(format!(
                    "security user '{}' has no password",
                    user.username
                )));
            }
        }
        Ok(Self { config, entries })
    }

    pub fn disabled() -> Self {
        Self {
            config: SecurityConfig::default(),
            entries: Vec::new(),
        }
    }

    pub fn enabled(&self) -> bool {
        self.config.enabled
    }

    pub fn authenticate(
        &self,
        username: Option<&str>,
        password: Option<&str>,
    ) -> Result<Principal, SecurityError> {
        let username = username.filter(|name| !name.is_empty());
        if !self.config.enabled {
            return Ok(Principal {
                username: username.unwrap_or("anonymous").to_owned(),
                groups: BTreeSet::new(),
                system: true,
            });
        }
        let Some(username) = username else {
            if self.config.allow_anonymous {
                return Ok(Principal {
                    username: self
                        .config
                        .anonymous_user
                        .clone()
                        .unwrap_or_else(|| "anonymous".to_owned()),
                    groups: if self.config.anonymous_groups.is_empty() {
                        BTreeSet::from(["anonymous".to_owned()])
                    } else {
                        self.config.anonymous_groups.iter().cloned().collect()
                    },
                    system: false,
                });
            }
            return Err(SecurityError(
                "User name [null] or password is invalid.".to_owned(),
            ));
        };
        let password = password.unwrap_or_default();
        let user = self
            .config
            .users
            .iter()
            .find(|user| user.username == username);
        let valid = user.is_some_and(|user| {
            if let Some(expected) = &user.password_sha256 {
                let digest = Sha256::digest(password.as_bytes());
                let actual = digest
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                constant_time_eq(actual.as_bytes(), expected.to_ascii_lowercase().as_bytes())
            } else {
                user.password.as_deref().is_some_and(|expected| {
                    constant_time_eq(expected.as_bytes(), password.as_bytes())
                })
            }
        });
        match (valid, user) {
            (true, Some(user)) => Ok(Principal {
                username: user.username.clone(),
                groups: user.groups.iter().cloned().collect(),
                system: false,
            }),
            _ => Err(SecurityError(format!(
                "User name [{username}] or password is invalid."
            ))),
        }
    }

    pub fn authorize(
        &self,
        principal: &Principal,
        destination: &Destination,
        permission: Permission,
    ) -> Result<(), SecurityError> {
        if principal.system || !self.config.enabled || self.entries.is_empty() {
            return Ok(());
        }
        for component in destination.components() {
            if !self.allowed(principal, &component, permission) {
                return Err(SecurityError(format!(
                    "User {} is not authorized to {} from: {}",
                    principal.username,
                    permission,
                    component.to_uri()
                )));
            }
        }
        Ok(())
    }

    fn allowed(
        &self,
        principal: &Principal,
        destination: &Destination,
        permission: Permission,
    ) -> bool {
        // Union of all matching entries, as in ActiveMQ's DefaultAuthorizationMap.
        let mut matched = false;
        let mut groups = BTreeSet::new();
        for entry in &self.entries {
            if entry.pattern.kind == destination.kind && entry.pattern.matches(destination) {
                matched = true;
                groups.extend(
                    match permission {
                        Permission::Read => &entry.read,
                        Permission::Write => &entry.write,
                        Permission::Admin => &entry.admin,
                    }
                    .iter()
                    .cloned(),
                );
            }
        }
        if !matched {
            // Advisory topics and temporary destinations are usable by any
            // authenticated user unless explicitly restricted.
            return destination.is_advisory()
                || matches!(
                    destination.kind,
                    DestinationKind::TempQueue | DestinationKind::TempTopic
                );
        }
        groups.contains("*")
            || groups.contains(&principal.username)
            || principal.groups.iter().any(|group| groups.contains(group))
    }
}

pub fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        difference |= usize::from(
            left.get(index).copied().unwrap_or_default()
                ^ right.get(index).copied().unwrap_or_default(),
        );
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager() -> SecurityManager {
        SecurityManager::new(SecurityConfig {
            enabled: true,
            allow_anonymous: false,
            anonymous_user: None,
            anonymous_groups: Vec::new(),
            users: vec![
                UserConfig {
                    username: "admin".to_owned(),
                    password: Some("secret".to_owned()),
                    password_sha256: None,
                    groups: vec!["admins".to_owned()],
                },
                UserConfig {
                    username: "app".to_owned(),
                    password: None,
                    // sha256("app-pass")
                    password_sha256: Some(
                        "5c7fd6c1bb1a8e3c20cd7b7b6a1b3e2ed2a9b3f8c1d0e4f5a6b7c8d9e0f1a2b3"
                            .to_owned(),
                    ),
                    groups: vec!["users".to_owned()],
                },
            ],
            authorization: vec![
                AuthorizationEntry {
                    destination: "queue://>".to_owned(),
                    read: vec!["admins".to_owned()],
                    write: vec!["admins".to_owned()],
                    admin: vec!["admins".to_owned()],
                },
                AuthorizationEntry {
                    destination: "queue://orders.>".to_owned(),
                    read: vec!["users".to_owned()],
                    write: vec!["users".to_owned()],
                    admin: vec!["users".to_owned()],
                },
            ],
        })
        .unwrap()
    }

    #[test]
    fn authenticates_users() {
        let manager = manager();
        assert!(manager.authenticate(Some("admin"), Some("secret")).is_ok());
        assert!(manager.authenticate(Some("admin"), Some("wrong")).is_err());
        assert!(manager.authenticate(None, None).is_err());
        assert!(manager.authenticate(Some("nobody"), Some("x")).is_err());
    }

    #[test]
    fn hashed_passwords_are_verified() {
        let digest = Sha256::digest(b"app-pass");
        let hex = digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let mut config = manager().config.clone();
        config.users[1].password_sha256 = Some(hex);
        let manager = SecurityManager::new(config).unwrap();
        assert!(manager.authenticate(Some("app"), Some("app-pass")).is_ok());
        assert!(manager.authenticate(Some("app"), Some("nope")).is_err());
    }

    #[test]
    fn authorizes_by_group_and_wildcard() {
        let manager = manager();
        let admin = manager.authenticate(Some("admin"), Some("secret")).unwrap();
        let user = Principal {
            username: "app".to_owned(),
            groups: BTreeSet::from(["users".to_owned()]),
            system: false,
        };
        let orders = Destination::queue("orders.eu");
        let billing = Destination::queue("billing");
        assert!(
            manager
                .authorize(&admin, &billing, Permission::Write)
                .is_ok()
        );
        assert!(manager.authorize(&user, &orders, Permission::Write).is_ok());
        assert!(
            manager
                .authorize(&user, &billing, Permission::Read)
                .is_err()
        );
        assert!(
            manager
                .authorize(&user, &Destination::topic("unlisted"), Permission::Read)
                .is_err()
        );
        assert!(
            manager
                .authorize(
                    &user,
                    &Destination::topic("ActiveMQ.Advisory.TempQueue"),
                    Permission::Read
                )
                .is_ok()
        );
    }
}

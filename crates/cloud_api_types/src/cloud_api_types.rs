mod extension;
mod known_or_unknown;
mod timestamp;

use std::collections::BTreeMap;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

pub use crate::extension::*;
pub use crate::known_or_unknown::*;
pub use crate::timestamp::Timestamp;

pub const ZED_SYSTEM_ID_HEADER_NAME: &str = "x-zed-system-id";

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct GetAuthenticatedUserResponse {
    pub user: AuthenticatedUser,
    pub feature_flags: Vec<String>,
    #[serde(default)]
    pub organizations: Vec<Organization>,
    #[serde(default)]
    pub default_organization_id: Option<OrganizationId>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct AuthenticatedUser {
    pub id_v2: String,
    pub legacy_user_id: i32,
    pub metrics_id: String,
    pub username: String,
    pub avatar_url: String,
    pub github_login: String,
    pub name: Option<String>,
    pub is_staff: bool,
    pub accepted_tos_at: Option<Timestamp>,
    pub has_connected_to_collab_once: bool,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize)]
pub struct OrganizationId(pub Arc<str>);

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Organization {
    pub id: OrganizationId,
    pub name: Arc<str>,
    pub is_personal: bool,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct OrganizationEditPredictionConfiguration {
    pub is_enabled: bool,
    pub is_feedback_enabled: bool,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct LlmToken(pub String);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct CreateLlmTokenBody {
    pub organization_id: OrganizationId,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct CreateLlmTokenResponse {
    pub token: LlmToken,
}

#[derive(Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
pub struct UpdateSystemSettingsBody {
    pub selected_organization_id: Option<OrganizationId>,
}

#[derive(Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
pub struct SystemSettings {
    pub selected_organization_id: Option<OrganizationId>,
}

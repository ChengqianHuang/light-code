use std::{path::Path, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use gpui::{App, AsyncApp, Entity};
use language::Buffer;
use lsp::{AdapterServerCapabilities, LanguageServer, LanguageServerId};
use serde::{Deserialize, Serialize};

use crate::{
    lsp_command::LspCommand,
    lsp_store::{LanguageServerToQuery, LspStore},
};

pub const EMMET_SERVER_NAME: lsp::LanguageServerName =
    lsp::LanguageServerName::new_static("emmet-language-server");

pub enum LspExpandAbbreviation {}

impl lsp::request::Request for LspExpandAbbreviation {
    type Params = ExpandAbbreviationParams;
    type Result = Option<String>;
    const METHOD: &'static str = "emmet/expandAbbreviation";
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ExpandAbbreviationParams {
    pub abbreviation: String,
    pub language: String,
    pub options: ExpandAbbreviationOptions,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ExpandAbbreviationOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<Vec<String>>,
    pub options: EmmetOutputOptions,
}

#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Eq)]
pub struct EmmetOutputOptions {
    #[serde(rename = "output.indent")]
    pub indent: String,
    #[serde(rename = "output.baseIndent")]
    pub base_indent: String,
    #[serde(rename = "comment.enabled", skip_serializing_if = "Option::is_none")]
    pub comment_enabled: Option<bool>,
    #[serde(rename = "bem.enabled", skip_serializing_if = "Option::is_none")]
    pub bem_enabled: Option<bool>,
    #[serde(rename = "output.inlineBreak", skip_serializing_if = "Option::is_none")]
    pub inline_break: Option<u32>,
}

#[derive(Debug)]
pub struct ExpandAbbreviation {
    pub abbreviation: String,
    pub text: Option<Vec<String>>,
    pub language: String,
    pub server_id: LanguageServerId,
    pub indent: String,
    pub base_indent: String,
    pub comment_filter: bool,
    pub bem_filter: bool,
}

#[async_trait(?Send)]
impl LspCommand for ExpandAbbreviation {
    type Response = Option<String>;
    type LspRequest = LspExpandAbbreviation;

    fn display_name(&self) -> &str {
        "Expand Emmet abbreviation"
    }

    fn check_capabilities(&self, _: AdapterServerCapabilities) -> bool {
        true
    }

    fn language_server_to_query(&self) -> LanguageServerToQuery {
        LanguageServerToQuery::Other(self.server_id)
    }

    fn to_lsp(
        &self,
        _: &Path,
        _: &Buffer,
        server: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<ExpandAbbreviationParams> {
        anyhow::ensure!(
            server.name() == EMMET_SERVER_NAME,
            "cannot expand Emmet abbreviations with the {} server",
            server.name()
        );
        let multiline = self.text.as_ref().is_some_and(|text| text.len() > 1);
        Ok(ExpandAbbreviationParams {
            abbreviation: self.abbreviation.clone(),
            language: self.language.clone(),
            options: ExpandAbbreviationOptions {
                text: self.text.clone(),
                options: EmmetOutputOptions {
                    indent: self.indent.clone(),
                    base_indent: self.base_indent.clone(),
                    comment_enabled: self.comment_filter.then_some(true),
                    bem_enabled: self.bem_filter.then_some(true),
                    inline_break: multiline.then_some(1),
                },
            },
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<String>,
        _: Entity<LspStore>,
        _: Entity<Buffer>,
        _: LanguageServerId,
        _: AsyncApp,
    ) -> anyhow::Result<Option<String>> {
        Ok(message.filter(|expansion| !expansion.is_empty()))
    }
}

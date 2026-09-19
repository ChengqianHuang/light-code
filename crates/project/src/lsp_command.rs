pub mod signature_help;

use crate::{
    CodeAction, CompletionSource, CoreCompletion, CoreCompletionResponse, DocumentColor,
    DocumentHighlight, DocumentSymbol, Hover, HoverBlock, HoverBlockKind, InlayHint,
    InlayHintLabel, InlayHintLabelPart, InlayHintLabelPartTooltip, InlayHintTooltip, Location,
    LocationLink, LspAction, LspPullDiagnostics, MarkupContent, PrepareRenameResponse, ProjectPath,
    ProjectTransaction, PulledDiagnostics, ResolveState,
    lsp_store::{LanguageServerToQuery, LocalLspStore, LspDocumentLink, LspFoldingRange, LspStore},
};
use anyhow::{Context as _, Result};
use async_trait::async_trait;
use collections::HashMap;
use futures::future;
use gpui::{App, AsyncApp, Entity, SharedString, prelude::FluentBuilder};
use language::{
    Anchor, Bias, Buffer, BufferSnapshot, CachedLspAdapter, CharKind, CharScopeContext,
    OffsetRangeExt, PointUtf16, ToOffset, ToPointUtf16, Transaction, Unclipped,
    language_settings::{InlayHintKind, LanguageSettings},
    lsp_to_symbol_kind, point_from_lsp, point_to_lsp, range_from_lsp, range_to_lsp,
};
use lsp::{
    AdapterServerCapabilities, CodeActionKind, CodeActionOptions,
    CompletionContext, CompletionListItemDefaultsEditRange, LanguageServer, LanguageServerId, LinkedEditingRangeServerCapabilities,
    OneOf, RenameOptions, ServerCapabilities,
};

use std::{cmp::Reverse, collections::hash_map, ops::Range, path::Path, sync::Arc};
use text::LineEnding;
use util::debug_panic;

pub use signature_help::SignatureHelp;

fn code_action_kind_matches(requested: &lsp::CodeActionKind, actual: &lsp::CodeActionKind) -> bool {
    let requested_str = requested.as_str();
    let actual_str = actual.as_str();

    // Exact match or hierarchical match
    actual_str == requested_str
        || actual_str
            .strip_prefix(requested_str)
            .is_some_and(|suffix| suffix.starts_with('.'))
}

pub fn lsp_formatting_options(settings: &LanguageSettings) -> lsp::FormattingOptions {
    lsp::FormattingOptions {
        tab_size: settings.tab_size.into(),
        insert_spaces: !settings.hard_tabs,
        trim_trailing_whitespace: Some(settings.remove_trailing_whitespace_on_save),
        trim_final_newlines: Some(settings.ensure_final_newline_on_save),
        insert_final_newline: Some(settings.ensure_final_newline_on_save),
        ..lsp::FormattingOptions::default()
    }
}

pub fn file_path_to_lsp_url(path: &Path) -> Result<lsp::Uri> {
    match lsp::Uri::from_file_path(path) {
        Ok(url) => Ok(url),
        Err(()) => anyhow::bail!("Invalid file path provided to LSP request: {path:?}"),
    }
}

pub(crate) fn make_text_document_identifier(path: &Path) -> Result<lsp::TextDocumentIdentifier> {
    Ok(lsp::TextDocumentIdentifier {
        uri: file_path_to_lsp_url(path)?,
    })
}

pub(crate) fn make_lsp_text_document_position(
    path: &Path,
    position: PointUtf16,
) -> Result<lsp::TextDocumentPositionParams> {
    Ok(lsp::TextDocumentPositionParams {
        text_document: make_text_document_identifier(path)?,
        position: point_to_lsp(position),
    })
}

#[async_trait(?Send)]
pub trait LspCommand: 'static + Sized + Send + std::fmt::Debug {
    type Response: 'static + Default + Send + std::fmt::Debug;
    type LspRequest: 'static + Send + lsp::request::Request;

    fn display_name(&self) -> &str;

    fn status(&self) -> Option<String> {
        None
    }

    fn language_server_to_query(&self) -> LanguageServerToQuery {
        LanguageServerToQuery::FirstCapable
    }

    /// Returns whether the given static or dynamic capability supports this request.
    fn check_capabilities(&self, _: AdapterServerCapabilities<'_>) -> bool;

    fn response_without_request<'a, I>(&self, _applicable_capabilities: I) -> Option<Self::Response>
    where
        I: Iterator<Item = AdapterServerCapabilities<'a>>,
    {
        None
    }

    fn to_lsp(
        &self,
        path: &Path,
        buffer: &Buffer,
        language_server: &Arc<LanguageServer>,
        cx: &App,
    ) -> Result<<Self::LspRequest as lsp::request::Request>::Params>;

    async fn response_from_lsp(
        self,
        message: <Self::LspRequest as lsp::request::Request>::Result,
        lsp_store: Entity<LspStore>,
        buffer: Entity<Buffer>,
        server_id: LanguageServerId,
        cx: AsyncApp,
    ) -> Result<Self::Response>;
}

#[derive(Debug)]
pub(crate) struct PerformRename {
    pub position: PointUtf16,
    pub new_name: String,
    pub push_to_history: bool,
    pub language_server_id: Option<LanguageServerId>,
}

#[derive(Debug, Clone, Copy)]
pub struct GetDefinitions {
    pub position: PointUtf16,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EditPredictionDefinition {
    pub path: ProjectPath,
    pub range: Range<Unclipped<PointUtf16>>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct GetEditPredictionDefinitions {
    pub position: PointUtf16,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct GetDeclarations {
    pub position: PointUtf16,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct GetTypeDefinitions {
    pub position: PointUtf16,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct GetEditPredictionTypeDefinitions {
    pub position: PointUtf16,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct GetImplementations {
    pub position: PointUtf16,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct GetReferences {
    pub position: PointUtf16,
}

#[derive(Debug)]
pub(crate) struct GetDocumentHighlights {
    pub position: PointUtf16,
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct GetDocumentSymbols;

#[derive(Clone, Debug)]
pub(crate) struct GetSignatureHelp {
    pub position: PointUtf16,
}

#[derive(Clone, Debug)]
pub(crate) struct GetHover {
    pub position: PointUtf16,
}

#[derive(Debug)]
pub(crate) struct GetCompletions {
    pub position: PointUtf16,
    pub context: CompletionContext,
    pub server_id: Option<lsp::LanguageServerId>,
}

#[derive(Clone, Debug)]
pub(crate) struct GetCodeActions {
    pub range: Range<Anchor>,
    pub kinds: Option<Vec<lsp::CodeActionKind>>,
}

#[derive(Debug)]
pub(crate) struct OnTypeFormatting {
    pub position: PointUtf16,
    pub trigger: String,
    pub options: lsp::FormattingOptions,
    pub push_to_history: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct InlayHints {
    pub range: Range<Anchor>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SemanticTokensFull {
    pub for_server: Option<LanguageServerId>,
}

#[derive(Debug, Clone)]
pub(crate) struct SemanticTokensDelta {
    pub previous_result_id: SharedString,
}

#[derive(Debug)]
pub(crate) enum SemanticTokensResponse {
    Full {
        data: Vec<u32>,
        result_id: Option<SharedString>,
    },
    Delta {
        edits: Vec<SemanticTokensEdit>,
        result_id: Option<SharedString>,
    },
}

impl Default for SemanticTokensResponse {
    fn default() -> Self {
        Self::Delta {
            edits: Vec::new(),
            result_id: None,
        }
    }
}

#[derive(Debug)]
pub(crate) struct SemanticTokensEdit {
    pub start: u32,
    pub delete_count: u32,
    pub data: Vec<u32>,
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct GetCodeLens;

#[derive(Debug, Copy, Clone)]
pub(crate) struct GetDocumentColor;

#[derive(Debug, Copy, Clone)]
pub(crate) struct GetFoldingRanges;

#[derive(Debug, Copy, Clone)]
pub(crate) struct GetDocumentLinks;

impl GetCodeLens {
    pub(crate) fn can_resolve_lens(capabilities: &ServerCapabilities) -> bool {
        capabilities
            .code_lens_provider
            .as_ref()
            .and_then(|code_lens_options| code_lens_options.resolve_provider)
            .unwrap_or(false)
    }
}

#[derive(Debug)]
pub(crate) struct LinkedEditingRange {
    pub position: Anchor,
}

#[derive(Clone, Debug)]
pub struct GetDocumentDiagnostics {
    /// We cannot blindly rely on server's capabilities.diagnostic_provider, as they're a singular field, whereas
    /// a server can register multiple diagnostic providers post-mortem.
    pub registration_id: Option<SharedString>,
    pub identifier: Option<SharedString>,
    pub previous_result_id: Option<SharedString>,
}

#[derive(Debug, Clone)]
pub struct CallHierarchyItem {
    pub buffer: Entity<Buffer>,
    pub server_id: LanguageServerId,
    pub name: String,
    pub kind: lsp::SymbolKind,
    pub detail: Option<String>,
    pub range: Range<Anchor>,
    pub selection_range: Range<Anchor>,
    pub data: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct PrepareCallHierarchy {
    pub position: PointUtf16,
}

async fn call_hierarchy_item_from_lsp(
    item: lsp::CallHierarchyItem,
    server_id: LanguageServerId,
    lsp_store: &Entity<LspStore>,
    cx: &mut AsyncApp,
) -> Result<CallHierarchyItem> {
    let buffer = lsp_store
        .update(cx, |lsp_store, cx| {
            lsp_store.open_local_buffer_via_lsp(item.uri, server_id, cx)
        })
        .await?;
    let (range, selection_range) = buffer.read_with(cx, |buffer, _| {
        (
            anchor_range_from_lsp(item.range, buffer),
            anchor_range_from_lsp(item.selection_range, buffer),
        )
    });
    Ok(CallHierarchyItem {
        buffer,
        server_id,
        name: item.name,
        kind: item.kind,
        detail: item.detail,
        range,
        selection_range,
        data: item.data,
    })
}

fn anchor_range_from_lsp(range: lsp::Range, buffer: &Buffer) -> Range<Anchor> {
    let range = range_from_lsp(range);
    let start = buffer.clip_point_utf16(range.start, Bias::Left);
    let end = buffer.clip_point_utf16(range.end, Bias::Left);
    buffer.anchor_after(start)..buffer.anchor_before(end)
}

fn call_hierarchy_item_to_lsp(
    item: &CallHierarchyItem,
    path: &Path,
    buffer: &Buffer,
) -> Result<lsp::CallHierarchyItem> {
    Ok(lsp::CallHierarchyItem {
        name: item.name.clone(),
        kind: item.kind,
        tags: None,
        detail: item.detail.clone(),
        uri: file_path_to_lsp_url(path)?,
        range: range_to_lsp(item.range.to_point_utf16(buffer))?,
        selection_range: range_to_lsp(item.selection_range.to_point_utf16(buffer))?,
        data: item.data.clone(),
    })
}

#[async_trait(?Send)]
impl LspCommand for PrepareCallHierarchy {
    type Response = Vec<CallHierarchyItem>;
    type LspRequest = lsp::request::CallHierarchyPrepare;

    fn display_name(&self) -> &str {
        "Prepare call hierarchy"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities) -> bool {
        capabilities
            .server_capabilities
            .call_hierarchy_provider
            .as_ref()
            .is_some_and(|capability| match capability {
                lsp::CallHierarchyServerCapability::Simple(supported) => *supported,
                lsp::CallHierarchyServerCapability::Options(_) => true,
            })
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::CallHierarchyPrepareParams> {
        Ok(lsp::CallHierarchyPrepareParams {
            text_document_position_params: make_lsp_text_document_position(path, self.position)?,
            work_done_progress_params: lsp::WorkDoneProgressParams::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<Vec<lsp::CallHierarchyItem>>,
        lsp_store: Entity<LspStore>,
        _buffer: Entity<Buffer>,
        server_id: LanguageServerId,
        mut cx: AsyncApp,
    ) -> Result<Vec<CallHierarchyItem>> {
        let mut items = Vec::new();
        for item in message.unwrap_or_default() {
            items.push(call_hierarchy_item_from_lsp(item, server_id, &lsp_store, &mut cx).await?);
        }
        Ok(items)
    }
}

#[derive(Debug, Clone)]
pub struct IncomingCall {
    pub from: CallHierarchyItem,
    pub from_ranges: Vec<Location>,
}

#[derive(Debug, Clone)]
pub struct OutgoingCall {
    pub to: CallHierarchyItem,
    pub from_ranges: Vec<Location>,
}

#[derive(Debug, Clone)]
pub struct GetIncomingCalls {
    pub item: CallHierarchyItem,
}

#[async_trait(?Send)]
impl LspCommand for GetIncomingCalls {
    type Response = Vec<IncomingCall>;
    type LspRequest = lsp::request::CallHierarchyIncomingCalls;

    fn display_name(&self) -> &str {
        "Get incoming calls"
    }

    /// Follow-up requests operate on a server-issued item, so the server's support is
    /// already proven and no capability gate applies.
    fn check_capabilities(&self, _: AdapterServerCapabilities) -> bool {
        true
    }

    fn to_lsp(
        &self,
        path: &Path,
        buffer: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::CallHierarchyIncomingCallsParams> {
        Ok(lsp::CallHierarchyIncomingCallsParams {
            item: call_hierarchy_item_to_lsp(&self.item, path, buffer)?,
            work_done_progress_params: lsp::WorkDoneProgressParams::default(),
            partial_result_params: lsp::PartialResultParams::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<Vec<lsp::CallHierarchyIncomingCall>>,
        lsp_store: Entity<LspStore>,
        _buffer: Entity<Buffer>,
        server_id: LanguageServerId,
        mut cx: AsyncApp,
    ) -> Result<Vec<IncomingCall>> {
        let mut calls = Vec::new();
        for call in message.unwrap_or_default() {
            let from =
                call_hierarchy_item_from_lsp(call.from, server_id, &lsp_store, &mut cx).await?;
            let from_ranges = from.buffer.read_with(&cx, |buffer, _| {
                call.from_ranges
                    .into_iter()
                    .map(|range| Location {
                        buffer: from.buffer.clone(),
                        range: anchor_range_from_lsp(range, buffer),
                    })
                    .collect()
            });
            calls.push(IncomingCall { from, from_ranges });
        }
        Ok(calls)
    }
}

#[derive(Debug, Clone)]
pub struct GetOutgoingCalls {
    pub item: CallHierarchyItem,
}

#[async_trait(?Send)]
impl LspCommand for GetOutgoingCalls {
    type Response = Vec<OutgoingCall>;
    type LspRequest = lsp::request::CallHierarchyOutgoingCalls;

    fn display_name(&self) -> &str {
        "Get outgoing calls"
    }

    /// Follow-up requests operate on a server-issued item, so the server's support is
    /// already proven and no capability gate applies.
    fn check_capabilities(&self, _: AdapterServerCapabilities) -> bool {
        true
    }

    fn to_lsp(
        &self,
        path: &Path,
        buffer: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::CallHierarchyOutgoingCallsParams> {
        Ok(lsp::CallHierarchyOutgoingCallsParams {
            item: call_hierarchy_item_to_lsp(&self.item, path, buffer)?,
            work_done_progress_params: lsp::WorkDoneProgressParams::default(),
            partial_result_params: lsp::PartialResultParams::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<Vec<lsp::CallHierarchyOutgoingCall>>,
        lsp_store: Entity<LspStore>,
        buffer: Entity<Buffer>,
        server_id: LanguageServerId,
        mut cx: AsyncApp,
    ) -> Result<Vec<OutgoingCall>> {
        let mut calls = Vec::new();
        for call in message.unwrap_or_default() {
            let to = call_hierarchy_item_from_lsp(call.to, server_id, &lsp_store, &mut cx).await?;
            let from_ranges = buffer.read_with(&cx, |queried_buffer, _| {
                call.from_ranges
                    .into_iter()
                    .map(|range| Location {
                        buffer: buffer.clone(),
                        range: anchor_range_from_lsp(range, queried_buffer),
                    })
                    .collect()
            });
            calls.push(OutgoingCall { to, from_ranges });
        }
        Ok(calls)
    }
}

#[derive(Debug)]
pub(crate) struct PrepareRename {
    pub position: PointUtf16,
}

#[async_trait(?Send)]
impl LspCommand for PrepareRename {
    type Response = PrepareRenameResponse;
    type LspRequest = lsp::request::PrepareRenameRequest;

    fn display_name(&self) -> &str {
        "Prepare rename"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        capabilities
            .server_capabilities
            .rename_provider
            .as_ref()
            .is_some_and(|capability| match capability {
                OneOf::Left(enabled) => *enabled,
                OneOf::Right(_) => true,
            })
    }

    fn response_without_request<'a, I>(
        &self,
        mut applicable_capabilities: I,
    ) -> Option<Self::Response>
    where
        I: Iterator<Item = AdapterServerCapabilities<'a>>,
    {
        (!applicable_capabilities.any(|capabilities| {
            matches!(
                capabilities.server_capabilities.rename_provider.as_ref(),
                Some(lsp::OneOf::Right(RenameOptions {
                    prepare_provider: Some(true),
                    ..
                }))
            )
        }))
        .then_some(PrepareRenameResponse::OnlyUnpreparedRenameSupported)
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::TextDocumentPositionParams> {
        make_lsp_text_document_position(path, self.position)
    }

    async fn response_from_lsp(
        self,
        message: Option<lsp::PrepareRenameResponse>,
        _: Entity<LspStore>,
        buffer: Entity<Buffer>,
        server_id: LanguageServerId,
        cx: AsyncApp,
    ) -> Result<PrepareRenameResponse> {
        buffer.read_with(&cx, |buffer, _| match message {
            Some(lsp::PrepareRenameResponse::Range(range))
            | Some(lsp::PrepareRenameResponse::RangeWithPlaceholder { range, .. }) => {
                let Range { start, end } = range_from_lsp(range);
                if buffer.clip_point_utf16(start, Bias::Left) == start.0
                    && buffer.clip_point_utf16(end, Bias::Left) == end.0
                {
                    Ok(PrepareRenameResponse::Success {
                        range: buffer.anchor_after(start)..buffer.anchor_before(end),
                        language_server_id: Some(server_id),
                    })
                } else {
                    Ok(PrepareRenameResponse::InvalidPosition)
                }
            }
            Some(lsp::PrepareRenameResponse::DefaultBehavior { .. }) => {
                let snapshot = buffer.snapshot();
                let (range, _) = snapshot.surrounding_word(self.position, None);
                let range = snapshot.anchor_after(range.start)..snapshot.anchor_before(range.end);
                Ok(PrepareRenameResponse::Success {
                    range,
                    language_server_id: Some(server_id),
                })
            }
            None => Ok(PrepareRenameResponse::InvalidPosition),
        })
    }
}

#[async_trait(?Send)]
impl LspCommand for PerformRename {
    type Response = ProjectTransaction;
    type LspRequest = lsp::request::Rename;

    fn display_name(&self) -> &str {
        "Rename"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        capabilities
            .server_capabilities
            .rename_provider
            .as_ref()
            .is_some_and(|capability| match capability {
                OneOf::Left(enabled) => *enabled,
                OneOf::Right(_) => true,
            })
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::RenameParams> {
        Ok(lsp::RenameParams {
            text_document_position: make_lsp_text_document_position(path, self.position)?,
            new_name: self.new_name.clone(),
            work_done_progress_params: Default::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<lsp::WorkspaceEdit>,
        lsp_store: Entity<LspStore>,
        buffer: Entity<Buffer>,
        server_id: LanguageServerId,
        mut cx: AsyncApp,
    ) -> Result<ProjectTransaction> {
        if let Some(edit) = message {
            let (_, lsp_server) =
                language_server_for_buffer(&lsp_store, &buffer, server_id, &mut cx)?;
            LocalLspStore::deserialize_workspace_edit(
                lsp_store,
                edit,
                self.push_to_history,
                lsp_server,
                &mut cx,
            )
            .await
        } else {
            Ok(ProjectTransaction::default())
        }
    }
}

#[async_trait(?Send)]
impl LspCommand for GetDefinitions {
    type Response = Vec<LocationLink>;
    type LspRequest = lsp::request::GotoDefinition;

    fn display_name(&self) -> &str {
        "Get definition"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        capabilities
            .server_capabilities
            .definition_provider
            .as_ref()
            .is_some_and(|capability| match capability {
                OneOf::Left(supported) => *supported,
                OneOf::Right(_options) => true,
            })
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::GotoDefinitionParams> {
        Ok(lsp::GotoDefinitionParams {
            text_document_position_params: make_lsp_text_document_position(path, self.position)?,
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<lsp::GotoDefinitionResponse>,
        lsp_store: Entity<LspStore>,
        buffer: Entity<Buffer>,
        server_id: LanguageServerId,
        cx: AsyncApp,
    ) -> Result<Vec<LocationLink>> {
        location_links_from_lsp(message, lsp_store, buffer, server_id, cx).await
    }
}

#[async_trait(?Send)]
impl LspCommand for GetEditPredictionDefinitions {
    type Response = Vec<EditPredictionDefinition>;
    type LspRequest = lsp::request::GotoDefinition;

    fn display_name(&self) -> &str {
        "Get edit prediction definition"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        capabilities
            .server_capabilities
            .definition_provider
            .as_ref()
            .is_some_and(|capability| match capability {
                OneOf::Left(supported) => *supported,
                OneOf::Right(_options) => true,
            })
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::GotoDefinitionParams> {
        Ok(lsp::GotoDefinitionParams {
            text_document_position_params: make_lsp_text_document_position(path, self.position)?,
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<lsp::GotoDefinitionResponse>,
        lsp_store: Entity<LspStore>,
        _: Entity<Buffer>,
        _: LanguageServerId,
        cx: AsyncApp,
    ) -> Result<Vec<EditPredictionDefinition>> {
        edit_prediction_definitions_from_lsp(message, lsp_store, cx)
    }
}

#[async_trait(?Send)]
impl LspCommand for GetDeclarations {
    type Response = Vec<LocationLink>;
    type LspRequest = lsp::request::GotoDeclaration;

    fn display_name(&self) -> &str {
        "Get declaration"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        capabilities
            .server_capabilities
            .declaration_provider
            .as_ref()
            .is_some_and(|capability| match capability {
                lsp::DeclarationCapability::Simple(supported) => *supported,
                lsp::DeclarationCapability::RegistrationOptions(..) => true,
                lsp::DeclarationCapability::Options(..) => true,
            })
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::GotoDeclarationParams> {
        Ok(lsp::GotoDeclarationParams {
            text_document_position_params: make_lsp_text_document_position(path, self.position)?,
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<lsp::GotoDeclarationResponse>,
        lsp_store: Entity<LspStore>,
        buffer: Entity<Buffer>,
        server_id: LanguageServerId,
        cx: AsyncApp,
    ) -> Result<Vec<LocationLink>> {
        location_links_from_lsp(message, lsp_store, buffer, server_id, cx).await
    }
}

#[async_trait(?Send)]
impl LspCommand for GetImplementations {
    type Response = Vec<LocationLink>;
    type LspRequest = lsp::request::GotoImplementation;

    fn display_name(&self) -> &str {
        "Get implementation"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        capabilities
            .server_capabilities
            .implementation_provider
            .as_ref()
            .is_some_and(|capability| match capability {
                lsp::ImplementationProviderCapability::Simple(enabled) => *enabled,
                lsp::ImplementationProviderCapability::Options(_options) => true,
            })
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::GotoImplementationParams> {
        Ok(lsp::GotoImplementationParams {
            text_document_position_params: make_lsp_text_document_position(path, self.position)?,
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<lsp::GotoImplementationResponse>,
        lsp_store: Entity<LspStore>,
        buffer: Entity<Buffer>,
        server_id: LanguageServerId,
        cx: AsyncApp,
    ) -> Result<Vec<LocationLink>> {
        location_links_from_lsp(message, lsp_store, buffer, server_id, cx).await
    }
}

#[async_trait(?Send)]
impl LspCommand for GetTypeDefinitions {
    type Response = Vec<LocationLink>;
    type LspRequest = lsp::request::GotoTypeDefinition;

    fn display_name(&self) -> &str {
        "Get type definition"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        !matches!(
            &capabilities.server_capabilities.type_definition_provider,
            None | Some(lsp::TypeDefinitionProviderCapability::Simple(false))
        )
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::GotoTypeDefinitionParams> {
        Ok(lsp::GotoTypeDefinitionParams {
            text_document_position_params: make_lsp_text_document_position(path, self.position)?,
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<lsp::GotoTypeDefinitionResponse>,
        project: Entity<LspStore>,
        buffer: Entity<Buffer>,
        server_id: LanguageServerId,
        cx: AsyncApp,
    ) -> Result<Vec<LocationLink>> {
        location_links_from_lsp(message, project, buffer, server_id, cx).await
    }
}

#[async_trait(?Send)]
impl LspCommand for GetEditPredictionTypeDefinitions {
    type Response = Vec<EditPredictionDefinition>;
    type LspRequest = lsp::request::GotoTypeDefinition;

    fn display_name(&self) -> &str {
        "Get edit prediction type definition"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        !matches!(
            &capabilities.server_capabilities.type_definition_provider,
            None | Some(lsp::TypeDefinitionProviderCapability::Simple(false))
        )
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::GotoTypeDefinitionParams> {
        Ok(lsp::GotoTypeDefinitionParams {
            text_document_position_params: make_lsp_text_document_position(path, self.position)?,
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<lsp::GotoDefinitionResponse>,
        lsp_store: Entity<LspStore>,
        _: Entity<Buffer>,
        _: LanguageServerId,
        cx: AsyncApp,
    ) -> Result<Vec<EditPredictionDefinition>> {
        edit_prediction_definitions_from_lsp(message, lsp_store, cx)
    }
}

fn language_server_for_buffer(
    lsp_store: &Entity<LspStore>,
    buffer: &Entity<Buffer>,
    server_id: LanguageServerId,
    cx: &mut AsyncApp,
) -> Result<(Arc<CachedLspAdapter>, Arc<LanguageServer>)> {
    lsp_store
        .update(cx, |lsp_store, cx| {
            buffer.update(cx, |buffer, cx| {
                lsp_store
                    .language_server_for_local_buffer(buffer, server_id, cx)
                    .map(|(adapter, server)| (adapter.clone(), server.clone()))
            })
        })
        .context("no language server found for buffer")
}

pub async fn location_links_from_lsp(
    message: Option<lsp::GotoDefinitionResponse>,
    lsp_store: Entity<LspStore>,
    buffer: Entity<Buffer>,
    server_id: LanguageServerId,
    mut cx: AsyncApp,
) -> Result<Vec<LocationLink>> {
    let unresolved_links = definition_locations_from_lsp(message);

    let (_, language_server) = language_server_for_buffer(&lsp_store, &buffer, server_id, &mut cx)?;
    let mut definitions = Vec::new();
    for (origin_range, target_uri, target_range) in unresolved_links {
        let target_buffer_handle = lsp_store
            .update(&mut cx, |this, cx| {
                this.open_local_buffer_via_lsp(target_uri, language_server.server_id(), cx)
            })
            .await?;

        cx.update(|cx| {
            let origin_location = origin_range.map(|origin_range| {
                let origin_buffer = buffer.read(cx);
                let origin_range = range_from_lsp(origin_range);
                let origin_start = origin_buffer.clip_point_utf16(origin_range.start, Bias::Left);
                let origin_end = origin_buffer.clip_point_utf16(origin_range.end, Bias::Left);
                Location {
                    buffer: buffer.clone(),
                    range: origin_buffer.anchor_after(origin_start)
                        ..origin_buffer.anchor_before(origin_end),
                }
            });

            let target_buffer = target_buffer_handle.read(cx);
            let target_range = range_from_lsp(target_range);
            let target_start = target_buffer.clip_point_utf16(target_range.start, Bias::Left);
            let target_end = target_buffer.clip_point_utf16(target_range.end, Bias::Left);
            let target_location = Location {
                buffer: target_buffer_handle,
                range: target_buffer.anchor_after(target_start)
                    ..target_buffer.anchor_before(target_end),
            };

            definitions.push(LocationLink {
                origin: origin_location,
                target: target_location,
            })
        });
    }
    Ok(definitions)
}

fn definition_locations_from_lsp(
    message: Option<lsp::GotoDefinitionResponse>,
) -> Vec<(Option<lsp::Range>, lsp::Uri, lsp::Range)> {
    let Some(message) = message else {
        return Vec::new();
    };

    let mut locations = Vec::new();
    match message {
        lsp::GotoDefinitionResponse::Scalar(location) => {
            locations.push((None, location.uri, location.range));
        }

        lsp::GotoDefinitionResponse::Array(locations_from_lsp) => {
            locations.extend(
                locations_from_lsp
                    .into_iter()
                    .map(|location| (None, location.uri, location.range)),
            );
        }

        lsp::GotoDefinitionResponse::Link(links) => {
            locations.extend(links.into_iter().map(|link| {
                (
                    link.origin_selection_range,
                    link.target_uri,
                    link.target_selection_range,
                )
            }));
        }
    }
    locations
}

fn edit_prediction_definitions_from_lsp(
    message: Option<lsp::GotoDefinitionResponse>,
    lsp_store: Entity<LspStore>,
    mut cx: AsyncApp,
) -> Result<Vec<EditPredictionDefinition>> {
    let unresolved_locations = definition_locations_from_lsp(message);
    lsp_store.update(&mut cx, |lsp_store, cx| {
        use util::paths::UrlExt as _;
        let mut definitions = Vec::new();
        let worktree_store = lsp_store.worktree_store().read(cx);
        let path_style = worktree_store.path_style();

        for (_, uri, range) in unresolved_locations {
            let Ok(abs_path) = uri.to_file_path_ext(path_style) else {
                continue;
            };
            let Some((worktree, relative_path)) = worktree_store.find_worktree(&abs_path, cx)
            else {
                continue;
            };
            let worktree = worktree.read(cx);
            if !worktree.is_visible() || worktree.is_single_file() {
                continue;
            }
            definitions.push(EditPredictionDefinition {
                path: ProjectPath {
                    worktree_id: worktree.id(),
                    path: relative_path,
                },
                range: range_from_lsp(range),
            });
        }

        Ok(definitions)
    })
}

pub async fn location_link_from_lsp(
    link: lsp::LocationLink,
    lsp_store: &Entity<LspStore>,
    buffer: &Entity<Buffer>,
    server_id: LanguageServerId,
    cx: &mut AsyncApp,
) -> Result<LocationLink> {
    let (_, language_server) = language_server_for_buffer(lsp_store, buffer, server_id, cx)?;

    let (origin_range, target_uri, target_range) = (
        link.origin_selection_range,
        link.target_uri,
        link.target_selection_range,
    );

    let target_buffer_handle = lsp_store
        .update(cx, |lsp_store, cx| {
            lsp_store.open_local_buffer_via_lsp(target_uri, language_server.server_id(), cx)
        })
        .await?;

    Ok(cx.update(|cx| {
        let origin_location = origin_range.map(|origin_range| {
            let origin_buffer = buffer.read(cx);
            let origin_range = range_from_lsp(origin_range);
            let origin_start = origin_buffer.clip_point_utf16(origin_range.start, Bias::Left);
            let origin_end = origin_buffer.clip_point_utf16(origin_range.end, Bias::Left);
            Location {
                buffer: buffer.clone(),
                range: origin_buffer.anchor_after(origin_start)
                    ..origin_buffer.anchor_before(origin_end),
            }
        });

        let target_buffer = target_buffer_handle.read(cx);
        let target_range = range_from_lsp(target_range);
        let target_start = target_buffer.clip_point_utf16(target_range.start, Bias::Left);
        let target_end = target_buffer.clip_point_utf16(target_range.end, Bias::Left);
        let target_location = Location {
            buffer: target_buffer_handle,
            range: target_buffer.anchor_after(target_start)
                ..target_buffer.anchor_before(target_end),
        };

        LocationLink {
            origin: origin_location,
            target: target_location,
        }
    }))
}

#[async_trait(?Send)]
impl LspCommand for GetReferences {
    type Response = Vec<Location>;
    type LspRequest = lsp::request::References;

    fn display_name(&self) -> &str {
        "Find all references"
    }

    fn status(&self) -> Option<String> {
        Some("Finding references...".to_owned())
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        match &capabilities.server_capabilities.references_provider {
            Some(OneOf::Left(has_support)) => *has_support,
            Some(OneOf::Right(_)) => true,
            None => false,
        }
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::ReferenceParams> {
        Ok(lsp::ReferenceParams {
            text_document_position: make_lsp_text_document_position(path, self.position)?,
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
            context: lsp::ReferenceContext {
                include_declaration: true,
            },
        })
    }

    async fn response_from_lsp(
        self,
        locations: Option<Vec<lsp::Location>>,
        lsp_store: Entity<LspStore>,
        buffer: Entity<Buffer>,
        server_id: LanguageServerId,
        mut cx: AsyncApp,
    ) -> Result<Vec<Location>> {
        let mut references = Vec::new();
        let (_, language_server) =
            language_server_for_buffer(&lsp_store, &buffer, server_id, &mut cx)?;

        if let Some(locations) = locations {
            for lsp_location in locations {
                let target_buffer_handle = lsp_store
                    .update(&mut cx, |lsp_store, cx| {
                        lsp_store.open_local_buffer_via_lsp(
                            lsp_location.uri,
                            language_server.server_id(),
                            cx,
                        )
                    })
                    .await?;

                target_buffer_handle
                    .clone()
                    .read_with(&cx, |target_buffer, _| {
                        let range = range_from_lsp(lsp_location.range);
                        let target_start = target_buffer.clip_point_utf16(range.start, Bias::Left);
                        let target_end = target_buffer.clip_point_utf16(range.end, Bias::Left);
                        references.push(Location {
                            buffer: target_buffer_handle,
                            range: target_buffer.anchor_after(target_start)
                                ..target_buffer.anchor_before(target_end),
                        });
                    });
            }
        }

        Ok(references)
    }
}

#[async_trait(?Send)]
impl LspCommand for GetDocumentHighlights {
    type Response = Vec<DocumentHighlight>;
    type LspRequest = lsp::request::DocumentHighlightRequest;

    fn display_name(&self) -> &str {
        "Get document highlights"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        capabilities
            .server_capabilities
            .document_highlight_provider
            .as_ref()
            .is_some_and(|capability| match capability {
                OneOf::Left(supported) => *supported,
                OneOf::Right(_options) => true,
            })
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::DocumentHighlightParams> {
        Ok(lsp::DocumentHighlightParams {
            text_document_position_params: make_lsp_text_document_position(path, self.position)?,
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        })
    }

    async fn response_from_lsp(
        self,
        lsp_highlights: Option<Vec<lsp::DocumentHighlight>>,
        _: Entity<LspStore>,
        buffer: Entity<Buffer>,
        _: LanguageServerId,
        cx: AsyncApp,
    ) -> Result<Vec<DocumentHighlight>> {
        Ok(buffer.read_with(&cx, |buffer, _| {
            let mut lsp_highlights = lsp_highlights.unwrap_or_default();
            lsp_highlights.sort_unstable_by_key(|h| (h.range.start, Reverse(h.range.end)));
            lsp_highlights
                .into_iter()
                .map(|lsp_highlight| {
                    let range = range_from_lsp(lsp_highlight.range);
                    let start = buffer.clip_point_utf16(range.start, Bias::Left);
                    let end = buffer.clip_point_utf16(range.end, Bias::Left);
                    DocumentHighlight {
                        range: buffer.anchor_after(start)..buffer.anchor_before(end),
                        kind: lsp_highlight
                            .kind
                            .unwrap_or(lsp::DocumentHighlightKind::READ),
                    }
                })
                .collect()
        }))
    }
}

#[async_trait(?Send)]
impl LspCommand for GetDocumentSymbols {
    type Response = Vec<DocumentSymbol>;
    type LspRequest = lsp::request::DocumentSymbolRequest;

    fn display_name(&self) -> &str {
        "Get document symbols"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        capabilities
            .server_capabilities
            .document_symbol_provider
            .as_ref()
            .is_some_and(|capability| match capability {
                OneOf::Left(supported) => *supported,
                OneOf::Right(_options) => true,
            })
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::DocumentSymbolParams> {
        Ok(lsp::DocumentSymbolParams {
            text_document: make_text_document_identifier(path)?,
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        })
    }

    async fn response_from_lsp(
        self,
        lsp_symbols: Option<lsp::DocumentSymbolResponse>,
        _: Entity<LspStore>,
        _: Entity<Buffer>,
        _: LanguageServerId,
        _: AsyncApp,
    ) -> Result<Vec<DocumentSymbol>> {
        let Some(lsp_symbols) = lsp_symbols else {
            return Ok(Vec::new());
        };

        let symbols = match lsp_symbols {
            lsp::DocumentSymbolResponse::Flat(symbol_information) => symbol_information
                .into_iter()
                .map(|lsp_symbol| DocumentSymbol {
                    name: lsp_symbol.name,
                    kind: lsp_to_symbol_kind(lsp_symbol.kind),
                    range: range_from_lsp(lsp_symbol.location.range),
                    selection_range: range_from_lsp(lsp_symbol.location.range),
                    children: Vec::new(),
                })
                .collect(),
            lsp::DocumentSymbolResponse::Nested(nested_responses) => {
                fn convert_symbol(lsp_symbol: lsp::DocumentSymbol) -> DocumentSymbol {
                    DocumentSymbol {
                        name: lsp_symbol.name,
                        kind: lsp_to_symbol_kind(lsp_symbol.kind),
                        range: range_from_lsp(lsp_symbol.range),
                        selection_range: range_from_lsp(lsp_symbol.selection_range),
                        children: lsp_symbol
                            .children
                            .map(|children| {
                                children.into_iter().map(convert_symbol).collect::<Vec<_>>()
                            })
                            .unwrap_or_default(),
                    }
                }
                nested_responses.into_iter().map(convert_symbol).collect()
            }
        };
        Ok(symbols)
    }
}

#[async_trait(?Send)]
impl LspCommand for GetSignatureHelp {
    type Response = Option<SignatureHelp>;
    type LspRequest = lsp::SignatureHelpRequest;

    fn display_name(&self) -> &str {
        "Get signature help"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        capabilities
            .server_capabilities
            .signature_help_provider
            .is_some()
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _cx: &App,
    ) -> Result<lsp::SignatureHelpParams> {
        Ok(lsp::SignatureHelpParams {
            text_document_position_params: make_lsp_text_document_position(path, self.position)?,
            context: None,
            work_done_progress_params: Default::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<lsp::SignatureHelp>,
        lsp_store: Entity<LspStore>,
        _: Entity<Buffer>,
        id: LanguageServerId,
        cx: AsyncApp,
    ) -> Result<Self::Response> {
        let Some(message) = message else {
            return Ok(None);
        };
        Ok(cx.update(|cx| {
            SignatureHelp::new(
                message,
                Some(lsp_store.read(cx).languages.clone()),
                Some(id),
                cx,
            )
        }))
    }
}

#[async_trait(?Send)]
impl LspCommand for GetHover {
    type Response = Option<Hover>;
    type LspRequest = lsp::request::HoverRequest;

    fn display_name(&self) -> &str {
        "Get hover"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        match capabilities.server_capabilities.hover_provider.as_ref() {
            Some(lsp::HoverProviderCapability::Simple(enabled)) => *enabled,
            Some(lsp::HoverProviderCapability::Options(_)) => true,
            None => false,
        }
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::HoverParams> {
        Ok(lsp::HoverParams {
            text_document_position_params: make_lsp_text_document_position(path, self.position)?,
            work_done_progress_params: Default::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<lsp::Hover>,
        _: Entity<LspStore>,
        buffer: Entity<Buffer>,
        _: LanguageServerId,
        cx: AsyncApp,
    ) -> Result<Self::Response> {
        let Some(hover) = message else {
            return Ok(None);
        };

        let (language, range) = buffer.read_with(&cx, |buffer, _| {
            (
                buffer.language().cloned(),
                hover.range.map(|range| {
                    let range = range_from_lsp(range);
                    let token_start = buffer.clip_point_utf16(range.start, Bias::Left);
                    let token_end = buffer.clip_point_utf16(range.end, Bias::Left);
                    buffer.anchor_after(token_start)..buffer.anchor_before(token_end)
                }),
            )
        });

        fn hover_blocks_from_marked_string(marked_string: lsp::MarkedString) -> Option<HoverBlock> {
            let block = match marked_string {
                lsp::MarkedString::String(content) => HoverBlock {
                    text: content,
                    kind: HoverBlockKind::Markdown,
                },
                lsp::MarkedString::LanguageString(lsp::LanguageString { language, value }) => {
                    HoverBlock {
                        text: value,
                        kind: HoverBlockKind::Code { language },
                    }
                }
            };
            if block.text.is_empty() {
                None
            } else {
                Some(block)
            }
        }

        let contents = match hover.contents {
            lsp::HoverContents::Scalar(marked_string) => {
                hover_blocks_from_marked_string(marked_string)
                    .into_iter()
                    .collect()
            }
            lsp::HoverContents::Array(marked_strings) => marked_strings
                .into_iter()
                .filter_map(hover_blocks_from_marked_string)
                .collect(),
            lsp::HoverContents::Markup(markup_content) => vec![HoverBlock {
                text: markup_content.value,
                kind: if markup_content.kind == lsp::MarkupKind::Markdown {
                    HoverBlockKind::Markdown
                } else {
                    HoverBlockKind::PlainText
                },
            }],
        };

        Ok(Some(Hover {
            contents,
            range,
            language,
        }))
    }
}

impl GetCompletions {
    pub fn can_resolve_completions(capabilities: &lsp::ServerCapabilities) -> bool {
        capabilities
            .completion_provider
            .as_ref()
            .and_then(|options| options.resolve_provider)
            .unwrap_or(false)
    }
}

#[async_trait(?Send)]
impl LspCommand for GetCompletions {
    type Response = CoreCompletionResponse;
    type LspRequest = lsp::request::Completion;

    fn display_name(&self) -> &str {
        "Get completion"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        capabilities
            .server_capabilities
            .completion_provider
            .is_some()
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::CompletionParams> {
        Ok(lsp::CompletionParams {
            text_document_position: make_lsp_text_document_position(path, self.position)?,
            context: Some(self.context.clone()),
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        })
    }

    async fn response_from_lsp(
        self,
        completions: Option<lsp::CompletionResponse>,
        lsp_store: Entity<LspStore>,
        buffer: Entity<Buffer>,
        server_id: LanguageServerId,
        mut cx: AsyncApp,
    ) -> Result<Self::Response> {
        let mut response_list = None;
        let (mut completions, mut is_incomplete) = if let Some(completions) = completions {
            match completions {
                lsp::CompletionResponse::Array(completions) => (completions, false),
                lsp::CompletionResponse::List(mut list) => {
                    let is_incomplete = list.is_incomplete;
                    let items = std::mem::take(&mut list.items);
                    response_list = Some(list);
                    (items, is_incomplete)
                }
            }
        } else {
            (Vec::new(), false)
        };

        let unfiltered_completions_count = completions.len();

        let language_server_adapter = lsp_store
            .read_with(&cx, |lsp_store, _| {
                lsp_store.language_server_adapter_for_id(server_id)
            })
            .with_context(|| format!("no language server with id {server_id}"))?;

        let lsp_defaults = response_list
            .as_ref()
            .and_then(|list| list.item_defaults.clone())
            .map(Arc::new);

        let mut completion_edits = Vec::new();
        buffer.update(&mut cx, |buffer, _cx| {
            let snapshot = buffer.snapshot();
            let clipped_position = buffer.clip_point_utf16(Unclipped(self.position), Bias::Left);

            let mut range_for_token = None;
            completions.retain(|lsp_completion| {
                let lsp_edit = lsp_completion.text_edit.clone().or_else(|| {
                    let default_text_edit = lsp_defaults.as_deref()?.edit_range.as_ref()?;
                    let new_text = lsp_completion
                        .text_edit_text
                        .as_ref()
                        .unwrap_or(&lsp_completion.label)
                        .clone();
                    match default_text_edit {
                        CompletionListItemDefaultsEditRange::Range(range) => {
                            Some(lsp::CompletionTextEdit::Edit(lsp::TextEdit {
                                range: *range,
                                new_text,
                            }))
                        }
                        CompletionListItemDefaultsEditRange::InsertAndReplace {
                            insert,
                            replace,
                        } => Some(lsp::CompletionTextEdit::InsertAndReplace(
                            lsp::InsertReplaceEdit {
                                new_text,
                                insert: *insert,
                                replace: *replace,
                            },
                        )),
                    }
                });

                let edit = match lsp_edit {
                    // If the language server provides a range to overwrite, then
                    // check that the range is valid.
                    Some(completion_text_edit) => {
                        match parse_completion_text_edit(&completion_text_edit, &snapshot) {
                            Some(edit) => edit,
                            None => return false,
                        }
                    }
                    // If the language server does not provide a range, then infer
                    // the range based on the syntax tree.
                    None => {
                        if self.position != clipped_position {
                            log::info!("completion out of expected range ");
                            return false;
                        }

                        let default_edit_range = lsp_defaults.as_ref().and_then(|lsp_defaults| {
                            lsp_defaults
                                .edit_range
                                .as_ref()
                                .and_then(|range| match range {
                                    CompletionListItemDefaultsEditRange::Range(r) => Some(r),
                                    _ => None,
                                })
                        });

                        let range = if let Some(range) = default_edit_range {
                            let range = range_from_lsp(*range);
                            let start = snapshot.clip_point_utf16(range.start, Bias::Left);
                            let end = snapshot.clip_point_utf16(range.end, Bias::Left);
                            if start != range.start.0 || end != range.end.0 {
                                log::info!("completion out of expected range");
                                return false;
                            }

                            snapshot.anchor_before(start)..snapshot.anchor_after(end)
                        } else {
                            range_for_token
                                .get_or_insert_with(|| {
                                    let offset = self.position.to_offset(&snapshot);
                                    let (range, kind) = snapshot.surrounding_word(
                                        offset,
                                        Some(CharScopeContext::Completion),
                                    );
                                    let range = if kind == Some(CharKind::Word) {
                                        range
                                    } else {
                                        offset..offset
                                    };

                                    snapshot.anchor_before(range.start)
                                        ..snapshot.anchor_after(range.end)
                                })
                                .clone()
                        };

                        // We already know text_edit is None here
                        let text = lsp_completion
                            .insert_text
                            .as_ref()
                            .unwrap_or(&lsp_completion.label)
                            .clone();

                        ParsedCompletionEdit {
                            replace_range: range,
                            insert_range: None,
                            new_text: text,
                        }
                    }
                };

                completion_edits.push(edit);
                true
            });
        });

        // If completions were filtered out due to errors that may be transient, mark the result
        // incomplete so that it is re-queried.
        if unfiltered_completions_count != completions.len() {
            is_incomplete = true;
        }

        language_server_adapter
            .process_completions(&mut completions)
            .await;

        let completions = completions
            .into_iter()
            .zip(completion_edits)
            .map(|(mut lsp_completion, mut edit)| {
                LineEnding::normalize(&mut edit.new_text);
                if lsp_completion.data.is_none()
                    && let Some(default_data) = lsp_defaults
                        .as_ref()
                        .and_then(|item_defaults| item_defaults.data.clone())
                {
                    // Servers (e.g. JDTLS) prefer unchanged completions, when resolving the items later,
                    // so we do not insert the defaults here, but `data` is needed for resolving, so this is an exception.
                    lsp_completion.data = Some(default_data);
                }
                CoreCompletion {
                    replace_range: edit.replace_range,
                    new_text: edit.new_text,
                    source: CompletionSource::Lsp {
                        insert_range: edit.insert_range,
                        server_id,
                        lsp_completion: Box::new(lsp_completion),
                        lsp_defaults: lsp_defaults.clone(),
                        resolved: false,
                    },
                }
            })
            .collect();

        Ok(CoreCompletionResponse {
            completions,
            is_incomplete,
        })
    }
}

pub struct ParsedCompletionEdit {
    pub replace_range: Range<Anchor>,
    pub insert_range: Option<Range<Anchor>>,
    pub new_text: String,
}

pub(crate) fn parse_completion_text_edit(
    edit: &lsp::CompletionTextEdit,
    snapshot: &BufferSnapshot,
) -> Option<ParsedCompletionEdit> {
    let (replace_range, insert_range, new_text) = match edit {
        lsp::CompletionTextEdit::Edit(edit) => (edit.range, None, &edit.new_text),
        lsp::CompletionTextEdit::InsertAndReplace(edit) => {
            (edit.replace, Some(edit.insert), &edit.new_text)
        }
    };

    let replace_range = {
        let range = range_from_lsp(replace_range);
        let start = snapshot.clip_point_utf16(range.start, Bias::Left);
        let end = snapshot.clip_point_utf16(range.end, Bias::Left);
        if start != range.start.0 || end != range.end.0 {
            log::info!(
                "completion out of expected range, start: {start:?}, end: {end:?}, range: {range:?}"
            );
            return None;
        }
        snapshot.anchor_before(start)..snapshot.anchor_after(end)
    };

    let insert_range = match insert_range {
        None => None,
        Some(insert_range) => {
            let range = range_from_lsp(insert_range);
            let start = snapshot.clip_point_utf16(range.start, Bias::Left);
            let end = snapshot.clip_point_utf16(range.end, Bias::Left);
            if start != range.start.0 || end != range.end.0 {
                log::info!("completion (insert) out of expected range");
                return None;
            }
            Some(snapshot.anchor_before(start)..snapshot.anchor_after(end))
        }
    };

    Some(ParsedCompletionEdit {
        insert_range,
        replace_range,
        new_text: new_text.clone(),
    })
}

#[async_trait(?Send)]
impl LspCommand for GetCodeActions {
    type Response = Vec<CodeAction>;
    type LspRequest = lsp::request::CodeActionRequest;

    fn display_name(&self) -> &str {
        "Get code actions"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        match &capabilities.server_capabilities.code_action_provider {
            None => false,
            Some(lsp::CodeActionProviderCapability::Simple(false)) => false,
            _ => {
                // If we do know that we want specific code actions AND we know that
                // the server only supports specific code actions, then we want to filter
                // down to the ones that are supported.
                if let Some((requested, supported)) = self
                    .kinds
                    .as_ref()
                    .zip(Self::supported_code_action_kinds(capabilities))
                {
                    requested.iter().any(|requested_kind| {
                        supported.iter().any(|supported_kind| {
                            code_action_kind_matches(requested_kind, supported_kind)
                        })
                    })
                } else {
                    true
                }
            }
        }
    }

    fn to_lsp(
        &self,
        path: &Path,
        buffer: &Buffer,
        language_server: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::CodeActionParams> {
        let text_document = make_text_document_identifier(path)?;
        let snapshot = buffer.snapshot();
        let mut relevant_diagnostics = Vec::new();
        let target_server_id = language_server.server_id();
        for (source_server_id, entry) in
            snapshot.diagnostic_entries_in_range_with_server_id(self.range.clone(), false)
        {
            let downgrade_markup =
                source_server_id != target_server_id && entry.diagnostic.message.has_lsp_markup();
            let entry = entry.clone().map_coordinates(|range| {
                range.start.to_point_utf16(&snapshot)..range.end.to_point_utf16(&snapshot)
            });
            let mut diagnostic = entry.to_lsp_diagnostic_stub(&text_document.uri)?;
            if downgrade_markup {
                diagnostic.message =
                    lsp::DiagnosticMessage::from(entry.diagnostic.message.as_str());
            }
            relevant_diagnostics.push(diagnostic);
        }

        Ok(lsp::CodeActionParams {
            text_document: make_text_document_identifier(path)?,
            range: range_to_lsp(self.range.to_point_utf16(buffer))?,
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
            context: lsp::CodeActionContext {
                diagnostics: relevant_diagnostics,
                only: self.kinds.clone(),
                ..lsp::CodeActionContext::default()
            },
        })
    }

    async fn response_from_lsp(
        self,
        actions: Option<lsp::CodeActionResponse>,
        lsp_store: Entity<LspStore>,
        _: Entity<Buffer>,
        server_id: LanguageServerId,
        cx: AsyncApp,
    ) -> Result<Vec<CodeAction>> {
        let requested_kinds = self.kinds.as_ref();

        let language_server = cx.update(|cx| {
            lsp_store
                .read(cx)
                .language_server_for_id(server_id)
                .with_context(|| {
                    format!("Missing the language server that just returned a response {server_id}")
                })
        })?;

        let server_capabilities = language_server.capabilities();
        let available_commands = server_capabilities
            .execute_command_provider
            .as_ref()
            .map(|options| options.commands.as_slice())
            .unwrap_or_default();
        Ok(actions
            .unwrap_or_default()
            .into_iter()
            .filter_map(|entry| {
                let (lsp_action, resolved) = match entry {
                    lsp::CodeActionOrCommand::CodeAction(lsp_action) => {
                        if let Some(command) = lsp_action.command.as_ref()
                            && !available_commands.contains(&command.command)
                        {
                            return None;
                        }
                        (LspAction::Action(Box::new(lsp_action)), false)
                    }
                    lsp::CodeActionOrCommand::Command(command) => {
                        if available_commands.contains(&command.command) {
                            (LspAction::Command(command), true)
                        } else {
                            return None;
                        }
                    }
                };

                if let Some((kinds, kind)) = requested_kinds.zip(lsp_action.action_kind())
                    && !kinds
                        .iter()
                        .any(|requested_kind| code_action_kind_matches(requested_kind, &kind))
                {
                    return None;
                }

                Some(CodeAction {
                    server_id,
                    range: self.range.clone(),
                    lsp_action,
                    resolved,
                })
            })
            .collect())
    }
}

impl GetCodeActions {
    fn supported_code_action_kinds<'a>(
        capabilities: AdapterServerCapabilities<'a>,
    ) -> Option<&'a [CodeActionKind]> {
        match capabilities
            .server_capabilities
            .code_action_provider
            .as_ref()
        {
            Some(lsp::CodeActionProviderCapability::Options(CodeActionOptions {
                code_action_kinds: Some(supported_action_kinds),
                ..
            })) => Some(supported_action_kinds),
            _ => capabilities.code_action_kinds,
        }
    }

    pub fn can_resolve_actions(capabilities: &ServerCapabilities) -> bool {
        capabilities
            .code_action_provider
            .as_ref()
            .and_then(|options| match options {
                lsp::CodeActionProviderCapability::Simple(_is_supported) => None,
                lsp::CodeActionProviderCapability::Options(options) => options.resolve_provider,
            })
            .unwrap_or(false)
    }
}

impl OnTypeFormatting {
    pub fn supports_on_type_formatting(trigger: &str, capabilities: &ServerCapabilities) -> bool {
        let Some(on_type_formatting_options) = &capabilities.document_on_type_formatting_provider
        else {
            return false;
        };
        on_type_formatting_options
            .first_trigger_character
            .contains(trigger)
            || on_type_formatting_options
                .more_trigger_character
                .iter()
                .flatten()
                .any(|chars| chars.contains(trigger))
    }
}

#[async_trait(?Send)]
impl LspCommand for OnTypeFormatting {
    type Response = Option<Transaction>;
    type LspRequest = lsp::request::OnTypeFormatting;

    fn display_name(&self) -> &str {
        "Formatting on typing"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        Self::supports_on_type_formatting(&self.trigger, &capabilities.server_capabilities)
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::DocumentOnTypeFormattingParams> {
        Ok(lsp::DocumentOnTypeFormattingParams {
            text_document_position: make_lsp_text_document_position(path, self.position)?,
            ch: self.trigger.clone(),
            options: self.options.clone(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<Vec<lsp::TextEdit>>,
        lsp_store: Entity<LspStore>,
        buffer: Entity<Buffer>,
        server_id: LanguageServerId,
        mut cx: AsyncApp,
    ) -> Result<Option<Transaction>> {
        if let Some(edits) = message {
            let (lsp_adapter, lsp_server) =
                language_server_for_buffer(&lsp_store, &buffer, server_id, &mut cx)?;
            LocalLspStore::deserialize_text_edits(
                lsp_store,
                buffer,
                edits,
                self.push_to_history,
                lsp_adapter,
                lsp_server,
                &mut cx,
            )
            .await
        } else {
            Ok(None)
        }
    }
}

impl InlayHints {
    pub async fn lsp_to_project_hint(
        lsp_hint: lsp::InlayHint,
        buffer_handle: &Entity<Buffer>,
        server_id: LanguageServerId,
        resolve_state: ResolveState,
        force_no_type_left_padding: bool,
        cx: &mut AsyncApp,
    ) -> anyhow::Result<InlayHint> {
        let kind = lsp_hint.kind.and_then(|kind| match kind {
            lsp::InlayHintKind::TYPE => Some(InlayHintKind::Type),
            lsp::InlayHintKind::PARAMETER => Some(InlayHintKind::Parameter),
            _ => None,
        });

        let position = buffer_handle.read_with(cx, |buffer, _| {
            let position = buffer.clip_point_utf16(point_from_lsp(lsp_hint.position), Bias::Left);
            if kind == Some(InlayHintKind::Parameter) {
                buffer.anchor_before(position)
            } else {
                buffer.anchor_after(position)
            }
        });
        let label = Self::lsp_inlay_label_to_project(lsp_hint.label, server_id)
            .await
            .context("lsp to project inlay hint conversion")?;
        let padding_left = if force_no_type_left_padding && kind == Some(InlayHintKind::Type) {
            false
        } else {
            lsp_hint.padding_left.unwrap_or(false)
        };

        Ok(InlayHint {
            position,
            padding_left,
            padding_right: lsp_hint.padding_right.unwrap_or(false),
            label,
            kind,
            tooltip: lsp_hint.tooltip.map(|tooltip| match tooltip {
                lsp::InlayHintTooltip::String(s) => InlayHintTooltip::String(s),
                lsp::InlayHintTooltip::MarkupContent(markup_content) => {
                    InlayHintTooltip::MarkupContent(MarkupContent {
                        kind: match markup_content.kind {
                            lsp::MarkupKind::PlainText => HoverBlockKind::PlainText,
                            lsp::MarkupKind::Markdown => HoverBlockKind::Markdown,
                        },
                        value: markup_content.value,
                    })
                }
            }),
            resolve_state,
        })
    }

    async fn lsp_inlay_label_to_project(
        lsp_label: lsp::InlayHintLabel,
        server_id: LanguageServerId,
    ) -> anyhow::Result<InlayHintLabel> {
        let label = match lsp_label {
            lsp::InlayHintLabel::String(s) => InlayHintLabel::String(s),
            lsp::InlayHintLabel::LabelParts(lsp_parts) => {
                let mut parts = Vec::with_capacity(lsp_parts.len());
                for lsp_part in lsp_parts {
                    parts.push(InlayHintLabelPart {
                        value: lsp_part.value,
                        tooltip: lsp_part.tooltip.map(|tooltip| match tooltip {
                            lsp::InlayHintLabelPartTooltip::String(s) => {
                                InlayHintLabelPartTooltip::String(s)
                            }
                            lsp::InlayHintLabelPartTooltip::MarkupContent(markup_content) => {
                                InlayHintLabelPartTooltip::MarkupContent(MarkupContent {
                                    kind: match markup_content.kind {
                                        lsp::MarkupKind::PlainText => HoverBlockKind::PlainText,
                                        lsp::MarkupKind::Markdown => HoverBlockKind::Markdown,
                                    },
                                    value: markup_content.value,
                                })
                            }
                        }),
                        location: Some(server_id).zip(lsp_part.location),
                        command: Some(server_id).zip(lsp_part.command),
                    });
                }
                InlayHintLabel::LabelParts(parts)
            }
        };

        Ok(label)
    }

    pub fn project_to_lsp_hint(hint: InlayHint, snapshot: &BufferSnapshot) -> lsp::InlayHint {
        lsp::InlayHint {
            position: point_to_lsp(hint.position.to_point_utf16(snapshot)),
            kind: hint.kind.map(|kind| match kind {
                InlayHintKind::Type => lsp::InlayHintKind::TYPE,
                InlayHintKind::Parameter => lsp::InlayHintKind::PARAMETER,
            }),
            text_edits: None,
            tooltip: hint.tooltip.and_then(|tooltip| {
                Some(match tooltip {
                    InlayHintTooltip::String(s) => lsp::InlayHintTooltip::String(s),
                    InlayHintTooltip::MarkupContent(markup_content) => {
                        lsp::InlayHintTooltip::MarkupContent(lsp::MarkupContent {
                            kind: match markup_content.kind {
                                HoverBlockKind::PlainText => lsp::MarkupKind::PlainText,
                                HoverBlockKind::Markdown => lsp::MarkupKind::Markdown,
                                HoverBlockKind::Code { .. } => return None,
                            },
                            value: markup_content.value,
                        })
                    }
                })
            }),
            label: match hint.label {
                InlayHintLabel::String(s) => lsp::InlayHintLabel::String(s),
                InlayHintLabel::LabelParts(label_parts) => lsp::InlayHintLabel::LabelParts(
                    label_parts
                        .into_iter()
                        .map(|part| lsp::InlayHintLabelPart {
                            value: part.value,
                            tooltip: part.tooltip.and_then(|tooltip| {
                                Some(match tooltip {
                                    InlayHintLabelPartTooltip::String(s) => {
                                        lsp::InlayHintLabelPartTooltip::String(s)
                                    }
                                    InlayHintLabelPartTooltip::MarkupContent(markup_content) => {
                                        lsp::InlayHintLabelPartTooltip::MarkupContent(
                                            lsp::MarkupContent {
                                                kind: match markup_content.kind {
                                                    HoverBlockKind::PlainText => {
                                                        lsp::MarkupKind::PlainText
                                                    }
                                                    HoverBlockKind::Markdown => {
                                                        lsp::MarkupKind::Markdown
                                                    }
                                                    HoverBlockKind::Code { .. } => return None,
                                                },
                                                value: markup_content.value,
                                            },
                                        )
                                    }
                                })
                            }),
                            location: part.location.map(|(_, location)| location),
                            command: part.command.map(|(_, command)| command),
                        })
                        .collect(),
                ),
            },
            padding_left: Some(hint.padding_left),
            padding_right: Some(hint.padding_right),
            data: match hint.resolve_state {
                ResolveState::CanResolve(_, data) => data,
                ResolveState::Resolving | ResolveState::Resolved => None,
            },
        }
    }

    pub fn can_resolve_inlays(capabilities: &ServerCapabilities) -> bool {
        capabilities
            .inlay_hint_provider
            .as_ref()
            .and_then(|options| match options {
                OneOf::Left(_is_supported) => None,
                OneOf::Right(capabilities) => match capabilities {
                    lsp::InlayHintServerCapabilities::Options(o) => o.resolve_provider,
                    lsp::InlayHintServerCapabilities::RegistrationOptions(o) => {
                        o.inlay_hint_options.resolve_provider
                    }
                },
            })
            .unwrap_or(false)
    }

    pub fn check_capabilities(capabilities: &ServerCapabilities) -> bool {
        capabilities
            .inlay_hint_provider
            .as_ref()
            .is_some_and(|inlay_hint_provider| match inlay_hint_provider {
                lsp::OneOf::Left(enabled) => *enabled,
                lsp::OneOf::Right(_) => true,
            })
    }
}

#[async_trait(?Send)]
impl LspCommand for InlayHints {
    type Response = Vec<InlayHint>;
    type LspRequest = lsp::InlayHintRequest;

    fn display_name(&self) -> &str {
        "Inlay hints"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        Self::check_capabilities(&capabilities.server_capabilities)
    }

    fn to_lsp(
        &self,
        path: &Path,
        buffer: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::InlayHintParams> {
        Ok(lsp::InlayHintParams {
            text_document: lsp::TextDocumentIdentifier {
                uri: file_path_to_lsp_url(path)?,
            },
            range: range_to_lsp(self.range.to_point_utf16(buffer))?,
            work_done_progress_params: Default::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<Vec<lsp::InlayHint>>,
        lsp_store: Entity<LspStore>,
        buffer: Entity<Buffer>,
        server_id: LanguageServerId,
        mut cx: AsyncApp,
    ) -> anyhow::Result<Vec<InlayHint>> {
        let (lsp_adapter, lsp_server) =
            language_server_for_buffer(&lsp_store, &buffer, server_id, &mut cx)?;
        // `typescript-language-server` adds padding to the left for type hints, turning
        // `const foo: boolean` into `const foo : boolean` which looks odd.
        // `rust-analyzer` does not have the padding for this case, and we have to accommodate both.
        //
        // We could trim the whole string, but being pessimistic on par with the situation above,
        // there might be a hint with multiple whitespaces at the end(s) which we need to display properly.
        // Hence let's use a heuristic first to handle the most awkward case and look for more.
        let force_no_type_left_padding =
            lsp_adapter.name.0.as_ref() == "typescript-language-server";
        let can_resolve = lsp_store.update(&mut cx, |lsp_store, cx| {
            lsp_store.text_document_capability_matches_for_server(
                &buffer,
                server_id,
                "textDocument/inlayHint",
                |capabilities| InlayHints::can_resolve_inlays(capabilities.server_capabilities),
                cx,
            )
        });

        let hints = message.unwrap_or_default().into_iter().map(|lsp_hint| {
            let resolve_state = if can_resolve {
                ResolveState::CanResolve(lsp_server.server_id(), lsp_hint.data.clone())
            } else {
                ResolveState::Resolved
            };

            let buffer = buffer.clone();
            cx.spawn(async move |cx| {
                InlayHints::lsp_to_project_hint(
                    lsp_hint,
                    &buffer,
                    server_id,
                    resolve_state,
                    force_no_type_left_padding,
                    cx,
                )
                .await
            })
        });
        future::join_all(hints)
            .await
            .into_iter()
            .collect::<anyhow::Result<_>>()
            .context("lsp to project inlay hints conversion")
    }
}

#[async_trait(?Send)]
impl LspCommand for SemanticTokensFull {
    type Response = SemanticTokensResponse;
    type LspRequest = lsp::SemanticTokensFullRequest;

    fn display_name(&self) -> &str {
        "Semantic tokens full"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        capabilities
            .server_capabilities
            .semantic_tokens_provider
            .as_ref()
            .is_some_and(|semantic_tokens_provider| {
                let options = match semantic_tokens_provider {
                    lsp::SemanticTokensServerCapabilities::SemanticTokensOptions(opts) => opts,
                    lsp::SemanticTokensServerCapabilities::SemanticTokensRegistrationOptions(
                        opts,
                    ) => &opts.semantic_tokens_options,
                };

                match options.full {
                    Some(lsp::SemanticTokensFullOptions::Bool(is_supported)) => is_supported,
                    Some(lsp::SemanticTokensFullOptions::Delta { .. }) => true,
                    None => false,
                }
            })
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::SemanticTokensParams> {
        Ok(lsp::SemanticTokensParams {
            text_document: lsp::TextDocumentIdentifier {
                uri: file_path_to_lsp_url(path)?,
            },
            partial_result_params: Default::default(),
            work_done_progress_params: Default::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<lsp::SemanticTokensResult>,
        _: Entity<LspStore>,
        _: Entity<Buffer>,
        _: LanguageServerId,
        _: AsyncApp,
    ) -> anyhow::Result<SemanticTokensResponse> {
        match message {
            Some(lsp::SemanticTokensResult::Tokens(tokens)) => Ok(SemanticTokensResponse::Full {
                data: tokens.data,
                result_id: tokens.result_id.map(SharedString::new),
            }),
            Some(lsp::SemanticTokensResult::Partial(_)) => {
                anyhow::bail!(
                    "Unexpected semantic tokens response with partial result for inlay hints"
                )
            }
            None => Ok(Default::default()),
        }
    }
}

#[async_trait(?Send)]
impl LspCommand for SemanticTokensDelta {
    type Response = SemanticTokensResponse;
    type LspRequest = lsp::SemanticTokensFullDeltaRequest;

    fn display_name(&self) -> &str {
        "Semantic tokens delta"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        capabilities
            .server_capabilities
            .semantic_tokens_provider
            .as_ref()
            .is_some_and(|semantic_tokens_provider| {
                let options = match semantic_tokens_provider {
                    lsp::SemanticTokensServerCapabilities::SemanticTokensOptions(opts) => opts,
                    lsp::SemanticTokensServerCapabilities::SemanticTokensRegistrationOptions(
                        opts,
                    ) => &opts.semantic_tokens_options,
                };

                match options.full {
                    Some(lsp::SemanticTokensFullOptions::Delta { delta }) => delta.unwrap_or(false),
                    // `full: true` (instead of `full: { delta: true }`) means no support for delta.
                    _ => false,
                }
            })
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::SemanticTokensDeltaParams> {
        Ok(lsp::SemanticTokensDeltaParams {
            text_document: lsp::TextDocumentIdentifier {
                uri: file_path_to_lsp_url(path)?,
            },
            previous_result_id: self.previous_result_id.clone().map(|s| s.to_string()),
            partial_result_params: Default::default(),
            work_done_progress_params: Default::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<lsp::SemanticTokensFullDeltaResult>,
        _: Entity<LspStore>,
        _: Entity<Buffer>,
        _: LanguageServerId,
        _: AsyncApp,
    ) -> anyhow::Result<SemanticTokensResponse> {
        match message {
            Some(lsp::SemanticTokensFullDeltaResult::Tokens(tokens)) => {
                Ok(SemanticTokensResponse::Full {
                    data: tokens.data,
                    result_id: tokens.result_id.map(SharedString::new),
                })
            }
            Some(lsp::SemanticTokensFullDeltaResult::TokensDelta(delta)) => {
                Ok(SemanticTokensResponse::Delta {
                    edits: delta
                        .edits
                        .into_iter()
                        .map(|e| SemanticTokensEdit {
                            start: e.start,
                            delete_count: e.delete_count,
                            data: e.data.unwrap_or_default(),
                        })
                        .collect(),
                    result_id: delta.result_id.map(SharedString::new),
                })
            }
            Some(lsp::SemanticTokensFullDeltaResult::PartialTokensDelta { .. }) => {
                anyhow::bail!(
                    "Unexpected semantic tokens response with partial result for inlay hints"
                )
            }
            None => Ok(Default::default()),
        }
    }
}

#[async_trait(?Send)]
impl LspCommand for GetCodeLens {
    type Response = Vec<CodeAction>;
    type LspRequest = lsp::CodeLensRequest;

    fn display_name(&self) -> &str {
        "Code Lens"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        capabilities
            .server_capabilities
            .code_lens_provider
            .is_some()
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::CodeLensParams> {
        Ok(lsp::CodeLensParams {
            text_document: lsp::TextDocumentIdentifier {
                uri: file_path_to_lsp_url(path)?,
            },
            work_done_progress_params: lsp::WorkDoneProgressParams::default(),
            partial_result_params: lsp::PartialResultParams::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<Vec<lsp::CodeLens>>,
        _lsp_store: Entity<LspStore>,
        buffer: Entity<Buffer>,
        server_id: LanguageServerId,
        cx: AsyncApp,
    ) -> anyhow::Result<Vec<CodeAction>> {
        let snapshot = buffer.read_with(&cx, |buffer, _| buffer.snapshot());
        let code_lenses = message.unwrap_or_default();

        Ok(code_lenses
            .into_iter()
            .map(|code_lens| {
                let code_lens_range = range_from_lsp(code_lens.range);
                let start = snapshot.clip_point_utf16(code_lens_range.start, Bias::Left);
                let end = snapshot.clip_point_utf16(code_lens_range.end, Bias::Right);
                let range = snapshot.anchor_before(start)..snapshot.anchor_after(end);
                let resolved = code_lens.command.is_some();
                CodeAction {
                    server_id,
                    range,
                    lsp_action: LspAction::CodeLens(code_lens),
                    resolved,
                }
            })
            .collect())
    }
}

impl LinkedEditingRange {
    pub fn check_server_capabilities(capabilities: &ServerCapabilities) -> bool {
        let Some(linked_editing_options) = capabilities.linked_editing_range_provider.as_ref()
        else {
            return false;
        };
        if let LinkedEditingRangeServerCapabilities::Simple(false) = linked_editing_options {
            return false;
        }
        true
    }
}

#[async_trait(?Send)]
impl LspCommand for LinkedEditingRange {
    type Response = Vec<Range<Anchor>>;
    type LspRequest = lsp::request::LinkedEditingRange;

    fn display_name(&self) -> &str {
        "Linked editing range"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        Self::check_server_capabilities(&capabilities.server_capabilities)
    }

    fn to_lsp(
        &self,
        path: &Path,
        buffer: &Buffer,
        _server: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::LinkedEditingRangeParams> {
        let position = self.position.to_point_utf16(&buffer.snapshot());
        Ok(lsp::LinkedEditingRangeParams {
            text_document_position_params: make_lsp_text_document_position(path, position)?,
            work_done_progress_params: Default::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<lsp::LinkedEditingRanges>,
        _: Entity<LspStore>,
        buffer: Entity<Buffer>,
        _server_id: LanguageServerId,
        cx: AsyncApp,
    ) -> Result<Vec<Range<Anchor>>> {
        if let Some(lsp::LinkedEditingRanges { mut ranges, .. }) = message {
            ranges.sort_by_key(|range| range.start);

            Ok(buffer.read_with(&cx, |buffer, _| {
                ranges
                    .into_iter()
                    .map(|range| {
                        let range = range_from_lsp(range);
                        let start = buffer.clip_point_utf16(range.start, Bias::Left);
                        let end = buffer.clip_point_utf16(range.end, Bias::Left);
                        buffer.anchor_before(start)..buffer.anchor_after(end)
                    })
                    .collect()
            }))
        } else {
            Ok(vec![])
        }
    }
}

impl GetDocumentDiagnostics {
    pub fn deserialize_workspace_diagnostics_report(
        report: lsp::WorkspaceDiagnosticReportResult,
        server_id: LanguageServerId,
        registration_id: Option<SharedString>,
    ) -> Vec<WorkspaceLspPullDiagnostics> {
        let mut pulled_diagnostics = HashMap::default();
        match report {
            lsp::WorkspaceDiagnosticReportResult::Report(workspace_diagnostic_report) => {
                for report in workspace_diagnostic_report.items {
                    match report {
                        lsp::WorkspaceDocumentDiagnosticReport::Full(report) => {
                            process_full_workspace_diagnostics_report(
                                &mut pulled_diagnostics,
                                server_id,
                                report,
                                registration_id.clone(),
                            )
                        }
                        lsp::WorkspaceDocumentDiagnosticReport::Unchanged(report) => {
                            process_unchanged_workspace_diagnostics_report(
                                &mut pulled_diagnostics,
                                server_id,
                                report,
                                registration_id.clone(),
                            )
                        }
                    }
                }
            }
            lsp::WorkspaceDiagnosticReportResult::Partial(
                workspace_diagnostic_report_partial_result,
            ) => {
                for report in workspace_diagnostic_report_partial_result.items {
                    match report {
                        lsp::WorkspaceDocumentDiagnosticReport::Full(report) => {
                            process_full_workspace_diagnostics_report(
                                &mut pulled_diagnostics,
                                server_id,
                                report,
                                registration_id.clone(),
                            )
                        }
                        lsp::WorkspaceDocumentDiagnosticReport::Unchanged(report) => {
                            process_unchanged_workspace_diagnostics_report(
                                &mut pulled_diagnostics,
                                server_id,
                                report,
                                registration_id.clone(),
                            )
                        }
                    }
                }
            }
        }
        pulled_diagnostics.into_values().collect()
    }
}

#[derive(Debug)]
pub struct WorkspaceLspPullDiagnostics {
    pub version: Option<i32>,
    pub diagnostics: LspPullDiagnostics,
}

fn process_full_workspace_diagnostics_report(
    diagnostics: &mut HashMap<lsp::Uri, WorkspaceLspPullDiagnostics>,
    server_id: LanguageServerId,
    report: lsp::WorkspaceFullDocumentDiagnosticReport,
    registration_id: Option<SharedString>,
) {
    let mut new_diagnostics = HashMap::default();
    process_full_diagnostics_report(
        &mut new_diagnostics,
        server_id,
        report.uri,
        report.full_document_diagnostic_report,
        registration_id,
    );
    diagnostics.extend(new_diagnostics.into_iter().map(|(uri, diagnostics)| {
        (
            uri,
            WorkspaceLspPullDiagnostics {
                version: report.version.map(|v| v as i32),
                diagnostics,
            },
        )
    }));
}

fn process_unchanged_workspace_diagnostics_report(
    diagnostics: &mut HashMap<lsp::Uri, WorkspaceLspPullDiagnostics>,
    server_id: LanguageServerId,
    report: lsp::WorkspaceUnchangedDocumentDiagnosticReport,
    registration_id: Option<SharedString>,
) {
    let mut new_diagnostics = HashMap::default();
    process_unchanged_diagnostics_report(
        &mut new_diagnostics,
        server_id,
        report.uri,
        report.unchanged_document_diagnostic_report,
        registration_id,
    );
    diagnostics.extend(new_diagnostics.into_iter().map(|(uri, diagnostics)| {
        (
            uri,
            WorkspaceLspPullDiagnostics {
                version: report.version.map(|v| v as i32),
                diagnostics,
            },
        )
    }));
}

#[async_trait(?Send)]
impl LspCommand for GetDocumentDiagnostics {
    type Response = Vec<LspPullDiagnostics>;
    type LspRequest = lsp::request::DocumentDiagnosticRequest;

    fn display_name(&self) -> &str {
        "Get diagnostics"
    }

    fn check_capabilities(&self, capabilities: AdapterServerCapabilities<'_>) -> bool {
        capabilities
            .server_capabilities
            .diagnostic_provider
            .is_some()
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::DocumentDiagnosticParams> {
        Ok(lsp::DocumentDiagnosticParams {
            text_document: lsp::TextDocumentIdentifier {
                uri: file_path_to_lsp_url(path)?,
            },
            identifier: self.identifier.as_ref().map(ToString::to_string),
            previous_result_id: self.previous_result_id.as_ref().map(ToString::to_string),
            partial_result_params: Default::default(),
            work_done_progress_params: Default::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: lsp::DocumentDiagnosticReportResult,
        _: Entity<LspStore>,
        buffer: Entity<Buffer>,
        server_id: LanguageServerId,
        cx: AsyncApp,
    ) -> Result<Self::Response> {
        let url = buffer.read_with(&cx, |buffer, cx| {
            buffer
                .file()
                .and_then(|file| file.as_local())
                .map(|file| {
                    let abs_path = file.abs_path(cx);
                    file_path_to_lsp_url(&abs_path)
                })
                .transpose()?
                .with_context(|| format!("missing url on buffer {}", buffer.remote_id()))
        })?;

        let mut pulled_diagnostics = HashMap::default();
        match message {
            lsp::DocumentDiagnosticReportResult::Report(report) => match report {
                lsp::DocumentDiagnosticReport::Full(report) => {
                    if let Some(related_documents) = report.related_documents {
                        process_related_documents(
                            &mut pulled_diagnostics,
                            server_id,
                            related_documents,
                            self.registration_id.clone(),
                        );
                    }
                    process_full_diagnostics_report(
                        &mut pulled_diagnostics,
                        server_id,
                        url,
                        report.full_document_diagnostic_report,
                        self.registration_id,
                    );
                }
                lsp::DocumentDiagnosticReport::Unchanged(report) => {
                    if let Some(related_documents) = report.related_documents {
                        process_related_documents(
                            &mut pulled_diagnostics,
                            server_id,
                            related_documents,
                            self.registration_id.clone(),
                        );
                    }
                    process_unchanged_diagnostics_report(
                        &mut pulled_diagnostics,
                        server_id,
                        url,
                        report.unchanged_document_diagnostic_report,
                        self.registration_id,
                    );
                }
            },
            lsp::DocumentDiagnosticReportResult::Partial(report) => {
                if let Some(related_documents) = report.related_documents {
                    process_related_documents(
                        &mut pulled_diagnostics,
                        server_id,
                        related_documents,
                        self.registration_id,
                    );
                }
            }
        }

        Ok(pulled_diagnostics.into_values().collect())
    }
}

#[async_trait(?Send)]
impl LspCommand for GetDocumentColor {
    type Response = Vec<DocumentColor>;
    type LspRequest = lsp::request::DocumentColor;

    fn display_name(&self) -> &str {
        "Document color"
    }

    fn check_capabilities(&self, server_capabilities: AdapterServerCapabilities<'_>) -> bool {
        server_capabilities
            .server_capabilities
            .color_provider
            .as_ref()
            .is_some_and(|capability| match capability {
                lsp::ColorProviderCapability::Simple(supported) => *supported,
                lsp::ColorProviderCapability::ColorProvider(..) => true,
                lsp::ColorProviderCapability::Options(..) => true,
            })
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::DocumentColorParams> {
        Ok(lsp::DocumentColorParams {
            text_document: make_text_document_identifier(path)?,
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Vec<lsp::ColorInformation>,
        _: Entity<LspStore>,
        _: Entity<Buffer>,
        _: LanguageServerId,
        _: AsyncApp,
    ) -> Result<Self::Response> {
        Ok(message
            .into_iter()
            .map(|color| DocumentColor {
                lsp_range: color.range,
                color: color.color,
                resolved: false,
                color_presentations: Vec::new(),
            })
            .collect())
    }
}

#[async_trait(?Send)]
impl LspCommand for GetFoldingRanges {
    type Response = Vec<LspFoldingRange>;
    type LspRequest = lsp::request::FoldingRangeRequest;

    fn display_name(&self) -> &str {
        "Folding ranges"
    }

    fn check_capabilities(&self, server_capabilities: AdapterServerCapabilities<'_>) -> bool {
        server_capabilities
            .server_capabilities
            .folding_range_provider
            .as_ref()
            .is_some_and(|capability| match capability {
                lsp::FoldingRangeProviderCapability::Simple(supported) => *supported,
                lsp::FoldingRangeProviderCapability::FoldingProvider(..)
                | lsp::FoldingRangeProviderCapability::Options(..) => true,
            })
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::FoldingRangeParams> {
        Ok(lsp::FoldingRangeParams {
            text_document: make_text_document_identifier(path)?,
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<Vec<lsp::FoldingRange>>,
        _: Entity<LspStore>,
        buffer: Entity<Buffer>,
        _: LanguageServerId,
        cx: AsyncApp,
    ) -> Result<Self::Response> {
        let snapshot = buffer.read_with(&cx, |buffer, _| buffer.snapshot());
        let max_point = snapshot.max_point_utf16();
        Ok(message
            .unwrap_or_default()
            .into_iter()
            .filter(|range| range.start_line < range.end_line)
            .filter(|range| range.start_line <= max_point.row && range.end_line <= max_point.row)
            .map(|folding_range| {
                let start_col = folding_range.start_character.unwrap_or(u32::MAX);
                let end_col = folding_range.end_character.unwrap_or(u32::MAX);
                let start = snapshot.clip_point_utf16(
                    Unclipped(PointUtf16::new(folding_range.start_line, start_col)),
                    Bias::Right,
                );
                let end = snapshot.clip_point_utf16(
                    Unclipped(PointUtf16::new(folding_range.end_line, end_col)),
                    Bias::Left,
                );
                let start = snapshot.anchor_after(start);
                let end = snapshot.anchor_before(end);
                let collapsed_text = folding_range
                    .collapsed_text
                    .filter(|t| !t.is_empty())
                    .map(|t| SharedString::from(crate::lsp_store::collapse_newlines(&t, " ")));
                LspFoldingRange {
                    range: start..end,
                    collapsed_text,
                }
            })
            .collect())
    }
}

#[async_trait(?Send)]
impl LspCommand for GetDocumentLinks {
    type Response = Vec<LspDocumentLink>;
    type LspRequest = lsp::request::DocumentLinkRequest;

    fn display_name(&self) -> &str {
        "Document links"
    }

    fn check_capabilities(&self, server_capabilities: AdapterServerCapabilities<'_>) -> bool {
        server_capabilities
            .server_capabilities
            .document_link_provider
            .is_some()
    }

    fn to_lsp(
        &self,
        path: &Path,
        _: &Buffer,
        _: &Arc<LanguageServer>,
        _: &App,
    ) -> Result<lsp::DocumentLinkParams> {
        Ok(lsp::DocumentLinkParams {
            text_document: make_text_document_identifier(path)?,
            work_done_progress_params: lsp::WorkDoneProgressParams::default(),
            partial_result_params: lsp::PartialResultParams::default(),
        })
    }

    async fn response_from_lsp(
        self,
        message: Option<Vec<lsp::DocumentLink>>,
        _: Entity<LspStore>,
        buffer: Entity<Buffer>,
        _: LanguageServerId,
        cx: AsyncApp,
    ) -> Result<Self::Response> {
        let snapshot = buffer.read_with(&cx, |buffer, _| buffer.snapshot());
        Ok(message
            .unwrap_or_default()
            .into_iter()
            .map(|link| {
                let start = snapshot.clip_point_utf16(
                    Unclipped(PointUtf16::new(
                        link.range.start.line,
                        link.range.start.character,
                    )),
                    Bias::Left,
                );
                let end = snapshot.clip_point_utf16(
                    Unclipped(PointUtf16::new(
                        link.range.end.line,
                        link.range.end.character,
                    )),
                    Bias::Right,
                );
                LspDocumentLink {
                    range: snapshot.anchor_after(start)..snapshot.anchor_before(end),
                    target: link.target.map(|url| url.to_string().into()),
                    tooltip: link.tooltip.map(SharedString::from),
                    data: link.data,
                    resolved: false,
                }
            })
            .collect())
    }
}

fn process_related_documents(
    diagnostics: &mut HashMap<lsp::Uri, LspPullDiagnostics>,
    server_id: LanguageServerId,
    documents: impl IntoIterator<Item = (lsp::Uri, lsp::DocumentDiagnosticReportKind)>,
    registration_id: Option<SharedString>,
) {
    for (url, report_kind) in documents {
        match report_kind {
            lsp::DocumentDiagnosticReportKind::Full(report) => process_full_diagnostics_report(
                diagnostics,
                server_id,
                url,
                report,
                registration_id.clone(),
            ),
            lsp::DocumentDiagnosticReportKind::Unchanged(report) => {
                process_unchanged_diagnostics_report(
                    diagnostics,
                    server_id,
                    url,
                    report,
                    registration_id.clone(),
                )
            }
        }
    }
}

fn process_unchanged_diagnostics_report(
    diagnostics: &mut HashMap<lsp::Uri, LspPullDiagnostics>,
    server_id: LanguageServerId,
    uri: lsp::Uri,
    report: lsp::UnchangedDocumentDiagnosticReport,
    registration_id: Option<SharedString>,
) {
    let result_id = SharedString::new(report.result_id);
    match diagnostics.entry(uri.clone()) {
        hash_map::Entry::Occupied(mut o) => match o.get_mut() {
            LspPullDiagnostics::Default => {
                o.insert(LspPullDiagnostics::Response {
                    server_id,
                    uri,
                    diagnostics: PulledDiagnostics::Unchanged { result_id },
                    registration_id,
                });
            }
            LspPullDiagnostics::Response {
                server_id: existing_server_id,
                uri: existing_uri,
                diagnostics: existing_diagnostics,
                ..
            } => {
                if server_id != *existing_server_id || &uri != existing_uri {
                    debug_panic!(
                        "Unexpected state: file {uri} has two different sets of diagnostics reported"
                    );
                }
                match existing_diagnostics {
                    PulledDiagnostics::Unchanged { .. } => {
                        *existing_diagnostics = PulledDiagnostics::Unchanged { result_id };
                    }
                    PulledDiagnostics::Changed { .. } => {}
                }
            }
        },
        hash_map::Entry::Vacant(v) => {
            v.insert(LspPullDiagnostics::Response {
                server_id,
                uri,
                diagnostics: PulledDiagnostics::Unchanged { result_id },
                registration_id,
            });
        }
    }
}

fn process_full_diagnostics_report(
    diagnostics: &mut HashMap<lsp::Uri, LspPullDiagnostics>,
    server_id: LanguageServerId,
    uri: lsp::Uri,
    report: lsp::FullDocumentDiagnosticReport,
    registration_id: Option<SharedString>,
) {
    let result_id = report.result_id.map(SharedString::new);
    match diagnostics.entry(uri.clone()) {
        hash_map::Entry::Occupied(mut o) => match o.get_mut() {
            LspPullDiagnostics::Default => {
                o.insert(LspPullDiagnostics::Response {
                    server_id,
                    uri,
                    diagnostics: PulledDiagnostics::Changed {
                        result_id,
                        diagnostics: report.items,
                    },
                    registration_id,
                });
            }
            LspPullDiagnostics::Response {
                server_id: existing_server_id,
                uri: existing_uri,
                diagnostics: existing_diagnostics,
                ..
            } => {
                if server_id != *existing_server_id || &uri != existing_uri {
                    debug_panic!(
                        "Unexpected state: file {uri} has two different sets of diagnostics reported"
                    );
                }
                match existing_diagnostics {
                    PulledDiagnostics::Unchanged { .. } => {
                        *existing_diagnostics = PulledDiagnostics::Changed {
                            result_id,
                            diagnostics: report.items,
                        };
                    }
                    PulledDiagnostics::Changed {
                        result_id: existing_result_id,
                        diagnostics: existing_diagnostics,
                    } => {
                        if result_id.is_some() {
                            *existing_result_id = result_id;
                        }
                        existing_diagnostics.extend(report.items);
                    }
                }
            }
        },
        hash_map::Entry::Vacant(v) => {
            v.insert(LspPullDiagnostics::Response {
                server_id,
                uri,
                diagnostics: PulledDiagnostics::Changed {
                    result_id,
                    diagnostics: report.items,
                },
                registration_id,
            });
        }
    }
}

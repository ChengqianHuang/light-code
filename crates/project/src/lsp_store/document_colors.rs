use std::{sync::Arc, time::Duration};

use anyhow::{Context as _, Result};
use collections::{HashMap, HashSet};
use futures::{
    FutureExt as _,
    future::{Shared, join_all},
};
use gpui::{AppContext as _, AsyncApp, Context, Entity, SharedString, Task};
use language::{Buffer, LocalFile as _, PointUtf16, point_to_lsp};
use lsp::LanguageServerId;
use settings::Settings as _;
use text::BufferId;
use util::ResultExt as _;
use worktree::File;

use crate::{
    ColorPresentation, DocumentColor, LspStore,
    lsp_command::{GetDocumentColor, LspCommand as _, make_text_document_identifier},
    lsp_store::{
        LspStoreEvent, RunningFetch, missing_servers_to_query, next_lsp_fetch_id,
        upstream_lsp_query_server_filter,
    },
    project_settings::ProjectSettings,
};

#[derive(Debug, Default, Clone)]
pub struct DocumentColors {
    pub colors: HashSet<DocumentColor>,
}

pub(super) type DocumentColorTask =
    Shared<Task<std::result::Result<DocumentColors, Arc<anyhow::Error>>>>;

#[derive(Debug, Default)]
pub(super) struct DocumentColorData {
    pub(super) colors: HashMap<LanguageServerId, HashSet<DocumentColor>>,
    fetched_servers: HashSet<LanguageServerId>,
    pub(super) colors_update: Option<RunningFetch<DocumentColorTask>>,
}

impl DocumentColorData {
    pub(super) fn remove_server_data(&mut self, server_id: LanguageServerId) {
        self.colors.remove(&server_id);
        self.fetched_servers.remove(&server_id);
        RunningFetch::discard_if_queried(&mut self.colors_update, server_id);
    }

    fn evict(&mut self, for_server: Option<LanguageServerId>) {
        match for_server {
            Some(server_id) => self.remove_server_data(server_id),
            None => {
                self.colors.clear();
                self.fetched_servers.clear();
                self.colors_update = None;
            }
        }
    }
}

impl LspStore {
    pub(super) fn refresh_document_colors(
        &mut self,
        for_server: Option<LanguageServerId>,
        cx: &mut Context<Self>,
    ) {
        for lsp_data in self.lsp_data.values_mut() {
            if let Some(document_colors) = &mut lsp_data.document_colors {
                document_colors.evict(for_server);
            }
        }

        cx.emit(LspStoreEvent::RefreshDocumentColors {
            server_id: for_server,
        });
    }

    pub fn document_colors(
        &mut self,
        buffer: Entity<Buffer>,
        cx: &mut Context<Self>,
    ) -> Option<DocumentColorTask> {
        let version_queried_for = buffer.read(cx).version();
        let buffer_id = buffer.read(cx).remote_id();

        let current_servers = self.language_server_ids_for_request(&buffer, &GetDocumentColor, cx);

        let mut servers_to_query = None;
        if let Some(lsp_data) = self.current_lsp_data(buffer_id) {
            if !version_queried_for.changed_since(&lsp_data.buffer_version)
                && let Some(cached_colors) = &mut lsp_data.document_colors
            {
                match missing_servers_to_query(
                    &mut cached_colors.colors,
                    &mut cached_colors.fetched_servers,
                    &current_servers,
                ) {
                    Some(missing_servers) => servers_to_query = Some(missing_servers),
                    None => {
                        return Some(
                            Task::ready(Ok(DocumentColors {
                                colors: cached_colors.colors.values().flatten().cloned().collect(),
                            }))
                            .shared(),
                        );
                    }
                }
            }
            if let Some(document_colors) = &lsp_data.document_colors
                && let Some(running) = &document_colors.colors_update
                && !version_queried_for.changed_since(&running.version)
                && servers_to_query
                    .as_ref()
                    .is_none_or(|missing| missing.is_subset(&running.servers))
            {
                return Some(running.task.clone());
            }
        }

        let color_lsp_data = self
            .latest_lsp_data(&buffer, cx)
            .document_colors
            .get_or_insert_default();
        let fetch_id = next_lsp_fetch_id();
        let queried_servers = servers_to_query
            .clone()
            .unwrap_or_else(|| current_servers.clone());
        let buffer_version_queried_for = version_queried_for.clone();
        let new_task = cx
            .spawn({
                let queried_servers = queried_servers.clone();
                async move |lsp_store, cx| {
                    cx.background_executor()
                        .timer(Duration::from_millis(30))
                        .await;
                    let fetched_colors = lsp_store
                        .update(cx, |lsp_store, cx| {
                            lsp_store.fetch_document_colors_for_buffer(
                                &buffer,
                                servers_to_query,
                                cx,
                            )
                        })?
                        .await
                        .context("fetching document colors")
                        .map_err(Arc::new);
                    let fetched_colors = match fetched_colors {
                        Ok(fetched_colors) => {
                            if buffer.update(cx, |buffer, _| {
                                buffer.version() != buffer_version_queried_for
                            }) {
                                return Ok(DocumentColors::default());
                            }
                            fetched_colors
                        }
                        Err(e) => {
                            lsp_store
                                .update(cx, |lsp_store, _| {
                                    if let Some(lsp_data) = lsp_store.lsp_data.get_mut(&buffer_id)
                                        && let Some(document_colors) = &mut lsp_data.document_colors
                                    {
                                        RunningFetch::take_finished(
                                            &mut document_colors.colors_update,
                                            fetch_id,
                                        );
                                    }
                                })
                                .ok();
                            return Err(e);
                        }
                    };

                    lsp_store
                        .update(cx, |lsp_store, cx| {
                            let lsp_data = lsp_store.latest_lsp_data(&buffer, cx);
                            let lsp_colors = lsp_data.document_colors.get_or_insert_default();

                            if RunningFetch::take_finished(&mut lsp_colors.colors_update, fetch_id)
                                && let Some(fetched_colors) = fetched_colors
                            {
                                if lsp_data.buffer_version == buffer_version_queried_for {
                                    lsp_colors.colors.extend(fetched_colors);
                                    lsp_colors.fetched_servers.extend(queried_servers);
                                } else if !lsp_data
                                    .buffer_version
                                    .changed_since(&buffer_version_queried_for)
                                {
                                    lsp_data.buffer_version = buffer_version_queried_for;
                                    lsp_colors.colors = fetched_colors;
                                    lsp_colors.fetched_servers = queried_servers;
                                }
                            }
                            let colors = lsp_colors
                                .colors
                                .values()
                                .flatten()
                                .cloned()
                                .collect::<HashSet<_>>();
                            DocumentColors { colors }
                        })
                        .map_err(Arc::new)
                }
            })
            .shared();
        color_lsp_data.colors_update = Some(RunningFetch {
            id: fetch_id,
            version: version_queried_for,
            servers: queried_servers,
            task: new_task.clone(),
        });
        Some(new_task)
    }

    pub fn resolve_color_presentation(
        &mut self,
        mut color: DocumentColor,
        buffer: Entity<Buffer>,
        server_id: LanguageServerId,
        cx: &mut Context<Self>,
    ) -> Task<Result<DocumentColor>> {
        if color.resolved {
            return Task::ready(Ok(color));
        }

        let path = match buffer
            .update(cx, |buffer, cx| {
                Some(File::from_dyn(buffer.file())?.abs_path(cx))
            })
            .context("buffer with the missing path")
        {
            Ok(path) => path,
            Err(e) => return Task::ready(Err(e)),
        };
        let Some(lang_server) = buffer.update(cx, |buffer, cx| {
            self.language_server_for_local_buffer(buffer, server_id, cx)
                .map(|(_, server)| server.clone())
        }) else {
            return Task::ready(Ok(color));
        };

        let request_timeout = ProjectSettings::get_global(cx)
            .global_lsp_settings
            .get_request_timeout();
        cx.background_spawn(async move {
            let resolve_task = lang_server.request::<lsp::request::ColorPresentationRequest>(
                lsp::ColorPresentationParams {
                    text_document: make_text_document_identifier(&path)?,
                    color: color.color,
                    range: color.lsp_range,
                    work_done_progress_params: Default::default(),
                    partial_result_params: Default::default(),
                },
                request_timeout,
            );
            color.color_presentations = resolve_task
                .await
                .into_response()
                .context("color presentation resolve LSP request")?
                .into_iter()
                .map(|presentation| ColorPresentation {
                    label: SharedString::from(presentation.label),
                    text_edit: presentation.text_edit,
                    additional_text_edits: presentation.additional_text_edits.unwrap_or_default(),
                })
                .collect();
            color.resolved = true;
            Ok(color)
        })
    }

    pub(super) fn fetch_document_colors_for_buffer(
        &mut self,
        buffer: &Entity<Buffer>,
        for_servers: Option<HashSet<LanguageServerId>>,
        cx: &mut Context<Self>,
    ) -> Task<anyhow::Result<Option<HashMap<LanguageServerId, HashSet<DocumentColor>>>>> {
        let document_colors_task = self.request_filtered_lsp_locally(
            buffer,
            None::<usize>,
            GetDocumentColor,
            for_servers.as_ref(),
            cx,
        );
        cx.background_spawn(async move {
            Ok(Some(
                document_colors_task
                    .await
                    .into_iter()
                    .fold(HashMap::default(), |mut acc, (server_id, colors)| {
                        acc.entry(server_id)
                            .or_insert_with(HashSet::default)
                            .extend(colors);
                        acc
                    })
                    .into_iter()
                    .collect(),
            ))
        })
    }
}

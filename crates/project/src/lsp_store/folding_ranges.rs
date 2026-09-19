use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use collections::{HashMap, HashSet};
use futures::FutureExt as _;
use futures::future::Shared;
use gpui::{AppContext as _, Context, Entity, SharedString, Task};
use itertools::Itertools;
use language::Buffer;
use lsp::LanguageServerId;
use text::Anchor;

use crate::lsp_command::GetFoldingRanges;
use crate::lsp_store::{
    LspStore, LspStoreEvent, RunningFetch, missing_servers_to_query, next_lsp_fetch_id,
};

#[derive(Clone, Debug)]
pub struct LspFoldingRange {
    pub range: Range<Anchor>,
    pub collapsed_text: Option<SharedString>,
}

pub(super) type FoldingRangeTask =
    Shared<Task<std::result::Result<Vec<LspFoldingRange>, Arc<anyhow::Error>>>>;

#[derive(Debug, Default)]
pub(super) struct FoldingRangeData {
    pub(super) ranges: HashMap<LanguageServerId, Vec<LspFoldingRange>>,
    fetched_servers: HashSet<LanguageServerId>,
    ranges_update: Option<RunningFetch<FoldingRangeTask>>,
}

impl FoldingRangeData {
    pub(super) fn remove_server_data(&mut self, server_id: LanguageServerId) {
        self.ranges.remove(&server_id);
        self.fetched_servers.remove(&server_id);
        RunningFetch::discard_if_queried(&mut self.ranges_update, server_id);
    }

    fn evict(&mut self, for_server: Option<LanguageServerId>) {
        match for_server {
            Some(server_id) => self.remove_server_data(server_id),
            None => {
                self.ranges.clear();
                self.fetched_servers.clear();
                self.ranges_update = None;
            }
        }
    }
}

impl LspStore {
    pub(super) fn refresh_folding_ranges(
        &mut self,
        for_server: Option<LanguageServerId>,
        cx: &mut Context<Self>,
    ) {
        for lsp_data in self.lsp_data.values_mut() {
            if let Some(folding_ranges) = &mut lsp_data.folding_ranges {
                folding_ranges.evict(for_server);
            }
        }

        cx.emit(LspStoreEvent::RefreshFoldingRanges {
            server_id: for_server,
        });
    }

    /// Returns a task that resolves to the folding ranges for the given buffer.
    ///
    /// Caches results per buffer version so repeated calls for the same version
    /// return immediately. Deduplicates concurrent in-flight requests.
    pub fn fetch_folding_ranges(
        &mut self,
        buffer: &Entity<Buffer>,
        cx: &mut Context<Self>,
    ) -> Task<Vec<LspFoldingRange>> {
        let version_queried_for = buffer.read(cx).version();
        let buffer_id = buffer.read(cx).remote_id();

        let current_servers = self.language_server_ids_for_request(buffer, &GetFoldingRanges, cx);

        let mut servers_to_query = None;
        if let Some(lsp_data) = self.current_lsp_data(buffer_id) {
            if !version_queried_for.changed_since(&lsp_data.buffer_version)
                && let Some(cached) = &mut lsp_data.folding_ranges
            {
                match missing_servers_to_query(
                    &mut cached.ranges,
                    &mut cached.fetched_servers,
                    &current_servers,
                ) {
                    Some(missing_servers) => servers_to_query = Some(missing_servers),
                    None => {
                        let snapshot = buffer.read(cx).snapshot();
                        return Task::ready(
                            cached
                                .ranges
                                .values()
                                .flatten()
                                .cloned()
                                .sorted_by(|a, b| a.range.start.cmp(&b.range.start, &snapshot))
                                .collect(),
                        );
                    }
                }
            }
            if let Some(folding_ranges) = &lsp_data.folding_ranges
                && let Some(running) = &folding_ranges.ranges_update
                && !version_queried_for.changed_since(&running.version)
                && servers_to_query
                    .as_ref()
                    .is_none_or(|missing| missing.is_subset(&running.servers))
            {
                let running = running.task.clone();
                return cx.background_spawn(async move { running.await.unwrap_or_default() });
            }
        }

        let folding_lsp_data = self
            .latest_lsp_data(buffer, cx)
            .folding_ranges
            .get_or_insert_default();
        let fetch_id = next_lsp_fetch_id();
        let queried_servers = servers_to_query
            .clone()
            .unwrap_or_else(|| current_servers.clone());
        let buffer = buffer.clone();
        let query_version = version_queried_for.clone();
        let new_task = cx
            .spawn({
                let queried_servers = queried_servers.clone();
                async move |lsp_store, cx| {
                    cx.background_executor()
                        .timer(Duration::from_millis(30))
                        .await;

                    let fetched = lsp_store
                        .update(cx, |lsp_store, cx| {
                            lsp_store.fetch_folding_ranges_for_buffer(&buffer, servers_to_query, cx)
                        })
                        .map_err(Arc::new)?
                        .await
                        .context("fetching folding ranges")
                        .map_err(Arc::new);

                    let fetched = match fetched {
                        Ok(fetched) => fetched,
                        Err(e) => {
                            lsp_store
                                .update(cx, |lsp_store, _| {
                                    if let Some(lsp_data) = lsp_store.lsp_data.get_mut(&buffer_id)
                                        && let Some(folding_ranges) = &mut lsp_data.folding_ranges
                                    {
                                        RunningFetch::take_finished(
                                            &mut folding_ranges.ranges_update,
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
                            let folding = lsp_data.folding_ranges.get_or_insert_default();

                            if RunningFetch::take_finished(&mut folding.ranges_update, fetch_id)
                                && let Some(fetched_ranges) = fetched
                            {
                                if lsp_data.buffer_version == query_version {
                                    folding.ranges.extend(fetched_ranges);
                                    folding.fetched_servers.extend(queried_servers);
                                } else if !lsp_data.buffer_version.changed_since(&query_version) {
                                    lsp_data.buffer_version = query_version;
                                    folding.ranges = fetched_ranges;
                                    folding.fetched_servers = queried_servers;
                                }
                            }
                            let snapshot = buffer.read(cx).snapshot();
                            folding
                                .ranges
                                .values()
                                .flatten()
                                .cloned()
                                .sorted_by(|a, b| a.range.start.cmp(&b.range.start, &snapshot))
                                .collect()
                        })
                        .map_err(Arc::new)
                }
            })
            .shared();

        folding_lsp_data.ranges_update = Some(RunningFetch {
            id: fetch_id,
            version: version_queried_for,
            servers: queried_servers,
            task: new_task.clone(),
        });

        cx.background_spawn(async move { new_task.await.unwrap_or_default() })
    }

    fn fetch_folding_ranges_for_buffer(
        &mut self,
        buffer: &Entity<Buffer>,
        for_servers: Option<HashSet<LanguageServerId>>,
        cx: &mut Context<Self>,
    ) -> Task<anyhow::Result<Option<HashMap<LanguageServerId, Vec<LspFoldingRange>>>>> {
        {
            let folding_task = self.request_filtered_lsp_locally(
                buffer,
                None::<usize>,
                GetFoldingRanges,
                for_servers.as_ref(),
                cx,
            );
            cx.background_spawn(async move { Ok(Some(folding_task.await.into_iter().collect())) })
        }
    }
}

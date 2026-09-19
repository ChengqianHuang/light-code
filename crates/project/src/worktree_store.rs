use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize},
    },
};

use anyhow::{Context as _, Result, anyhow};
use collections::HashMap;
use fs::{Fs, copy_recursive};
use futures::{FutureExt, future::Shared};
use gpui::{
    App, AppContext as _, Context, Entity, EntityId, EventEmitter, Global, Task,
    WeakEntity,
};
use postage::{prelude::Stream as _, watch};
use util::{
    path_list::PathList,
    paths::{PathStyle, SanitizedPath},
    rel_path::RelPath,
};
use worktree::{
    CreatedEntry, Entry, ProjectEntryId, UpdatedEntriesSet, UpdatedGitRepositoriesSet, Worktree,
    WorktreeId,
};

use crate::{ProjectPath, trusted_worktrees::TrustedWorktrees};

/// The current paths for a project's worktrees. Each folder path has a corresponding
/// main worktree path at the same position. The two lists are always the
/// same length and are modified together via `add_path` / `remove_main_path`.
///
/// For non-linked worktrees, the main path and folder path are identical.
/// For linked worktrees, the main path is the original repo and the folder
/// path is the linked worktree location.
#[derive(Default, Debug, Clone)]
pub struct WorktreePaths {
    paths: PathList,
    main_paths: PathList,
}

impl PartialEq for WorktreePaths {
    fn eq(&self, other: &Self) -> bool {
        self.ordered_pairs().eq(other.ordered_pairs())
    }
}

impl WorktreePaths {
    /// Build from two parallel `PathList`s that already share the same
    /// insertion order. Used for deserialization from DB.
    ///
    /// Returns an error if the two lists have different lengths, which
    /// indicates corrupted data from a prior migration bug.
    pub fn from_path_lists(
        main_worktree_paths: PathList,
        folder_paths: PathList,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            main_worktree_paths.paths().len() == folder_paths.paths().len(),
            "main_worktree_paths has {} entries but folder_paths has {}",
            main_worktree_paths.paths().len(),
            folder_paths.paths().len(),
        );
        Ok(Self {
            paths: folder_paths,
            main_paths: main_worktree_paths,
        })
    }

    /// Build for non-linked worktrees where main == folder for every path.
    pub fn from_folder_paths(folder_paths: &PathList) -> Self {
        Self {
            paths: folder_paths.clone(),
            main_paths: folder_paths.clone(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    /// The folder paths (for workspace matching / `threads_by_paths` index).
    pub fn folder_path_list(&self) -> &PathList {
        &self.paths
    }

    /// The main worktree paths (for group key / `threads_by_main_paths` index).
    pub fn main_worktree_path_list(&self) -> &PathList {
        &self.main_paths
    }

    /// Iterate the (main_worktree_path, folder_path) pairs in insertion order.
    pub fn ordered_pairs(&self) -> impl Iterator<Item = (&PathBuf, &PathBuf)> {
        self.main_paths
            .ordered_paths()
            .zip(self.paths.ordered_paths())
    }

    /// Add a new path pair. If the exact (main, folder) pair already exists,
    /// this is a no-op. Rebuilds both internal `PathList`s to maintain
    /// consistent ordering.
    pub fn add_path(&mut self, main_path: &Path, folder_path: &Path) {
        let already_exists = self
            .ordered_pairs()
            .any(|(m, f)| m.as_path() == main_path && f.as_path() == folder_path);
        if already_exists {
            return;
        }
        let (mut mains, mut folders): (Vec<PathBuf>, Vec<PathBuf>) = self
            .ordered_pairs()
            .map(|(m, f)| (m.clone(), f.clone()))
            .unzip();
        mains.push(main_path.to_path_buf());
        folders.push(folder_path.to_path_buf());
        self.main_paths = PathList::new(&mains);
        self.paths = PathList::new(&folders);
    }

    /// Remove all pairs whose main worktree path matches the given path.
    /// This removes the corresponding entries from both lists.
    pub fn remove_main_path(&mut self, main_path: &Path) {
        let (mains, folders): (Vec<PathBuf>, Vec<PathBuf>) = self
            .ordered_pairs()
            .filter(|(m, _)| m.as_path() != main_path)
            .map(|(m, f)| (m.clone(), f.clone()))
            .unzip();
        self.main_paths = PathList::new(&mains);
        self.paths = PathList::new(&folders);
    }

    /// Remove all pairs whose folder path matches the given path.
    /// This removes the corresponding entries from both lists.
    pub fn remove_folder_path(&mut self, folder_path: &Path) {
        let (mains, folders): (Vec<PathBuf>, Vec<PathBuf>) = self
            .ordered_pairs()
            .filter(|(_, f)| f.as_path() != folder_path)
            .map(|(m, f)| (m.clone(), f.clone()))
            .unzip();
        self.main_paths = PathList::new(&mains);
        self.paths = PathList::new(&folders);
    }
}

enum WorktreeStoreState {
    Local {
        fs: Arc<dyn Fs>,
    },
}

#[derive(Clone)]
pub struct WorktreeIdCounter(Arc<AtomicU64>);

impl Default for WorktreeIdCounter {
    fn default() -> Self {
        Self(Arc::new(AtomicU64::new(1)))
    }
}

impl WorktreeIdCounter {
    pub fn get(cx: &mut App) -> Self {
        cx.default_global::<Self>().clone()
    }

    fn next(&self) -> u64 {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

impl Global for WorktreeIdCounter {}

/// Summarizes worktree ownership and current snapshot sizes.
#[derive(Debug, Default)]
pub struct WorktreeStoreDiagnostics {
    pub worktree_slots: usize,
    pub live_worktrees: usize,
    pub visible_worktrees: usize,
    pub strong_handles: usize,
    pub dead_weak_handles: usize,
    pub loading_worktrees: usize,
    pub total_entries: usize,
    pub visible_entries: usize,
    pub largest_worktree: Option<LargestWorktreeDiagnostics>,
}

/// Identifies the worktree with the largest current snapshot.
#[derive(Debug)]
pub struct LargestWorktreeDiagnostics {
    pub path: PathBuf,
    pub entries: usize,
    pub visible_entries: usize,
}

pub struct WorktreeStore {
    next_entry_id: Arc<AtomicUsize>,
    next_worktree_id: WorktreeIdCounter,
    retain_worktrees: bool,
    worktrees: Vec<WorktreeHandle>,
    scanning_enabled: bool,
    #[allow(clippy::type_complexity)]
    loading_worktrees:
        HashMap<Arc<SanitizedPath>, Shared<Task<Result<Entity<Worktree>, Arc<anyhow::Error>>>>>,
    initial_scan_complete: (watch::Sender<bool>, watch::Receiver<bool>),
    state: WorktreeStoreState,
}

#[derive(Debug)]
pub enum WorktreeStoreEvent {
    WorktreeAdded(Entity<Worktree>),
    WorktreeRemoved(EntityId, WorktreeId),
    WorktreeReleased(EntityId, WorktreeId),
    WorktreeOrderChanged,
    WorktreeUpdateSent(Entity<Worktree>),
    WorktreeUpdatedEntries(WorktreeId, UpdatedEntriesSet),
    WorktreeUpdatedGitRepositories(WorktreeId, UpdatedGitRepositoriesSet),
    WorktreeDeletedEntry(WorktreeId, ProjectEntryId),
    WorktreeUpdatedRootRepoCommonDir(WorktreeId),
}

impl EventEmitter<WorktreeStoreEvent> for WorktreeStore {}

impl WorktreeStore {

    pub fn local(
        retain_worktrees: bool,
        fs: Arc<dyn Fs>,
        next_worktree_id: WorktreeIdCounter,
    ) -> Self {
        Self {
            next_entry_id: Default::default(),
            next_worktree_id,
            loading_worktrees: Default::default(),
            worktrees: Vec::new(),
            scanning_enabled: true,
            retain_worktrees,
            initial_scan_complete: watch::channel_with(true),
            state: WorktreeStoreState::Local { fs },
        }
    }

    pub fn next_worktree_id(&self) -> impl Future<Output = Result<WorktreeId>> + use<> {
        let id = self.next_worktree_id.next();
        async move { Ok(WorktreeId::from_proto(id)) }
    }

    pub fn disable_scanner(&mut self) {
        self.scanning_enabled = false;
        *self.initial_scan_complete.0.borrow_mut() = true;
    }

    /// Returns a future that resolves when all visible worktrees have completed
    /// their initial scan (entries populated, git repos detected).
    pub fn wait_for_initial_scan(&self) -> impl Future<Output = ()> + use<> {
        let mut rx = self.initial_scan_complete.1.clone();
        async move {
            let mut done = *rx.borrow();
            while !done {
                if let Some(value) = rx.recv().await {
                    done = value;
                } else {
                    break;
                }
            }
        }
    }

    /// Returns whether all visible worktrees have completed their initial scan.
    pub fn initial_scan_completed(&self) -> bool {
        *self.initial_scan_complete.1.borrow()
    }

    /// Checks whether all visible worktrees have completed their initial scan
    /// and no worktree creations are pending, and updates the watch channel accordingly.
    fn update_initial_scan_state(&mut self, cx: &App) {
        let complete = self.loading_worktrees.is_empty()
            && self
                .visible_worktrees(cx)
                .all(|wt| wt.read(cx).completed_scan_id() >= 1);
        *self.initial_scan_complete.0.borrow_mut() = complete;
    }

    /// Spawns a detached task that waits for a worktree's initial scan to complete,
    /// then rechecks and updates the aggregate initial scan state.
    fn observe_worktree_scan_completion(
        &mut self,
        worktree: &Entity<Worktree>,
        cx: &mut Context<Self>,
    ) {
        let await_scan = worktree.update(cx, |worktree, _cx| worktree.wait_for_snapshot(1));
        cx.spawn(async move |this, cx| {
            await_scan.await.ok();
            this.update(cx, |this, cx| {
                this.update_initial_scan_state(cx);
            })
            .ok();
            anyhow::Ok(())
        })
        .detach();
    }

    /// Returns worktree ownership and current snapshot size diagnostics.
    pub fn diagnostics(&self, cx: &App) -> WorktreeStoreDiagnostics {
        let mut diagnostics = WorktreeStoreDiagnostics {
            worktree_slots: self.worktrees.len(),
            loading_worktrees: self.loading_worktrees.len(),
            ..WorktreeStoreDiagnostics::default()
        };

        for handle in &self.worktrees {
            let worktree = match handle {
                WorktreeHandle::Strong(worktree) => {
                    diagnostics.strong_handles += 1;
                    Some(worktree.clone())
                }
                WorktreeHandle::Weak(worktree) => {
                    let worktree = worktree.upgrade();
                    if worktree.is_none() {
                        diagnostics.dead_weak_handles += 1;
                    }
                    worktree
                }
            };
            let Some(worktree) = worktree else {
                continue;
            };

            diagnostics.live_worktrees += 1;
            let worktree = worktree.read(cx);
            diagnostics.visible_worktrees += usize::from(worktree.is_visible());
            let snapshot = worktree.snapshot();
            let entries = snapshot.entry_count();
            let visible_entries = snapshot.visible_entry_count();
            diagnostics.total_entries += entries;
            diagnostics.visible_entries += visible_entries;

            let is_largest = diagnostics
                .largest_worktree
                .as_ref()
                .is_none_or(|largest| entries > largest.entries);
            if is_largest {
                diagnostics.largest_worktree = Some(LargestWorktreeDiagnostics {
                    path: worktree.abs_path().to_path_buf(),
                    entries,
                    visible_entries,
                });
            }
        }

        diagnostics
    }

    /// Iterates through all worktrees, including ones that don't appear in the project panel
    pub fn worktrees(&self) -> impl '_ + DoubleEndedIterator<Item = Entity<Worktree>> {
        self.worktrees
            .iter()
            .filter_map(move |worktree| worktree.upgrade())
    }

    /// Iterates through all user-visible worktrees, the ones that appear in the project panel.
    pub fn visible_worktrees<'a>(
        &'a self,
        cx: &'a App,
    ) -> impl 'a + DoubleEndedIterator<Item = Entity<Worktree>> {
        self.worktrees()
            .filter(|worktree| worktree.read(cx).is_visible())
    }

    /// Iterates through all user-visible worktrees (directories and files that appear in the project panel) and other, invisible single files that could appear e.g. due to drag and drop.
    pub fn visible_worktrees_and_single_files<'a>(
        &'a self,
        cx: &'a App,
    ) -> impl 'a + DoubleEndedIterator<Item = Entity<Worktree>> {
        self.worktrees()
            .filter(|worktree| worktree.read(cx).is_visible() || worktree.read(cx).is_single_file())
    }

    pub fn worktree_for_id(&self, id: WorktreeId, cx: &App) -> Option<Entity<Worktree>> {
        self.worktrees()
            .find(|worktree| worktree.read(cx).id() == id)
    }

    pub fn worktree_for_entry(
        &self,
        entry_id: ProjectEntryId,
        cx: &App,
    ) -> Option<Entity<Worktree>> {
        self.worktrees()
            .find(|worktree| worktree.read(cx).contains_entry(entry_id))
    }

    pub fn find_worktree(
        &self,
        abs_path: impl AsRef<Path>,
        cx: &App,
    ) -> Option<(Entity<Worktree>, Arc<RelPath>)> {
        let abs_path = SanitizedPath::new(abs_path.as_ref());
        for tree in self.worktrees() {
            let path_style = tree.read(cx).path_style();
            if let Some(relative_path) =
                path_style.strip_prefix(abs_path.as_ref(), tree.read(cx).abs_path().as_ref())
            {
                return Some((tree.clone(), relative_path.into_arc()));
            }
        }
        None
    }

    pub fn project_path_for_absolute_path(&self, abs_path: &Path, cx: &App) -> Option<ProjectPath> {
        self.find_worktree(abs_path, cx)
            .map(|(worktree, relative_path)| ProjectPath {
                worktree_id: worktree.read(cx).id(),
                path: relative_path,
            })
    }

    pub fn absolutize(&self, project_path: &ProjectPath, cx: &App) -> Option<PathBuf> {
        let worktree = self.worktree_for_id(project_path.worktree_id, cx)?;
        Some(worktree.read(cx).absolutize(&project_path.path))
    }

    pub fn path_style(&self) -> PathStyle {
        match &self.state {
            WorktreeStoreState::Local { .. } => PathStyle::local(),
        }
    }

    pub fn find_or_create_worktree(
        &mut self,
        abs_path: impl AsRef<Path>,
        visible: bool,
        cx: &mut Context<Self>,
    ) -> Task<Result<(Entity<Worktree>, Arc<RelPath>)>> {
        let abs_path = abs_path.as_ref();
        if let Some((tree, relative_path)) = self.find_worktree(abs_path, cx) {
            Task::ready(Ok((tree, relative_path)))
        } else {
            let worktree = self.create_worktree(abs_path, visible, cx);
            cx.background_spawn(async move { Ok((worktree.await?, RelPath::empty_arc())) })
        }
    }

    pub fn entry_for_id<'a>(&'a self, entry_id: ProjectEntryId, cx: &'a App) -> Option<&'a Entry> {
        self.worktrees()
            .find_map(|worktree| worktree.read(cx).entry_for_id(entry_id))
    }

    pub fn worktree_and_entry_for_id<'a>(
        &'a self,
        entry_id: ProjectEntryId,
        cx: &'a App,
    ) -> Option<(Entity<Worktree>, &'a Entry)> {
        self.worktrees().find_map(|worktree| {
            worktree
                .read(cx)
                .entry_for_id(entry_id)
                .map(|e| (worktree.clone(), e))
        })
    }

    pub fn entry_for_path<'a>(&'a self, path: &ProjectPath, cx: &'a App) -> Option<&'a Entry> {
        self.worktree_for_id(path.worktree_id, cx)?
            .read(cx)
            .entry_for_path(&path.path)
    }

    pub fn copy_entry(
        &mut self,
        entry_id: ProjectEntryId,
        new_project_path: ProjectPath,
        cx: &mut Context<Self>,
    ) -> Task<Result<Option<Entry>>> {
        let Some(old_worktree) = self.worktree_for_entry(entry_id, cx) else {
            return Task::ready(Err(anyhow!("no such worktree")));
        };
        let Some(old_entry) = old_worktree.read(cx).entry_for_id(entry_id) else {
            return Task::ready(Err(anyhow!("no such entry")));
        };
        let Some(new_worktree) = self.worktree_for_id(new_project_path.worktree_id, cx) else {
            return Task::ready(Err(anyhow!("no such worktree")));
        };

        match &self.state {
            WorktreeStoreState::Local { fs } => {
                let old_abs_path = old_worktree.read(cx).absolutize(&old_entry.path);
                let new_abs_path = new_worktree.read(cx).absolutize(&new_project_path.path);
                let fs = fs.clone();
                let copy = cx.background_spawn(async move {
                    copy_recursive(
                        fs.as_ref(),
                        &old_abs_path,
                        &new_abs_path,
                        Default::default(),
                    )
                    .await
                });

                cx.spawn(async move |_, cx| {
                    copy.await?;
                    new_worktree
                        .update(cx, |this, cx| {
                            this.as_local_mut().unwrap().refresh_entry(
                                new_project_path.path,
                                None,
                                cx,
                            )
                        })
                        .await
                })
            }
        }
    }

    pub fn rename_entry(
        &mut self,
        entry_id: ProjectEntryId,
        new_project_path: ProjectPath,
        cx: &mut Context<Self>,
    ) -> Task<Result<CreatedEntry>> {
        let Some(old_worktree) = self.worktree_for_entry(entry_id, cx) else {
            return Task::ready(Err(anyhow!("no such worktree")));
        };
        let Some(old_entry) = old_worktree.read(cx).entry_for_id(entry_id).cloned() else {
            return Task::ready(Err(anyhow!("no such entry")));
        };
        let Some(new_worktree) = self.worktree_for_id(new_project_path.worktree_id, cx) else {
            return Task::ready(Err(anyhow!("no such worktree")));
        };

        match &self.state {
            WorktreeStoreState::Local { fs } => {
                let abs_old_path = old_worktree.read(cx).absolutize(&old_entry.path);
                let new_worktree_ref = new_worktree.read(cx);
                let is_root_entry = new_worktree_ref
                    .root_entry()
                    .is_some_and(|e| e.id == entry_id);
                let abs_new_path = if is_root_entry {
                    let abs_path = new_worktree_ref.abs_path();
                    let Some(root_parent_path) = abs_path.parent() else {
                        return Task::ready(Err(anyhow!("no parent for path {:?}", abs_path)));
                    };
                    root_parent_path.join(new_project_path.path.as_std_path())
                } else {
                    new_worktree_ref.absolutize(&new_project_path.path)
                };

                let fs = fs.clone();
                let case_sensitive = new_worktree
                    .read(cx)
                    .as_local()
                    .unwrap()
                    .fs_is_case_sensitive();

                let do_rename =
                    async move |fs: &dyn Fs, old_path: &Path, new_path: &Path, overwrite| {
                        fs.rename(
                            &old_path,
                            &new_path,
                            fs::RenameOptions {
                                overwrite,
                                ..fs::RenameOptions::default()
                            },
                        )
                        .await
                        .with_context(|| format!("renaming {old_path:?} into {new_path:?}"))
                    };

                let rename = cx.background_spawn({
                    let abs_new_path = abs_new_path.clone();
                    async move {
                        // If we're on a case-insensitive FS and we're doing a case-only rename (i.e. `foobar` to `FOOBAR`)
                        // we want to overwrite, because otherwise we run into a file-already-exists error.
                        let overwrite = !case_sensitive
                            && abs_old_path != abs_new_path
                            && abs_old_path.to_str().map(|p| p.to_lowercase())
                                == abs_new_path.to_str().map(|p| p.to_lowercase());

                        // The directory we're renaming into might not exist yet
                        if let Err(e) =
                            do_rename(fs.as_ref(), &abs_old_path, &abs_new_path, overwrite).await
                        {
                            if let Some(err) = e.downcast_ref::<std::io::Error>()
                                && err.kind() == std::io::ErrorKind::NotFound
                            {
                                if let Some(parent) = abs_new_path.parent() {
                                    fs.create_dir(parent).await.with_context(|| {
                                        format!("creating parent directory {parent:?}")
                                    })?;
                                    return do_rename(
                                        fs.as_ref(),
                                        &abs_old_path,
                                        &abs_new_path,
                                        overwrite,
                                    )
                                    .await;
                                }
                            }
                            return Err(e);
                        }
                        Ok(())
                    }
                });

                cx.spawn(async move |_, cx| {
                    rename.await?;
                    Ok(new_worktree
                        .update(cx, |this, cx| {
                            let local = this.as_local_mut().unwrap();
                            if is_root_entry {
                                // We eagerly update `abs_path` and refresh this worktree.
                                // Otherwise, the FS watcher would do it on the `RootUpdated` event,
                                // but with a noticeable delay, so we handle it proactively.
                                local.update_abs_path_and_refresh(
                                    SanitizedPath::new_arc(&abs_new_path),
                                    cx,
                                );
                                Task::ready(Ok(this.root_entry().cloned()))
                            } else {
                                // First refresh the parent directory (in case it was newly created)
                                if let Some(parent) = new_project_path.path.parent() {
                                    let _ = local.refresh_entries_for_paths(vec![parent.into()]);
                                }
                                // Then refresh the new path
                                local.refresh_entry(
                                    new_project_path.path.clone(),
                                    Some(old_entry.path),
                                    cx,
                                )
                            }
                        })
                        .await?
                        .map(CreatedEntry::Included)
                        .unwrap_or_else(|| CreatedEntry::Excluded {
                            abs_path: abs_new_path,
                        }))
                })
            }
        }
    }
    pub fn create_worktree(
        &mut self,
        abs_path: impl AsRef<Path>,
        visible: bool,
        cx: &mut Context<Self>,
    ) -> Task<Result<Entity<Worktree>>> {
        let abs_path: Arc<SanitizedPath> = SanitizedPath::new_arc(&abs_path);
        let is_via_collab = false;
        if !self.loading_worktrees.contains_key(&abs_path) {
            let task = match &self.state {
                WorktreeStoreState::Local { fs } => {
                    self.create_local_worktree(fs.clone(), abs_path.clone(), visible, cx)
                }
            };

            let loading_task = cx.spawn({
                let abs_path = abs_path.clone();
                async move |this, cx| {
                    let result = task.await;
                    this.update(cx, |this, cx| {
                        this.loading_worktrees.remove(&abs_path);
                        if !visible || !this.scanning_enabled || result.is_err() {
                            this.update_initial_scan_state(cx);
                        }
                    })
                    .ok();

                    if let Ok(worktree) = &result
                        && !is_via_collab
                    {
                        if let Some((trusted_worktrees, worktree_store)) = this
                            .update(cx, |_, cx| {
                                TrustedWorktrees::try_get_global(cx).zip(Some(cx.entity()))
                            })
                            .ok()
                            .flatten()
                        {
                            trusted_worktrees.update(cx, |trusted_worktrees, cx| {
                                trusted_worktrees.can_trust(
                                    &worktree_store,
                                    worktree.read(cx).id(),
                                    cx,
                                );
                            });
                        }

                        this.update(cx, |this, cx| {
                            if this.scanning_enabled && visible {
                                this.observe_worktree_scan_completion(worktree, cx);
                            }
                        })
                        .ok();
                    }

                    result
                }
            });

            self.loading_worktrees
                .insert(abs_path.clone(), loading_task.shared());

            if visible && self.scanning_enabled {
                *self.initial_scan_complete.0.borrow_mut() = false;
            }
        }
        let task = self.loading_worktrees.get(&abs_path).unwrap().clone();
        cx.background_spawn(async move { task.await.map_err(|error| anyhow!("{error}")) })
    }

    fn create_local_worktree(
        &mut self,
        fs: Arc<dyn Fs>,
        abs_path: Arc<SanitizedPath>,
        visible: bool,
        cx: &mut Context<Self>,
    ) -> Task<Result<Entity<Worktree>, Arc<anyhow::Error>>> {
        let next_entry_id = self.next_entry_id.clone();
        let scanning_enabled = self.scanning_enabled;

        let next_worktree_id = self.next_worktree_id();

        cx.spawn(async move |this, cx| {
            let worktree_id = next_worktree_id.await?;
            let worktree = Worktree::local(
                SanitizedPath::cast_arc(abs_path.clone()),
                visible,
                fs,
                next_entry_id,
                scanning_enabled,
                worktree_id,
                cx,
            )
            .await?;

            this.update(cx, |this, cx| this.add(&worktree, cx))?;

            if visible {
                cx.update(|cx| {
                    cx.add_recent_document(abs_path.as_path());
                });
            }

            Ok(worktree)
        })
    }

    pub fn add(&mut self, worktree: &Entity<Worktree>, cx: &mut Context<Self>) {
        let worktree_id = worktree.read(cx).id();
        debug_assert!(self.worktrees().all(|w| w.read(cx).id() != worktree_id));

        let push_strong_handle = self.retain_worktrees || worktree.read(cx).is_visible();
        let handle = if push_strong_handle {
            WorktreeHandle::Strong(worktree.clone())
        } else {
            WorktreeHandle::Weak(worktree.downgrade())
        };
        self.worktrees.push(handle);

        cx.emit(WorktreeStoreEvent::WorktreeAdded(worktree.clone()));

        let handle_id = worktree.entity_id();
        cx.subscribe(worktree, |_, worktree, event, cx| {
            let worktree_id = worktree.read(cx).id();
            match event {
                worktree::Event::UpdatedEntries(changes) => {
                    cx.emit(WorktreeStoreEvent::WorktreeUpdatedEntries(
                        worktree_id,
                        changes.clone(),
                    ));
                }
                worktree::Event::UpdatedGitRepositories(set) => {
                    cx.emit(WorktreeStoreEvent::WorktreeUpdatedGitRepositories(
                        worktree_id,
                        set.clone(),
                    ));
                }
                worktree::Event::DeletedEntry(id) => {
                    cx.emit(WorktreeStoreEvent::WorktreeDeletedEntry(worktree_id, *id))
                }
                worktree::Event::Deleted => {
                    // The worktree root itself has been deleted (for single-file worktrees)
                    // The worktree will be removed via the observe_release callback
                }
                worktree::Event::UpdatedRootRepoCommonDir { .. } => {
                    cx.emit(WorktreeStoreEvent::WorktreeUpdatedRootRepoCommonDir(
                        worktree_id,
                    ));
                }
            }
        })
        .detach();
        cx.observe_release(worktree, move |_this, worktree, cx| {
            cx.emit(WorktreeStoreEvent::WorktreeReleased(
                handle_id,
                worktree.id(),
            ));
            cx.emit(WorktreeStoreEvent::WorktreeRemoved(
                handle_id,
                worktree.id(),
            ));
        })
        .detach();
    }

    pub fn remove_worktree(&mut self, id_to_remove: WorktreeId, cx: &mut Context<Self>) {
        self.worktrees.retain(|worktree| {
            if let Some(worktree) = worktree.upgrade() {
                if worktree.read(cx).id() == id_to_remove {
                    cx.emit(WorktreeStoreEvent::WorktreeRemoved(
                        worktree.entity_id(),
                        id_to_remove,
                    ));
                    false
                } else {
                    true
                }
            } else {
                false
            }
        });
        self.update_initial_scan_state(cx);
    }

    pub fn worktree_for_main_worktree_path(
        &self,
        path: &Path,
        cx: &App,
    ) -> Option<Entity<Worktree>> {
        self.visible_worktrees(cx).find(|worktree| {
            let worktree = worktree.read(cx);
            if let Some(common_dir) = worktree.root_repo_common_dir() {
                common_dir.parent() == Some(path)
            } else {
                worktree.abs_path().as_ref() == path
            }
        })
    }

    pub fn move_worktree(
        &mut self,
        source: WorktreeId,
        destination: WorktreeId,
        cx: &mut Context<Self>,
    ) -> Result<()> {
        if source == destination {
            return Ok(());
        }

        let mut source_index = None;
        let mut destination_index = None;
        for (i, worktree) in self.worktrees.iter().enumerate() {
            if let Some(worktree) = worktree.upgrade() {
                let worktree_id = worktree.read(cx).id();
                if worktree_id == source {
                    source_index = Some(i);
                    if destination_index.is_some() {
                        break;
                    }
                } else if worktree_id == destination {
                    destination_index = Some(i);
                    if source_index.is_some() {
                        break;
                    }
                }
            }
        }

        let source_index =
            source_index.with_context(|| format!("Missing worktree for id {source}"))?;
        let destination_index =
            destination_index.with_context(|| format!("Missing worktree for id {destination}"))?;

        if source_index == destination_index {
            return Ok(());
        }

        let worktree_to_move = self.worktrees.remove(source_index);
        self.worktrees.insert(destination_index, worktree_to_move);
        cx.emit(WorktreeStoreEvent::WorktreeOrderChanged);
        cx.notify();
        Ok(())
    }

    pub fn fs(&self) -> Option<Arc<dyn Fs>> {
        match &self.state {
            WorktreeStoreState::Local { fs } => Some(fs.clone()),
        }
    }

    pub fn paths(&self, cx: &App) -> WorktreePaths {
        let (mains, folders): (Vec<PathBuf>, Vec<PathBuf>) = self
            .visible_worktrees(cx)
            .map(|worktree| {
                let snapshot = worktree.read(cx).snapshot();
                let folder_path = snapshot.abs_path().to_path_buf();
                let main_path = snapshot
                    .root_repo_common_dir()
                    .filter(|dir| !crate::git_store::is_submodule_git_dir(dir))
                    .map(|dir| crate::git_store::repo_identity_path(dir, snapshot.path_style()))
                    .filter(|repo_path| {
                        snapshot.root_repo_is_linked_worktree()
                            || *repo_path == folder_path.as_path()
                            || !folder_path.starts_with(*repo_path)
                    })
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|| folder_path.clone());
                (main_path, folder_path)
            })
            .unzip();

        WorktreePaths {
            paths: PathList::new(&folders),
            main_paths: PathList::new(&mains),
        }
    }
}

#[derive(Clone, Debug)]
enum WorktreeHandle {
    Strong(Entity<Worktree>),
    Weak(WeakEntity<Worktree>),
}

impl WorktreeHandle {
    fn upgrade(&self) -> Option<Entity<Worktree>> {
        match self {
            WorktreeHandle::Strong(handle) => Some(handle.clone()),
            WorktreeHandle::Weak(handle) => handle.upgrade(),
        }
    }
}

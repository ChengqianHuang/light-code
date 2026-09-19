mod conflict_set;
pub mod diff_buffer_list;
pub mod git_traversal;
pub mod job_debug_queue;
pub mod pending_op;

use crate::{
    Project, ProjectEnvironment, ProjectItem, ProjectPath,
    buffer_store::{BufferStore, BufferStoreEvent},
    project_settings::ProjectSettings,
    trusted_worktrees::{
        PathTrust, TrustedWorktrees, TrustedWorktreesEvent, TrustedWorktreesStore,
    },
    worktree_store::{WorktreeStore, WorktreeStoreEvent},
};
use anyhow::{Context as _, Result, anyhow, bail};
use askpass::AskPassDelegate;
use buffer_diff::{
    BufferDiff, DiffHunk, DiffHunkSecondaryStatus, DiffOperations, PendingHunk, PendingSense,
};
use collections::HashMap;
pub use conflict_set::{ConflictRegion, ConflictSet, ConflictSetSnapshot, ConflictSetUpdate};
use fs::{Fs, RemoveOptions};
use futures::{
    FutureExt, SinkExt, Stream, StreamExt,
    channel::{
        mpsc,
        oneshot::{self, Canceled},
    },
    future::{self, Shared},
    stream::{FuturesOrdered, FuturesUnordered},
};
use git::{
    BuildPermalinkParams, GitHostingProviderRegistry, Oid, RunHook,
    blame::Blame,
    parse_git_remote_url,
    repository::{
        Branch, BranchesScanResult, CommitData, CommitDetails, CommitFileStatus, CommitOptions,
        CreateWorktreeTarget, DiffStatType, DiffType, FetchOptions, FileHistoryChangedFileSets,
        GitCommitTemplate, GitRepository, GitRepositoryCheckpoint, InitialGraphCommitData,
        LogOrder, LogSource, PushOptions, Remote, RemoteCommandOutput, RepoPath, ResetMode,
        SearchCommitArgs, Worktree as GitWorktree, delete_branch_flag,
        is_binary_content,
    },
    stash::GitStash,
    status::{
        self, DiffStat, DiffTreeType, FileStatus, GitSummary, TreeDiff,
    },
};
use gpui::{
    App, AppContext, AsyncApp, BackgroundExecutor, Context, Entity, EventEmitter, SharedString,
    Subscription, Task, TaskExt, WeakEntity,
};
use language::{
    Anchor, Buffer, BufferEvent, Capability, Language, LanguageRegistry, decode_text, encode_text,
};
use paths::{config_dir, home_dir};
use pending_op::{PendingOp, PendingOpId, PendingOps, PendingOpsSummary};
use postage::stream::Stream as _;
use serde::Deserialize;
use settings::{GitDiffBaseSetting, Settings, SettingsLocation, SettingsStore, WorktreeId};
use smol::future::yield_now;
use std::{
    collections::{BTreeSet, HashSet, VecDeque, hash_map::Entry},
    future::Future,
    mem,
    ops::Range,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc,
        atomic::{self, AtomicU64},
    },
    time::{Duration, Instant, SystemTime},
};
use sum_tree::{Edit, SumTree, TreeMap};
use task::Shell;
use text::{Bias, BufferId, OffsetRangeExt, Rope, ToOffset};
use util::{
    ResultExt, debug_panic,
    paths::{PathStyle, SanitizedPath},
    post_inc,
    rel_path::RelPath,
};
use worktree::{
    File, PathChange, PathKey, PathProgress, PathSummary, PathTarget, ProjectEntryId,
    UpdatedGitRepositoriesSet, UpdatedGitRepository, Worktree, WorktreeSettings,
};
use zeroize::Zeroize;

pub struct GitStore {
    state: GitStoreState,
    project: Option<WeakEntity<Project>>,
    buffer_store: Entity<BufferStore>,
    worktree_store: Entity<WorktreeStore>,
    repositories: HashMap<RepositoryId, Entity<Repository>>,
    parked_repositories: Vec<ParkedRepository>,
    diff_base: GitDiffBaseSetting,
    display_diffs: HashMap<RepositoryId, DisplayDiff>,
    worktree_ids: HashMap<RepositoryId, HashSet<WorktreeId>>,
    active_repo_id: Option<RepositoryId>,
    #[allow(clippy::type_complexity)]
    loading_diffs:
        HashMap<(BufferId, DiffKind), Shared<Task<Result<Entity<BufferDiff>, Arc<anyhow::Error>>>>>,
    diffs: HashMap<BufferId, Entity<BufferGitState>>,
    buffer_ids_by_index_text_buffer_id: HashMap<BufferId, BufferId>,
    _subscriptions: Vec<Subscription>,
}

const MIN_PARKED_REPOSITORY_DEPTH: usize = 2;

#[derive(Clone, Debug)]
pub struct ParkedRepository {
    worktree_id: WorktreeId,
    work_directory_abs_path: Arc<Path>,
    dot_git_abs_path: Arc<Path>,
    repository_dir_abs_path: Arc<Path>,
    common_dir_abs_path: Arc<Path>,
}

impl ParkedRepository {
    #[cfg(feature = "test-support")]
    pub fn work_directory_abs_path(&self) -> &Path {
        &self.work_directory_abs_path
    }
}

#[derive(Default)]
struct DisplayDiff {
    buffers: Option<(Entity<diff_buffer_list::DiffBufferList>, Subscription)>,
    refresh: Option<Task<()>>,
}

struct BufferGitState {
    unstaged_diff: Option<WeakEntity<BufferDiff>>,
    staged_diff: Option<(WeakEntity<BufferDiff>, Entity<Buffer>)>,
    uncommitted_diff: Option<WeakEntity<BufferDiff>>,
    oid_diffs: HashMap<Option<git::Oid>, WeakEntity<BufferDiff>>,
    conflict_set: Option<WeakEntity<ConflictSet>>,
    recalculate_diff_task: Option<Task<Result<()>>>,
    reparse_conflict_markers_task: Option<Task<Result<()>>>,
    language: Option<Arc<Language>>,
    language_registry: Option<Arc<LanguageRegistry>>,
    conflict_updated_futures: Vec<oneshot::Sender<()>>,
    recalculating_tx: postage::watch::Sender<bool>,

    /// These operation counts are used to ensure that head and index text
    /// values read from the git repository are up-to-date with any hunk staging
    /// operations that have been performed on the BufferDiff.
    ///
    /// The operation count is incremented immediately when the user initiates a
    /// hunk stage/unstage operation. Then, upon finishing writing the new index
    /// text do disk, the `operation count as of write` is updated to reflect
    /// the operation count that prompted the write.
    hunk_staging_operation_count: usize,
    hunk_staging_operation_count_as_of_write: usize,

    head_text: Option<Arc<str>>,
    index_text: Option<Arc<str>>,
    /// The optimistic, in-flight index state: the sole input to the index write,
    /// expressed relative to the currently-loaded index text (`I0`). Never shown
    /// to any view (views use per-diff pending hunks). Cleared when a
    /// recalculation settles.
    ///
    /// `I0` is immutable for the lifetime of any pending edit (the index text
    /// buffer is only fast-forwarded when a recalculation settles, which clears
    /// this state in the same update), so byte offsets stay valid for the whole
    /// window.
    pending_index_edits: Option<Vec<(Range<usize>, Arc<str>)>>,
    oid_texts: HashMap<git::Oid, Arc<str>>,
    head_text_buffer: WeakEntity<Buffer>,
    index_text_buffer: WeakEntity<Buffer>,
    index_text_buffer_language_enabled: bool,
    head_changed: bool,
    index_changed: bool,
    language_changed: bool,
}

fn pending_hunks(
    hunks: &[DiffHunk],
    version: &clock::Global,
    sense: PendingSense,
) -> Vec<PendingHunk> {
    hunks
        .iter()
        .map(|hunk| {
            PendingHunk::new(
                hunk.buffer_range.clone(),
                hunk.diff_base_byte_range.clone(),
                version.clone(),
                sense,
            )
        })
        .collect()
}

fn decode_git_text(bytes: Vec<u8>) -> Result<String> {
    Ok(decode_text(bytes)?.text)
}

#[derive(Debug)]
pub struct CommitDiff {
    pub files: Vec<CommitFile>,
    pub is_shallow_boundary: bool,
}

#[derive(Debug)]
pub struct CommitFile {
    pub path: RepoPath,
    pub old_text: Option<String>,
    pub new_text: Option<String>,
    pub is_binary: bool,
}

impl CommitFile {
    pub fn status(&self) -> CommitFileStatus {
        match (&self.old_text, &self.new_text) {
            (None, Some(_)) => CommitFileStatus::Added,
            (Some(_), None) => CommitFileStatus::Deleted,
            _ => CommitFileStatus::Modified,
        }
    }
}

fn decode_commit_diff(diff: git::repository::CommitDiff) -> CommitDiff {
    let files = diff
        .files
        .into_iter()
        .map(|file| {
            let git::repository::CommitFile {
                path,
                old_content,
                new_content,
                mut is_binary,
            } = file;

            if is_binary {
                return CommitFile {
                    path,
                    old_text: old_content.map(|_| String::new()),
                    new_text: new_content.map(|_| String::new()),
                    is_binary,
                };
            }

            let mut decode_content = |content: Option<Vec<u8>>| {
                content.map(|content| match decode_git_text(content) {
                    Ok(text) => text,
                    Err(error) => {
                        log::debug!(
                            "treating commit content for {} as binary after decoding failed: {error:#}",
                            path.as_unix_str()
                        );
                        is_binary = true;
                        String::new()
                    }
                })
            };
            let mut old_text = decode_content(old_content);
            let mut new_text = decode_content(new_content);
            if is_binary {
                old_text = old_text.map(|_| String::new());
                new_text = new_text.map(|_| String::new());
            }

            CommitFile {
                path,
                old_text,
                new_text,
                is_binary,
            }
        })
        .collect();
    CommitDiff {
        files,
        is_shallow_boundary: diff.is_shallow_boundary,
    }
}

#[derive(Clone, Debug)]
enum DiffBasesChange {
    SetIndex(Option<String>),
    SetHead(Option<String>),
    SetEach {
        index: Option<String>,
        head: Option<String>,
    },
    SetBoth(Option<String>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum DiffKind {
    Unstaged,
    Staged,
    Uncommitted,
    SinceOid(Option<git::Oid>),
}

struct GitDiffOperations {
    project: WeakEntity<Project>,
    kind: DiffKind,
}

impl DiffOperations for GitDiffOperations {
    fn supports_staging(&self) -> bool {
        matches!(self.kind, DiffKind::Unstaged | DiffKind::Uncommitted)
    }

    fn supports_unstaging(&self) -> bool {
        matches!(self.kind, DiffKind::Staged | DiffKind::Uncommitted)
    }

    fn supports_restore(&self) -> bool {
        matches!(self.kind, DiffKind::Unstaged | DiffKind::Uncommitted)
    }

    fn stage(
        &self,
        diff: Entity<BufferDiff>,
        buffer: Option<Entity<Buffer>>,
        buffer_ranges: Vec<Range<Anchor>>,
        cx: &mut App,
    ) {
        let result = self.project.update(cx, |project, cx| match self.kind {
            DiffKind::Unstaged => {
                let buffer = buffer.context("unstaged diff has no worktree buffer")?;
                project.stage_hunks(buffer, diff, buffer_ranges, cx)
            }
            DiffKind::Uncommitted => {
                let buffer = buffer.context("uncommitted diff has no worktree buffer")?;
                let secondary_diff = diff
                    .read(cx)
                    .secondary_diff()
                    .context("diff has no unstaged secondary")?;
                project.stage_hunks(buffer, secondary_diff, buffer_ranges, cx)
            }
            _ => Ok(()),
        });
        result.and_then(|result| result).log_err();
    }

    fn unstage(
        &self,
        diff: Entity<BufferDiff>,
        buffer: Option<Entity<Buffer>>,
        buffer_ranges: Vec<Range<Anchor>>,
        cx: &mut App,
    ) {
        let result = self.project.update(cx, |project, cx| match self.kind {
            DiffKind::Staged => project.unstage_staged_hunks(diff, buffer_ranges, cx),
            DiffKind::Uncommitted => {
                let buffer = buffer.context("uncommitted diff has no worktree buffer")?;
                project.unstage_uncommitted_hunks(buffer, diff, buffer_ranges, cx)
            }
            _ => Ok(()),
        });
        result.and_then(|result| result).log_err();
    }
}

struct IndexTextFile {
    path: Arc<RelPath>,
    full_path: PathBuf,
    path_style: PathStyle,
    file_name: String,
    worktree_id: WorktreeId,
    is_private: bool,
}

impl IndexTextFile {
    fn new(file: &dyn language::File, cx: &App) -> Self {
        Self {
            path: file.path().clone(),
            full_path: file.full_path(cx),
            path_style: file.path_style(cx),
            file_name: file.file_name(cx).to_string(),
            worktree_id: file.worktree_id(cx),
            is_private: file.is_private(),
        }
    }
}

impl language::File for IndexTextFile {
    fn as_local(&self) -> Option<&dyn language::LocalFile> {
        None
    }

    fn disk_state(&self) -> language::DiskState {
        language::DiskState::Historic { was_deleted: false }
    }

    fn path(&self) -> &Arc<RelPath> {
        &self.path
    }

    fn full_path(&self, _: &App) -> PathBuf {
        self.full_path.clone()
    }

    fn path_style(&self, _: &App) -> PathStyle {
        self.path_style
    }

    fn file_name<'a>(&'a self, _: &'a App) -> &'a str {
        &self.file_name
    }

    fn worktree_id(&self, _: &App) -> WorktreeId {
        self.worktree_id
    }

    fn is_private(&self) -> bool {
        self.is_private
    }
}

#[derive(Debug, Clone, Copy)]
pub enum GitAccess {
    /// Either:
    /// - the user owns `.git`
    /// - the user doesn't own `.git`, but has both of:
    ///   - OS-level read permissions
    ///   - the directory is marked as safe (git config safe.directory)
    Yes,

    /// The user is not the owner of `.git`, and one of the following is true:
    /// - the directory is not marked as safe (git config safe.directory)
    /// - the user does not have OS-level read permissions to `.git`
    No,
}

enum GitStoreState {
    Local {
        next_repository_id: Arc<AtomicU64>,
        project_environment: Entity<ProjectEnvironment>,
        fs: Arc<dyn Fs>,
        _fs_watches: Box<[Task<()>]>,
    },
}

#[derive(Clone, Debug)]
pub struct GitStoreCheckpoint {
    checkpoints_by_work_dir_abs_path: HashMap<Arc<Path>, GitRepositoryCheckpoint>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatusEntry {
    pub repo_path: RepoPath,
    pub status: FileStatus,
    pub diff_stat: Option<DiffStat>,
    pub staged_diff_stat: Option<DiffStat>,
    pub unstaged_diff_stat: Option<DiffStat>,
}

impl sum_tree::Item for StatusEntry {
    type Summary = PathSummary<GitSummary>;

    fn summary(&self, _: <Self::Summary as sum_tree::Summary>::Context<'_>) -> Self::Summary {
        PathSummary {
            max_path: self.repo_path.as_ref().clone(),
            item_summary: self.status.summary(),
        }
    }
}

impl sum_tree::KeyedItem for StatusEntry {
    type Key = PathKey;

    fn key(&self) -> Self::Key {
        PathKey(self.repo_path.as_ref().clone())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RepositoryId(pub u64);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MergeDetails {
    pub merge_heads_by_conflicted_path: TreeMap<RepoPath, Vec<Option<SharedString>>>,
    pub message: Option<SharedString>,
}

#[derive(Clone)]
pub enum CommitDataState {
    Loading(Option<Shared<oneshot::Receiver<Arc<CommitData>>>>),
    Loaded(Arc<CommitData>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepositorySnapshot {
    pub id: RepositoryId,
    pub statuses_by_path: SumTree<StatusEntry>,
    pub work_directory_abs_path: Arc<Path>,
    pub dot_git_abs_path: Arc<Path>,
    /// Absolute path to the directory holding this worktree's Git state.
    ///
    /// For a linked worktree this is the worktree-specific directory under the
    /// common Git directory, such as `<main>/.git/worktrees/<name>`.
    pub repository_dir_abs_path: Arc<Path>,
    /// Absolute path to the repository's common Git directory.
    ///
    /// For a normal checkout this is `<work_directory>/.git`. For a linked
    /// worktree this is the common Git directory shared by all worktrees. If
    /// that common directory is a bare repository, there may be no main
    /// worktree path to derive from it.
    pub common_dir_abs_path: Arc<Path>,
    pub path_style: PathStyle,
    pub branch: Option<Branch>,
    pub branch_list: Arc<[Branch]>,
    pub branch_list_error: Option<SharedString>,
    pub head_commit: Option<CommitDetails>,
    pub scan_id: u64,
    pub merge: MergeDetails,
    pub remote_origin_url: Option<String>,
    pub remote_upstream_url: Option<String>,
    pub stash_entries: GitStash,
    pub linked_worktrees: Arc<[GitWorktree]>,
}

type JobId = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobInfo {
    pub start: Instant,
    pub message: SharedString,
}

struct CommitDataHandler {
    _task: Task<()>,
    commit_data_request: async_channel::Sender<Oid>,
    completion_senders: HashMap<Oid, oneshot::Sender<Arc<CommitData>>>,
    pending_requests: HashSet<Oid>,
}

/// Represents the handler of a git cat-file --batch process within Zed
/// It's used to lazily fetch commit data as needed (whatever a user is viewing)
enum CommitDataHandlerState {
    /// The handler is open and processing requests
    Open(CommitDataHandler),
    /// The handler closed because it didn't receive any requests in the last 10s
    /// or hasn't been open before
    Closed,
}

enum NextCommitDataRequest {
    Idle,
    Closed,
}

pub struct InitialGitGraphData {
    fetch_task: Task<()>,
    pub error: Option<SharedString>,
    pub commit_data: Vec<Arc<InitialGraphCommitData>>,
    pub commit_oid_to_index: HashMap<Oid, usize>,
    subscribers: Vec<async_channel::Sender<Result<Vec<Arc<InitialGraphCommitData>>, SharedString>>>,
}

pub struct GraphDataResponse<'a> {
    pub commits: &'a [Arc<InitialGraphCommitData>],
    pub is_loading: bool,
    pub error: Option<SharedString>,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum UnshallowState {
    #[default]
    Idle,
    InProgress,
    Unshallowed,
}

pub struct Repository {
    this: WeakEntity<Self>,
    snapshot: RepositorySnapshot,
    unshallow_state: UnshallowState,
    commit_message_buffer: Option<Entity<Buffer>>,
    git_store: WeakEntity<GitStore>,
    // For a local repository, holds paths that have had worktree events since the last status scan completed,
    // and that should be examined during the next status scan.
    paths_needing_status_update: Vec<Vec<RepoPath>>,
    job_sender: mpsc::UnboundedSender<GitJob>,
    _worker_task: Task<()>,
    active_jobs: HashMap<JobId, JobInfo>,
    job_debug_queue: job_debug_queue::GitJobDebugQueue,
    pending_ops: SumTree<PendingOps>,
    job_id: JobId,
    repository_state: Shared<Task<Result<RepositoryState, String>>>,
    initial_graph_data: HashMap<(LogSource, LogOrder), InitialGitGraphData>,
    commit_data_handler: CommitDataHandlerState,
    commit_data: HashMap<Oid, CommitDataState>,
}

impl std::ops::Deref for Repository {
    type Target = RepositorySnapshot;

    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

#[derive(Clone)]
pub struct LocalRepositoryState {
    pub fs: Arc<dyn Fs>,
    pub backend: Arc<dyn GitRepository>,
    pub environment: Arc<HashMap<String, String>>,
}

impl LocalRepositoryState {
    async fn new(
        work_directory_abs_path: Arc<Path>,
        dot_git_abs_path: Arc<Path>,
        project_environment: WeakEntity<ProjectEnvironment>,
        fs: Arc<dyn Fs>,
        is_trusted: bool,
        cx: &mut AsyncApp,
    ) -> anyhow::Result<Self> {
        let environment = project_environment
                .update(cx, |project_environment, cx| {
                    project_environment.local_directory_environment(&Shell::System, work_directory_abs_path.clone(), cx)
                })?
                .await
                .unwrap_or_else(|| {
                    log::error!("failed to get working directory environment for repository {work_directory_abs_path:?}");
                    HashMap::default()
                });
        let search_paths = environment.get("PATH").map(|val| val.to_owned());
        let backend = cx
            .background_spawn({
                let fs = fs.clone();
                async move {
                    let system_git_binary_path = search_paths
                        .and_then(|search_paths| {
                            which::which_in("git", Some(search_paths), &work_directory_abs_path)
                                .ok()
                        })
                        .or_else(|| which::which("git").ok());
                    fs.open_repo(&dot_git_abs_path, system_git_binary_path.as_deref())
                        .with_context(|| format!("opening repository at {dot_git_abs_path:?}"))
                }
            })
            .await?;
        backend.set_trusted(is_trusted);
        Ok(LocalRepositoryState {
            backend,
            environment: Arc::new(environment),
            fs,
        })
    }
}

#[derive(Clone)]
pub enum RepositoryState {
    Local(LocalRepositoryState),
}

enum PermalinkTarget {
    Buffer {
        buffer_id: BufferId,
        selection: Range<u32>,
    },
    File(ProjectPath),
}

impl PermalinkTarget {
    fn selection(&self) -> Option<Range<u32>> {
        match self {
            Self::Buffer { selection, .. } => Some(selection.clone()),
            Self::File(_) => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GitGraphEvent {
    CountUpdated(usize),
    FullyLoaded,
    LoadingError,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepositoryEvent {
    StatusesChanged,
    HeadChanged,
    BranchListChanged,
    StashEntriesChanged,
    GitWorktreeListChanged,
    PendingOpsChanged { pending_ops: SumTree<PendingOps> },
    GraphEvent((LogSource, LogOrder), GitGraphEvent),
}

#[derive(Clone, Debug)]
pub struct JobsUpdated;

#[derive(Debug)]
pub enum GitStoreEvent {
    ActiveRepositoryChanged(Option<RepositoryId>),
    /// Bool is true when the repository that's updated is the active repository
    RepositoryUpdated(RepositoryId, RepositoryEvent, bool),
    RepositoryAdded,
    RepositoryRemoved(RepositoryId),
    IndexWriteError(anyhow::Error),
    JobsUpdated,
    ConflictsUpdated,
    GlobalConfigurationUpdated,
    DiffBaseChanged(Option<RepositoryId>),
}

impl EventEmitter<RepositoryEvent> for Repository {}
impl EventEmitter<JobsUpdated> for Repository {}
impl EventEmitter<GitStoreEvent> for GitStore {}

pub struct GitJob {
    id: JobId,
    job: Box<dyn FnOnce(RepositoryState, &mut AsyncApp) -> Task<()>>,
    key: Option<GitJobKey>,
}

#[derive(PartialEq, Eq)]
enum GitJobKey {
    WriteIndex(Vec<RepoPath>),
    ReloadBufferDiffBases,
    RefreshStatuses,
    ReloadGitState,
}

impl GitStore {
    pub fn local(
        worktree_store: &Entity<WorktreeStore>,
        buffer_store: Entity<BufferStore>,
        environment: Entity<ProjectEnvironment>,
        fs: Arc<dyn Fs>,
        cx: &mut Context<Self>,
    ) -> Self {
        let _fs_watches = if fs.is_fake() {
            Box::new([])
        } else {
            [
                config_dir().join("git/config"),
                home_dir().join(".gitconfig"),
            ]
            .into_iter()
            .map(|path| {
                let fs = fs.clone();

                cx.spawn(async move |this, cx| {
                    let watcher = fs.watch(&path, Duration::from_millis(100));
                    let (mut watcher, _) = watcher.await;
                    while let Some(_) = watcher.next().await {
                        let Ok(_) = this.update(cx, |this, cx| {
                            let GitStoreState::Local {
                                project_environment,
                                fs,
                                ..
                            } = &this.state
                            else {
                                return;
                            };
                            let project_environment = project_environment.downgrade();
                            let fs = fs.clone();
                            let repositories_to_respawn = this
                                .repositories
                                .iter()
                                .filter_map(|(repository_id, repo)| {
                                    repo.read(cx)
                                        .job_sender
                                        .is_closed()
                                        .then_some((*repository_id, repo.clone()))
                                })
                                .collect::<Vec<_>>();
                            for (repository_id, repo) in repositories_to_respawn {
                                let is_trusted = this.repository_is_trusted(repository_id, cx);
                                repo.update(cx, |repo, cx| {
                                    repo.respawn_local_worker(
                                        project_environment.clone(),
                                        fs.clone(),
                                        is_trusted,
                                        cx,
                                    );
                                    repo.schedule_scan(cx);
                                })
                            }
                            let display_repo_ids =
                                this.display_diffs.keys().copied().collect::<Vec<_>>();
                            for repository_id in display_repo_ids {
                                this.refresh_diff_base_for_repo(repository_id, cx);
                            }
                            cx.emit(GitStoreEvent::GlobalConfigurationUpdated);
                        }) else {
                            return;
                        };
                    }
                })
            })
            .collect::<Vec<_>>()
            .into_boxed_slice()
        };

        Self::new(
            worktree_store.clone(),
            buffer_store,
            GitStoreState::Local {
                next_repository_id: Arc::new(AtomicU64::new(1)),
                project_environment: environment,
                _fs_watches,
                fs,
            },
            cx,
        )
    }

    fn new(
        worktree_store: Entity<WorktreeStore>,
        buffer_store: Entity<BufferStore>,
        state: GitStoreState,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut _subscriptions = vec![
            cx.subscribe(&worktree_store, Self::on_worktree_store_event),
            cx.subscribe(&buffer_store, Self::on_buffer_store_event),
        ];

        if let Some(trusted_worktrees) = TrustedWorktrees::try_get_global(cx) {
            _subscriptions.push(cx.subscribe(&trusted_worktrees, Self::on_trusted_worktrees_event));
        }

        _subscriptions.push(cx.observe_global::<SettingsStore>(|this, cx| {
            let setting = ProjectSettings::get_global(cx).git.diff_base;
            if setting != this.diff_base {
                this.set_diff_base(setting, cx);
            }
            this.activate_parked_repositories_where_parking_disabled(cx);
        }));

        let diff_base_setting = ProjectSettings::get_global(cx).git.diff_base;
        GitStore {
            state,
            project: None,
            buffer_store,
            worktree_store,
            repositories: HashMap::default(),
            parked_repositories: Vec::new(),
            diff_base: diff_base_setting,
            display_diffs: HashMap::default(),
            worktree_ids: HashMap::default(),
            active_repo_id: None,
            _subscriptions,
            loading_diffs: HashMap::default(),
            diffs: HashMap::default(),
            buffer_ids_by_index_text_buffer_id: HashMap::default(),
        }
    }

    pub(crate) fn set_project(&mut self, project: WeakEntity<Project>) {
        self.project = Some(project);
    }

    pub fn is_local(&self) -> bool {
        matches!(self.state, GitStoreState::Local { .. })
    }

    fn set_active_repo_id(&mut self, repo_id: RepositoryId, cx: &mut Context<Self>) {
        if self.active_repo_id != Some(repo_id) {
            self.active_repo_id = Some(repo_id);
            cx.emit(GitStoreEvent::ActiveRepositoryChanged(Some(repo_id)));
        }
    }

    pub fn set_active_repo_for_path(&mut self, project_path: &ProjectPath, cx: &mut Context<Self>) {
        if let Some((repo, _)) = self.repository_and_path_for_project_path(project_path, cx) {
            self.set_active_repo_id(repo.read(cx).id, cx);
        }
    }

    pub fn set_active_repo_for_worktree(
        &mut self,
        worktree_id: WorktreeId,
        cx: &mut Context<Self>,
    ) {
        let Some(worktree) = self
            .worktree_store
            .read(cx)
            .worktree_for_id(worktree_id, cx)
        else {
            return;
        };
        let worktree_abs_path = worktree.read(cx).abs_path();
        let Some(repo_id) = self
            .repositories
            .values()
            .filter(|repo| {
                let repo_path = &repo.read(cx).work_directory_abs_path;
                // The folder opened in Zed isn't necessarily the repo root; it may be
                // a subdirectory of it, e.g. opening `~/code/myrepo/backend` when the
                // repo lives at `~/code/myrepo`. So match any repo whose work directory
                // contains the folder. Nested repos can produce multiple matches, e.g.
                // opening `~/code/myrepo/vendor/lib` where `vendor/lib` is a submodule
                // matches both `myrepo` and the submodule; `max_by_key` then picks the
                // innermost match (the submodule), which the folder actually belongs to.
                worktree_abs_path.starts_with(repo_path.as_ref())
            })
            .max_by_key(|repo| repo.read(cx).work_directory_abs_path.as_os_str().len())
            .map(|repo| repo.read(cx).id)
        else {
            return;
        };

        self.set_active_repo_id(repo_id, cx);
    }

    pub fn active_repository(&self) -> Option<Entity<Repository>> {
        self.active_repo_id
            .as_ref()
            .map(|id| self.repositories[id].clone())
    }

    fn file_is_symlink(file: &File, cx: &App) -> bool {
        file.worktree
            .read(cx)
            .entry_for_path(&file.path)
            .is_some_and(|entry| entry.canonical_path.is_some())
    }

    fn buffer_is_symlink(buffer: &Entity<Buffer>, cx: &App) -> bool {
        File::from_dyn(buffer.read(cx).file()).is_some_and(|file| Self::file_is_symlink(file, cx))
    }

    pub fn open_unstaged_diff(
        &mut self,
        buffer: Entity<Buffer>,
        cx: &mut Context<Self>,
    ) -> Task<Result<Entity<BufferDiff>>> {
        let buffer_id = buffer.read(cx).remote_id();
        if let Some(diff_state) = self.diffs.get(&buffer_id)
            && let Some(unstaged_diff) = diff_state
                .read(cx)
                .unstaged_diff
                .as_ref()
                .and_then(|weak| weak.upgrade())
        {
            // If this unstaged diff was first opened as the uncommitted diff's
            // secondary, its index text wasn't highlighted. Enable it now and
            // recalc so the language gets applied to the deleted (base) side.
            diff_state.update(cx, |diff_state, cx| {
                if !diff_state.index_text_buffer_language_enabled {
                    diff_state.index_text_buffer_language_enabled = true;
                    let buffer_snapshot = buffer.read(cx).text_snapshot();
                    diff_state.recalculate_diffs(buffer_snapshot, cx);
                }
            });
            if let Some(task) =
                diff_state.update(cx, |diff_state, _| diff_state.wait_for_recalculation())
            {
                return cx.background_executor().spawn(async move {
                    task.await;
                    Ok(unstaged_diff)
                });
            }
            return Task::ready(Ok(unstaged_diff));
        }

        let Some((repo, repo_path)) =
            self.repository_and_path_for_buffer_id(buffer.read(cx).remote_id(), cx)
        else {
            return Task::ready(Err(anyhow!("failed to find git repository for buffer")));
        };

        let is_symlink = Self::buffer_is_symlink(&buffer, cx);
        let task = self
            .loading_diffs
            .entry((buffer_id, DiffKind::Unstaged))
            .or_insert_with(|| {
                let staged_text = if is_symlink {
                    Task::ready(Ok(None))
                } else {
                    repo.update(cx, |repo, cx| {
                        repo.load_staged_text(buffer_id, repo_path, cx)
                    })
                };
                cx.spawn(async move |this, cx| {
                    Self::open_diff_internal(
                        this,
                        DiffKind::Unstaged,
                        staged_text.await.map(DiffBasesChange::SetIndex),
                        buffer,
                        cx,
                    )
                    .await
                    .map_err(Arc::new)
                })
                .shared()
            })
            .clone();

        cx.background_spawn(async move { task.await.map_err(|e| anyhow!("{e}")) })
    }

    /// Opens the staged (HEAD-vs-index) diff for the given buffer, along with
    /// the index text buffer that is the diff's main buffer.
    pub fn open_staged_diff(
        &mut self,
        buffer: Entity<Buffer>,
        cx: &mut Context<Self>,
    ) -> Task<Result<(Entity<BufferDiff>, Entity<Buffer>)>> {
        let buffer_id = buffer.read(cx).remote_id();

        if let Some(diff_state) = self.diffs.get(&buffer_id)
            && let Some(staged_diff) = diff_state.read(cx).staged_diff_and_index_text_buffer()
        {
            if let Some(task) =
                diff_state.update(cx, |diff_state, _| diff_state.wait_for_recalculation())
            {
                return cx.background_executor().spawn(async move {
                    task.await;
                    Ok(staged_diff)
                });
            }
            return Task::ready(Ok(staged_diff));
        }

        let Some((repo, repo_path)) =
            self.repository_and_path_for_buffer_id(buffer.read(cx).remote_id(), cx)
        else {
            return Task::ready(Err(anyhow!("failed to find git repository for buffer")));
        };

        let task = self
            .loading_diffs
            .entry((buffer_id, DiffKind::Staged))
            .or_insert_with(|| {
                let changes = repo.update(cx, |repo, cx| {
                    repo.load_committed_text(buffer_id, repo_path, cx)
                });

                cx.spawn(async move |this, cx| {
                    Self::open_diff_internal(this, DiffKind::Staged, changes.await, buffer, cx)
                        .await
                        .map_err(Arc::new)
                })
                .shared()
            })
            .clone();

        cx.spawn(async move |this, cx| {
            let diff = task.await.map_err(|e| anyhow!("{e}"))?;
            this.update(cx, |this, cx| {
                let index_text_buffer = this
                    .diffs
                    .get(&buffer_id)
                    .and_then(|diff_state| {
                        let (_, index_text_buffer) = diff_state.read(cx).staged_diff.as_ref()?;
                        Some(index_text_buffer.clone())
                    })
                    .context("index text buffer missing after opening staged diff")?;
                Ok((diff, index_text_buffer))
            })?
        })
    }

    /// Stages the worktree changes covered by `worktree_ranges`, acting on the
    /// given unstaged (index-vs-worktree) diff. Used by both the unstaged-changes
    /// view and the uncommitted (gutter) controls: "stage" means the same index
    /// change regardless of which view it was invoked from, so callers holding an
    /// uncommitted diff pass its unstaged secondary.
    ///
    /// Decomposes the worktree region into the unstaged hunks it covers, so no
    /// worktree->index projection is needed. Optimistically suppresses the staged
    /// hunks from the unstaged diff and, if the uncommitted diff happens to be
    /// open, marks the corresponding uncommitted hunks as staging.
    pub fn stage_hunks(
        &mut self,
        buffer: Entity<Buffer>,
        unstaged_diff: Entity<BufferDiff>,
        worktree_ranges: Vec<Range<Anchor>>,
        cx: &mut Context<Self>,
    ) -> Result<()> {
        if worktree_ranges.is_empty() {
            return Ok(());
        }
        let buffer_snapshot = buffer.read(cx).snapshot();
        let buffer_id = buffer_snapshot.remote_id();
        let file_exists = buffer_snapshot
            .file()
            .is_some_and(|file| file.disk_state().exists());

        let unstaged_snapshot = unstaged_diff.read(cx).snapshot(cx);

        // Decompose: the unstaged hunks the worktree region covers carry the
        // index range directly. Sorting by buffer offset also sorts by index
        // offset, since the hunks are non-overlapping. We read the raw hunks
        // (ignoring optimistic suppression) so that re-staging a hunk that was
        // optimistically staged and then unstaged still finds it. The footprints
        // let the patch drop earlier edits the region covers even where the raw
        // diff is empty (re-staging a hunk that is staged on disk but
        // optimistically unstaged).
        let mut unstaged_hunks = Vec::new();
        let mut index_footprints = Vec::new();
        for range in &worktree_ranges {
            unstaged_hunks.extend(
                unstaged_snapshot.raw_hunks_intersecting_range(range.clone(), &buffer_snapshot),
            );
            index_footprints.push(
                unstaged_snapshot.base_text_range_for_buffer_range(range.clone(), &buffer_snapshot),
            );
        }
        unstaged_hunks.sort_by_key(|hunk| hunk.buffer_range.start.to_offset(&buffer_snapshot));
        unstaged_hunks.dedup_by(|a, b| a.buffer_range.start == b.buffer_range.start);

        // The uncommitted hunks the region covers get the optimistic
        // "staging" secondary status (free cross-view update).
        let uncommitted_diff = self.get_uncommitted_diff(buffer_id, cx);
        let mut uncommitted_hunks = Vec::new();
        if let Some(uncommitted_diff) = &uncommitted_diff {
            let uncommitted_snapshot = uncommitted_diff.read(cx).snapshot(cx);
            for range in &worktree_ranges {
                uncommitted_hunks.extend(
                    uncommitted_snapshot
                        .hunks_intersecting_range(range.clone(), &buffer_snapshot)
                        .filter(|hunk| {
                            hunk.secondary_status != DiffHunkSecondaryStatus::NoSecondaryHunk
                        }),
                );
            }
            uncommitted_hunks
                .sort_by_key(|hunk| hunk.buffer_range.start.to_offset(&buffer_snapshot));
            uncommitted_hunks.dedup_by(|a, b| a.buffer_range.start == b.buffer_range.start);
        }

        let index_edits = if !file_exists {
            // The worktree file is gone: staging removes it from the index.
            None
        } else {
            Some(
                unstaged_hunks
                    .iter()
                    .map(|hunk| {
                        let worktree_range = hunk.buffer_range.to_offset(&buffer_snapshot);
                        let replacement: Arc<str> = Arc::from(
                            buffer_snapshot
                                .text_for_range(worktree_range)
                                .collect::<String>(),
                        );
                        (hunk.diff_base_byte_range.clone(), replacement)
                    })
                    .collect::<Vec<_>>(),
            )
        };

        let version = buffer_snapshot.version().clone();
        let unstaged_pending = pending_hunks(&unstaged_hunks, &version, PendingSense::Suppress);
        let uncommitted_pending = pending_hunks(
            &uncommitted_hunks,
            &version,
            PendingSense::SetSecondaryStatus { stage: true },
        );
        drop(unstaged_snapshot);

        let diff_state = self
            .diffs
            .get(&buffer_id)
            .cloned()
            .context("failed to find git state for buffer")?;
        diff_state.update(cx, |diff_state, _| {
            diff_state.remove_overlapping_pending_index_edits(&index_footprints);
            diff_state.insert_pending_index_edits(index_edits);
        });

        if let Some(uncommitted_diff) = uncommitted_diff {
            uncommitted_diff.update(cx, |diff, cx| {
                diff.set_pending_hunks(&uncommitted_pending, &buffer_snapshot, cx);
            });
        }
        unstaged_diff.update(cx, |diff, cx| {
            diff.set_pending_hunks(&unstaged_pending, &buffer_snapshot, cx);
        });

        self.write_optimistic_index(buffer_id, cx);
        Ok(())
    }

    /// Unstages the worktree changes covered by `worktree_ranges`, acting on the
    /// given uncommitted (HEAD-vs-worktree) diff, invoked from the uncommitted
    /// (gutter) controls. Uses the worktree->index projection (the hard part)
    /// because the acted-on hunks are HEAD-vs-worktree.
    pub fn unstage_uncommitted_hunks(
        &mut self,
        buffer: Entity<Buffer>,
        uncommitted_diff: Entity<BufferDiff>,
        worktree_ranges: Vec<Range<Anchor>>,
        cx: &mut Context<Self>,
    ) -> Result<()> {
        if worktree_ranges.is_empty() {
            return Ok(());
        }
        let buffer_snapshot = buffer.read(cx).snapshot();
        let buffer_id = buffer_snapshot.remote_id();
        let file_exists = buffer_snapshot
            .file()
            .is_some_and(|file| file.disk_state().exists());

        let uncommitted_snapshot = uncommitted_diff.read(cx).snapshot(cx);
        let unstaged_snapshot = uncommitted_snapshot
            .secondary_diff()
            .context("diff has no unstaged secondary")?;

        let mut hunks = Vec::new();
        for range in &worktree_ranges {
            hunks.extend(
                uncommitted_snapshot.hunks_intersecting_range(range.clone(), &buffer_snapshot),
            );
        }
        hunks.sort_by_key(|hunk| hunk.buffer_range.start.to_offset(&buffer_snapshot));
        hunks.dedup_by(|a, b| a.buffer_range.start == b.buffer_range.start);

        let (index_edits, pending) = uncommitted_snapshot.compute_uncommitted_index_edits(
            unstaged_snapshot,
            false,
            &hunks,
            &buffer_snapshot,
            file_exists,
        );
        drop(uncommitted_snapshot);

        let diff_state = self
            .diffs
            .get(&buffer_id)
            .cloned()
            .context("failed to find git state for buffer")?;
        diff_state.update(cx, |diff_state, _| {
            diff_state.insert_pending_index_edits(index_edits)
        });

        uncommitted_diff.update(cx, |diff, cx| {
            diff.set_pending_hunks(&pending, &buffer_snapshot, cx);
        });

        self.write_optimistic_index(buffer_id, cx);
        Ok(())
    }

    /// Unstages staged (HEAD-vs-index) hunks covered by `index_ranges` (in the
    /// index text buffer's coordinates), acting on the given staged diff,
    /// invoked from the staged-changes view. The acted-on hunks already carry
    /// an index range, so no projection is needed; optimistically suppresses
    /// them from the staged diff.
    pub fn unstage_staged_hunks(
        &mut self,
        staged_diff: Entity<BufferDiff>,
        index_ranges: Vec<Range<Anchor>>,
        cx: &mut Context<Self>,
    ) -> Result<()> {
        if index_ranges.is_empty() {
            return Ok(());
        }
        let index_buffer_id = staged_diff.read(cx).buffer_id;
        let buffer_id = *self
            .buffer_ids_by_index_text_buffer_id
            .get(&index_buffer_id)
            .context("failed to find git state for index text buffer")?;
        let diff_state = self
            .diffs
            .get(&buffer_id)
            .cloned()
            .context("failed to find git state for buffer")?;
        let index_buffer = diff_state
            .read(cx)
            .index_text_buffer()
            .context("index text is not loaded")?;
        let index_snapshot = index_buffer.read(cx).text_snapshot();
        let staged_snapshot = staged_diff.read(cx).snapshot(cx);

        let mut hunks = Vec::new();
        for range in &index_ranges {
            hunks.extend(staged_snapshot.hunks_intersecting_range(range.clone(), &index_snapshot));
        }
        hunks.sort_by_key(|hunk| hunk.buffer_range.start.to_offset(&index_snapshot));
        hunks.dedup_by(|a, b| a.buffer_range.start == b.buffer_range.start);

        let index_edits = staged_diff
            .read(cx)
            .unstage_staged_hunks(&hunks, &index_snapshot);
        let version = index_snapshot.version().clone();
        let pending = pending_hunks(&hunks, &version, PendingSense::Suppress);
        drop(staged_snapshot);

        diff_state.update(cx, |diff_state, _| {
            diff_state.insert_pending_index_edits(index_edits)
        });
        staged_diff.update(cx, |diff, cx| {
            diff.set_pending_hunks(&pending, &index_snapshot, cx);
        });

        self.write_optimistic_index(buffer_id, cx);
        Ok(())
    }

    /// Derives the desired index text from the buffer's optimistic patch and
    /// schedules the write.
    fn write_optimistic_index(&mut self, buffer_id: BufferId, cx: &mut Context<Self>) {
        let Some(diff_state) = self.diffs.get(&buffer_id) else {
            return;
        };
        let new_index_text = diff_state
            .read(cx)
            .pending_index_text(cx)
            .map(|rope| rope.to_string());
        self.write_index_text_for_buffer_id(buffer_id, new_index_text, cx);
    }

    pub fn open_diff_since(
        &mut self,
        oid: Option<git::Oid>,
        buffer: Entity<Buffer>,
        repo: Entity<Repository>,
        cx: &mut Context<Self>,
    ) -> Task<Result<Entity<BufferDiff>>> {
        let buffer_id = buffer.read(cx).remote_id();

        if let Some(diff_state) = self.diffs.get(&buffer_id)
            && let Some(oid_diff) = diff_state.read(cx).oid_diff(oid)
        {
            if let Some(task) =
                diff_state.update(cx, |diff_state, _| diff_state.wait_for_recalculation())
            {
                return cx.background_executor().spawn(async move {
                    task.await;
                    Ok(oid_diff)
                });
            }
            return Task::ready(Ok(oid_diff));
        }

        let diff_kind = DiffKind::SinceOid(oid);
        if let Some(task) = self.loading_diffs.get(&(buffer_id, diff_kind)) {
            let task = task.clone();
            return cx.background_spawn(async move { task.await.map_err(|e| anyhow!("{e}")) });
        }

        let task = cx
            .spawn(async move |this, cx| {
                let result: Result<Entity<BufferDiff>> = async {
                    let buffer_snapshot = buffer.update(cx, |buffer, _| buffer.snapshot());
                    let language_registry =
                        buffer.update(cx, |buffer, _| buffer.language_registry());
                    let content: Option<Arc<str>> = match oid {
                        None => None,
                        Some(oid) => Some({
                            let mut content = repo
                                .update(cx, |repo, cx| repo.load_blob_content(oid, cx))
                                .await?;
                            text::LineEnding::normalize(&mut content);
                            content.into()
                        }),
                    };
                    let buffer_diff = cx.new(|cx| {
                        BufferDiff::new(
                            &buffer_snapshot,
                            buffer_snapshot.language().cloned(),
                            language_registry,
                            cx,
                        )
                    });

                    buffer_diff
                        .update(cx, |buffer_diff, cx| {
                            buffer_diff.set_base_text(content.clone(), buffer_snapshot.text, cx)
                        })
                        .await;
                    let unstaged_diff = this
                        .update(cx, |this, cx| this.open_unstaged_diff(buffer.clone(), cx))?
                        .await?;
                    buffer_diff.update(cx, |buffer_diff, _| {
                        buffer_diff.set_secondary_diff(unstaged_diff);
                    });

                    this.update(cx, |this, cx| {
                        this.loading_diffs.remove(&(buffer_id, diff_kind));

                        let git_store = cx.weak_entity();
                        let diff_state = this
                            .diffs
                            .entry(buffer_id)
                            .or_insert_with(|| cx.new(|cx| BufferGitState::new(git_store, cx)));

                        diff_state.update(cx, |state, _| {
                            if let Some(oid) = oid {
                                if let Some(content) = content {
                                    state.oid_texts.insert(oid, content);
                                }
                            }
                            state.oid_diffs.insert(oid, buffer_diff.downgrade());
                        });
                    })?;

                    Ok(buffer_diff)
                }
                .await;
                result.map_err(Arc::new)
            })
            .shared();

        self.loading_diffs
            .insert((buffer_id, diff_kind), task.clone());
        cx.background_spawn(async move { task.await.map_err(|e| anyhow!("{e}")) })
    }

    /// Opens the diff that editors display in the gutter: the uncommitted diff
    /// when the diff base is HEAD, otherwise a diff against the base blob.
    pub fn open_display_diff(
        &mut self,
        buffer: Entity<Buffer>,
        cx: &mut Context<Self>,
    ) -> Task<Result<Entity<BufferDiff>>> {
        let mut branch_base = None;
        if self.diff_base == GitDiffBaseSetting::DefaultBranch
            && let Some((repo, repo_path)) =
                self.repository_and_path_for_buffer_id(buffer.read(cx).remote_id(), cx)
            && let Some((buffers, _)) = self
                .display_diffs
                .get(&repo.read(cx).id)
                .and_then(|state| state.buffers.as_ref())
        {
            let buffers = buffers.read(cx);
            // The inner `None` means the file was added since the base and
            // diffs against empty content.
            let base_oid = buffers.base_oid_for_path(&repo_path).or_else(|| {
                buffers
                    .status_for_path(&repo_path, cx)
                    .is_some_and(FileStatus::is_created)
                    .then_some(None)
            });
            if let Some(oid) = base_oid {
                branch_base = Some((repo, oid));
            }
        }
        match branch_base {
            Some((repo, oid)) => self.open_diff_since(oid, buffer, repo, cx),
            None => self.open_uncommitted_diff(buffer, cx),
        }
    }

    #[ztracing::instrument(skip_all)]
    pub fn open_uncommitted_diff(
        &mut self,
        buffer: Entity<Buffer>,
        cx: &mut Context<Self>,
    ) -> Task<Result<Entity<BufferDiff>>> {
        let buffer_id = buffer.read(cx).remote_id();

        if let Some(diff_state) = self.diffs.get(&buffer_id)
            && let Some(uncommitted_diff) = diff_state
                .read(cx)
                .uncommitted_diff
                .as_ref()
                .and_then(|weak| weak.upgrade())
        {
            if let Some(task) =
                diff_state.update(cx, |diff_state, _| diff_state.wait_for_recalculation())
            {
                return cx.background_executor().spawn(async move {
                    task.await;
                    Ok(uncommitted_diff)
                });
            }
            return Task::ready(Ok(uncommitted_diff));
        }

        let Some((repo, repo_path)) =
            self.repository_and_path_for_buffer_id(buffer.read(cx).remote_id(), cx)
        else {
            return Task::ready(Err(anyhow!("failed to find git repository for buffer")));
        };

        let is_symlink = Self::buffer_is_symlink(&buffer, cx);
        let task = self
            .loading_diffs
            .entry((buffer_id, DiffKind::Uncommitted))
            .or_insert_with(|| {
                let changes = if is_symlink {
                    Task::ready(Ok(DiffBasesChange::SetBoth(None)))
                } else {
                    repo.update(cx, |repo, cx| {
                        repo.load_committed_text(buffer_id, repo_path, cx)
                    })
                };

                // todo(lw): hot foreground spawn
                cx.spawn(async move |this, cx| {
                    Self::open_diff_internal(this, DiffKind::Uncommitted, changes.await, buffer, cx)
                        .await
                        .map_err(Arc::new)
                })
                .shared()
            })
            .clone();

        cx.background_spawn(async move { task.await.map_err(|e| anyhow!("{e}")) })
    }

    #[ztracing::instrument(skip_all)]
    async fn open_diff_internal(
        this: WeakEntity<Self>,
        kind: DiffKind,
        texts: Result<DiffBasesChange>,
        buffer_entity: Entity<Buffer>,
        cx: &mut AsyncApp,
    ) -> Result<Entity<BufferDiff>> {
        let diff_bases_change = match texts {
            Err(e) => {
                this.update(cx, |this, cx| {
                    let buffer = buffer_entity.read(cx);
                    let buffer_id = buffer.remote_id();
                    this.loading_diffs.remove(&(buffer_id, kind));
                })?;
                return Err(e);
            }
            Ok(change) => change,
        };

        this.update(cx, |this, cx| {
            let buffer = buffer_entity.read(cx);
            let buffer_id = buffer.remote_id();
            let language = buffer.language().cloned();
            let language_registry = buffer.language_registry();
            let index_text_file = buffer.file().map(|file| {
                Arc::new(IndexTextFile::new(file.as_ref(), cx)) as Arc<dyn language::File>
            });
            let text_snapshot = buffer.text_snapshot();
            this.loading_diffs.remove(&(buffer_id, kind));

            let git_store = cx.weak_entity();
            let project = this.project.clone();
            let diff_state = this
                .diffs
                .entry(buffer_id)
                .or_insert_with(|| cx.new(|cx| BufferGitState::new(git_store.clone(), cx)));

            let existing_unstaged_diff = diff_state.read(cx).unstaged_diff();

            let mut staged_index_text_buffer = None;
            let diff = if kind == DiffKind::Unstaged
                && let Some(existing_unstaged_diff) = existing_unstaged_diff.clone()
            {
                existing_unstaged_diff
            } else {
                let diff = match kind {
                    DiffKind::Unstaged => {
                        let base_text_buffer = diff_state.update(cx, |diff_state, cx| {
                            diff_state.get_or_create_index_text_buffer(index_text_file.clone(), cx)
                        });
                        cx.new(|cx| {
                            let mut diff = BufferDiff::new_with_base_text_buffer(
                                &text_snapshot,
                                base_text_buffer,
                                cx,
                            );
                            if let Some(project) = project.clone() {
                                diff.set_operations(Arc::new(GitDiffOperations { project, kind }));
                            }
                            diff
                        })
                    }
                    DiffKind::Staged => {
                        let (index_text_buffer, base_text_buffer) =
                            diff_state.update(cx, |diff_state, cx| {
                                (
                                    diff_state.get_or_create_index_text_buffer(
                                        index_text_file.clone(),
                                        cx,
                                    ),
                                    diff_state.get_or_create_head_text_buffer(cx),
                                )
                            });
                        index_text_buffer.update(cx, |index_text_buffer, cx| {
                            if let Some(language_registry) = language_registry.clone() {
                                index_text_buffer.set_language_registry(language_registry);
                            }
                            index_text_buffer.set_language_async(language.clone(), cx);
                        });
                        let index_text_snapshot = index_text_buffer.read(cx).text_snapshot();
                        staged_index_text_buffer = Some(index_text_buffer);
                        cx.new(|cx| {
                            let mut diff = BufferDiff::new_with_base_text_buffer(
                                &index_text_snapshot,
                                base_text_buffer,
                                cx,
                            );
                            if let Some(project) = project.clone() {
                                diff.set_operations(Arc::new(GitDiffOperations { project, kind }));
                            }
                            diff
                        })
                    }
                    DiffKind::Uncommitted => {
                        let base_text_buffer = diff_state.update(cx, |diff_state, cx| {
                            diff_state.get_or_create_head_text_buffer(cx)
                        });
                        cx.new(|cx| {
                            let mut diff = BufferDiff::new_with_base_text_buffer(
                                &text_snapshot,
                                base_text_buffer,
                                cx,
                            );
                            if let Some(project) = project.clone() {
                                diff.set_operations(Arc::new(GitDiffOperations { project, kind }));
                            }
                            diff
                        })
                    }
                    DiffKind::SinceOid(_) => {
                        unreachable!("open_diff_internal is not used for OID diffs")
                    }
                };
                diff
            };
            let rx = diff_state.update(cx, |diff_state, cx| {
                diff_state.language = language;
                diff_state.language_registry = language_registry;

                match kind {
                    DiffKind::Unstaged => {
                        diff_state.unstaged_diff = Some(diff.downgrade());
                        // The deleted (base) side of a standalone unstaged diff
                        // is the index, so highlight it. The recalc kicked off by
                        // `diff_bases_changed` below applies the language.
                        diff_state.index_text_buffer_language_enabled = true;
                    }
                    DiffKind::Staged => {
                        diff_state.index_text_buffer_language_enabled = true;
                        let index_text_buffer = staged_index_text_buffer
                            .take()
                            .context("index text buffer was not created for staged diff")?;
                        diff_state.staged_diff = Some((diff.downgrade(), index_text_buffer));
                    }
                    DiffKind::Uncommitted => {
                        let unstaged_diff = if let Some(diff) = existing_unstaged_diff {
                            diff
                        } else {
                            let base_text_buffer =
                                diff_state.get_or_create_index_text_buffer(index_text_file, cx);
                            let unstaged_diff = cx.new(|cx| {
                                let mut diff = BufferDiff::new_with_base_text_buffer(
                                    &text_snapshot,
                                    base_text_buffer,
                                    cx,
                                );
                                if let Some(project) = project.clone() {
                                    diff.set_operations(Arc::new(GitDiffOperations {
                                        project,
                                        kind: DiffKind::Unstaged,
                                    }));
                                }
                                diff
                            });
                            diff_state.unstaged_diff = Some(unstaged_diff.downgrade());
                            unstaged_diff
                        };

                        diff.update(cx, |diff, _| diff.set_secondary_diff(unstaged_diff));
                        diff_state.uncommitted_diff = Some(diff.downgrade())
                    }
                    DiffKind::SinceOid(_) => {
                        unreachable!("open_diff_internal is not used for OID diffs")
                    }
                }

                diff_state.diff_bases_changed(text_snapshot, Some(diff_bases_change), cx);
                let rx = diff_state.wait_for_recalculation();

                anyhow::Ok(async move {
                    if let Some(rx) = rx {
                        rx.await;
                    }
                    Ok(diff)
                })
            });
            let diff_state = this.diffs.get(&buffer_id).cloned();
            if let Some(index_text_buffer) =
                diff_state.and_then(|diff_state| diff_state.read(cx).index_text_buffer())
            {
                this.buffer_ids_by_index_text_buffer_id
                    .insert(index_text_buffer.read(cx).remote_id(), buffer_id);
            }
            rx
        })??
        .await
    }

    pub fn get_unstaged_diff(&self, buffer_id: BufferId, cx: &App) -> Option<Entity<BufferDiff>> {
        let diff_state = self.diffs.get(&buffer_id)?;
        diff_state.read(cx).unstaged_diff.as_ref()?.upgrade()
    }

    pub fn get_staged_diff(&self, buffer_id: BufferId, cx: &App) -> Option<Entity<BufferDiff>> {
        let diff_state = self.diffs.get(&buffer_id)?;
        diff_state.read(cx).staged_diff()
    }

    pub fn get_uncommitted_diff(
        &self,
        buffer_id: BufferId,
        cx: &App,
    ) -> Option<Entity<BufferDiff>> {
        let diff_state = self.diffs.get(&buffer_id)?;
        diff_state.read(cx).uncommitted_diff.as_ref()?.upgrade()
    }

    pub fn get_diff_since_oid(
        &self,
        buffer_id: BufferId,
        oid: Option<git::Oid>,
        cx: &App,
    ) -> Option<Entity<BufferDiff>> {
        let diff_state = self.diffs.get(&buffer_id)?;
        diff_state.read(cx).oid_diff(oid)
    }

    /// Whether this buffer's index text is known to match its committed text
    /// without comparing contents, i.e. whether the texts share one allocation.
    /// In a downstream project, this can only be true if the upstream sent
    /// `Mode::IndexMatchesHead`.
    #[cfg(any(test, feature = "test-support"))]
    pub fn index_matches_head_for_buffer(&self, buffer_id: BufferId, cx: &App) -> bool {
        self.diffs
            .get(&buffer_id)
            .is_some_and(|diff_state| diff_state.read(cx).index_matches_head())
    }

    pub fn open_conflict_set(
        &mut self,
        buffer: Entity<Buffer>,
        cx: &mut Context<Self>,
    ) -> Task<Entity<ConflictSet>> {
        let buffer_id = buffer.read(cx).remote_id();

        if let Some(git_state) = self.diffs.get(&buffer_id)
            && let Some(conflict_set) = git_state
                .read(cx)
                .conflict_set
                .as_ref()
                .and_then(|weak| weak.upgrade())
        {
            let conflict_set = conflict_set;
            let buffer_snapshot = buffer.read(cx).text_snapshot();

            let rx = git_state.update(cx, |state, cx| {
                state.reparse_conflict_markers(buffer_snapshot, cx)
            });

            return cx.spawn(async move |_, _| {
                rx.await.ok();
                conflict_set
            });
        }

        let is_unmerged = self
            .repository_and_path_for_buffer_id(buffer_id, cx)
            .is_some_and(|(repo, path)| repo.read(cx).snapshot.has_conflict(&path));
        let git_store = cx.weak_entity();
        let buffer_git_state = self
            .diffs
            .entry(buffer_id)
            .or_insert_with(|| cx.new(|cx| BufferGitState::new(git_store, cx)));
        let conflict_set = cx.new(|cx| ConflictSet::new(buffer_id, is_unmerged, cx));

        self._subscriptions
            .push(cx.subscribe(&conflict_set, |_, _, _, cx| {
                cx.emit(GitStoreEvent::ConflictsUpdated);
            }));

        let rx = buffer_git_state.update(cx, |state, cx| {
            state.conflict_set = Some(conflict_set.downgrade());
            let buffer_snapshot = buffer.read(cx).text_snapshot();
            state.reparse_conflict_markers(buffer_snapshot, cx)
        });

        cx.spawn(async move |_, _| {
            rx.await.ok();
            conflict_set
        })
    }

    pub fn project_path_git_status(
        &self,
        project_path: &ProjectPath,
        cx: &App,
    ) -> Option<FileStatus> {
        let (repo, repo_path) = self.repository_and_path_for_project_path(project_path, cx)?;
        Some(repo.read(cx).status_for_path(&repo_path)?.status)
    }

    pub fn checkpoint(&self, cx: &mut App) -> Task<Result<GitStoreCheckpoint>> {
        let mut work_directory_abs_paths = Vec::new();
        let mut checkpoints = Vec::new();
        for repository in self.repositories.values() {
            repository.update(cx, |repository, _| {
                work_directory_abs_paths.push(repository.snapshot.work_directory_abs_path.clone());
                checkpoints.push(repository.checkpoint().map(|checkpoint| checkpoint?));
            });
        }

        cx.background_executor().spawn(async move {
            let checkpoints = future::try_join_all(checkpoints).await?;
            Ok(GitStoreCheckpoint {
                checkpoints_by_work_dir_abs_path: work_directory_abs_paths
                    .into_iter()
                    .zip(checkpoints)
                    .collect(),
            })
        })
    }

    pub fn restore_checkpoint(
        &self,
        checkpoint: GitStoreCheckpoint,
        cx: &mut App,
    ) -> Task<Result<()>> {
        let repositories_by_work_dir_abs_path = self
            .repositories
            .values()
            .map(|repo| (repo.read(cx).snapshot.work_directory_abs_path.clone(), repo))
            .collect::<HashMap<_, _>>();

        let mut tasks = Vec::new();
        for (work_dir_abs_path, checkpoint) in checkpoint.checkpoints_by_work_dir_abs_path {
            if let Some(repository) = repositories_by_work_dir_abs_path.get(&work_dir_abs_path) {
                let restore = repository.update(cx, |repository, _| {
                    repository.restore_checkpoint(checkpoint)
                });
                tasks.push(async move { restore.await? });
            }
        }
        cx.background_spawn(async move {
            future::try_join_all(tasks).await?;
            Ok(())
        })
    }

    /// Compares two checkpoints, returning true if they are equal.
    pub fn compare_checkpoints(
        &self,
        left: GitStoreCheckpoint,
        mut right: GitStoreCheckpoint,
        cx: &mut App,
    ) -> Task<Result<bool>> {
        let repositories_by_work_dir_abs_path = self
            .repositories
            .values()
            .map(|repo| (repo.read(cx).snapshot.work_directory_abs_path.clone(), repo))
            .collect::<HashMap<_, _>>();

        let mut tasks = Vec::new();
        for (work_dir_abs_path, left_checkpoint) in left.checkpoints_by_work_dir_abs_path {
            if let Some(right_checkpoint) = right
                .checkpoints_by_work_dir_abs_path
                .remove(&work_dir_abs_path)
            {
                if let Some(repository) = repositories_by_work_dir_abs_path.get(&work_dir_abs_path)
                {
                    let compare = repository.update(cx, |repository, _| {
                        repository.compare_checkpoints(left_checkpoint, right_checkpoint)
                    });

                    tasks.push(async move { compare.await? });
                }
            } else {
                return Task::ready(Ok(false));
            }
        }
        cx.background_spawn(async move {
            Ok(future::try_join_all(tasks)
                .await?
                .into_iter()
                .all(|result| result))
        })
    }

    /// Blames a buffer.
    pub fn blame_buffer(
        &self,
        buffer: &Entity<Buffer>,
        version: Option<clock::Global>,
        cx: &mut Context<Self>,
    ) -> Task<Result<Option<Blame>>> {
        let buffer = buffer.read(cx);
        let Some((repo, repo_path)) =
            self.repository_and_path_for_buffer_id(buffer.remote_id(), cx)
        else {
            return Task::ready(Err(anyhow!("failed to find a git repository for buffer")));
        };
        let content = match &version {
            Some(version) => buffer.rope_for_version(version),
            None => buffer.as_rope().clone(),
        };
        let line_ending = buffer.line_ending();
        let _version = version.unwrap_or(buffer.version());
        let _buffer_id = buffer.remote_id();

        let repo = repo.downgrade();
        cx.spawn(async move |_, cx| {
            let repository_state = repo
                .update(cx, |repo, _| repo.repository_state.clone())?
                .await
                .map_err(|err| anyhow::anyhow!(err))?;
            match repository_state {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => backend
                    .blame(repo_path.clone(), content, line_ending)
                    .await
                    .with_context(|| format!("Failed to blame {:?}", repo_path.as_ref()))
                    .map(Some),
            }
        })
    }

    pub fn get_permalink_to_line(
        &self,
        buffer: &Entity<Buffer>,
        selection: Range<u32>,
        cx: &mut App,
    ) -> Task<Result<url::Url>> {
        let Some(file) = File::from_dyn(buffer.read(cx).file()) else {
            return Task::ready(Err(anyhow!("buffer has no file")));
        };

        let project_path = ProjectPath {
            worktree_id: file.worktree.read(cx).id(),
            path: file.path.clone(),
        };

        if let Some((repo, repo_path)) =
            self.repository_and_path_for_project_path(&project_path, cx)
        {
            return self.build_permalink(
                repo,
                repo_path,
                PermalinkTarget::Buffer {
                    buffer_id: buffer.read(cx).remote_id(),
                    selection,
                },
                cx,
            );
        }

        // If we're not in a Git repo, check whether this is a Rust source
        // file in the Cargo registry (presumably opened with go-to-definition
        // from a normal Rust file). If so, we can put together a permalink
        // using crate metadata.
        if buffer
            .read(cx)
            .language()
            .is_none_or(|lang| lang.name() != "Rust")
        {
            return Task::ready(Err(anyhow!("no permalink available")));
        }
        let file_path = file.worktree.read(cx).absolutize(&file.path);
        cx.spawn(async move |cx| {
            let provider_registry = cx.update(GitHostingProviderRegistry::default_global);
            get_permalink_in_rust_registry_src(provider_registry, file_path, selection)
                .context("no permalink available")
        })
    }

    pub fn get_file_permalink(
        &self,
        project_path: &ProjectPath,
        cx: &mut App,
    ) -> Task<Result<url::Url>> {
        let Some((repo, repo_path)) = self.repository_and_path_for_project_path(project_path, cx)
        else {
            return Task::ready(Err(anyhow!("no permalink available")));
        };
        self.build_permalink(
            repo,
            repo_path,
            PermalinkTarget::File(project_path.clone()),
            cx,
        )
    }

    fn build_permalink(
        &self,
        repo: Entity<Repository>,
        repo_path: RepoPath,
        target: PermalinkTarget,
        cx: &mut App,
    ) -> Task<Result<url::Url>> {
        let branch = repo.read(cx).branch.clone();
        let remote = branch
            .as_ref()
            .and_then(|b| b.upstream.as_ref())
            .and_then(|b| b.remote_name())
            .unwrap_or("origin")
            .to_string();

        let rx = repo.update(cx, |repo, _| {
            repo.send_job("get_permalink", None, move |state, cx| async move {
                match state {
                    RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                        let origin_url = backend
                            .remote_url(&remote)
                            .await
                            .with_context(|| format!("remote \"{remote}\" not found"))?;

                        let sha = backend.head_sha().await.context("reading HEAD SHA")?;

                        let provider_registry =
                            cx.update(GitHostingProviderRegistry::default_global);

                        let (provider, remote) =
                            parse_git_remote_url(provider_registry, &origin_url)
                                .context("parsing Git remote URL")?;

                        Ok(provider.build_permalink(
                            remote,
                            BuildPermalinkParams::new(&sha, &repo_path, target.selection()),
                        ))
                    }
                }
            })
        });
        cx.spawn(|_: &mut AsyncApp| async move { rx.await? })
    }

    fn on_worktree_store_event(
        &mut self,
        worktree_store: Entity<WorktreeStore>,
        event: &WorktreeStoreEvent,
        cx: &mut Context<Self>,
    ) {
        let GitStoreState::Local {
            project_environment,
            fs,
            ..
        } = &self.state
        else {
            return;
        };

        match event {
            WorktreeStoreEvent::WorktreeUpdatedEntries(worktree_id, updated_entries) => {
                if let Some(worktree) = self
                    .worktree_store
                    .read(cx)
                    .worktree_for_id(*worktree_id, cx)
                {
                    let paths_by_git_repo =
                        self.process_updated_entries(&worktree, updated_entries, cx);
                    cx.spawn(async move |_, cx| {
                        let paths_by_git_repo = paths_by_git_repo.await;
                        for (repo, paths) in paths_by_git_repo {
                            repo.update(cx, |repo, cx| {
                                repo.paths_changed(paths, cx);
                            });
                        }
                    })
                    .detach();
                }
            }
            WorktreeStoreEvent::WorktreeUpdatedGitRepositories(worktree_id, changed_repos) => {
                let Some(worktree) = worktree_store.read(cx).worktree_for_id(*worktree_id, cx)
                else {
                    return;
                };
                log::debug!("received worktree update for repositories: {changed_repos:?}");
                self.update_repositories_from_worktree(
                    *worktree_id,
                    project_environment.clone(),
                    changed_repos.clone(),
                    fs.clone(),
                    cx,
                );
                self.local_worktree_git_repos_changed(worktree, changed_repos, cx);
            }
            WorktreeStoreEvent::WorktreeRemoved(_entity_id, worktree_id) => {
                self.parked_repositories
                    .retain(|parked| parked.worktree_id != *worktree_id);
                let repos_without_worktree: Vec<RepositoryId> = self
                    .worktree_ids
                    .iter_mut()
                    .filter_map(|(repo_id, worktree_ids)| {
                        worktree_ids.remove(worktree_id);
                        if worktree_ids.is_empty() {
                            Some(*repo_id)
                        } else {
                            None
                        }
                    })
                    .collect();
                let is_active_repo_removed = repos_without_worktree
                    .iter()
                    .any(|repo_id| self.active_repo_id == Some(*repo_id));

                for repo_id in repos_without_worktree {
                    self.display_diffs.remove(&repo_id);
                    self.repositories.remove(&repo_id);
                    self.worktree_ids.remove(&repo_id);
                }

                if is_active_repo_removed {
                    if let Some((&repo_id, _)) = self.repositories.iter().next() {
                        self.active_repo_id = Some(repo_id);
                        cx.emit(GitStoreEvent::ActiveRepositoryChanged(Some(repo_id)));
                    } else {
                        self.active_repo_id = None;
                        cx.emit(GitStoreEvent::ActiveRepositoryChanged(None));
                    }
                }
            }
            _ => {}
        }
    }
    fn on_repository_event(
        &mut self,
        repo: Entity<Repository>,
        event: &RepositoryEvent,
        cx: &mut Context<Self>,
    ) {
        let id = repo.read(cx).id;
        let repo_snapshot = repo.read(cx).snapshot.clone();
        for (buffer_id, diff) in self.diffs.iter() {
            if let Some((buffer_repo, repo_path)) =
                self.repository_and_path_for_buffer_id(*buffer_id, cx)
                && buffer_repo == repo
            {
                diff.update(cx, |diff, cx| {
                    if let Some(conflict_set) = &diff.conflict_set {
                        let conflict_status_changed =
                            conflict_set.update(cx, |conflict_set, cx| {
                                let has_conflict = repo_snapshot.has_conflict(&repo_path);
                                conflict_set.set_has_conflict(has_conflict, cx)
                            })?;
                        if conflict_status_changed {
                            let buffer_store = self.buffer_store.read(cx);
                            if let Some(buffer) = buffer_store.get(*buffer_id) {
                                let _ = diff
                                    .reparse_conflict_markers(buffer.read(cx).text_snapshot(), cx);
                            }
                        }
                    }
                    anyhow::Ok(())
                })
                .ok();
            }
        }

        if matches!(event, RepositoryEvent::BranchListChanged) {
            self.refresh_diff_base_for_repo(id, cx);
        }

        cx.emit(GitStoreEvent::RepositoryUpdated(
            id,
            event.clone(),
            self.active_repo_id == Some(id),
        ))
    }

    fn on_jobs_updated(&mut self, _: Entity<Repository>, _: &JobsUpdated, cx: &mut Context<Self>) {
        cx.emit(GitStoreEvent::JobsUpdated)
    }

    fn repository_is_trusted(&self, repository_id: RepositoryId, cx: &mut Context<Self>) -> bool {
        let Some(worktree_ids) = self.worktree_ids.get(&repository_id) else {
            return false;
        };
        let Some(trusted_worktrees) = TrustedWorktrees::try_get_global(cx) else {
            return false;
        };

        worktree_ids.iter().any(|worktree_id| {
            trusted_worktrees.update(cx, |trusted_worktrees, cx| {
                trusted_worktrees.can_trust(&self.worktree_store, *worktree_id, cx)
            })
        })
    }

    /// Update our list of repositories and schedule git scans in response to a notification from a worktree,
    fn update_repositories_from_worktree(
        &mut self,
        worktree_id: WorktreeId,
        project_environment: Entity<ProjectEnvironment>,
        updated_git_repositories: UpdatedGitRepositoriesSet,
        fs: Arc<dyn Fs>,
        cx: &mut Context<Self>,
    ) {
        let mut removed_ids = Vec::new();

        let is_trusted = TrustedWorktrees::try_get_global(cx)
            .map(|trusted_worktrees| {
                trusted_worktrees.update(cx, |trusted_worktrees, cx| {
                    trusted_worktrees.can_trust(&self.worktree_store, worktree_id, cx)
                })
            })
            .unwrap_or(false);

        let mut sorted_updates = updated_git_repositories.iter().collect::<Vec<_>>();
        sorted_updates.sort_by_key(|update| {
            update
                .new_work_directory_abs_path
                .as_ref()
                .map(|work_directory_abs_path| work_directory_abs_path.components().count())
        });
        for update in sorted_updates {
            self.retarget_parked_repositories(update);

            if let Some((repo_id, existing)) = self
                .repositories
                .iter()
                .find(|(_, repo)| {
                    let existing_work_directory_abs_path = &repo.read(cx).work_directory_abs_path;
                    Some(existing_work_directory_abs_path)
                        == update.old_work_directory_abs_path.as_ref()
                        || Some(existing_work_directory_abs_path)
                            == update.new_work_directory_abs_path.as_ref()
                })
                .map(|(id, repo)| (*id, repo.clone()))
            {
                if let Some(new_work_directory_abs_path) =
                    update.new_work_directory_abs_path.clone()
                {
                    let mut merged_worktree_ids = self.take_parked_at(&new_work_directory_abs_path);
                    merged_worktree_ids.insert(worktree_id);
                    self.worktree_ids
                        .entry(repo_id)
                        .or_default()
                        .extend(merged_worktree_ids);
                    let path_changed = update.old_work_directory_abs_path.as_ref()
                        != update.new_work_directory_abs_path.as_ref();
                    if path_changed
                        && let Some(dot_git_abs_path) = update.dot_git_abs_path.clone()
                        && let Some(repository_dir_abs_path) =
                            update.repository_dir_abs_path.clone()
                        && let Some(common_dir_abs_path) = update.common_dir_abs_path.clone()
                    {
                        existing.update(cx, |existing, cx| {
                            existing.reinitialize_local_backend(
                                new_work_directory_abs_path,
                                dot_git_abs_path,
                                repository_dir_abs_path,
                                common_dir_abs_path,
                                project_environment.downgrade(),
                                fs.clone(),
                                is_trusted,
                                cx,
                            );
                            existing.schedule_scan(cx);
                        });
                    } else {
                        existing.update(cx, |existing, cx| {
                            existing.snapshot.work_directory_abs_path = new_work_directory_abs_path;
                            existing.schedule_scan(cx);
                        });
                    }
                } else {
                    if let Some(worktree_ids) = self.worktree_ids.get_mut(&repo_id) {
                        worktree_ids.remove(&worktree_id);
                        if worktree_ids.is_empty() {
                            removed_ids.push(repo_id);
                        }
                    }
                }
            } else if let UpdatedGitRepository {
                new_work_directory_abs_path: Some(work_directory_abs_path),
                dot_git_abs_path: Some(dot_git_abs_path),
                repository_dir_abs_path: Some(repository_dir_abs_path),
                common_dir_abs_path: Some(common_dir_abs_path),
                ..
            } = update
            {
                let Some(worktree) = self
                    .worktree_store
                    .read(cx)
                    .worktree_for_id(worktree_id, cx)
                else {
                    continue;
                };
                let parked_repository = ParkedRepository {
                    worktree_id,
                    work_directory_abs_path: work_directory_abs_path.clone(),
                    dot_git_abs_path: dot_git_abs_path.clone(),
                    repository_dir_abs_path: repository_dir_abs_path.clone(),
                    common_dir_abs_path: common_dir_abs_path.clone(),
                };
                if self.should_park_repository(&parked_repository, &worktree, cx) {
                    self.parked_repositories.retain(|parked| {
                        parked.worktree_id != parked_repository.worktree_id
                            || parked.work_directory_abs_path
                                != parked_repository.work_directory_abs_path
                    });
                    self.parked_repositories.push(parked_repository);
                } else {
                    self.register_local_repository(parked_repository, cx);
                }
            }
        }

        for id in removed_ids {
            if self.active_repo_id == Some(id) {
                self.active_repo_id = None;
                cx.emit(GitStoreEvent::ActiveRepositoryChanged(None));
            }
            self.display_diffs.remove(&id);
            self.repositories.remove(&id);
        }
    }

    fn retarget_parked_repositories(&mut self, update: &UpdatedGitRepository) {
        match (
            &update.old_work_directory_abs_path,
            &update.new_work_directory_abs_path,
        ) {
            (Some(old_work_directory_abs_path), Some(new_work_directory_abs_path))
                if old_work_directory_abs_path != new_work_directory_abs_path =>
            {
                if let (
                    Some(dot_git_abs_path),
                    Some(repository_dir_abs_path),
                    Some(common_dir_abs_path),
                ) = (
                    &update.dot_git_abs_path,
                    &update.repository_dir_abs_path,
                    &update.common_dir_abs_path,
                ) {
                    for worktree_id in self.take_parked_at(old_work_directory_abs_path) {
                        let already_parked = self.parked_repositories.iter().any(|parked| {
                            parked.worktree_id == worktree_id
                                && parked.work_directory_abs_path == *new_work_directory_abs_path
                        });
                        if !already_parked {
                            self.parked_repositories.push(ParkedRepository {
                                worktree_id,
                                work_directory_abs_path: new_work_directory_abs_path.clone(),
                                dot_git_abs_path: dot_git_abs_path.clone(),
                                repository_dir_abs_path: repository_dir_abs_path.clone(),
                                common_dir_abs_path: common_dir_abs_path.clone(),
                            });
                        }
                    }
                } else {
                    self.parked_repositories.retain(|parked| {
                        &parked.work_directory_abs_path != old_work_directory_abs_path
                    });
                }
            }
            (Some(old_work_directory_abs_path), None) => {
                self.parked_repositories.retain(|parked| {
                    &parked.work_directory_abs_path != old_work_directory_abs_path
                });
            }
            _ => {}
        }
    }

    fn should_park_repository(
        &self,
        repository: &ParkedRepository,
        worktree: &Entity<Worktree>,
        cx: &App,
    ) -> bool {
        if !Self::parking_enabled(repository.worktree_id, cx) {
            return false;
        }
        let worktree = worktree.read(cx);
        if worktree.root_repo_common_dir().is_some() {
            return false;
        }
        let deep_enough = match repository
            .work_directory_abs_path
            .strip_prefix(worktree.abs_path())
        {
            Ok(relative_path) => relative_path.components().count() >= MIN_PARKED_REPOSITORY_DEPTH,
            Err(_) => false,
        };
        deep_enough
            && !self.repositories.values().any(|active_repository| {
                repository
                    .work_directory_abs_path
                    .starts_with(&active_repository.read(cx).work_directory_abs_path)
            })
            && !self.buffer_store.read(cx).buffers().any(|buffer| {
                buffer
                    .read(cx)
                    .project_path(cx)
                    .and_then(|project_path| {
                        self.worktree_store.read(cx).absolutize(&project_path, cx)
                    })
                    .is_some_and(|abs_path| {
                        abs_path.starts_with(&repository.work_directory_abs_path)
                    })
            })
    }

    fn parking_enabled(worktree_id: WorktreeId, cx: &App) -> bool {
        let settings_location = SettingsLocation {
            worktree_id,
            path: RelPath::empty(),
        };
        WorktreeSettings::get(Some(settings_location), cx)
            .file_scan_depth
            .is_some()
    }

    fn take_parked_at(&mut self, work_directory_abs_path: &Path) -> HashSet<WorktreeId> {
        let mut worktree_ids = HashSet::default();
        self.parked_repositories.retain(|parked| {
            if parked.work_directory_abs_path.as_ref() == work_directory_abs_path {
                worktree_ids.insert(parked.worktree_id);
                false
            } else {
                true
            }
        });
        worktree_ids
    }

    fn activate_parked_repositories(
        &mut self,
        mut should_activate: impl FnMut(&ParkedRepository) -> bool,
        cx: &mut Context<Self>,
    ) {
        if self.parked_repositories.is_empty() {
            return;
        }
        let (to_activate, to_keep) = mem::take(&mut self.parked_repositories)
            .into_iter()
            .partition::<Vec<_>, _>(|parked| should_activate(parked));
        self.parked_repositories = to_keep;
        for parked in to_activate {
            self.register_local_repository(parked, cx);
        }
    }

    fn activate_parked_repositories_where_parking_disabled(&mut self, cx: &mut Context<Self>) {
        let activatable_worktree_ids = self
            .parked_repositories
            .iter()
            .map(|parked| parked.worktree_id)
            .filter(|worktree_id| !Self::parking_enabled(*worktree_id, cx))
            .collect::<HashSet<_>>();
        if activatable_worktree_ids.is_empty() {
            return;
        }
        self.activate_parked_repositories(
            |parked| activatable_worktree_ids.contains(&parked.worktree_id),
            cx,
        );
    }

    fn register_local_repository(&mut self, repository: ParkedRepository, cx: &mut Context<Self>) {
        if !matches!(self.state, GitStoreState::Local { .. }) {
            return;
        }
        let mut worktree_ids = self.take_parked_at(&repository.work_directory_abs_path);
        worktree_ids.insert(repository.worktree_id);
        if let Some((&existing_id, _)) = self.repositories.iter().find(|(_, existing)| {
            existing.read(cx).work_directory_abs_path == repository.work_directory_abs_path
        }) {
            self.worktree_ids
                .entry(existing_id)
                .or_default()
                .extend(worktree_ids);
            self.activate_parked_repositories_under(&repository.work_directory_abs_path, cx);
            return;
        }
        let GitStoreState::Local {
            next_repository_id,
            project_environment,
            fs,
            ..
        } = &self.state
        else {
            return;
        };
        let project_environment = project_environment.downgrade();
        let fs = fs.clone();
        let is_trusted = TrustedWorktrees::try_get_global(cx)
            .map(|trusted_worktrees| {
                trusted_worktrees.update(cx, |trusted_worktrees, cx| {
                    trusted_worktrees.can_trust(&self.worktree_store, repository.worktree_id, cx)
                })
            })
            .unwrap_or(false);

        let id = RepositoryId(next_repository_id.fetch_add(1, atomic::Ordering::Release));
        let git_store = cx.weak_entity();
        let repo = cx.new(|cx| {
            let mut repo = Repository::local(
                id,
                repository.work_directory_abs_path.clone(),
                repository.repository_dir_abs_path.clone(),
                repository.common_dir_abs_path.clone(),
                repository.dot_git_abs_path.clone(),
                project_environment,
                fs,
                is_trusted,
                git_store,
                cx,
            );
            repo.schedule_scan(cx);
            repo
        });
        self._subscriptions
            .push(cx.subscribe(&repo, Self::on_repository_event));
        self._subscriptions
            .push(cx.subscribe(&repo, Self::on_jobs_updated));
        self.repositories.insert(id, repo);
        self.worktree_ids.insert(id, worktree_ids);
        cx.emit(GitStoreEvent::RepositoryAdded);
        self.refresh_diff_base_for_repo(id, cx);
        self.active_repo_id.get_or_insert_with(|| {
            cx.emit(GitStoreEvent::ActiveRepositoryChanged(Some(id)));
            id
        });
        self.activate_parked_repositories_under(&repository.work_directory_abs_path, cx);
    }

    fn activate_parked_repositories_under(
        &mut self,
        work_directory_abs_path: &Path,
        cx: &mut Context<Self>,
    ) {
        self.activate_parked_repositories(
            |parked| {
                parked
                    .work_directory_abs_path
                    .starts_with(work_directory_abs_path)
            },
            cx,
        );
    }

    #[cfg(feature = "test-support")]
    pub fn parked_repositories(&self) -> &[ParkedRepository] {
        &self.parked_repositories
    }

    #[cfg(feature = "test-support")]
    pub fn activate_all_parked_repositories(&mut self, cx: &mut Context<Self>) {
        self.activate_parked_repositories(|_| true, cx);
    }

    fn activate_parked_repositories_for_buffer(
        &mut self,
        buffer: &Entity<Buffer>,
        cx: &mut Context<Self>,
    ) {
        if self.parked_repositories.is_empty() {
            return;
        }
        if let Some(abs_path) = buffer
            .read(cx)
            .project_path(cx)
            .and_then(|project_path| self.worktree_store.read(cx).absolutize(&project_path, cx))
        {
            self.activate_parked_repositories(
                |parked| abs_path.starts_with(&parked.work_directory_abs_path),
                cx,
            );
        }
    }

    fn on_trusted_worktrees_event(
        &mut self,
        _: Entity<TrustedWorktreesStore>,
        event: &TrustedWorktreesEvent,
        cx: &mut Context<Self>,
    ) {
        if !matches!(self.state, GitStoreState::Local { .. }) {
            return;
        }

        let (is_trusted, event_paths) = match event {
            TrustedWorktreesEvent::Trusted(_, trusted_paths) => (true, trusted_paths),
            TrustedWorktreesEvent::Restricted(_, restricted_paths) => (false, restricted_paths),
        };

        for (repo_id, worktree_ids) in &self.worktree_ids {
            if worktree_ids
                .iter()
                .any(|worktree_id| event_paths.contains(&PathTrust::Worktree(*worktree_id)))
            {
                if let Some(repo) = self.repositories.get(repo_id) {
                    let repository_state = repo.read(cx).repository_state.clone();
                    cx.background_spawn(async move {
                        if let Ok(RepositoryState::Local(state)) = repository_state.await {
                            state.backend.set_trusted(is_trusted);
                        }
                    })
                    .detach();
                }
            }
        }
    }

    fn on_buffer_store_event(
        &mut self,
        _: Entity<BufferStore>,
        event: &BufferStoreEvent,
        cx: &mut Context<Self>,
    ) {
        match event {
            BufferStoreEvent::BufferAdded(buffer) => {
                self.activate_parked_repositories_for_buffer(buffer, cx);
                cx.subscribe(buffer, |this, buffer, event, cx| {
                    if let BufferEvent::LanguageChanged(_) = event {
                        let buffer_id = buffer.read(cx).remote_id();
                        if let Some(diff_state) = this.diffs.get(&buffer_id) {
                            diff_state.update(cx, |diff_state, cx| {
                                diff_state.buffer_language_changed(buffer, cx);
                            });
                        }
                    }
                })
                .detach();
            }
            BufferStoreEvent::BufferDropped(buffer_id) => {
                self.diffs.remove(buffer_id);
                self.buffer_ids_by_index_text_buffer_id
                    .retain(|_, main_buffer_id| main_buffer_id != buffer_id);
            }
            BufferStoreEvent::BufferChangedFilePath { buffer, .. } => {
                self.activate_parked_repositories_for_buffer(buffer, cx);
                // Whenever a buffer's file path changes, it's possible that the
                // new path is actually a path that is being tracked by a git
                // repository. In that case, we'll want to update the buffer's
                // `BufferDiffState`, in case it already has one.
                let buffer_id = buffer.read(cx).remote_id();
                let diff_state = self.diffs.get(&buffer_id);
                let repo = self.repository_and_path_for_buffer_id(buffer_id, cx);

                if let Some(diff_state) = diff_state
                    && let Some((repo, repo_path)) = repo
                {
                    let buffer = buffer.clone();
                    let diff_state = diff_state.clone();
                    let is_symlink = Self::buffer_is_symlink(&buffer, cx);

                    cx.spawn(async move |_git_store, cx| {
                        async {
                            let diff_bases_change = if is_symlink {
                                DiffBasesChange::SetBoth(None)
                            } else {
                                repo.update(cx, |repo, cx| {
                                    repo.load_committed_text(buffer_id, repo_path, cx)
                                })
                                .await?
                            };

                            diff_state.update(cx, |diff_state, cx| {
                                let buffer_snapshot = buffer.read(cx).text_snapshot();
                                diff_state.diff_bases_changed(
                                    buffer_snapshot,
                                    Some(diff_bases_change),
                                    cx,
                                );
                            });
                            anyhow::Ok(())
                        }
                        .await
                        .log_err();
                    })
                    .detach();
                }
            }
        }
    }

    pub fn recalculate_buffer_diffs(
        &mut self,
        buffers: Vec<Entity<Buffer>>,
        cx: &mut Context<Self>,
    ) -> impl Future<Output = ()> + use<> {
        let mut futures = Vec::new();
        for buffer in buffers {
            if let Some(diff_state) = self.diffs.get_mut(&buffer.read(cx).remote_id()) {
                let buffer = buffer.read(cx).text_snapshot();
                diff_state.update(cx, |diff_state, cx| {
                    diff_state.recalculate_diffs(buffer.clone(), cx);
                    futures.extend(diff_state.wait_for_recalculation().map(FutureExt::boxed));
                });
                futures.push(diff_state.update(cx, |diff_state, cx| {
                    diff_state
                        .reparse_conflict_markers(buffer, cx)
                        .map(|_| {})
                        .boxed()
                }));
            }
        }
        async move {
            futures::future::join_all(futures).await;
        }
    }

    fn write_index_text_for_buffer_id(
        &mut self,
        buffer_id: BufferId,
        new_index_text: Option<String>,
        cx: &mut Context<Self>,
    ) {
        let Some(diff_state) = self.diffs.get(&buffer_id) else {
            return;
        };
        let hunk_staging_operation_count = diff_state.update(cx, |diff_state, _| {
            diff_state.hunk_staging_operation_count += 1;
            diff_state.hunk_staging_operation_count
        });
        let Some((repo, path)) = self.repository_and_path_for_buffer_id(buffer_id, cx) else {
            return;
        };
        let (encoding, has_bom) = self
            .buffer_store
            .read(cx)
            .get(buffer_id)
            .map(|buffer| {
                let buffer = buffer.read(cx);
                (buffer.encoding(), buffer.has_bom())
            })
            .unwrap_or((encoding_rs::UTF_8, false));
        let recv = repo.update(cx, |repo, cx| {
            log::debug!("hunks changed for {}", path.as_unix_str());
            repo.spawn_set_index_text_job(
                path,
                new_index_text,
                encoding,
                has_bom,
                Some(hunk_staging_operation_count),
                cx,
            )
        });
        cx.spawn(async move |this, cx| {
            if let Ok(Err(error)) = cx.background_spawn(recv).await {
                this.update(cx, |this, cx| {
                    if let Some(diff_state) = this.diffs.get(&buffer_id).cloned() {
                        diff_state.update(cx, |diff_state, cx| {
                            diff_state.clear_pending_index_edits_and_hunks(cx);
                        });
                    }
                    cx.emit(GitStoreEvent::IndexWriteError(error));
                })
                .ok();
            }
        })
        .detach();
    }

    fn local_worktree_git_repos_changed(
        &mut self,
        worktree: Entity<Worktree>,
        changed_repos: &UpdatedGitRepositoriesSet,
        cx: &mut Context<Self>,
    ) {
        log::debug!("local worktree repos changed");
        debug_assert!(worktree.read(cx).is_local());

        for repository in self.repositories.values() {
            repository.update(cx, |repository, cx| {
                let repo_abs_path = &repository.work_directory_abs_path;
                if changed_repos.iter().any(|update| {
                    update.old_work_directory_abs_path.as_ref() == Some(repo_abs_path)
                        || update.new_work_directory_abs_path.as_ref() == Some(repo_abs_path)
                }) {
                    repository.reload_buffer_diff_bases(cx);
                }
            });
        }
    }

    pub fn repositories(&self) -> &HashMap<RepositoryId, Entity<Repository>> {
        &self.repositories
    }

    /// Returns the main repository working directory for the given worktree.
    /// For normal checkouts this equals the worktree's own path. For linked
    /// worktrees it points back to the main worktree, if one exists. Linked
    /// worktrees attached to a bare repository have no main worktree path.
    pub fn original_repo_path_for_worktree(
        &self,
        worktree_id: WorktreeId,
        cx: &App,
    ) -> Option<Arc<Path>> {
        self.active_repo_id
            .iter()
            .chain(self.worktree_ids.keys())
            .find(|repo_id| {
                self.worktree_ids
                    .get(repo_id)
                    .is_some_and(|ids| ids.contains(&worktree_id))
            })
            .and_then(|repo_id| self.repositories.get(repo_id))
            .and_then(|repo| {
                repo.read(cx)
                    .snapshot()
                    .main_worktree_abs_path()
                    .map(Arc::from)
            })
    }

    pub fn status_for_buffer_id(&self, buffer_id: BufferId, cx: &App) -> Option<FileStatus> {
        let (repo, path) = self.repository_and_path_for_buffer_id(buffer_id, cx)?;
        let status = repo.read(cx).snapshot.status_for_path(&path)?;
        Some(status.status)
    }

    pub fn repository_and_path_for_buffer_id(
        &self,
        buffer_id: BufferId,
        cx: &App,
    ) -> Option<(Entity<Repository>, RepoPath)> {
        let buffer = self.buffer_store.read(cx).get(buffer_id)?;
        let project_path = buffer.read(cx).project_path(cx)?;
        self.repository_and_path_for_project_path(&project_path, cx)
    }

    pub fn repository_and_path_for_project_path(
        &self,
        path: &ProjectPath,
        cx: &App,
    ) -> Option<(Entity<Repository>, RepoPath)> {
        let abs_path = self.worktree_store.read(cx).absolutize(path, cx)?;
        self.repositories
            .values()
            .filter_map(|repo| {
                let repo_path = repo.read(cx).abs_path_to_repo_path(&abs_path)?;
                Some((repo.clone(), repo_path))
            })
            .max_by_key(|(repo, _)| repo.read(cx).work_directory_abs_path.clone())
    }

    pub fn git_init(
        &self,
        path: Arc<Path>,
        fallback_branch_name: String,
        cx: &App,
    ) -> Task<Result<()>> {
        match &self.state {
            GitStoreState::Local { fs, .. } => {
                let fs = fs.clone();
                cx.background_executor()
                    .spawn(async move { fs.git_init(&path, fallback_branch_name).await })
            }
        }
    }

    pub fn git_clone(
        &self,
        repo: String,
        path: impl Into<Arc<std::path::Path>>,
        cx: &App,
    ) -> Task<Result<()>> {
        let path = path.into();
        match &self.state {
            GitStoreState::Local { fs, .. } => {
                let fs = fs.clone();
                cx.background_executor()
                    .spawn(async move { fs.git_clone(&path, &repo).await })
            }
        }
    }

    pub fn git_config(&self, path: Arc<Path>, args: Vec<String>, cx: &App) -> Task<Result<String>> {
        match &self.state {
            GitStoreState::Local { fs, .. } => {
                let fs = fs.clone();
                cx.background_executor()
                    .spawn(async move { fs.git_config(&path, args).await })
            }
        }
    }

    fn repository_for_request(
        this: &Entity<Self>,
        id: RepositoryId,
        cx: &mut AsyncApp,
    ) -> Result<Entity<Repository>> {
        this.read_with(cx, |this, _| {
            this.repositories
                .get(&id)
                .context("missing repository handle")
                .cloned()
        })
    }

    pub fn repo_snapshots(&self, cx: &App) -> HashMap<RepositoryId, RepositorySnapshot> {
        self.repositories
            .iter()
            .map(|(id, repo)| (*id, repo.read(cx).snapshot.clone()))
            .collect()
    }

    pub fn display_repo_snapshots(&self, cx: &App) -> HashMap<RepositoryId, RepositorySnapshot> {
        let mut snapshots = self.repo_snapshots(cx);
        if self.diff_base == GitDiffBaseSetting::Head {
            return snapshots;
        }
        for (id, snapshot) in &mut snapshots {
            if let Some(statuses) = self
                .display_diffs
                .get(id)
                .and_then(|state| state.buffers.as_ref())
                .and_then(|(buffers, _)| buffers.read(cx).statuses_by_path())
            {
                snapshot.statuses_by_path = statuses;
            }
        }
        snapshots
    }

    pub fn diff_base(&self) -> GitDiffBaseSetting {
        self.diff_base
    }

    fn set_diff_base(&mut self, setting: GitDiffBaseSetting, cx: &mut Context<Self>) {
        if self.diff_base == setting {
            return;
        }
        self.diff_base = setting;
        self.display_diffs.clear();
        if setting == GitDiffBaseSetting::DefaultBranch {
            let repo_ids = self.repositories.keys().copied().collect::<Vec<_>>();
            for repo_id in repo_ids {
                self.refresh_diff_base_for_repo(repo_id, cx);
            }
        }
        cx.emit(GitStoreEvent::DiffBaseChanged(None));
    }

    pub fn display_status_for_buffer_id(
        &self,
        buffer_id: BufferId,
        cx: &App,
    ) -> Option<FileStatus> {
        let (repo, repo_path) = self.repository_and_path_for_buffer_id(buffer_id, cx)?;
        self.display_status(&repo, &repo_path, cx)
    }

    pub fn display_status_for_project_path(
        &self,
        project_path: &ProjectPath,
        cx: &App,
    ) -> Option<FileStatus> {
        let (repo, repo_path) = self.repository_and_path_for_project_path(project_path, cx)?;
        self.display_status(&repo, &repo_path, cx)
    }

    fn display_status(
        &self,
        repo: &Entity<Repository>,
        repo_path: &RepoPath,
        cx: &App,
    ) -> Option<FileStatus> {
        if self.diff_base == GitDiffBaseSetting::DefaultBranch
            && let Some((buffers, _)) = self
                .display_diffs
                .get(&repo.read(cx).id)
                .and_then(|state| state.buffers.as_ref())
        {
            return buffers.read(cx).status_for_path(repo_path, cx);
        }
        repo.read(cx)
            .status_for_path(repo_path)
            .map(|entry| entry.status)
    }

    pub fn display_diff_for_repo(
        &self,
        repo_id: RepositoryId,
    ) -> Option<Entity<diff_buffer_list::DiffBufferList>> {
        let (buffers, _) = self.display_diffs.get(&repo_id)?.buffers.as_ref()?;
        Some(buffers.clone())
    }

    pub fn ensure_display_diff(
        &mut self,
        repo: Entity<Repository>,
        resolved_ref: SharedString,
        cx: &mut Context<Self>,
    ) -> Entity<diff_buffer_list::DiffBufferList> {
        let repo_id = repo.read(cx).id;
        if let Some((buffers, _)) = self
            .display_diffs
            .get(&repo_id)
            .and_then(|state| state.buffers.as_ref())
        {
            let buffers = buffers.clone();
            buffers.update(cx, |buffers, cx| {
                buffers.set_diff_base(
                    diff_buffer_list::DiffBase::Merge {
                        base_ref: resolved_ref,
                    },
                    cx,
                )
            });
            return buffers;
        }

        let git_store = cx.entity();
        let buffers = cx.new(|cx| {
            diff_buffer_list::DiffBufferList::new(
                diff_buffer_list::DiffBase::Merge {
                    base_ref: resolved_ref,
                },
                git_store,
                Some(repo),
                cx,
            )
        });
        let subscription = cx.subscribe(&buffers, move |_, _, event, cx| {
            if matches!(event, diff_buffer_list::BranchDiffEvent::FileListChanged) {
                cx.emit(GitStoreEvent::DiffBaseChanged(Some(repo_id)));
            }
        });
        self.display_diffs.entry(repo_id).or_default().buffers =
            Some((buffers.clone(), subscription));
        buffers
    }

    fn refresh_diff_base_for_repo(&mut self, repo_id: RepositoryId, cx: &mut Context<Self>) {
        if self.diff_base == GitDiffBaseSetting::Head {
            return;
        }
        let Some(repo) = self.repositories.get(&repo_id).cloned() else {
            return;
        };

        let task = cx.spawn(async move |this, cx| {
            let result: Result<Option<SharedString>> =
                async { repo.update(cx, |repo, _| repo.default_branch(true)).await? }.await;
            this.update(cx, |this, cx| {
                let Some(state) = this.display_diffs.get_mut(&repo_id) else {
                    return;
                };
                state.refresh.take();
                match result.as_ref() {
                    Ok(Some(resolved_ref)) => {
                        this.ensure_display_diff(repo, resolved_ref.clone(), cx);
                        cx.emit(GitStoreEvent::DiffBaseChanged(Some(repo_id)));
                    }
                    Ok(None) => {
                        if state.buffers.take().is_some() {
                            cx.emit(GitStoreEvent::DiffBaseChanged(Some(repo_id)));
                        }
                    }
                    Err(_) => {}
                }
            })
            .log_err();
            result.log_err();
        });
        self.display_diffs.entry(repo_id).or_default().refresh = Some(task);
    }

    fn coalesce_repo_paths(mut paths: Vec<RepoPath>) -> Vec<RepoPath> {
        paths.sort();

        let mut coalesced = Vec::with_capacity(paths.len());
        for path in paths {
            if coalesced
                .last()
                .is_some_and(|ancestor: &RepoPath| path.starts_with(ancestor))
            {
                continue;
            }
            coalesced.push(path);
        }

        coalesced
    }

    fn process_updated_entries(
        &self,
        worktree: &Entity<Worktree>,
        updated_entries: &[(Arc<RelPath>, ProjectEntryId, PathChange)],
        cx: &mut App,
    ) -> Task<HashMap<Entity<Repository>, Vec<RepoPath>>> {
        let path_style = worktree.read(cx).path_style();
        let mut repo_paths = self
            .repositories
            .values()
            .map(|repo| (repo.read(cx).work_directory_abs_path.clone(), repo.clone()))
            .collect::<Vec<_>>();
        let mut entries: Vec<_> = updated_entries
            .iter()
            .map(|(path, _, _)| path.clone())
            .collect();
        entries.sort();
        let worktree = worktree.read(cx);

        let entries = entries
            .into_iter()
            .map(|path| worktree.absolutize(&path))
            .collect::<Arc<[_]>>();

        let executor = cx.background_executor().clone();
        cx.background_executor().spawn(async move {
            repo_paths.sort_by(|lhs, rhs| lhs.0.cmp(&rhs.0));
            let mut paths_by_git_repo = HashMap::<_, Vec<_>>::default();
            let mut tasks = FuturesOrdered::new();
            for (repo_path, repo) in repo_paths.into_iter().rev() {
                let entries = entries.clone();
                let task = executor.spawn(async move {
                    // Find all repository paths that belong to this repo
                    let mut ix = entries.partition_point(|path| path < &*repo_path);
                    if ix == entries.len() {
                        return None;
                    };

                    let mut paths = Vec::new();
                    // All paths prefixed by a given repo will constitute a continuous range.
                    while let Some(path) = entries.get(ix)
                        && let Some(repo_path) = RepositorySnapshot::abs_path_to_repo_path_inner(
                            &repo_path, path, path_style,
                        )
                    {
                        paths.push((repo_path, ix));
                        ix += 1;
                    }
                    if paths.is_empty() {
                        None
                    } else {
                        Some((repo, paths))
                    }
                });
                tasks.push_back(task);
            }

            // Now, let's filter out the "duplicate" entries that were processed by multiple distinct repos.
            let mut path_was_used = vec![false; entries.len()];
            let tasks = tasks.collect::<Vec<_>>().await;
            // Process tasks from the back: iterating backwards allows us to see more-specific paths first.
            // We always want to assign a path to it's innermost repository.
            for t in tasks {
                let Some((repo, paths)) = t else {
                    continue;
                };
                let entry = paths_by_git_repo.entry(repo).or_default();
                for (repo_path, ix) in paths {
                    if path_was_used[ix] {
                        continue;
                    }
                    path_was_used[ix] = true;
                    entry.push(repo_path);
                }
            }

            for paths in paths_by_git_repo.values_mut() {
                *paths = Self::coalesce_repo_paths(mem::take(paths));
            }

            paths_by_git_repo
        })
    }
}

impl BufferGitState {
    fn new(_git_store: WeakEntity<GitStore>, _cx: &mut Context<Self>) -> Self {
        Self {
            unstaged_diff: Default::default(),
            staged_diff: Default::default(),
            uncommitted_diff: Default::default(),
            oid_diffs: Default::default(),
            recalculate_diff_task: Default::default(),
            language: Default::default(),
            language_registry: Default::default(),
            recalculating_tx: postage::watch::channel_with(false).0,
            hunk_staging_operation_count: 0,
            hunk_staging_operation_count_as_of_write: 0,
            head_text: Default::default(),
            index_text: Default::default(),
            pending_index_edits: Some(Vec::new()),
            oid_texts: Default::default(),
            head_text_buffer: WeakEntity::new_invalid(),
            index_text_buffer: WeakEntity::new_invalid(),
            index_text_buffer_language_enabled: Default::default(),
            head_changed: Default::default(),
            index_changed: Default::default(),
            language_changed: Default::default(),
            conflict_updated_futures: Default::default(),
            conflict_set: Default::default(),
            reparse_conflict_markers_task: Default::default(),
        }
    }

    fn get_or_create_head_text_buffer(&mut self, cx: &mut Context<Self>) -> Entity<Buffer> {
        if let Some(buffer) = self.head_text_buffer.upgrade() {
            return buffer;
        }
        let head_text = self.head_text.clone();
        let buffer = cx.new(|cx| {
            let mut buffer = Buffer::local(head_text.as_deref().unwrap_or(""), cx);
            buffer.set_capability(Capability::ReadOnly, cx);
            buffer
        });
        self.head_text_buffer = buffer.downgrade();
        buffer
    }

    fn index_text_buffer(&self) -> Option<Entity<Buffer>> {
        self.index_text_buffer.upgrade()
    }

    fn get_or_create_index_text_buffer(
        &mut self,
        file: Option<Arc<dyn language::File>>,
        cx: &mut Context<Self>,
    ) -> Entity<Buffer> {
        if let Some(buffer) = self.index_text_buffer.upgrade() {
            if let Some(file) = file {
                buffer.update(cx, |buffer, cx| buffer.file_updated(file, cx));
            }
            return buffer;
        }
        let index_text = self.index_text.clone();
        let buffer = cx.new(|cx| {
            let mut buffer = Buffer::local(index_text.as_deref().unwrap_or(""), cx);
            if let Some(file) = file {
                buffer.file_updated(file, cx);
            }
            buffer.set_capability(Capability::ReadOnly, cx);
            buffer
        });
        self.index_text_buffer = buffer.downgrade();
        buffer
    }

    #[ztracing::instrument(skip_all)]
    fn buffer_language_changed(&mut self, buffer: Entity<Buffer>, cx: &mut Context<Self>) {
        self.language = buffer.read(cx).language().cloned();
        self.language_changed = true;
        let _ = self.recalculate_diffs(buffer.read(cx).text_snapshot(), cx);
    }

    fn reparse_conflict_markers(
        &mut self,
        buffer: text::BufferSnapshot,
        cx: &mut Context<Self>,
    ) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();

        let Some(conflict_set) = self
            .conflict_set
            .as_ref()
            .and_then(|conflict_set| conflict_set.upgrade())
        else {
            return rx;
        };

        let has_conflict = conflict_set.read_with(cx, |conflict_set, _| conflict_set.has_conflict);
        if !has_conflict {
            return rx;
        }

        let old_snapshot = conflict_set.read_with(cx, |conflict_set, _| conflict_set.snapshot());
        self.conflict_updated_futures.push(tx);
        self.reparse_conflict_markers_task = Some(cx.spawn(async move |this, cx| {
            let (snapshot, changed_range) = cx
                .background_spawn(async move {
                    let new_snapshot = ConflictSet::parse(&buffer);
                    let changed_range = old_snapshot.compare(&new_snapshot, &buffer);
                    (new_snapshot, changed_range)
                })
                .await;
            this.update(cx, |this, cx| {
                if let Some(conflict_set) = &this.conflict_set {
                    conflict_set
                        .update(cx, |conflict_set, cx| {
                            conflict_set.set_snapshot(snapshot, changed_range, cx);
                        })
                        .ok();
                }
                let futures = std::mem::take(&mut this.conflict_updated_futures);
                for tx in futures {
                    tx.send(()).ok();
                }
            })
        }));

        rx
    }

    fn unstaged_diff(&self) -> Option<Entity<BufferDiff>> {
        self.unstaged_diff.as_ref().and_then(|set| set.upgrade())
    }

    fn staged_diff(&self) -> Option<Entity<BufferDiff>> {
        self.staged_diff.as_ref().and_then(|(set, _)| set.upgrade())
    }

    fn staged_diff_and_index_text_buffer(&self) -> Option<(Entity<BufferDiff>, Entity<Buffer>)> {
        let (diff, index_text_buffer) = self.staged_diff.as_ref()?;
        Some((diff.upgrade()?, index_text_buffer.clone()))
    }

    fn uncommitted_diff(&self) -> Option<Entity<BufferDiff>> {
        self.uncommitted_diff.as_ref().and_then(|set| set.upgrade())
    }

    fn oid_diff(&self, oid: Option<git::Oid>) -> Option<Entity<BufferDiff>> {
        self.oid_diffs.get(&oid).and_then(|weak| weak.upgrade())
    }

    /// Whether the index text is known to match the committed text, without
    /// comparing their contents. Always true when both texts were set by a
    /// single `DiffBasesChange::SetBoth`, which shares one allocation between
    /// them. May be false even when the contents are equal, if the texts were
    /// loaded separately.
    fn index_matches_head(&self) -> bool {
        match (self.index_text.as_ref(), self.head_text.as_ref()) {
            (Some(index), Some(head)) => Arc::ptr_eq(index, head),
            (None, None) => true,
            _ => false,
        }
    }

    fn remove_overlapping_pending_index_edits(&mut self, ranges: &[Range<usize>]) {
        if let Some(edits) = &mut self.pending_index_edits {
            edits.retain(|(existing, _)| {
                ranges.iter().all(|footprint| {
                    existing.end < footprint.start || footprint.end < existing.start
                })
            });
        }
    }

    fn insert_pending_index_edits(&mut self, edits: Option<Vec<(Range<usize>, Arc<str>)>>) {
        match edits {
            None => {
                self.pending_index_edits = None;
            }
            Some(new_edits) => {
                let mut edits = self.pending_index_edits.take().unwrap_or_default();
                for (range, replacement) in new_edits {
                    edits.retain(|(existing, _)| {
                        existing.end < range.start || range.end < existing.start
                    });
                    let position =
                        edits.partition_point(|(existing, _)| existing.start < range.start);
                    edits.insert(position, (range, replacement));
                }
                self.pending_index_edits = Some(edits);
            }
        }
    }

    fn pending_index_text(&self, cx: &App) -> Option<Rope> {
        let index_text_buffer = self.index_text_buffer.upgrade()?;
        let edits = self.pending_index_edits.as_ref()?;
        #[cfg(debug_assertions)]
        for window in edits.windows(2) {
            debug_assert!(window[0].0.end <= window[1].0.start);
        }
        let mut index_text = index_text_buffer.read(cx).text_snapshot().as_rope().clone();
        for (old_range, replacement_text) in edits.iter().rev() {
            index_text.replace(old_range.clone(), replacement_text);
        }
        Some(index_text)
    }

    fn clear_pending_index_edits(&mut self) {
        self.pending_index_edits = Some(Vec::new());
    }

    fn clear_pending_hunks(&mut self, cx: &mut Context<Self>) {
        for diff in [
            self.uncommitted_diff(),
            self.unstaged_diff(),
            self.staged_diff(),
        ]
        .into_iter()
        .flatten()
        {
            diff.update(cx, |diff, cx| diff.clear_pending_hunks(cx));
        }
    }

    fn mark_whole_file_stage_or_unstage_pending(
        &mut self,
        stage: bool,
        buffer_snapshot: &text::BufferSnapshot,
        cx: &mut Context<Self>,
    ) {
        if let Some(uncommitted_diff) = self.uncommitted_diff() {
            uncommitted_diff.update(cx, |uncommitted_diff, cx| {
                uncommitted_diff.mark_all_hunks_pending(stage, buffer_snapshot, cx);
            });
        }

        if stage {
            if let Some(unstaged_diff) = self.unstaged_diff() {
                unstaged_diff.update(cx, |unstaged_diff, cx| {
                    unstaged_diff.suppress_all_hunks_pending(buffer_snapshot, cx);
                });
            }
        } else if let Some(staged_diff) = self.staged_diff()
            && let Some(index_text_buffer) = self.index_text_buffer()
        {
            let index_snapshot = index_text_buffer.read(cx).text_snapshot();
            staged_diff.update(cx, |staged_diff, cx| {
                staged_diff.suppress_all_hunks_pending(&index_snapshot, cx);
            });
        }
    }

    fn clear_pending_index_edits_and_hunks(&mut self, cx: &mut Context<Self>) {
        self.clear_pending_index_edits();
        self.clear_pending_hunks(cx);
    }

    pub fn wait_for_recalculation(&mut self) -> Option<impl Future<Output = ()> + use<>> {
        if *self.recalculating_tx.borrow() {
            let mut rx = self.recalculating_tx.subscribe();
            Some(async move {
                loop {
                    let is_recalculating = rx.recv().await;
                    if is_recalculating != Some(true) {
                        break;
                    }
                }
            })
        } else {
            None
        }
    }

    fn diff_bases_changed(
        &mut self,
        buffer: text::BufferSnapshot,
        diff_bases_change: Option<DiffBasesChange>,
        cx: &mut Context<Self>,
    ) {
        match diff_bases_change {
            Some(DiffBasesChange::SetIndex(index)) => {
                self.index_text = index.map(|mut index| {
                    text::LineEnding::normalize(&mut index);
                    Arc::from(index.as_str())
                });
                self.index_changed = true;
            }
            Some(DiffBasesChange::SetHead(head)) => {
                self.head_text = head.map(|mut head| {
                    text::LineEnding::normalize(&mut head);
                    Arc::from(head.as_str())
                });
                self.head_changed = true;
            }
            Some(DiffBasesChange::SetBoth(text)) => {
                let text = text.map(|mut text| {
                    text::LineEnding::normalize(&mut text);
                    Arc::from(text.as_str())
                });
                self.head_text = text.clone();
                self.index_text = text;
                self.head_changed = true;
                self.index_changed = true;
            }
            Some(DiffBasesChange::SetEach { index, head }) => {
                self.index_text = index.map(|mut index| {
                    text::LineEnding::normalize(&mut index);
                    Arc::from(index.as_str())
                });
                self.index_changed = true;
                self.head_text = head.map(|mut head| {
                    text::LineEnding::normalize(&mut head);
                    Arc::from(head.as_str())
                });
                self.head_changed = true;
            }
            None => {}
        }

        self.recalculate_diffs(buffer, cx)
    }

    #[ztracing::instrument(skip_all)]
    fn recalculate_diffs(&mut self, buffer: text::BufferSnapshot, cx: &mut Context<Self>) {
        *self.recalculating_tx.borrow_mut() = true;

        let language = self.language.clone();
        let language_registry = self.language_registry.clone();
        let unstaged_diff = self.unstaged_diff();
        let staged_diff = self.staged_diff();
        let uncommitted_diff = self.uncommitted_diff();
        let head = self.head_text.clone();
        let index = self.index_text.clone();
        let head_text_buffer = self.head_text_buffer.upgrade();
        let index_text_buffer = self.index_text_buffer.upgrade();
        let index_text_buffer_language_enabled = self.index_text_buffer_language_enabled;
        let index_changed = self.index_changed;
        let head_changed = self.head_changed;
        let language_changed = self.language_changed;
        let prev_hunk_staging_operation_count = self.hunk_staging_operation_count_as_of_write;
        let index_matches_head = self.index_matches_head();

        let oid_diffs: Vec<(
            Option<git::Oid>,
            Entity<BufferDiff>,
            Entity<Buffer>,
            Option<Arc<str>>,
        )> = self
            .oid_diffs
            .iter()
            .filter_map(|(oid, weak)| {
                let diff = weak.upgrade()?;
                let base_text_buffer = diff.read(cx).base_text_buffer().clone();
                let base_text = match oid {
                    Some(oid) => Some(self.oid_texts.get(oid)?.clone()),
                    None => None,
                };
                Some((*oid, diff, base_text_buffer, base_text))
            })
            .collect();

        self.oid_diffs.retain(|oid, weak| {
            let alive = weak.upgrade().is_some();
            if !alive {
                if let Some(oid) = oid {
                    self.oid_texts.remove(oid);
                }
            }
            alive
        });
        if self
            .staged_diff
            .as_ref()
            .is_some_and(|(weak, _)| !weak.is_upgradable())
        {
            self.staged_diff = None;
        }
        self.recalculate_diff_task = Some(cx.spawn(async move |this, cx| {
            log::debug!(
                "start recalculating diffs for buffer {}",
                buffer.remote_id()
            );

            if index_text_buffer_language_enabled
                && let Some(index_text_buffer) = &index_text_buffer
            {
                index_text_buffer.update(cx, |index_text_buffer, cx| {
                    if let Some(language_registry) = language_registry.clone() {
                        index_text_buffer.set_language_registry(language_registry);
                    }
                    index_text_buffer.set_language_async(language.clone(), cx);
                });
            }
            if let Some(head_text_buffer) = &head_text_buffer {
                head_text_buffer.update(cx, |head_text_buffer, cx| {
                    if let Some(language_registry) = language_registry.clone() {
                        head_text_buffer.set_language_registry(language_registry);
                    }
                    head_text_buffer.set_language_async(language.clone(), cx);
                });
            }

            for (_, _, base_text_buffer, _) in &oid_diffs {
                base_text_buffer.update(cx, |base_text_buffer, cx| {
                    if let Some(language_registry) = language_registry.clone() {
                        base_text_buffer.set_language_registry(language_registry);
                    }
                    base_text_buffer.set_language_async(language.clone(), cx);
                });
            }

            let mut edited_index_text = None;

            let index_text_snapshot = if let Some(index_text_buffer) = &index_text_buffer
                && (unstaged_diff.is_some() || staged_diff.is_some())
            {
                let index_text_snapshot = if index_changed || language_changed {
                    let new_index_text = index.clone().unwrap_or_default();
                    let index_text_diff = index_text_buffer
                        .update(cx, |index_text_buffer, cx| {
                            index_text_buffer.diff(new_index_text.clone(), cx)
                        })
                        .await;
                    let edited = index_text_buffer
                        .update(cx, |index_text_buffer, cx| {
                            index_text_buffer.snapshot_with_edits(index_text_diff.edits, cx)
                        })
                        .await;
                    let snapshot = edited.snapshot().clone();
                    edited_index_text = Some(edited);
                    snapshot
                } else {
                    index_text_buffer.read_with(cx, |buffer, _| buffer.snapshot())
                };
                Some(index_text_snapshot)
            } else {
                None
            };

            let mut new_unstaged_diff = None;

            if let (Some(unstaged_diff), Some(index_text_snapshot)) =
                (unstaged_diff.as_ref(), index_text_snapshot.as_ref())
            {
                new_unstaged_diff = Some(
                    cx.update(|cx| {
                        unstaged_diff.read(cx).update_diff(
                            buffer.clone(),
                            index_text_snapshot,
                            index.clone(),
                            cx,
                        )
                    })
                    .await,
                );
            }

            // Dropping BufferDiff can be expensive, so yield back to the event loop
            // for a bit
            yield_now().await;

            let mut edited_head_text = None;
            let mut new_staged_diff = None;
            let mut new_uncommitted_diff = None;
            if let Some(head_text_buffer) = &head_text_buffer
                && (staged_diff.is_some() || uncommitted_diff.is_some())
            {
                let head_base_text_exists = head.is_some();
                let head_text_snapshot = if head_changed || language_changed {
                    let new_head_text = head.clone().unwrap_or_default();
                    let head_text_diff = head_text_buffer
                        .update(cx, |head_text_buffer, cx| {
                            head_text_buffer.diff(new_head_text.clone(), cx)
                        })
                        .await;
                    let edited = head_text_buffer
                        .update(cx, |base_text_buffer, cx| {
                            base_text_buffer.snapshot_with_edits(head_text_diff.edits, cx)
                        })
                        .await;
                    let snapshot = edited.snapshot().clone();
                    edited_head_text = Some(edited);
                    snapshot
                } else {
                    head_text_buffer.read_with(cx, |buffer, _| buffer.snapshot())
                };
                if let (Some(staged_diff), Some(index_base_text_snapshot)) =
                    (staged_diff.as_ref(), index_text_snapshot.as_ref())
                {
                    new_staged_diff = Some(
                        cx.update(|cx| {
                            staged_diff.read(cx).update_diff(
                                index_base_text_snapshot.text.clone(),
                                &head_text_snapshot,
                                head.clone(),
                                cx,
                            )
                        })
                        .await,
                    );
                }

                if let Some(uncommitted_diff) = &uncommitted_diff {
                    new_uncommitted_diff = if index_matches_head {
                        new_unstaged_diff.clone().map(|mut update| {
                            update.set_base_text_snapshot(
                                head_text_snapshot.clone(),
                                head_base_text_exists,
                            );
                            update
                        })
                    } else {
                        None
                    };
                    if new_uncommitted_diff.is_none() {
                        new_uncommitted_diff = Some(
                            cx.update(|cx| {
                                uncommitted_diff.read(cx).update_diff(
                                    buffer.clone(),
                                    &head_text_snapshot,
                                    head.clone(),
                                    cx,
                                )
                            })
                            .await,
                        );
                    }
                }
            }

            // Dropping BufferDiff can be expensive, so yield back to the event loop
            // for a bit
            yield_now().await;

            let cancel = this.update(cx, |this, _| {
                // This checks whether all pending stage/unstage operations
                // have quiesced (i.e. both the corresponding write and the
                // read of that write have completed). If not, then we cancel
                // this recalculation attempt to avoid invalidating pending
                // state too quickly; another recalculation will come along
                // later and clear the pending state once the state of the index has settled.
                if this.hunk_staging_operation_count > prev_hunk_staging_operation_count {
                    *this.recalculating_tx.borrow_mut() = false;
                    true
                } else {
                    false
                }
            })?;
            if cancel {
                log::debug!(
                    concat!(
                        "aborting recalculating diffs for buffer {}",
                        "due to subsequent hunk operations",
                    ),
                    buffer.remote_id()
                );
                return Ok(());
            }

            this.update(cx, |this, cx| {
                this.clear_pending_index_edits();

                if let (Some(staged_diff), Some(new_staged_diff)) =
                    (staged_diff.as_ref(), new_staged_diff.clone())
                {
                    staged_diff.update(cx, |diff, cx| {
                        if let Some(edited_base_text) = edited_index_text.take()
                            && let Some(index_text_buffer) = &index_text_buffer
                        {
                            index_text_buffer.update(cx, |index_text_buffer, cx| {
                                index_text_buffer.fast_forward(edited_base_text, cx)
                            });
                        }
                        if let Some(edited_head_text) = edited_head_text.take()
                            && let Some(head_text_buffer) = &head_text_buffer
                        {
                            head_text_buffer.update(cx, |head_text_buffer, cx| {
                                head_text_buffer.fast_forward(edited_head_text, cx)
                            });
                        }
                        diff.set_snapshot_with_secondary(new_staged_diff, None, true, cx)
                    });
                }

                let unstaged_changed_range = if let (Some(unstaged_diff), Some(new_unstaged_diff)) =
                    (unstaged_diff.as_ref(), new_unstaged_diff.clone())
                {
                    Some(unstaged_diff.update(cx, |diff, cx| {
                        if let Some(edited_index_text) = edited_index_text.take()
                            && let Some(index_text_buffer) = &index_text_buffer
                        {
                            index_text_buffer.update(cx, |index_text_buffer, cx| {
                                index_text_buffer.fast_forward(edited_index_text, cx)
                            });
                        }
                        diff.set_snapshot_with_secondary(new_unstaged_diff, None, true, cx)
                    }))
                } else {
                    None
                };

                if let (Some(uncommitted_diff), Some(new_uncommitted_diff)) =
                    (uncommitted_diff.as_ref(), new_uncommitted_diff.clone())
                {
                    uncommitted_diff.update(cx, |diff, cx| {
                        if let Some(edited_base_text) = edited_head_text.take()
                            && let Some(head_text_buffer) = &head_text_buffer
                        {
                            head_text_buffer.update(cx, |head_text_buffer, cx| {
                                head_text_buffer.fast_forward(edited_base_text, cx)
                            });
                        }
                        diff.set_snapshot_with_secondary(
                            new_uncommitted_diff,
                            unstaged_changed_range.flatten(),
                            true,
                            cx,
                        )
                    });
                }
            })?;

            yield_now().await;

            for (oid, oid_diff, base_text_buffer, base_text) in oid_diffs {
                let base_text_snapshot =
                    base_text_buffer.read_with(cx, |buffer, _| buffer.snapshot());
                let new_oid_diff = cx
                    .update(|cx| {
                        oid_diff.read(cx).update_diff(
                            buffer.clone(),
                            &base_text_snapshot,
                            base_text.clone(),
                            cx,
                        )
                    })
                    .await;

                oid_diff.update(cx, |diff, cx| diff.set_snapshot(new_oid_diff, cx));

                log::debug!(
                    "finished recalculating oid diff for buffer {} oid {:?}",
                    buffer.remote_id(),
                    oid
                );

                yield_now().await;
            }

            log::debug!(
                "finished recalculating diffs for buffer {}",
                buffer.remote_id()
            );

            if let Some(this) = this.upgrade() {
                this.update(cx, |this, _| {
                    this.index_changed = false;
                    this.head_changed = false;
                    this.language_changed = false;
                    *this.recalculating_tx.borrow_mut() = false;
                });
            }

            Ok(())
        }));
    }
}

impl RepositorySnapshot {
    fn empty(
        id: RepositoryId,
        work_directory_abs_path: Arc<Path>,
        repository_dir_abs_path: Option<Arc<Path>>,
        dot_git_abs_path: Option<Arc<Path>>,
        common_dir_abs_path: Option<Arc<Path>>,
        path_style: PathStyle,
    ) -> Self {
        let repository_dir_abs_path =
            repository_dir_abs_path.unwrap_or_else(|| work_directory_abs_path.join(".git").into());
        let dot_git_abs_path =
            dot_git_abs_path.unwrap_or_else(|| work_directory_abs_path.join(".git").into());
        let common_dir_abs_path =
            common_dir_abs_path.unwrap_or_else(|| repository_dir_abs_path.clone());

        Self {
            id,
            statuses_by_path: Default::default(),
            repository_dir_abs_path,
            dot_git_abs_path,
            common_dir_abs_path,
            work_directory_abs_path,
            branch: None,
            branch_list: Arc::from([]),
            branch_list_error: None,
            head_commit: None,
            scan_id: 0,
            merge: Default::default(),
            remote_origin_url: None,
            remote_upstream_url: None,
            stash_entries: Default::default(),
            linked_worktrees: Arc::from([]),
            path_style,
        }
    }

    /// Returns the main worktree path for this repository, if one exists.
    ///
    /// Linked worktrees attached to bare repositories do not have a main
    /// worktree. For linked worktrees attached to a non-bare repository, the
    /// common Git directory is the main worktree's `.git` directory.
    pub fn main_worktree_abs_path(&self) -> Option<&Path> {
        if self.is_linked_worktree() {
            if self.common_dir_abs_path.file_name()? == std::ffi::OsStr::new(".git") {
                self.common_dir_abs_path.parent()
            } else {
                None
            }
        } else {
            Some(self.work_directory_abs_path.as_ref())
        }
    }

    /// The main worktree is the original checkout that other worktrees were
    /// created from.
    ///
    /// For example, if you had both `~/code/zed` and `~/code/worktrees/zed-2`,
    /// then `~/code/zed` is the main worktree and `~/code/worktrees/zed-2` is a linked worktree.
    ///
    /// Submodules also return `true` here, since they are not linked worktrees.
    pub fn is_main_worktree(&self) -> bool {
        !self.is_linked_worktree()
    }

    /// Returns true if this repository is a linked worktree, that is, one that
    /// was created from another worktree.
    ///
    /// Returns `false` for both the main worktree and submodules.
    pub fn is_linked_worktree(&self) -> bool {
        self.repository_dir_abs_path != self.common_dir_abs_path
    }

    pub fn linked_worktrees(&self) -> &[GitWorktree] {
        &self.linked_worktrees
    }

    pub fn status(&self) -> impl Iterator<Item = StatusEntry> + '_ {
        self.statuses_by_path.iter().cloned()
    }

    pub fn status_summary(&self) -> GitSummary {
        self.statuses_by_path.summary().item_summary
    }

    pub fn status_for_path(&self, path: &RepoPath) -> Option<StatusEntry> {
        self.statuses_by_path
            .get(&PathKey(path.as_ref().clone()), ())
            .cloned()
    }

    pub fn diff_stat_for_path(&self, path: &RepoPath) -> Option<DiffStat> {
        self.statuses_by_path
            .get(&PathKey(path.as_ref().clone()), ())
            .and_then(|entry| entry.diff_stat)
    }

    pub fn abs_path_to_repo_path(&self, abs_path: &Path) -> Option<RepoPath> {
        Self::abs_path_to_repo_path_inner(&self.work_directory_abs_path, abs_path, self.path_style)
    }

    pub fn repo_path_to_abs_path(&self, repo_path: &RepoPath) -> PathBuf {
        let repo_path = repo_path.display(self.path_style);
        PathBuf::from(
            self.path_style
                .join(&self.work_directory_abs_path, repo_path.as_ref())
                .unwrap(),
        )
    }

    #[inline]
    fn abs_path_to_repo_path_inner(
        work_directory_abs_path: &Path,
        abs_path: &Path,
        path_style: PathStyle,
    ) -> Option<RepoPath> {
        let rel_path = path_style.strip_prefix(abs_path, work_directory_abs_path)?;
        Some(RepoPath::from_rel_path(&rel_path))
    }

    pub fn had_conflict_on_last_merge_head_change(&self, repo_path: &RepoPath) -> bool {
        self.merge
            .merge_heads_by_conflicted_path
            .contains_key(repo_path)
    }

    pub fn has_conflict(&self, repo_path: &RepoPath) -> bool {
        let had_conflict_on_last_merge_head_change = self
            .merge
            .merge_heads_by_conflicted_path
            .contains_key(repo_path);
        let has_conflict_currently = self
            .status_for_path(repo_path)
            .is_some_and(|entry| entry.status.is_conflicted());
        had_conflict_on_last_merge_head_change || has_conflict_currently
    }

    /// This is the name that will be displayed in the repository selector for this repository.
    pub fn display_name(&self) -> SharedString {
        self.work_directory_abs_path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string()
            .into()
    }
}

impl MergeDetails {
    async fn update(
        &mut self,
        backend: &Arc<dyn GitRepository>,
        current_conflicted_paths: Vec<RepoPath>,
    ) -> bool {
        log::debug!("load merge details");
        self.message = backend.merge_message().await.map(SharedString::from);
        let heads = backend
            .revparse_batch(vec![
                "MERGE_HEAD".into(),
                "CHERRY_PICK_HEAD".into(),
                "REBASE_HEAD".into(),
                "REVERT_HEAD".into(),
                "APPLY_HEAD".into(),
            ])
            .await
            .log_err()
            .unwrap_or_default()
            .into_iter()
            .map(|opt| opt.map(SharedString::from))
            .collect::<Vec<_>>();

        let mut conflicts_changed = false;

        // Record the merge state for newly conflicted paths
        for path in &current_conflicted_paths {
            if self.merge_heads_by_conflicted_path.get(&path).is_none() {
                conflicts_changed = true;
                self.merge_heads_by_conflicted_path
                    .insert(path.clone(), heads.clone());
            }
        }

        // Clear state for paths that are no longer conflicted and for which the merge heads have changed
        self.merge_heads_by_conflicted_path
            .retain(|path, old_merge_heads| {
                let keep = current_conflicted_paths.contains(path)
                    || (old_merge_heads == &heads
                        && old_merge_heads.iter().any(|head| head.is_some()));
                if !keep {
                    conflicts_changed = true;
                }
                keep
            });

        conflicts_changed
    }
}

impl Repository {
    pub fn is_trusted(&self) -> bool {
        match self.repository_state.peek() {
            Some(Ok(RepositoryState::Local(state))) => state.backend.is_trusted(),
            _ => false,
        }
    }

    pub fn snapshot(&self) -> RepositorySnapshot {
        self.snapshot.clone()
    }

    pub fn pending_ops(&self) -> impl Iterator<Item = PendingOps> + '_ {
        self.pending_ops.iter().cloned()
    }

    pub fn pending_ops_summary(&self) -> PathSummary<PendingOpsSummary> {
        self.pending_ops.summary().clone()
    }

    pub fn pending_ops_for_path(&self, path: &RepoPath) -> Option<PendingOps> {
        self.pending_ops
            .get(&PathKey(path.as_ref().clone()), ())
            .cloned()
    }

    fn respawn_local_worker(
        &mut self,
        project_environment: WeakEntity<ProjectEnvironment>,
        fs: Arc<dyn Fs>,
        is_trusted: bool,
        cx: &mut Context<Self>,
    ) {
        let work_directory_abs_path = self.snapshot.work_directory_abs_path.clone();
        let dot_git_abs_path = self.snapshot.dot_git_abs_path.clone();

        let state = cx
            .spawn(async move |_, cx| {
                LocalRepositoryState::new(
                    work_directory_abs_path,
                    dot_git_abs_path,
                    project_environment,
                    fs,
                    is_trusted,
                    cx,
                )
                .await
                .map_err(|err| err.to_string())
            })
            .shared();
        self.job_sender.close_channel();
        self._worker_task = Task::ready(());
        self.active_jobs.clear();
        self.job_debug_queue
            .mark_unfinished_complete(job_debug_queue::CompletedJobStatus::Skipped);
        cx.notify();

        let (job_sender, worker_task) = Repository::spawn_local_git_worker(state.clone(), cx);
        self.job_sender = job_sender;
        self._worker_task = worker_task;
        self.repository_state = cx
            .spawn(async move |_, _| {
                let state = state.await?;
                Ok(RepositoryState::Local(state))
            })
            .shared();
    }

    fn reinitialize_local_backend(
        &mut self,
        work_directory_abs_path: Arc<Path>,
        dot_git_abs_path: Arc<Path>,
        repository_dir_abs_path: Arc<Path>,
        common_dir_abs_path: Arc<Path>,
        project_environment: WeakEntity<ProjectEnvironment>,
        fs: Arc<dyn Fs>,
        is_trusted: bool,
        cx: &mut Context<Self>,
    ) {
        self.snapshot.work_directory_abs_path = work_directory_abs_path;
        self.snapshot.dot_git_abs_path = dot_git_abs_path;
        self.snapshot.repository_dir_abs_path = repository_dir_abs_path;
        self.snapshot.common_dir_abs_path = common_dir_abs_path;
        self.respawn_local_worker(project_environment, fs, is_trusted, cx);
    }

    fn local(
        id: RepositoryId,
        work_directory_abs_path: Arc<Path>,
        repository_dir_abs_path: Arc<Path>,
        common_dir_abs_path: Arc<Path>,
        dot_git_abs_path: Arc<Path>,
        project_environment: WeakEntity<ProjectEnvironment>,
        fs: Arc<dyn Fs>,
        is_trusted: bool,
        git_store: WeakEntity<GitStore>,
        cx: &mut Context<Self>,
    ) -> Self {
        let snapshot = RepositorySnapshot::empty(
            id,
            work_directory_abs_path,
            Some(repository_dir_abs_path),
            Some(dot_git_abs_path),
            Some(common_dir_abs_path),
            PathStyle::local(),
        );

        let mut repo = Repository {
            this: cx.weak_entity(),
            git_store,
            snapshot,
            unshallow_state: UnshallowState::default(),
            pending_ops: Default::default(),
            repository_state: Task::ready(Err("not yet initialized".into())).shared(),
            _worker_task: Task::ready(()),
            commit_message_buffer: None,
            paths_needing_status_update: Default::default(),
            job_sender: mpsc::unbounded().0,
            job_id: 0,
            active_jobs: Default::default(),
            job_debug_queue: job_debug_queue::GitJobDebugQueue::new(),
            initial_graph_data: Default::default(),
            commit_data: Default::default(),
            commit_data_handler: CommitDataHandlerState::Closed,
        };
        repo.respawn_local_worker(project_environment, fs, is_trusted, cx);
        cx.subscribe_self(Self::handle_subscribe_self).detach();
        repo
    }

    fn handle_subscribe_self(&mut self, event: &RepositoryEvent, _: &mut Context<Self>) {
        // scan id greater than 2 means the initial snapshot was calculated,
        // otherwise we don't need to refresh the graph state
        match event {
            RepositoryEvent::HeadChanged | RepositoryEvent::BranchListChanged => {
                if self.scan_id > 2 {
                    self.initial_graph_data.clear();
                }
            }
            RepositoryEvent::StashEntriesChanged => {
                if self.scan_id > 2 {
                    self.initial_graph_data
                        .retain(|(log_source, _), _| *log_source != LogSource::All);
                }
            }
            _ => {}
        }
    }

    pub fn git_store(&self) -> Option<Entity<GitStore>> {
        self.git_store.upgrade()
    }

    fn reload_buffer_diff_bases(&mut self, cx: &mut Context<Self>) {
        let this = cx.weak_entity();
        let git_store = self.git_store.clone();
        let _ = self.send_keyed_job(
            "reload_buffer_diff_bases",
            Some(GitJobKey::ReloadBufferDiffBases),
            None,
            |state, mut cx| async move {
                let RepositoryState::Local(LocalRepositoryState { backend, .. }) = state else {
                    log::error!("tried to recompute diffs for a non-local repository");
                    return Ok(());
                };

                let Some(this) = this.upgrade() else {
                    return Ok(());
                };

                let repo_diff_state_updates = this.update(&mut cx, |this, cx| {
                    git_store.update(cx, |git_store, cx| {
                        git_store
                            .diffs
                            .iter()
                            .filter_map(|(buffer_id, diff_state)| {
                                let buffer_store = git_store.buffer_store.read(cx);
                                let buffer = buffer_store.get(*buffer_id)?;
                                let file = File::from_dyn(buffer.read(cx).file())?;
                                let abs_path = file.worktree.read(cx).absolutize(&file.path);
                                let repo_path = this.abs_path_to_repo_path(&abs_path)?;
                                let is_symlink = GitStore::file_is_symlink(file, cx);
                                log::debug!(
                                    "start reload diff bases for repo path {}",
                                    repo_path.as_unix_str()
                                );
                                diff_state.update(cx, |diff_state, _| {
                                    let has_unstaged_diff = diff_state
                                        .unstaged_diff
                                        .as_ref()
                                        .is_some_and(|diff| diff.is_upgradable());
                                    let has_staged_diff = diff_state
                                        .staged_diff
                                        .as_ref()
                                        .is_some_and(|(diff, _)| diff.is_upgradable());
                                    let has_uncommitted_diff = diff_state
                                        .uncommitted_diff
                                        .as_ref()
                                        .is_some_and(|set| set.is_upgradable());

                                    Some((
                                        buffer,
                                        repo_path,
                                        is_symlink,
                                        (has_unstaged_diff || has_staged_diff)
                                            .then(|| diff_state.index_text.clone()),
                                        (has_staged_diff || has_uncommitted_diff)
                                            .then(|| diff_state.head_text.clone()),
                                    ))
                                })
                            })
                            .collect::<Vec<_>>()
                    })
                })?;

                let buffer_diff_base_changes = cx
                    .background_spawn(async move {
                        let mut revisions = Vec::new();
                        for (_, repo_path, is_symlink, current_index_text, current_head_text) in
                            &repo_diff_state_updates
                        {
                            if current_index_text.is_some() && !*is_symlink {
                                revisions.push(format!(":{}", repo_path.as_unix_str()));
                            }
                            if current_head_text.is_some() && !*is_symlink {
                                revisions.push(format!("HEAD:{}", repo_path.as_unix_str()));
                            }
                        }

                        let mut loaded_revisions = backend
                            .load_revisions(revisions)
                            .await
                            .log_err()
                            .into_iter()
                            .flatten();

                        let mut changes = Vec::new();
                        for (buffer, _, is_symlink, current_index_text, current_head_text) in
                            &repo_diff_state_updates
                        {
                            let index_text = (current_index_text.is_some() && !*is_symlink)
                                .then(|| loaded_revisions.next().flatten())
                                .flatten()
                                .and_then(|bytes| decode_git_text(bytes).log_err());

                            let head_text = (current_head_text.is_some() && !*is_symlink)
                                .then(|| loaded_revisions.next().flatten())
                                .flatten()
                                .and_then(|bytes| decode_git_text(bytes).log_err());

                            let change =
                                match (current_index_text.as_ref(), current_head_text.as_ref()) {
                                    (Some(current_index), Some(current_head)) => {
                                        let index_changed =
                                            index_text.as_deref() != current_index.as_deref();
                                        let head_changed =
                                            head_text.as_deref() != current_head.as_deref();
                                        if index_changed && head_changed {
                                            if index_text == head_text {
                                                Some(DiffBasesChange::SetBoth(head_text))
                                            } else {
                                                Some(DiffBasesChange::SetEach {
                                                    index: index_text,
                                                    head: head_text,
                                                })
                                            }
                                        } else if index_changed {
                                            Some(DiffBasesChange::SetIndex(index_text))
                                        } else if head_changed {
                                            Some(DiffBasesChange::SetHead(head_text))
                                        } else {
                                            None
                                        }
                                    }
                                    (Some(current_index), None) => {
                                        let index_changed =
                                            index_text.as_deref() != current_index.as_deref();
                                        index_changed
                                            .then_some(DiffBasesChange::SetIndex(index_text))
                                    }
                                    (None, Some(current_head)) => {
                                        let head_changed =
                                            head_text.as_deref() != current_head.as_deref();
                                        head_changed.then_some(DiffBasesChange::SetHead(head_text))
                                    }
                                    (None, None) => None,
                                };

                            changes.push((buffer.clone(), change))
                        }
                        changes
                    })
                    .await;

                git_store.update(&mut cx, |git_store, cx| {
                    for (buffer, diff_bases_change) in buffer_diff_base_changes {
                        let buffer_snapshot = buffer.read(cx).text_snapshot();
                        let buffer_id = buffer_snapshot.remote_id();
                        let Some(diff_state) = git_store.diffs.get(&buffer_id) else {
                            continue;
                        };

                        diff_state.update(cx, |diff_state, cx| {
                            diff_state.diff_bases_changed(buffer_snapshot, diff_bases_change, cx);
                        });
                    }
                })
            },
        );
    }

    pub fn send_job<F, Fut, R>(
        &mut self,
        description: &'static str,
        status: Option<SharedString>,
        job: F,
    ) -> oneshot::Receiver<R>
    where
        F: FnOnce(RepositoryState, AsyncApp) -> Fut + 'static,
        Fut: Future<Output = R> + 'static,
        R: Send + 'static,
    {
        self.send_keyed_job(description, None, status, job)
    }

    fn send_keyed_job<F, Fut, R>(
        &mut self,
        description: &'static str,
        key: Option<GitJobKey>,
        status: Option<SharedString>,
        job: F,
    ) -> oneshot::Receiver<R>
    where
        F: FnOnce(RepositoryState, AsyncApp) -> Fut + 'static,
        Fut: Future<Output = R> + 'static,
        R: Send + 'static,
    {
        let (result_tx, result_rx) = futures::channel::oneshot::channel();
        let job_id = post_inc(&mut self.job_id);
        let this = self.this.clone();

        let key_label = key.as_ref().map(format_job_key);
        self.job_debug_queue.add(job_id, description, key_label);

        self.job_sender
            .unbounded_send(GitJob {
                id: job_id,
                key,
                job: Box::new(move |state, cx: &mut AsyncApp| {
                    let job = job(state, cx.clone());
                    cx.spawn(async move |cx| {
                        this.update(cx, |this, cx| {
                            this.job_debug_queue.mark_running(job_id);
                            if let Some(s) = status {
                                this.active_jobs.insert(
                                    job_id,
                                    JobInfo {
                                        start: Instant::now(),
                                        message: s,
                                    },
                                );
                            }
                            cx.notify();
                        })
                        .ok();

                        let result = job.await;

                        this.update(cx, |this, cx| {
                            this.job_debug_queue.mark_complete(
                                job_id,
                                job_debug_queue::CompletedJobStatus::Finished,
                            );
                            this.active_jobs.remove(&job_id);
                            cx.notify();
                        })
                        .ok();

                        result_tx.send(result).ok();
                    })
                }),
            })
            .ok();
        result_rx
    }

    pub fn set_as_active_repository(&self, cx: &mut Context<Self>) {
        let Some(git_store) = self.git_store.upgrade() else {
            return;
        };
        let entity = cx.entity();
        git_store.update(cx, |git_store, cx| {
            let Some((&id, _)) = git_store
                .repositories
                .iter()
                .find(|(_, handle)| *handle == &entity)
            else {
                return;
            };
            git_store.active_repo_id = Some(id);
            cx.emit(GitStoreEvent::ActiveRepositoryChanged(Some(id)));
        });
    }

    pub fn cached_status(&self) -> impl '_ + Iterator<Item = StatusEntry> {
        self.snapshot.status()
    }

    pub fn diff_stat_for_path(&self, path: &RepoPath) -> Option<DiffStat> {
        self.snapshot.diff_stat_for_path(path)
    }

    pub fn cached_stash(&self) -> GitStash {
        self.snapshot.stash_entries.clone()
    }

    pub fn repo_path_to_project_path(&self, path: &RepoPath, cx: &App) -> Option<ProjectPath> {
        let git_store = self.git_store.upgrade()?;
        let worktree_store = git_store.read(cx).worktree_store.read(cx);
        let abs_path = self.snapshot.repo_path_to_abs_path(path);
        let abs_path = SanitizedPath::new(&abs_path);
        let (worktree, relative_path) = worktree_store.find_worktree(abs_path, cx)?;
        Some(ProjectPath {
            worktree_id: worktree.read(cx).id(),
            path: relative_path,
        })
    }

    pub fn project_path_to_repo_path(&self, path: &ProjectPath, cx: &App) -> Option<RepoPath> {
        let git_store = self.git_store.upgrade()?;
        let worktree_store = git_store.read(cx).worktree_store.read(cx);
        let abs_path = worktree_store.absolutize(path, cx)?;
        self.snapshot.abs_path_to_repo_path(&abs_path)
    }

    pub fn contains_sub_repo(&self, other: &Entity<Self>, cx: &App) -> bool {
        other
            .read(cx)
            .snapshot
            .work_directory_abs_path
            .starts_with(&self.snapshot.work_directory_abs_path)
    }

    pub fn commit_message_buffer(&self) -> Option<&Entity<Buffer>> {
        self.commit_message_buffer.as_ref()
    }

    pub fn open_commit_buffer(
        &mut self,
        languages: Option<Arc<LanguageRegistry>>,
        buffer_store: Entity<BufferStore>,
        cx: &mut Context<Self>,
    ) -> Task<Result<Entity<Buffer>>> {
        let _id = self.id;
        if let Some(buffer) = self.commit_message_buffer.clone() {
            return Task::ready(Ok(buffer));
        }
        let this = cx.weak_entity();

        let rx = self.send_job(
            "open_commit_buffer",
            None,
            move |state, mut cx| async move {
                let Some(this) = this.upgrade() else {
                    bail!("git store was dropped");
                };
                match state {
                    RepositoryState::Local(..) => {
                        this.update(&mut cx, |_, cx| {
                            Self::open_local_commit_buffer(languages, buffer_store, cx)
                        })
                        .await
                    }
                }
            },
        );

        cx.spawn(|_, _: &mut AsyncApp| async move { rx.await? })
    }

    fn open_local_commit_buffer(
        language_registry: Option<Arc<LanguageRegistry>>,
        buffer_store: Entity<BufferStore>,
        cx: &mut Context<Self>,
    ) -> Task<Result<Entity<Buffer>>> {
        cx.spawn(async move |repository, cx| {
            let git_commit_language = match language_registry {
                Some(language_registry) => {
                    Some(language_registry.language_for_name("Git Commit").await?)
                }
                None => None,
            };
            let buffer = buffer_store
                .update(cx, |buffer_store, cx| {
                    buffer_store.create_buffer(git_commit_language, false, cx)
                })
                .await?;

            repository.update(cx, |repository, _| {
                repository.commit_message_buffer = Some(buffer.clone());
            })?;
            Ok(buffer)
        })
    }

    pub fn checkout_files(
        &mut self,
        commit: &str,
        paths: Vec<RepoPath>,
        cx: &mut Context<Self>,
    ) -> Task<Result<()>> {
        let commit = commit.to_string();
        let _id = self.id;

        self.spawn_job_with_tracking(
            paths.clone(),
            pending_op::GitStatus::Reverted,
            cx,
            async move |this, cx| {
                this.update(cx, |this, _cx| {
                    this.send_job(
                        "checkout_files",
                        Some(format!("git checkout {}", commit).into()),
                        move |git_repo, _| async move {
                            match git_repo {
                                RepositoryState::Local(LocalRepositoryState {
                                    backend,
                                    environment,
                                    ..
                                }) => {
                                    backend
                                        .checkout_files(commit, paths, environment.clone())
                                        .await
                                }
                            }
                        },
                    )
                })?
                .await?
            },
        )
    }

    pub fn reset(
        &mut self,
        commit: String,
        reset_mode: ResetMode,
        _cx: &mut Context<Self>,
    ) -> oneshot::Receiver<Result<()>> {
        let _id = self.id;

        let receiver = self.send_job("reset", None, move |git_repo, _| async move {
            match git_repo {
                RepositoryState::Local(LocalRepositoryState {
                    backend,
                    environment,
                    ..
                }) => backend.reset(commit, reset_mode, environment).await,
            }
        });

        receiver
    }

    pub fn show(&mut self, commit: String) -> oneshot::Receiver<Result<CommitDetails>> {
        let _id = self.id;
        self.send_job("show", None, move |git_repo, _cx| async move {
            match git_repo {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                    backend.show(commit).await
                }
            }
        })
    }

    pub fn load_commit_diff(
        &mut self,
        commit: String,
        ignore_shallow_boundary: bool,
    ) -> oneshot::Receiver<Result<CommitDiff>> {
        let _id = self.id;
        self.send_job("load_commit_diff", None, move |git_repo, cx| async move {
            match git_repo {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => backend
                    .load_commit(commit, ignore_shallow_boundary, cx)
                    .await
                    .map(decode_commit_diff),
            }
        })
    }

    pub fn file_history_changed_files(
        &mut self,
        paths: Vec<RepoPath>,
        commit_limit: usize,
    ) -> oneshot::Receiver<Result<Vec<FileHistoryChangedFileSets>>> {
        self.send_job(
            "file_history_changed_files",
            None,
            move |git_repo, _cx| async move {
                match git_repo {
                    RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                        backend
                            .file_history_changed_files(paths, commit_limit)
                            .await
                    }
                }
            },
        )
    }

    pub fn get_graph_data(
        &self,
        log_source: LogSource,
        log_order: LogOrder,
    ) -> Option<&InitialGitGraphData> {
        self.initial_graph_data.get(&(log_source, log_order))
    }

    pub fn search_commits(
        &mut self,
        log_source: LogSource,
        search_args: SearchCommitArgs,
        request_tx: async_channel::Sender<Oid>,
        cx: &mut Context<Self>,
    ) {
        let repository_state = self.repository_state.clone();
        let _repository_id = self.id;

        cx.background_spawn(async move {
            let repo_state = repository_state.await;

            match repo_state {
                Ok(RepositoryState::Local(LocalRepositoryState { backend, .. })) => {
                    backend
                        .search_commits(log_source, search_args, request_tx)
                        .await
                        .log_err();
                }

                Err(error) => {
                    log::error!("failed to get repository state for commit search: {error}");
                }
            };
        })
        .detach();
    }

    pub fn graph_data(
        &mut self,
        log_source: LogSource,
        log_order: LogOrder,
        range: Range<usize>,
        cx: &mut Context<Self>,
    ) -> GraphDataResponse<'_> {
        let initial_commit_data = self
            .initial_graph_data
            .entry((log_source.clone(), log_order))
            .or_insert_with(|| {
                let state = self.repository_state.clone();
                let log_source = log_source.clone();

                let fetch_task = cx.spawn(async move |repository, cx| {
                    let state = state.await;
                    let result = match state {
                        Ok(RepositoryState::Local(LocalRepositoryState { backend, .. })) => {
                            Self::local_git_graph_data(
                                repository.clone(),
                                backend,
                                log_source.clone(),
                                log_order,
                                cx,
                            )
                            .await
                        }
                        Err(e) => Err(SharedString::from(e)),
                    };

                    repository
                        .update(cx, |repository, cx| {
                            if let Some(data) = repository
                                .initial_graph_data
                                .get_mut(&(log_source.clone(), log_order))
                            {
                                match &result {
                                    Ok(()) => {
                                        cx.emit(RepositoryEvent::GraphEvent(
                                            (log_source.clone(), log_order),
                                            GitGraphEvent::FullyLoaded,
                                        ));
                                    }
                                    Err(fetch_task_error) => {
                                        data.subscribers.retain(|sender| {
                                            sender.try_send(Err(fetch_task_error.clone())).is_ok()
                                        });
                                        data.error = Some(fetch_task_error.clone());
                                        cx.emit(RepositoryEvent::GraphEvent(
                                            (log_source.clone(), log_order),
                                            GitGraphEvent::LoadingError,
                                        ));
                                    }
                                }
                                data.subscribers.clear();
                            } else {
                                debug_panic!(
                                    "This task would be dropped if this entry doesn't exist"
                                );
                            }
                        })
                        .log_err();
                });

                InitialGitGraphData {
                    fetch_task,
                    error: None,
                    commit_data: Vec::new(),
                    commit_oid_to_index: HashMap::default(),
                    subscribers: Vec::new(),
                }
            });

        let max_start = initial_commit_data.commit_data.len().saturating_sub(1);
        let max_end = initial_commit_data.commit_data.len();

        GraphDataResponse {
            commits: &initial_commit_data.commit_data
                [range.start.min(max_start)..range.end.min(max_end)],
            is_loading: !initial_commit_data.fetch_task.is_ready(),
            error: initial_commit_data.error.clone(),
        }
    }

    async fn append_initial_graph_commits(
        this: &WeakEntity<Self>,
        graph_data_key: &(LogSource, LogOrder),
        initial_graph_commit_data: Vec<Arc<InitialGraphCommitData>>,
        cx: &mut AsyncApp,
    ) {
        this.update(cx, |repository, cx| {
            let graph_data = repository
                .initial_graph_data
                .entry(graph_data_key.clone())
                .and_modify(|graph_data| {
                    if !graph_data.subscribers.is_empty() {
                        graph_data.subscribers.retain(|sender| {
                            sender
                                .try_send(Ok(initial_graph_commit_data.clone()))
                                .is_ok()
                        });
                    }

                    for commit_data in initial_graph_commit_data {
                        graph_data
                            .commit_oid_to_index
                            .insert(commit_data.sha, graph_data.commit_data.len());
                        graph_data.commit_data.push(commit_data);
                    }
                    cx.emit(RepositoryEvent::GraphEvent(
                        graph_data_key.clone(),
                        GitGraphEvent::CountUpdated(graph_data.commit_data.len()),
                    ));
                });

            match &graph_data {
                Entry::Occupied(_) => {}
                Entry::Vacant(_) => {
                    debug_panic!("This task should be dropped if data doesn't exist");
                }
            }
        })
        .log_err();
    }

    async fn local_git_graph_data(
        this: WeakEntity<Self>,
        backend: Arc<dyn GitRepository>,
        log_source: LogSource,
        log_order: LogOrder,
        cx: &mut AsyncApp,
    ) -> Result<(), SharedString> {
        let (request_tx, request_rx) =
            async_channel::unbounded::<Vec<Arc<InitialGraphCommitData>>>();

        let task = cx.background_executor().spawn({
            let log_source = log_source.clone();
            async move {
                backend
                    .initial_graph_data(log_source, log_order, request_tx)
                    .await
                    .map_err(|err| SharedString::from(err.to_string()))
            }
        });

        let graph_data_key = (log_source, log_order);

        while let Ok(initial_graph_commit_data) = request_rx.recv().await {
            Self::append_initial_graph_commits(
                &this,
                &graph_data_key,
                initial_graph_commit_data,
                cx,
            )
            .await;
        }

        task.await?;
        Ok(())
    }

    pub fn fetch_commit_data(
        &mut self,
        sha: Oid,
        await_result: bool,
        cx: &mut Context<Self>,
    ) -> &CommitDataState {
        if self.commit_data.contains_key(&sha) {
            let data = &self.commit_data[&sha];

            if let CommitDataState::Loading(None) = data
                && await_result
            {
                let (tx, rx) = oneshot::channel();
                self.commit_data
                    .insert(sha, CommitDataState::Loading(Some(rx.shared())));

                let handler = self.get_handler(cx);
                handler.completion_senders.insert(sha, tx);
            }

            return &self.commit_data[&sha];
        }

        let (state, completer) = if await_result {
            let (tx, rx) = oneshot::channel();
            (CommitDataState::Loading(Some(rx.shared())), Some(tx))
        } else {
            (CommitDataState::Loading(None), None)
        };

        self.commit_data.insert(sha, state);

        let handler = self.get_handler(cx);
        if let Some(tx) = completer {
            handler.completion_senders.insert(sha, tx);
        }
        let mut has_failed = false;
        if handler.commit_data_request.try_send(sha).is_ok() {
            handler.pending_requests.insert(sha);
        } else {
            has_failed = true;
            handler.completion_senders.remove(&sha);
            debug_assert!(
                matches!(
                    self.commit_data.remove(&sha),
                    Some(CommitDataState::Loading(_))
                ),
                "Commit data should still be loading when enqueueing the request fails"
            );
        }

        &self.commit_data.get(&sha).unwrap_or_else(|| {
            debug_assert!(!has_failed, "This should always be inserted");
            &CommitDataState::Loading(None)
        })
    }

    fn get_handler(&mut self, cx: &mut Context<Self>) -> &mut CommitDataHandler {
        if matches!(self.commit_data_handler, CommitDataHandlerState::Closed) {
            self.commit_data_handler =
                CommitDataHandlerState::Open(self.open_commit_data_handler(cx));
        }

        match &mut self.commit_data_handler {
            CommitDataHandlerState::Open(handler) => handler,
            CommitDataHandlerState::Closed => unreachable!(),
        }
    }

    fn open_commit_data_handler(&self, cx: &Context<Self>) -> CommitDataHandler {
        let state = self.repository_state.clone();
        let (result_tx, result_rx) = async_channel::bounded::<(Oid, CommitData)>(64);
        let (request_tx, request_rx) = async_channel::unbounded::<Oid>();

        let foreground_task = cx.spawn(async move |this, cx| {
            while let Ok((sha, commit_data)) = result_rx.recv().await {
                if this
                    .update(cx, |this, cx| {
                        let data = Arc::new(commit_data);
                        if let CommitDataHandlerState::Open(handler) = &mut this.commit_data_handler
                        {
                            handler.pending_requests.remove(&sha);
                            if let Some(completion_sender) = handler.completion_senders.remove(&sha)
                            {
                                completion_sender.send(data.clone()).ok();
                            }
                        }
                        this.commit_data.insert(sha, CommitDataState::Loaded(data));
                        cx.notify();
                    })
                    .is_err()
                {
                    break;
                }
            }
        });

        let background_executor = cx.background_executor().clone();
        cx.background_spawn(async move {
            match state.await {
                Ok(RepositoryState::Local(LocalRepositoryState { backend, .. })) => {
                    Self::local_commit_data_reader(
                        backend,
                        request_rx,
                        result_tx,
                        background_executor,
                    )
                    .await;
                }
                Err(error) => log::error!("failed to get repository state: {error}"),
            }
        })
        .detach();

        CommitDataHandler {
            _task: foreground_task,
            commit_data_request: request_tx,
            completion_senders: HashMap::default(),
            pending_requests: HashSet::default(),
        }
    }

    async fn local_commit_data_reader(
        backend: Arc<dyn GitRepository>,
        request_rx: smol::channel::Receiver<Oid>,
        result_tx: smol::channel::Sender<(Oid, CommitData)>,
        background_executor: BackgroundExecutor,
    ) {
        async fn receive_commit_data_request(
            request_rx: &smol::channel::Receiver<Oid>,
        ) -> Option<Oid> {
            if request_rx.is_closed() && request_rx.is_empty() {
                future::pending().await
            } else {
                request_rx.recv().await.ok()
            }
        }

        let reader = match backend.commit_data_reader() {
            Ok(reader) => reader,
            Err(error) => {
                log::error!("failed to create commit data reader: {error:?}");
                return;
            }
        };

        let read_commit_data = |sha| reader.read(sha).map(move |result| (sha, result));
        let mut read_futures = FuturesUnordered::new();

        loop {
            if read_futures.is_empty() {
                let timeout = background_executor.timer(Duration::from_secs(10));

                futures::select_biased! {
                    sha = futures::FutureExt::fuse(receive_commit_data_request(&request_rx)) => {
                        if let Some(sha) = sha {
                            read_futures.push(read_commit_data(sha));
                        }
                    }
                    _ = futures::FutureExt::fuse(timeout) => {
                        break;
                    }
                }
            }

            let next_read = read_futures.next().fuse();
            futures::pin_mut!(next_read);

            futures::select_biased! {
                result = next_read => {
                    let Some((sha, result)) = result else {
                        continue;
                    };

                    match result {
                        Ok(commit_data) => {
                            if result_tx.send((sha, commit_data)).await.is_err() {
                                return;
                            }
                        }
                        Err(error) => {
                            log::error!("failed to read commit data for {sha}: {error:?}");
                        }
                    }
                }
                sha = futures::FutureExt::fuse(receive_commit_data_request(&request_rx)) => {
                    if let Some(sha) = sha {
                        read_futures.push(read_commit_data(sha));
                    }
                }
            }
        }

        drop(result_tx);
    }

    fn buffer_store(&self, cx: &App) -> Option<Entity<BufferStore>> {
        Some(self.git_store.upgrade()?.read(cx).buffer_store.clone())
    }

    fn save_buffers<'a>(
        &self,
        entries: impl IntoIterator<Item = &'a RepoPath>,
        cx: &mut Context<Self>,
    ) -> Vec<Task<anyhow::Result<()>>> {
        let mut save_futures = Vec::new();
        if let Some(buffer_store) = self.buffer_store(cx) {
            buffer_store.update(cx, |buffer_store, cx| {
                for path in entries {
                    let Some(project_path) = self.repo_path_to_project_path(path, cx) else {
                        continue;
                    };
                    if let Some(buffer) = buffer_store.get_by_path(&project_path)
                        && buffer
                            .read(cx)
                            .file()
                            .is_some_and(|file| file.disk_state().exists())
                        && buffer.read(cx).has_unsaved_edits()
                    {
                        save_futures.push(buffer_store.save_buffer(buffer, cx));
                    }
                }
            })
        }
        save_futures
    }

    pub fn stage_entries(
        &mut self,
        entries: Vec<RepoPath>,
        cx: &mut Context<Self>,
    ) -> Task<anyhow::Result<()>> {
        self.stage_or_unstage_entries(true, entries, cx)
    }

    pub fn unstage_entries(
        &mut self,
        entries: Vec<RepoPath>,
        cx: &mut Context<Self>,
    ) -> Task<anyhow::Result<()>> {
        self.stage_or_unstage_entries(false, entries, cx)
    }

    fn stage_or_unstage_entries(
        &mut self,
        stage: bool,
        entries: Vec<RepoPath>,
        cx: &mut Context<Self>,
    ) -> Task<anyhow::Result<()>> {
        if entries.is_empty() {
            return Task::ready(Ok(()));
        }
        let Some(git_store) = self.git_store.upgrade() else {
            return Task::ready(Ok(()));
        };
        let _id = self.id;
        let save_tasks = self.save_buffers(&entries, cx);
        let paths = entries
            .iter()
            .map(|p| p.as_unix_str())
            .collect::<Vec<_>>()
            .join(" ");
        let status = if stage {
            format!("git add {paths}")
        } else {
            format!("git reset {paths}")
        };
        let job_key = GitJobKey::WriteIndex(entries.clone());

        self.spawn_job_with_tracking(
            entries.clone(),
            if stage {
                pending_op::GitStatus::Staged
            } else {
                pending_op::GitStatus::Unstaged
            },
            cx,
            async move |this, cx| {
                for save_task in save_tasks {
                    save_task.await?;
                }

                this.update(cx, |this, cx| {
                    let weak_this = cx.weak_entity();
                    this.send_keyed_job(
                        "stage_or_unstage_entries",
                        Some(job_key),
                        Some(status.into()),
                        move |git_repo, mut cx| async move {
                            let hunk_staging_operation_counts = weak_this
                                .update(&mut cx, |this, cx| {
                                    let mut hunk_staging_operation_counts = HashMap::default();
                                    for path in &entries {
                                        let Some(project_path) =
                                            this.repo_path_to_project_path(path, cx)
                                        else {
                                            continue;
                                        };
                                        let Some(buffer) = git_store
                                            .read(cx)
                                            .buffer_store
                                            .read(cx)
                                            .get_by_path(&project_path)
                                        else {
                                            continue;
                                        };
                                        let Some(diff_state) = git_store
                                            .read(cx)
                                            .diffs
                                            .get(&buffer.read(cx).remote_id())
                                            .cloned()
                                        else {
                                            continue;
                                        };
                                        let buffer_snapshot = buffer.read(cx).text_snapshot();
                                        let hunk_staging_operation_count =
                                            diff_state.update(cx, |diff_state, cx| {
                                                diff_state
                                                    .mark_whole_file_stage_or_unstage_pending(
                                                        stage,
                                                        &buffer_snapshot,
                                                        cx,
                                                    );
                                                diff_state.hunk_staging_operation_count += 1;
                                                diff_state.hunk_staging_operation_count
                                            });
                                        hunk_staging_operation_counts.insert(
                                            diff_state.downgrade(),
                                            hunk_staging_operation_count,
                                        );
                                    }
                                    hunk_staging_operation_counts
                                })
                                .unwrap_or_default();

                            let result = match git_repo {
                                RepositoryState::Local(LocalRepositoryState {
                                    backend,
                                    environment,
                                    ..
                                }) => {
                                    if stage {
                                        backend.stage_paths(entries, environment.clone()).await
                                    } else {
                                        backend.unstage_paths(entries, environment.clone()).await
                                    }
                                }
                            };

                            for (diff_state, hunk_staging_operation_count) in
                                hunk_staging_operation_counts
                            {
                                diff_state
                                    .update(&mut cx, |diff_state, cx| {
                                        if result.is_ok() {
                                            diff_state.hunk_staging_operation_count_as_of_write =
                                                hunk_staging_operation_count;
                                        } else {
                                            diff_state.clear_pending_hunks(cx);
                                        }
                                    })
                                    .ok();
                            }

                            result
                        },
                    )
                })?
                .await?
            },
        )
    }

    pub fn stage_all(&mut self, cx: &mut Context<Self>) -> Task<anyhow::Result<()>> {
        let snapshot = self.snapshot.clone();
        let pending_ops = self.pending_ops.clone();
        let to_stage = cx.background_spawn(async move {
            snapshot
                .status()
                .filter_map(|entry| {
                    if let Some(ops) = pending_ops
                        .get(&PathKey(entry.repo_path.as_ref().clone()), ())
                        .filter(|ops| !ops.last_op_errored())
                    {
                        if ops.staging() || ops.staged() {
                            None
                        } else {
                            Some(entry.repo_path)
                        }
                    } else if entry.status.staging().is_fully_staged() {
                        None
                    } else {
                        Some(entry.repo_path)
                    }
                })
                .collect()
        });

        cx.spawn(async move |this, cx| {
            let to_stage = to_stage.await;
            this.update(cx, |this, cx| {
                this.stage_or_unstage_entries(true, to_stage, cx)
            })?
            .await
        })
    }

    pub fn unstage_all(&mut self, cx: &mut Context<Self>) -> Task<anyhow::Result<()>> {
        let snapshot = self.snapshot.clone();
        let pending_ops = self.pending_ops.clone();
        let to_unstage = cx.background_spawn(async move {
            snapshot
                .status()
                .filter_map(|entry| {
                    if let Some(ops) = pending_ops
                        .get(&PathKey(entry.repo_path.as_ref().clone()), ())
                        .filter(|ops| !ops.last_op_errored())
                    {
                        if !ops.staging() && !ops.staged() {
                            None
                        } else {
                            Some(entry.repo_path)
                        }
                    } else if entry.status.staging().is_fully_unstaged() {
                        None
                    } else {
                        Some(entry.repo_path)
                    }
                })
                .collect()
        });

        cx.spawn(async move |this, cx| {
            let to_unstage = to_unstage.await;
            this.update(cx, |this, cx| {
                this.stage_or_unstage_entries(false, to_unstage, cx)
            })?
            .await
        })
    }

    pub fn stash_all(
        &mut self,
        message: Option<String>,
        cx: &mut Context<Self>,
    ) -> Task<anyhow::Result<()>> {
        let to_stash = self.cached_status().map(|entry| entry.repo_path).collect();

        self.stash_entries(to_stash, message, cx)
    }

    pub fn stash_tracked(
        &mut self,
        message: Option<String>,
        cx: &mut Context<Self>,
    ) -> Task<anyhow::Result<()>> {
        // `is_created` rather than `is_untracked`, so that staging a new file does not
        // move it out of the untracked set: `git add` makes it `Tracked { Added }`, but
        // the panel still lists it under "Untracked" and the user expects it left alone.
        let to_stash: Vec<_> = self
            .cached_status()
            .filter(|entry| !entry.status.is_created())
            .map(|entry| entry.repo_path)
            .collect();

        // An empty pathspec would make `git stash push` stash everything.
        if to_stash.is_empty() {
            return Task::ready(Ok(()));
        }
        self.stash_entries(to_stash, message, cx)
    }

    pub fn stash_entries(
        &mut self,
        entries: Vec<RepoPath>,
        message: Option<String>,
        cx: &mut Context<Self>,
    ) -> Task<anyhow::Result<()>> {
        let _id = self.id;

        cx.spawn(async move |this, cx| {
            this.update(cx, |this, _| {
                this.send_job("stash_entries", None, move |git_repo, _cx| async move {
                    match git_repo {
                        RepositoryState::Local(LocalRepositoryState {
                            backend,
                            environment,
                            ..
                        }) => backend.stash_paths(entries, message, environment).await,
                    }
                })
            })?
            .await??;
            Ok(())
        })
    }

    pub fn stash_staged(
        &mut self,
        message: Option<String>,
        cx: &mut Context<Self>,
    ) -> Task<anyhow::Result<()>> {
        let _id = self.id;

        cx.spawn(async move |this, cx| {
            this.update(cx, |this, _| {
                this.send_job("stash_staged", None, move |git_repo, _cx| async move {
                    match git_repo {
                        RepositoryState::Local(LocalRepositoryState {
                            backend,
                            environment,
                            ..
                        }) => backend.stash_staged(message, environment).await,
                    }
                })
            })?
            .await??;
            Ok(())
        })
    }

    pub fn stash_pop(
        &mut self,
        index: Option<usize>,
        cx: &mut Context<Self>,
    ) -> Task<anyhow::Result<()>> {
        let _id = self.id;
        cx.spawn(async move |this, cx| {
            this.update(cx, |this, _| {
                this.send_job("stash_pop", None, move |git_repo, _cx| async move {
                    match git_repo {
                        RepositoryState::Local(LocalRepositoryState {
                            backend,
                            environment,
                            ..
                        }) => backend.stash_pop(index, environment).await,
                    }
                })
            })?
            .await??;
            Ok(())
        })
    }

    pub fn stash_apply(
        &mut self,
        index: Option<usize>,
        cx: &mut Context<Self>,
    ) -> Task<anyhow::Result<()>> {
        let _id = self.id;
        cx.spawn(async move |this, cx| {
            this.update(cx, |this, _| {
                this.send_job("stash_apply", None, move |git_repo, _cx| async move {
                    match git_repo {
                        RepositoryState::Local(LocalRepositoryState {
                            backend,
                            environment,
                            ..
                        }) => backend.stash_apply(index, environment).await,
                    }
                })
            })?
            .await??;
            Ok(())
        })
    }

    pub fn add_path_to_gitignore(
        &mut self,
        repo_path: &RepoPath,
        is_dir: bool,
    ) -> oneshot::Receiver<Result<()>> {
        let _id = self.id;
        let work_dir = self.snapshot.work_directory_abs_path.clone();
        let path_display = repo_path.as_ref().display(PathStyle::Unix);
        let _path = repo_path.as_unix_str().to_owned();
        let file_path_str = if is_dir {
            format!("{}/", path_display)
        } else {
            path_display.to_string()
        };

        self.send_job(
            "add_path_to_gitignore",
            None,
            move |git_repo, _cx| async move {
                match git_repo {
                    RepositoryState::Local(LocalRepositoryState { fs, .. }) => {
                        append_pattern_to_ignore_file(
                            fs,
                            work_dir.join(".gitignore"),
                            file_path_str,
                        )
                        .await
                    }
                }
            },
        )
    }

    pub fn add_path_to_git_info_exclude(
        &mut self,
        repo_path: &RepoPath,
        is_dir: bool,
    ) -> oneshot::Receiver<Result<()>> {
        let _id = self.id;
        let repository_dir = self.snapshot.repository_dir_abs_path.clone();
        let path_display = repo_path.as_ref().display(PathStyle::Unix);
        let _path = repo_path.as_unix_str().to_owned();
        let file_path_str = if is_dir {
            format!("{}/", path_display)
        } else {
            path_display.to_string()
        };

        self.send_job(
            "add_path_to_git_info_exclude",
            None,
            move |git_repo, _cx| async move {
                match git_repo {
                    RepositoryState::Local(LocalRepositoryState { fs, .. }) => {
                        append_pattern_to_ignore_file(
                            fs,
                            repository_dir.join(git::REPO_EXCLUDE),
                            file_path_str,
                        )
                        .await
                    }
                }
            },
        )
    }

    pub fn stash_drop(
        &mut self,
        index: Option<usize>,
        cx: &mut Context<Self>,
    ) -> oneshot::Receiver<anyhow::Result<()>> {
        let _id = self.id;
        let this = cx.weak_entity();
        self.send_job("stash_drop", None, move |git_repo, mut cx| async move {
            match git_repo {
                RepositoryState::Local(LocalRepositoryState {
                    backend,
                    environment,
                    ..
                }) => {
                    // TODO would be nice to not have to do this manually
                    let result = backend.stash_drop(index, environment).await;
                    if result.is_ok()
                        && let Ok(stash_entries) = backend.stash_entries().await
                    {
                        let _snapshot = this.update(&mut cx, |this, cx| {
                            this.snapshot.stash_entries = stash_entries;
                            cx.emit(RepositoryEvent::StashEntriesChanged);
                            this.snapshot.clone()
                        })?;
                    }

                    result
                }
            }
        })
    }

    // Kept for wire compatibility: older remote clients run the pre-commit hook explicitly
    // via `proto::RunGitHook` before committing. New code lets `git commit` run hooks itself.
    //
    // TODO: remove together with `proto::RunGitHook` once all supported peers commit without
    // sending it (see the deprecation note on the message in git.proto).
    pub fn run_hook(&mut self, hook: RunHook, _cx: &mut App) -> oneshot::Receiver<Result<()>> {
        let _id = self.id;
        self.send_job(
            "run_hook",
            Some(format!("git hook {}", hook.as_str()).into()),
            move |git_repo, _cx| async move {
                match git_repo {
                    RepositoryState::Local(LocalRepositoryState {
                        backend,
                        environment,
                        ..
                    }) => backend.run_hook(hook, environment.clone()).await,
                }
            },
        )
    }

    pub fn commit(
        &mut self,
        message: SharedString,
        name_and_email: Option<(SharedString, SharedString)>,
        options: CommitOptions,
        askpass: AskPassDelegate,
        _cx: &mut App,
    ) -> oneshot::Receiver<Result<()>> {
        let _id = self.id;

        self.send_job(
            "commit",
            Some("git commit".into()),
            move |git_repo, _cx| async move {
                match git_repo {
                    RepositoryState::Local(LocalRepositoryState {
                        backend,
                        environment,
                        ..
                    }) => {
                        backend
                            .commit(message, name_and_email, options, askpass, environment)
                            .await
                    }
                }
            },
        )
    }

    async fn refresh_branch_list(
        this: &WeakEntity<Self>,
        backend: Arc<dyn GitRepository>,
        cx: &mut AsyncApp,
    ) -> Result<()> {
        let branches_scan = backend.branches().await?;
        let branch_list_error = branches_scan.error;
        let branch_list: Arc<[Branch]> = branches_scan.branches.into();
        let branch = branch_list.iter().find(|branch| branch.is_head).cloned();
        log::info!("head branch after scan is {branch:?}");
        let _snapshot = this.update(cx, |this, cx| {
            let head_changed = branch != this.snapshot.branch;
            let branch_list_changed = *branch_list != *this.snapshot.branch_list;
            let branch_list_error_changed = this.snapshot.branch_list_error != branch_list_error;
            this.snapshot.branch = branch;
            this.snapshot.branch_list = branch_list;
            this.snapshot.branch_list_error = branch_list_error;
            if head_changed {
                cx.emit(RepositoryEvent::HeadChanged);
            }
            if branch_list_changed || branch_list_error_changed {
                cx.emit(RepositoryEvent::BranchListChanged);
            }
            this.snapshot.clone()
        })?;
        Ok(())
    }

    pub fn unshallow_state(&self) -> UnshallowState {
        self.unshallow_state
    }

    pub fn fetch_unshallow(
        &mut self,
        askpass: AskPassDelegate,
        cx: &mut Context<Self>,
    ) -> oneshot::Receiver<Result<RemoteCommandOutput>> {
        let receiver = self.fetch(FetchOptions::Unshallow, askpass, cx);
        self.unshallow_state = UnshallowState::InProgress;
        cx.notify();
        let (tx, rx) = oneshot::channel();
        cx.spawn(async move |this, cx| {
            let result = receiver.await;
            let succeeded = matches!(&result, Ok(Ok(_)));
            this.update(cx, |this, cx| {
                this.unshallow_state = if succeeded {
                    UnshallowState::Unshallowed
                } else {
                    UnshallowState::Idle
                };
                cx.notify();
            })
            .ok();
            if let Ok(result) = result {
                tx.send(result).ok();
            }
        })
        .detach();
        rx
    }

    pub fn fetch(
        &mut self,
        fetch_options: FetchOptions,
        askpass: AskPassDelegate,
        cx: &mut Context<Self>,
    ) -> oneshot::Receiver<Result<RemoteCommandOutput>> {
        let this = cx.weak_entity();
        self.send_job(
            "fetch",
            Some("git fetch".into()),
            move |git_repo, mut cx| async move {
                match git_repo {
                    RepositoryState::Local(LocalRepositoryState {
                        backend,
                        environment,
                        ..
                    }) => {
                        let result = backend
                            .fetch(fetch_options, askpass, environment, cx.clone())
                            .await;
                        if result.is_ok() {
                            Self::refresh_branch_list(&this, backend, &mut cx).await?;
                        }
                        result
                    }
                }
            },
        )
    }

    pub fn push(
        &mut self,
        branch: SharedString,
        remote_branch: SharedString,
        remote: SharedString,
        options: Option<PushOptions>,
        askpass: AskPassDelegate,
        cx: &mut Context<Self>,
    ) -> oneshot::Receiver<Result<RemoteCommandOutput>> {
        let args = options
            .map(|option| match option {
                PushOptions::SetUpstream => " --set-upstream",
                PushOptions::Force => " --force-with-lease",
            })
            .unwrap_or("");

        let this = cx.weak_entity();
        self.send_job(
            "push",
            Some(format!("git push {} {} {}:{}", args, remote, branch, remote_branch).into()),
            move |git_repo, mut cx| async move {
                match git_repo {
                    RepositoryState::Local(LocalRepositoryState {
                        backend,
                        environment,
                        ..
                    }) => {
                        let result = backend
                            .push(
                                branch.to_string(),
                                remote_branch.to_string(),
                                remote.to_string(),
                                options,
                                askpass,
                                environment.clone(),
                                cx.clone(),
                            )
                            .await;
                        // TODO would be nice to not have to do this manually
                        if result.is_ok() {
                            Self::refresh_branch_list(&this, backend, &mut cx).await?;
                        }
                        result
                    }
                }
            },
        )
    }

    pub fn pull(
        &mut self,
        branch: Option<SharedString>,
        remote: SharedString,
        rebase: bool,
        askpass: AskPassDelegate,
        _cx: &mut App,
    ) -> oneshot::Receiver<Result<RemoteCommandOutput>> {
        let mut status = "git pull".to_string();
        if rebase {
            status.push_str(" --rebase");
        }
        status.push_str(&format!(" {}", remote));
        if let Some(b) = &branch {
            status.push_str(&format!(" {}", b));
        }

        self.send_job(
            "pull",
            Some(status.into()),
            move |git_repo, cx| async move {
                match git_repo {
                    RepositoryState::Local(LocalRepositoryState {
                        backend,
                        environment,
                        ..
                    }) => {
                        backend
                            .pull(
                                branch.as_ref().map(|b| b.to_string()),
                                remote.to_string(),
                                rebase,
                                askpass,
                                environment.clone(),
                                cx,
                            )
                            .await
                    }
                }
            },
        )
    }

    fn spawn_set_index_text_job(
        &mut self,
        path: RepoPath,
        content: Option<String>,
        encoding: &'static encoding_rs::Encoding,
        has_bom: bool,
        hunk_staging_operation_count: Option<usize>,
        cx: &mut Context<Self>,
    ) -> oneshot::Receiver<anyhow::Result<()>> {
        let _id = self.id;
        let this = cx.weak_entity();
        let git_store = self.git_store.clone();
        let abs_path = self.snapshot.repo_path_to_abs_path(&path);
        self.send_keyed_job(
            "spawn_set_index_text_job",
            Some(GitJobKey::WriteIndex(vec![path.clone()])),
            None,
            move |git_repo, mut cx| async move {
                log::debug!(
                    "start updating index text for buffer {}",
                    path.as_unix_str()
                );

                match git_repo {
                    RepositoryState::Local(LocalRepositoryState {
                        fs,
                        backend,
                        environment,
                        ..
                    }) => {
                        let executable = match fs.metadata(&abs_path).await {
                            Ok(Some(meta)) => meta.is_executable,
                            Ok(None) => false,
                            Err(_err) => false,
                        };
                        let content =
                            content.map(|content| encode_text(content, encoding, has_bom));
                        backend
                            .set_index_text(path.clone(), content, environment.clone(), executable)
                            .await?;
                    }
                }
                log::debug!(
                    "finish updating index text for buffer {}",
                    path.as_unix_str()
                );

                if let Some(hunk_staging_operation_count) = hunk_staging_operation_count {
                    let project_path = this
                        .read_with(&cx, |this, cx| this.repo_path_to_project_path(&path, cx))
                        .ok()
                        .flatten();
                    git_store
                        .update(&mut cx, |git_store, cx| {
                            let buffer_id = git_store
                                .buffer_store
                                .read(cx)
                                .get_by_path(&project_path?)?
                                .read(cx)
                                .remote_id();
                            let diff_state = git_store.diffs.get(&buffer_id)?;
                            diff_state.update(cx, |diff_state, _| {
                                diff_state.hunk_staging_operation_count_as_of_write =
                                    hunk_staging_operation_count;
                            });
                            Some(())
                        })
                        .context("Git store dropped")?;
                }
                Ok(())
            },
        )
    }

    pub fn create_remote(
        &mut self,
        remote_name: String,
        remote_url: String,
    ) -> oneshot::Receiver<Result<()>> {
        let _id = self.id;
        self.send_job(
            "create_remote",
            Some(format!("git remote add {remote_name} {remote_url}").into()),
            move |repo, _cx| async move {
                match repo {
                    RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                        backend.create_remote(remote_name, remote_url).await
                    }
                }
            },
        )
    }

    pub fn remove_remote(&mut self, remote_name: String) -> oneshot::Receiver<Result<()>> {
        let _id = self.id;
        self.send_job(
            "remove_remote",
            Some(format!("git remove remote {remote_name}").into()),
            move |repo, _cx| async move {
                match repo {
                    RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                        backend.remove_remote(remote_name).await
                    }
                }
            },
        )
    }

    pub fn get_remotes(
        &mut self,
        branch_name: Option<String>,
        is_push: bool,
    ) -> oneshot::Receiver<Result<Vec<Remote>>> {
        let _id = self.id;
        self.send_job("get_remotes", None, move |repo, _cx| async move {
            match repo {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                    let remote = if let Some(branch_name) = branch_name {
                        if is_push {
                            backend.get_push_remote(branch_name).await?
                        } else {
                            backend.get_branch_remote(branch_name).await?
                        }
                    } else {
                        None
                    };

                    match remote {
                        Some(remote) => Ok(vec![remote]),
                        None => backend.get_all_remotes().await,
                    }
                }
            }
        })
    }

    pub fn remote_urls(&mut self) -> oneshot::Receiver<Result<HashMap<String, String>>> {
        let _id = self.id;
        self.send_job("remote_urls", None, move |repo, _cx| async move {
            match repo {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                    Ok(backend.remote_urls().await)
                }
            }
        })
    }

    pub fn branches(&mut self) -> oneshot::Receiver<Result<BranchesScanResult>> {
        let _id = self.id;
        self.send_job("branches", None, move |repo, _| async move {
            match repo {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                    backend.branches().await
                }
            }
        })
    }

    /// If this is a linked worktree (*NOT* the main checkout of a repository),
    /// returns the path for the linked worktree.
    ///
    /// Returns None if this is the main checkout.
    pub fn linked_worktree_path(&self) -> Option<&Arc<Path>> {
        self.snapshot
            .is_linked_worktree()
            .then_some(&self.work_directory_abs_path)
    }

    fn linked_worktree_anchor_path(&self) -> &Path {
        self.snapshot
            .main_worktree_abs_path()
            .or_else(|| repo_identity_path_if_local(&self.common_dir_abs_path, self.path_style))
            .unwrap_or(self.common_dir_abs_path.as_ref())
    }

    pub fn path_for_new_linked_worktree(
        &self,
        branch_name: &str,
        worktree_directory_setting: &str,
    ) -> Result<PathBuf> {
        let repository_anchor = self.linked_worktree_anchor_path();
        let project_name = repository_anchor
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow!("git repo must have a directory name"))?;
        let directory = worktrees_directory_for_repo(
            repository_anchor,
            worktree_directory_setting,
            self.path_style,
        )?;
        let directory = self.path_style.join_path(&directory, branch_name)?;
        self.path_style.join_path(&directory, project_name)
    }

    pub fn worktrees(&mut self) -> oneshot::Receiver<Result<Vec<GitWorktree>>> {
        let _id = self.id;
        self.send_job("worktrees", None, move |repo, _| async move {
            match repo {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                    backend.worktrees().await
                }
            }
        })
    }

    pub fn create_worktree(
        &mut self,
        target: CreateWorktreeTarget,
        path: PathBuf,
    ) -> oneshot::Receiver<Result<()>> {
        let _id = self.id;
        let job_description = match target.branch_name() {
            Some(branch_name) => format!("git worktree add: {branch_name}"),
            None => "git worktree add (detached)".to_string(),
        };
        self.send_job(
            "create_worktree",
            Some(job_description.into()),
            move |repo, _cx| async move {
                match repo {
                    RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                        backend.create_worktree(target, path).await
                    }
                }
            },
        )
    }

    /// Returns the creation time of a linked worktree's git metadata
    /// directory. See [`GitRepository::worktree_created_at`]. For remote
    /// projects the stat runs on the remote host, where the worktree's
    /// filesystem lives.
    pub fn worktree_created_at(
        &mut self,
        worktree_path: PathBuf,
    ) -> oneshot::Receiver<Result<Option<SystemTime>>> {
        let _id = self.id;
        self.send_job("worktree_created_at", None, move |repo, _cx| async move {
            match repo {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                    backend.worktree_created_at(worktree_path).await
                }
            }
        })
    }

    pub fn create_worktree_detached(
        &mut self,
        path: PathBuf,
        commit: String,
    ) -> oneshot::Receiver<Result<()>> {
        self.create_worktree(
            CreateWorktreeTarget::Detached {
                base_sha: Some(commit),
            },
            path,
        )
    }

    pub fn checkout_branch_in_worktree(
        &mut self,
        branch_name: String,
        worktree_path: PathBuf,
        create: bool,
    ) -> oneshot::Receiver<Result<()>> {
        let description = if create {
            format!("git checkout -b {branch_name}")
        } else {
            format!("git checkout {branch_name}")
        };
        self.send_job(
            "checkout_branch_in_worktree",
            Some(description.into()),
            move |repo, _cx| async move {
                match repo {
                    RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                        backend
                            .checkout_branch_in_worktree(branch_name, worktree_path, create)
                            .await
                    }
                }
            },
        )
    }

    pub fn head_sha(&mut self) -> oneshot::Receiver<Result<Option<String>>> {
        let _id = self.id;
        self.send_job("head_sha", None, move |repo, _cx| async move {
            match repo {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                    Ok(backend.head_sha().await)
                }
            }
        })
    }

    fn edit_ref(
        &mut self,
        ref_name: String,
        commit: Option<String>,
    ) -> oneshot::Receiver<Result<()>> {
        let _id = self.id;
        self.send_job("edit_ref", None, move |repo, _cx| async move {
            match repo {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => match commit {
                    Some(commit) => backend.update_ref(ref_name, commit).await,
                    None => backend.delete_ref(ref_name).await,
                },
            }
        })
    }

    pub fn update_ref(
        &mut self,
        ref_name: String,
        commit: String,
    ) -> oneshot::Receiver<Result<()>> {
        self.edit_ref(ref_name, Some(commit))
    }

    pub fn delete_ref(&mut self, ref_name: String) -> oneshot::Receiver<Result<()>> {
        self.edit_ref(ref_name, None)
    }

    pub fn repair_worktrees(&mut self) -> oneshot::Receiver<Result<()>> {
        let _id = self.id;
        self.send_job("repair_worktrees", None, move |repo, _cx| async move {
            match repo {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                    backend.repair_worktrees().await
                }
            }
        })
    }

    pub fn create_archive_checkpoint(&mut self) -> oneshot::Receiver<Result<(String, String)>> {
        let _id = self.id;
        self.send_job(
            "create_archive_checkpoint",
            None,
            move |repo, _cx| async move {
                match repo {
                    RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                        backend.create_archive_checkpoint().await
                    }
                }
            },
        )
    }

    pub fn restore_archive_checkpoint(
        &mut self,
        staged_sha: String,
        unstaged_sha: String,
    ) -> oneshot::Receiver<Result<()>> {
        let _id = self.id;
        self.send_job(
            "restore_archive_checkpoint",
            None,
            move |repo, _cx| async move {
                match repo {
                    RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                        backend
                            .restore_archive_checkpoint(staged_sha, unstaged_sha)
                            .await
                    }
                }
            },
        )
    }

    pub fn remove_worktree(&mut self, path: PathBuf, force: bool) -> oneshot::Receiver<Result<()>> {
        let _id = self.id;
        let repository_anchor_path: Arc<Path> = self.linked_worktree_anchor_path().into();
        self.send_job(
            "remove_worktree",
            Some(format!("git worktree remove: {}", path.display()).into()),
            move |repo, cx| async move {
                match repo {
                    RepositoryState::Local(LocalRepositoryState { backend, fs, .. }) => {
                        // When forcing, delete the worktree directory ourselves before
                        // invoking git. `git worktree remove` can remove the admin
                        // metadata in `.git/worktrees/<name>` but fail to delete the
                        // working directory (it continues past directory-removal errors),
                        // leaving an orphaned folder on disk. Deleting first guarantees
                        // the directory is gone, and `git worktree remove --force`
                        // tolerates a missing working tree while cleaning up the admin
                        // entry. We keep this inside the `Local` arm so that for remote
                        // projects the deletion runs on the remote machine (where the
                        // `GitRemoveWorktree` RPC is handled against the local repo on
                        // the headless server) using its own filesystem.
                        //
                        // After a successful removal, also delete any empty ancestor
                        // directories between the worktree path and the configured
                        // base directory used when creating linked worktrees.
                        //
                        // Non-force removals are left untouched before git runs:
                        // `git worktree remove` must see the dirty working tree to
                        // refuse the operation.
                        if force {
                            fs.remove_dir(
                                &path,
                                RemoveOptions {
                                    recursive: true,
                                    ignore_if_not_exists: true,
                                },
                            )
                            .await
                            .with_context(|| {
                                format!("failed to delete worktree directory '{}'", path.display())
                            })?;
                        }

                        backend.remove_worktree(path.clone(), force).await?;

                        let managed_worktree_base = cx.update(|cx| {
                            let setting = &ProjectSettings::get_global(cx).git.worktree_directory;
                            worktrees_directory_for_repo(
                                &repository_anchor_path,
                                setting,
                                PathStyle::local(),
                            )
                            .log_err()
                        });

                        if let Some(managed_worktree_base) = managed_worktree_base {
                            remove_empty_managed_worktree_ancestors(
                                fs.as_ref(),
                                &path,
                                &managed_worktree_base,
                            )
                            .await;
                        }

                        Ok(())
                    }
                }
            },
        )
    }

    pub fn rename_worktree(
        &mut self,
        old_path: PathBuf,
        new_path: PathBuf,
    ) -> oneshot::Receiver<Result<()>> {
        let _id = self.id;
        self.send_job(
            "rename_worktree",
            Some(format!("git worktree move: {}", old_path.display()).into()),
            move |repo, _cx| async move {
                match repo {
                    RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                        backend.rename_worktree(old_path, new_path).await
                    }
                }
            },
        )
    }

    pub fn default_branch(
        &mut self,
        include_remote_name: bool,
    ) -> oneshot::Receiver<Result<Option<SharedString>>> {
        let _id = self.id;
        self.send_job("default_branch", None, move |repo, _| async move {
            match repo {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                    backend.default_branch(include_remote_name).await
                }
            }
        })
    }

    pub fn diff_tree(
        &mut self,
        diff_type: DiffTreeType,
        _cx: &App,
    ) -> oneshot::Receiver<Result<TreeDiff>> {
        let _repository_id = self.snapshot.id;
        self.send_job("diff_tree", None, move |repo, _cx| async move {
            match repo {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                    backend.diff_tree(diff_type).await
                }
            }
        })
    }

    pub fn diff(&mut self, diff_type: DiffType, _cx: &App) -> oneshot::Receiver<Result<String>> {
        let _id = self.id;
        self.send_job("diff", None, move |repo, _cx| async move {
            match repo {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                    backend.diff(diff_type).await
                }
            }
        })
    }

    pub fn create_branch(
        &mut self,
        branch_name: String,
        base_branch: Option<String>,
    ) -> oneshot::Receiver<Result<()>> {
        let _id = self.id;
        let status_msg = if let Some(ref base) = base_branch {
            format!("git switch -c {branch_name} {base}").into()
        } else {
            format!("git switch -c {branch_name}").into()
        };
        self.send_job(
            "create_branch",
            Some(status_msg),
            move |repo, _cx| async move {
                match repo {
                    RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                        backend.create_branch(branch_name, base_branch).await
                    }
                }
            },
        )
    }

    pub fn change_branch(&mut self, branch_name: String) -> oneshot::Receiver<Result<()>> {
        let _id = self.id;
        self.send_job(
            "change_branch",
            Some(format!("git switch {branch_name}").into()),
            move |repo, _cx| async move {
                match repo {
                    RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                        backend.change_branch(branch_name).await
                    }
                }
            },
        )
    }

    pub fn delete_branch(
        &mut self,
        is_remote: bool,
        branch_name: String,
        force: bool,
    ) -> oneshot::Receiver<Result<()>> {
        let _id = self.id;
        let flag = delete_branch_flag(is_remote, force);
        self.send_job(
            "delete_branch",
            Some(format!("git branch {flag} {branch_name}").into()),
            move |repo, _cx| async move {
                match repo {
                    RepositoryState::Local(state) => {
                        state
                            .backend
                            .delete_branch(is_remote, branch_name, force)
                            .await
                    }
                }
            },
        )
    }

    pub fn rename_branch(
        &mut self,
        branch: String,
        new_name: String,
    ) -> oneshot::Receiver<Result<()>> {
        let _id = self.id;
        self.send_job(
            "rename_branch",
            Some(format!("git branch -m {branch} {new_name}").into()),
            move |repo, _cx| async move {
                match repo {
                    RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                        backend.rename_branch(branch, new_name).await
                    }
                }
            },
        )
    }

    pub fn check_for_pushed_commits(&mut self) -> oneshot::Receiver<Result<Vec<SharedString>>> {
        let _id = self.id;
        self.send_job(
            "check_for_pushed_commits",
            None,
            move |repo, _cx| async move {
                match repo {
                    RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                        backend.check_for_pushed_commit().await
                    }
                }
            },
        )
    }

    pub fn checkpoint(&mut self) -> oneshot::Receiver<Result<GitRepositoryCheckpoint>> {
        let _id = self.id;
        self.send_job("checkpoint", None, move |repo, _cx| async move {
            match repo {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                    backend.checkpoint().await
                }
            }
        })
    }

    pub fn restore_checkpoint(
        &mut self,
        checkpoint: GitRepositoryCheckpoint,
    ) -> oneshot::Receiver<Result<()>> {
        let _id = self.id;
        self.send_job("restore_checkpoint", None, move |repo, _cx| async move {
            match repo {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                    backend.restore_checkpoint(checkpoint).await
                }
            }
        })
    }

    pub fn compare_checkpoints(
        &mut self,
        left: GitRepositoryCheckpoint,
        right: GitRepositoryCheckpoint,
    ) -> oneshot::Receiver<Result<bool>> {
        let _id = self.id;
        self.send_job("compare_checkpoints", None, move |repo, _cx| async move {
            match repo {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                    backend.compare_checkpoints(left, right).await
                }
            }
        })
    }

    pub fn diff_checkpoints(
        &mut self,
        base_checkpoint: GitRepositoryCheckpoint,
        target_checkpoint: GitRepositoryCheckpoint,
    ) -> oneshot::Receiver<Result<String>> {
        let _id = self.id;
        self.send_job("diff_checkpoints", None, move |repo, _cx| async move {
            match repo {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                    backend
                        .diff_checkpoints(base_checkpoint, target_checkpoint)
                        .await
                }
            }
        })
    }

    fn clear_pending_ops(&mut self, cx: &mut Context<Self>) {
        let updated = SumTree::from_iter(
            self.pending_ops.iter().filter_map(|ops| {
                let inner_ops: Vec<PendingOp> =
                    ops.ops.iter().filter(|op| op.running()).cloned().collect();
                if inner_ops.is_empty() {
                    None
                } else {
                    Some(PendingOps {
                        repo_path: ops.repo_path.clone(),
                        ops: inner_ops,
                    })
                }
            }),
            (),
        );

        if updated != self.pending_ops {
            cx.emit(RepositoryEvent::PendingOpsChanged {
                pending_ops: self.pending_ops.clone(),
            })
        }

        self.pending_ops = updated;
    }

    fn schedule_scan(&mut self, cx: &mut Context<Self>) {
        let this = cx.weak_entity();
        let _ = self.send_keyed_job(
            "schedule_scan",
            Some(GitJobKey::ReloadGitState),
            None,
            |state, mut cx| async move {
                log::debug!("run scheduled git status scan");

                let Some(this) = this.upgrade() else {
                    return Ok(());
                };
                let RepositoryState::Local(LocalRepositoryState { backend, .. }) = state else {
                    bail!("not a local repository")
                };
                compute_snapshot(this.clone(), backend.clone(), &mut cx).await;
                this.update(&mut cx, |this, cx| {
                    this.clear_pending_ops(cx);
                });
                Ok(())
            },
        );
    }

    fn spawn_local_git_worker(
        state: Shared<Task<Result<LocalRepositoryState, String>>>,
        cx: &mut Context<Self>,
    ) -> (mpsc::UnboundedSender<GitJob>, Task<()>) {
        let (job_tx, mut job_rx) = mpsc::unbounded::<GitJob>();

        let worker_task = cx.spawn(async move |this, cx| {
            let Some(state) = state.await.log_err() else {
                return;
            };
            if let Some(git_hosting_provider_registry) =
                cx.update(|cx| GitHostingProviderRegistry::try_global(cx))
            {
                git_hosting_providers::register_additional_providers(
                    git_hosting_provider_registry,
                    state.backend.clone(),
                )
                .await;
            }
            let state = RepositoryState::Local(state);
            let mut jobs = VecDeque::new();
            loop {
                while let Ok(next_job) = job_rx.try_recv() {
                    jobs.push_back(next_job);
                }

                if let Some(job) = jobs.pop_front() {
                    if let Some(current_key) = &job.key
                        && jobs
                            .iter()
                            .any(|other_job| other_job.key.as_ref() == Some(current_key))
                    {
                        let skipped_job_id = job.id;
                        this.update(cx, |repo, _| {
                            repo.job_debug_queue.mark_complete(
                                skipped_job_id,
                                job_debug_queue::CompletedJobStatus::Skipped,
                            );
                        })
                        .ok();
                        continue;
                    }
                    (job.job)(state.clone(), cx).await;
                } else if let Some(job) = job_rx.next().await {
                    jobs.push_back(job);
                } else {
                    break;
                }
            }
        });

        (job_tx, worker_task)
    }

    fn load_staged_text(
        &mut self,
        _buffer_id: BufferId,
        repo_path: RepoPath,
        cx: &App,
    ) -> Task<Result<Option<String>>> {
        let rx = self.send_job("load_staged_text", None, move |state, _| async move {
            match state {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => backend
                    .load_index_text(repo_path)
                    .await
                    .map(decode_git_text)
                    .transpose(),
            }
        });
        cx.spawn(|_: &mut AsyncApp| async move { rx.await? })
    }

    fn load_committed_text(
        &mut self,
        _buffer_id: BufferId,
        repo_path: RepoPath,
        cx: &App,
    ) -> Task<Result<DiffBasesChange>> {
        let rx = self.send_job("load_committed_text", None, move |state, _| async move {
            match state {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                    let revisions = vec![
                        format!("HEAD:{}", repo_path.as_unix_str()),
                        format!(":{}", repo_path.as_unix_str()),
                    ];
                    let mut loaded_revisions = backend.load_revisions(revisions).await?.into_iter();
                    let committed_text = loaded_revisions
                        .next()
                        .flatten()
                        .map(decode_git_text)
                        .transpose()?;
                    let staged_text = loaded_revisions
                        .next()
                        .flatten()
                        .map(decode_git_text)
                        .transpose()?;
                    let diff_bases_change = if committed_text == staged_text {
                        DiffBasesChange::SetBoth(committed_text)
                    } else {
                        DiffBasesChange::SetEach {
                            index: staged_text,
                            head: committed_text,
                        }
                    };
                    anyhow::Ok(diff_bases_change)
                }
            }
        });

        cx.spawn(|_: &mut AsyncApp| async move { rx.await? })
    }

    pub fn load_commit_template_text(
        &mut self,
    ) -> oneshot::Receiver<Result<Option<GitCommitTemplate>>> {
        let _repository_id = self.snapshot.id;
        self.send_job(
            "load_commit_template_text",
            None,
            move |git_repo, _cx| async move {
                match git_repo {
                    RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                        backend.load_commit_template().await
                    }
                }
            },
        )
    }

    pub fn blame_buffer_at_revision(
        &mut self,
        path: RepoPath,
        revision: Oid,
        cx: &App,
    ) -> Task<Result<(String, git::blame::Blame)>> {
        let _repository_id = self.snapshot.id;
        let rx = self.send_job("blame_buffer_at_revision", None, {
            let path = path.clone();
            move |state, _| async move {
                match state {
                    RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                        let content_task = backend
                            .load_revisions(vec![format!("{revision}:{}", path.as_unix_str())]);
                        let blame_task = backend.blame_at_revision(path.clone(), revision);
                        let (mut contents, blame) = futures::try_join!(content_task, blame_task)?;
                        let content = contents.pop().flatten().with_context(|| {
                            format!(
                                "cannot load {:?} at revision {revision}: the file is missing or binary",
                                path.as_ref()
                            )
                        })?;
                        let content = decode_git_text(content)?;
                        anyhow::Ok((content, blame))
                    }
                }
            }
        });
        cx.spawn(move |_: &mut AsyncApp| async move {
            let (content, blame) = rx.await??;
            anyhow::ensure!(
                !is_binary_content(content.as_bytes()),
                "cannot blame binary file {:?} at revision {revision}",
                path.as_ref()
            );
            Ok((content, blame))
        })
    }

    fn load_blob_content(&mut self, oid: Oid, cx: &App) -> Task<Result<String>> {
        let _repository_id = self.snapshot.id;
        let rx = self.send_job("load_blob_content", None, move |state, _| async move {
            match state {
                RepositoryState::Local(LocalRepositoryState { backend, .. }) => {
                    decode_git_text(backend.load_blob_content(oid).await?)
                }
            }
        });
        cx.spawn(|_: &mut AsyncApp| async move { rx.await? })
    }

    fn paths_changed(&mut self, paths: Vec<RepoPath>, cx: &mut Context<Self>) {
        if !paths.is_empty() {
            self.paths_needing_status_update.push(paths);
        }

        let this = cx.weak_entity();
        let _ = self.send_keyed_job(
            "paths_changed",
            Some(GitJobKey::RefreshStatuses),
            None,
            |state, mut cx| async move {
                let (prev_snapshot, changed_paths) = this.update(&mut cx, |this, _| {
                    (
                        this.snapshot.clone(),
                        mem::take(&mut this.paths_needing_status_update),
                    )
                })?;
                let RepositoryState::Local(LocalRepositoryState { backend, .. }) = state else {
                    bail!("not a local repository")
                };

                if changed_paths.is_empty() {
                    return Ok(());
                }

                let has_head = prev_snapshot.head_commit.is_some();

                let changed_path_statuses = cx
                    .background_spawn(async move {
                        let changed_paths = GitStore::coalesce_repo_paths(
                            changed_paths
                                .into_iter()
                                .flatten()
                                .collect::<BTreeSet<_>>()
                                .into_iter()
                                .collect(),
                        );
                        let changed_paths_vec = changed_paths.iter().cloned().collect::<Vec<_>>();

                        let status_task = backend.status(&changed_paths_vec);
                        let diff_stat_future = |diff| {
                            if has_head {
                                backend.diff_stat(diff, &changed_paths_vec)
                            } else {
                                future::ready(Ok(status::GitDiffStat::default())).boxed()
                            }
                        };

                        let (statuses, diff_stats, staged_diff_stats, unstaged_diff_stats) =
                            futures::future::try_join4(
                                status_task,
                                diff_stat_future(DiffStatType::HeadToWorktree),
                                diff_stat_future(DiffStatType::HeadToIndex),
                                diff_stat_future(DiffStatType::IndexToWorktree),
                            )
                            .await?;

                        let diff_stats: HashMap<RepoPath, DiffStat> =
                            HashMap::from_iter(diff_stats.entries.into_iter().cloned());
                        let staged_diff_stats: HashMap<RepoPath, DiffStat> =
                            HashMap::from_iter(staged_diff_stats.entries.into_iter().cloned());
                        let unstaged_diff_stats: HashMap<RepoPath, DiffStat> =
                            HashMap::from_iter(unstaged_diff_stats.entries.into_iter().cloned());

                        let mut changed_path_statuses = Vec::new();
                        let prev_statuses = prev_snapshot.statuses_by_path.clone();
                        let current_status_paths = statuses
                            .entries
                            .iter()
                            .map(|(repo_path, _)| repo_path.clone())
                            .collect::<BTreeSet<_>>();

                        for path in &changed_paths {
                            let mut cursor = prev_statuses.cursor::<PathProgress>(());
                            cursor.seek_forward(&PathTarget::Path(path), Bias::Left);
                            while let Some(entry) = cursor.item() {
                                if !entry.repo_path.starts_with(path) {
                                    break;
                                }

                                if !current_status_paths.contains(&entry.repo_path) {
                                    changed_path_statuses.push(Edit::Remove(PathKey(
                                        entry.repo_path.as_ref().clone(),
                                    )));
                                }
                                cursor.next();
                            }
                        }

                        let mut cursor = prev_statuses.cursor::<PathProgress>(());

                        for (repo_path, status) in &*statuses.entries {
                            let current_diff_stat = diff_stats.get(repo_path).copied();
                            let current_staged_diff_stat =
                                staged_diff_stats.get(repo_path).copied();
                            let current_unstaged_diff_stat =
                                unstaged_diff_stats.get(repo_path).copied();

                            if cursor.seek_forward(&PathTarget::Path(repo_path), Bias::Left)
                                && cursor.item().is_some_and(|entry| {
                                    entry.status == *status
                                        && entry.diff_stat == current_diff_stat
                                        && entry.staged_diff_stat == current_staged_diff_stat
                                        && entry.unstaged_diff_stat == current_unstaged_diff_stat
                                })
                            {
                                continue;
                            }

                            changed_path_statuses.push(Edit::Insert(StatusEntry {
                                repo_path: repo_path.clone(),
                                status: *status,
                                diff_stat: current_diff_stat,
                                staged_diff_stat: current_staged_diff_stat,
                                unstaged_diff_stat: current_unstaged_diff_stat,
                            }));
                        }
                        anyhow::Ok(changed_path_statuses)
                    })
                    .await?;

                this.update(&mut cx, |this, cx| {
                    if !changed_path_statuses.is_empty() {
                        cx.emit(RepositoryEvent::StatusesChanged);
                        this.snapshot
                            .statuses_by_path
                            .edit(changed_path_statuses, ());
                        this.snapshot.scan_id += 1;
                    }
                })
            },
        );
    }

    /// currently running git command and when it started
    pub fn current_job(&self) -> Option<JobInfo> {
        self.active_jobs.values().next().cloned()
    }

    pub fn job_debug_queue(&self) -> &job_debug_queue::GitJobDebugQueue {
        &self.job_debug_queue
    }

    pub fn barrier(&mut self) -> oneshot::Receiver<()> {
        self.send_job("barrier", None, |_, _| async {})
    }

    fn spawn_job_with_tracking<AsyncFn>(
        &mut self,
        paths: Vec<RepoPath>,
        git_status: pending_op::GitStatus,
        cx: &mut Context<Self>,
        f: AsyncFn,
    ) -> Task<Result<()>>
    where
        AsyncFn: AsyncFnOnce(WeakEntity<Repository>, &mut AsyncApp) -> Result<()> + 'static,
    {
        let ids = self.new_pending_ops_for_paths(paths, git_status);

        cx.spawn(async move |this, cx| {
            let (job_status, result) = match f(this.clone(), cx).await {
                Ok(()) => (pending_op::JobStatus::Finished, Ok(())),
                Err(err) if err.is::<Canceled>() => (pending_op::JobStatus::Skipped, Ok(())),
                Err(err) => (pending_op::JobStatus::Error, Err(err)),
            };

            this.update(cx, |this, _| {
                let mut edits = Vec::with_capacity(ids.len());
                for (id, entry) in ids {
                    if let Some(mut ops) = this
                        .pending_ops
                        .get(&PathKey(entry.as_ref().clone()), ())
                        .cloned()
                    {
                        if let Some(op) = ops.op_by_id_mut(id) {
                            op.job_status = job_status;
                        }
                        edits.push(sum_tree::Edit::Insert(ops));
                    }
                }
                this.pending_ops.edit(edits, ());
            })?;

            result
        })
    }

    fn new_pending_ops_for_paths(
        &mut self,
        paths: Vec<RepoPath>,
        git_status: pending_op::GitStatus,
    ) -> Vec<(PendingOpId, RepoPath)> {
        let mut edits = Vec::with_capacity(paths.len());
        let mut ids = Vec::with_capacity(paths.len());
        for path in paths {
            let mut ops = self
                .pending_ops
                .get(&PathKey(path.as_ref().clone()), ())
                .cloned()
                .unwrap_or_else(|| PendingOps::new(&path));
            let id = ops.max_id() + 1;
            ops.ops.push(PendingOp {
                id,
                git_status,
                job_status: pending_op::JobStatus::Running,
            });
            edits.push(sum_tree::Edit::Insert(ops));
            ids.push((id, path));
        }
        self.pending_ops.edit(edits, ());
        ids
    }

    pub fn access(&mut self, _cx: &App) -> oneshot::Receiver<GitAccess> {
        self.send_job("access", None, move |git_repo, _cx| async move {
            match git_repo {
                RepositoryState::Local(state) => match state.backend.check_access().await {
                    Ok(_) => GitAccess::Yes,
                    Err(_) => GitAccess::No,
                },
            }
        })
    }

    pub fn default_remote_url(&self) -> Option<String> {
        self.remote_upstream_url
            .clone()
            .or(self.remote_origin_url.clone())
    }
}

fn format_job_key(key: &GitJobKey) -> SharedString {
    match key {
        GitJobKey::WriteIndex(paths) => {
            let paths_str: Vec<_> = paths
                .iter()
                .map(|p| {
                    let rel: &RelPath = p;
                    rel.display(PathStyle::local())
                })
                .collect();
            format!("WriteIndex({})", paths_str.join(", ")).into()
        }
        GitJobKey::ReloadBufferDiffBases => "ReloadBufferDiffBases".into(),
        GitJobKey::RefreshStatuses => "RefreshStatuses".into(),
        GitJobKey::ReloadGitState => "ReloadGitState".into(),
    }
}

/// If `path` is a git linked worktree checkout, resolves it to the main
/// repository's identity path. For regular linked worktrees this is the main
/// repository's working directory; for linked worktrees backed by a bare repo
/// such as `.bare`, this is the parent project directory users think of as the
/// repository root. Returns `None` if `path` is a normal repository, not a git
/// repo, or if resolution fails.
///
/// Resolution works by:
/// 1. Reading the `.git` file to get the `gitdir:` pointer
/// 2. Following that to the worktree-specific git directory
/// 3. Reading the `commondir` file to find the shared `.git` directory
/// 4. Deriving the main repo's identity path from the common dir
pub async fn resolve_git_worktree_to_main_repo(fs: &dyn Fs, path: &Path) -> Option<PathBuf> {
    let dot_git = path.join(".git");
    let metadata = fs.metadata(&dot_git).await.ok()??;
    if metadata.is_dir {
        return None; // Normal repo, not a linked worktree
    }
    // It's a .git file — parse the gitdir: pointer
    let content = fs.load(&dot_git).await.ok()?;
    let gitdir_rel = content.strip_prefix("gitdir:")?.trim();
    let gitdir_abs = fs.canonicalize(&path.join(gitdir_rel)).await.ok()?;
    // Submodules also use a `.git` file, but they are independent projects whose
    // identity is their own working directory (`path`), not the superproject's
    // `.git/modules/<name>` git dir. Leave them unresolved.
    if is_submodule_git_dir(&gitdir_abs) {
        return None;
    }
    // Read commondir to find the main .git directory
    let commondir_content = fs.load(&gitdir_abs.join("commondir")).await.ok()?;
    let common_dir = fs
        .canonicalize(&gitdir_abs.join(commondir_content.trim()))
        .await
        .ok()?;
    Some(repo_identity_path(&common_dir, PathStyle::local()).to_path_buf())
}

/// Validates that the resolved worktree directory is acceptable:
/// - The setting must not be an absolute path.
/// - The resolved path must be either a subdirectory of the working
///   directory or a subdirectory of its parent (i.e., a sibling).
///
/// Returns `Ok(resolved_path)` or an error with a user-facing message.
pub fn worktrees_directory_for_repo(
    repository_anchor_path: &Path,
    worktree_directory_setting: &str,
    path_style: PathStyle,
) -> Result<PathBuf> {
    // Check the original setting before trimming, since a path like "///"
    // is absolute but becomes "" after stripping trailing separators.
    // Also check for leading `/` or `\` explicitly, because on Windows
    // `Path::is_absolute()` requires a drive letter — so `/tmp/worktrees`
    // would slip through even though it's clearly not a relative path.
    if path_style.is_absolute(worktree_directory_setting)
        || worktree_directory_setting.starts_with('\\')
    {
        anyhow::bail!(
            "git.worktree_directory must be a relative path, got: {worktree_directory_setting:?}"
        );
    }

    if worktree_directory_setting.is_empty() {
        anyhow::bail!("git.worktree_directory must not be empty");
    }

    let trimmed = worktree_directory_setting.trim_end_matches(['/', '\\']);
    if trimmed == ".." {
        anyhow::bail!("git.worktree_directory must not be \"..\" (use \"../some-name\" instead)");
    }

    let joined = path_style.join_path(repository_anchor_path, trimmed)?;
    let resolved = if path_style.is_posix() {
        joined
    } else {
        path::normalize_path(&joined)
    };
    let resolved = if resolved.starts_with(repository_anchor_path) {
        resolved
    } else if let Some(repo_dir_name) = repository_anchor_path
        .file_name()
        .and_then(|name| name.to_str())
    {
        path_style.join_path(&resolved, repo_dir_name)?
    } else {
        resolved
    };

    let parent = repository_anchor_path
        .parent()
        .unwrap_or(repository_anchor_path);

    if !resolved.starts_with(parent) {
        anyhow::bail!(
            "git.worktree_directory resolved to {resolved:?}, which is outside \
             the project root and its parent directory. It must resolve to a \
             subdirectory of {repository_anchor_path:?} or a sibling of it."
        );
    }

    Ok(resolved)
}

async fn remove_empty_managed_worktree_ancestors(fs: &dyn Fs, child_path: &Path, base_path: &Path) {
    let mut current = child_path;
    while let Some(parent) = current.parent() {
        if parent == base_path {
            break;
        }
        if !parent.starts_with(base_path) {
            break;
        }

        let result = fs
            .remove_dir(
                parent,
                RemoveOptions {
                    recursive: false,
                    ignore_if_not_exists: true,
                },
            )
            .await;

        match result {
            Ok(()) => {
                log::info!(
                    "Removed empty managed worktree directory: {}",
                    parent.display()
                );
            }
            Err(error) => {
                log::debug!(
                    "Stopped removing managed worktree parent directories at {}: {error}",
                    parent.display()
                );
                break;
            }
        }

        current = parent;
    }
}

/// Returns the repository's identity path given its common Git directory.
///
/// This is the canonical, on-disk path used for project grouping and as the
/// basis for display names. The goal is to return the directory the user
/// thinks of as "the project":
///
/// - If `common_dir`'s last component starts with `.` (e.g. `.git` for a
///   normal checkout, or `.bare` for a bare clone), the parent directory is
///   returned. Both of these are internal Git directories; the parent is the
///   meaningful project root.
/// - Otherwise (e.g. `zed.git` for a bare clone), `common_dir` itself is
///   returned — it is already a meaningful on-disk path.
pub fn repo_identity_path(common_dir: &Path, path_style: PathStyle) -> &Path {
    let is_dot_entry = path_style
        .file_name(common_dir)
        .is_some_and(|n| n.starts_with('.'));
    if is_dot_entry {
        path_style.parent(common_dir).unwrap_or(common_dir)
    } else {
        common_dir
    }
}

/// Returns the repository identity only when `std::path` can interpret the path correctly.
///
/// Callers whose downstream path operations are not yet `PathStyle`-aware use this to preserve
/// their existing behavior for foreign path styles.
pub fn repo_identity_path_if_local(common_dir: &Path, path_style: PathStyle) -> Option<&Path> {
    (path_style == PathStyle::local()).then(|| repo_identity_path(common_dir, path_style))
}

/// Returns true if `git_dir` is a Git submodule's git directory.
///
/// Submodules store their git directory inside the superproject at
/// `<superproject>/.git/modules/<name>`. Unlike a linked worktree, a submodule
/// is an independent project whose identity is its own working directory, so its
/// path must not be resolved to the superproject's `.git/modules/...` directory
/// (from which the working directory cannot be derived).
pub fn is_submodule_git_dir(git_dir: &Path) -> bool {
    let mut previous_was_git_dir = false;
    for component in git_dir.components() {
        if let std::path::Component::Normal(name) = component {
            if previous_was_git_dir && name == std::ffi::OsStr::new("modules") {
                return true;
            }
            previous_was_git_dir = name.to_string_lossy().ends_with(".git");
        } else {
            previous_was_git_dir = false;
        }
    }
    false
}

/// Returns a short name for a linked worktree suitable for UI display
///
/// Uses the main worktree path to come up with a short name that disambiguates
/// the linked worktree from the main worktree.
pub fn linked_worktree_short_name(
    main_worktree_path: &Path,
    linked_worktree_path: &Path,
) -> Option<SharedString> {
    if main_worktree_path == linked_worktree_path {
        return None;
    }

    let project_name = main_worktree_path.file_name()?.to_str()?;
    let directory_name = linked_worktree_path.file_name()?.to_str()?;
    let name = if directory_name != project_name {
        directory_name.to_string()
    } else {
        linked_worktree_path
            .parent()?
            .file_name()?
            .to_str()?
            .to_string()
    };
    Some(name.into())
}

fn get_permalink_in_rust_registry_src(
    provider_registry: Arc<GitHostingProviderRegistry>,
    path: PathBuf,
    selection: Range<u32>,
) -> Result<url::Url> {
    #[derive(Deserialize)]
    struct CargoVcsGit {
        sha1: String,
    }

    #[derive(Deserialize)]
    struct CargoVcsInfo {
        git: CargoVcsGit,
        path_in_vcs: String,
    }

    #[derive(Deserialize)]
    struct CargoPackage {
        repository: String,
    }

    #[derive(Deserialize)]
    struct CargoToml {
        package: CargoPackage,
    }

    let Some((dir, cargo_vcs_info_json)) = path.ancestors().skip(1).find_map(|dir| {
        let json = std::fs::read_to_string(dir.join(".cargo_vcs_info.json")).ok()?;
        Some((dir, json))
    }) else {
        bail!("No .cargo_vcs_info.json found in parent directories")
    };
    let cargo_vcs_info = serde_json::from_str::<CargoVcsInfo>(&cargo_vcs_info_json)?;
    let cargo_toml = std::fs::read_to_string(dir.join("Cargo.toml"))?;
    let manifest = toml::from_str::<CargoToml>(&cargo_toml)?;
    let (provider, remote) = parse_git_remote_url(provider_registry, &manifest.package.repository)
        .context("parsing package.repository field of manifest")?;
    let path = PathBuf::from(cargo_vcs_info.path_in_vcs).join(path.strip_prefix(dir).unwrap());
    let permalink = provider.build_permalink(
        remote,
        BuildPermalinkParams::new(
            &cargo_vcs_info.git.sha1,
            &RepoPath::from_rel_path(
                &RelPath::new(&path, PathStyle::local()).context("invalid path")?,
            ),
            Some(selection),
        ),
    );
    Ok(permalink)
}

async fn append_pattern_to_ignore_file(
    fs: Arc<dyn Fs>,
    file_path: PathBuf,
    pattern: String,
) -> Result<()> {
    let existing_content = match fs.load(&file_path).await {
        Ok(content) => content,
        Err(error)
            if error
                .root_cause()
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
        {
            String::new()
        }
        Err(error) => {
            return Err(error).with_context(|| format!("loading {}", file_path.display()));
        }
    };

    if existing_content.lines().any(|line| line.trim() == pattern) {
        return Ok(());
    }

    let new_content = if existing_content.is_empty() {
        format!("{}\n", pattern)
    } else if existing_content.ends_with('\n') {
        format!("{}{}\n", existing_content, pattern)
    } else {
        format!("{}\n{}\n", existing_content, pattern)
    };

    fs.save(
        &file_path,
        &text::Rope::from(new_content.as_str()),
        text::LineEnding::Unix,
    )
    .await
}

#[cfg(any(test, feature = "test-support"))]
impl Repository {
    pub fn set_branch_list_for_test(&mut self, branches: Vec<Branch>, cx: &mut Context<Self>) {
        self.snapshot.branch_list = branches.into();
        cx.emit(RepositoryEvent::BranchListChanged);
    }

    pub fn loaded_commit_data_for_test(&self) -> HashMap<Oid, CommitData> {
        self.commit_data
            .iter()
            .filter_map(|(sha, state)| match state {
                CommitDataState::Loaded(data) => Some((*sha, data.as_ref().clone())),
                CommitDataState::Loading(_) => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use askpass::EncryptedPassword;
    use crate::Project;
    use fs::{FakeFs, Fs};
    use smallvec::SmallVec;
    use git::repository::{RepoPath, repo_path};
    use gpui::proptest::prelude::*;
    use gpui::{TestAppContext, UpdateGlobal};
    use rand::{SeedableRng, rngs::StdRng};
    use serde_json::json;
    use settings::SettingsStore;
    use std::path::{Path, PathBuf};

    fn init_test(cx: &mut TestAppContext) {
        cx.update(|cx| {
            let settings_store = SettingsStore::test(cx);
            cx.set_global(settings_store);
        });
    }

    type TestPasswordPrompt = (
        String,
        oneshot::Sender<EncryptedPassword>,
        oneshot::Receiver<()>,
    );

    fn test_askpass_delegate(
        cx: &mut TestAppContext,
    ) -> (AskPassDelegate, mpsc::UnboundedReceiver<TestPasswordPrompt>) {
        let (prompt_sender, prompt_receiver) = mpsc::unbounded();
        let delegate = AskPassDelegate::new_with_cancellation(
            &mut cx.to_async(),
            move |prompt, response_sender, cancellation, _| {
                prompt_sender
                    .unbounded_send((prompt, response_sender, cancellation))
                    .expect("prompt receiver should remain open");
            },
        );
        (delegate, prompt_receiver)
    }

    #[gpui::test]

    #[gpui::test]

    #[gpui::test]

    #[gpui::test]

    #[gpui::test]

    #[test]
    fn test_is_submodule_git_dir() {
        assert!(is_submodule_git_dir(Path::new("/Foo/.git/modules/Bar")));
        assert!(is_submodule_git_dir(Path::new(
            "/Foo/.git/modules/nested/modules/Baz"
        )));
        // Bare superprojects keep their git dir in a `*.git` directory.
        assert!(is_submodule_git_dir(Path::new("/Foo/foo.git/modules/Bar")));

        // Not submodules: a normal repo, a linked worktree, and a bare repo.
        assert!(!is_submodule_git_dir(Path::new("/Foo/Bar/.git")));
        assert!(!is_submodule_git_dir(Path::new(
            "/repo/.git/worktrees/feature"
        )));
        assert!(!is_submodule_git_dir(Path::new("/foo/.bare")));
        // A directory literally named `modules` that isn't under a git dir.
        assert!(!is_submodule_git_dir(Path::new("/Foo/modules/Bar")));
    }

    #[gpui::test]
    async fn test_get_permalink_for_file_without_selection(cx: &mut TestAppContext) {
        use util::rel_path::rel_path;

        init_test(cx);
        cx.update(|cx| {
            GitHostingProviderRegistry::default_global(cx);
            git_hosting_providers::init(cx);
        });

        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            Path::new("/project"),
            json!({
                ".git": {},
                "src": { "main.rs": "fn main() {}\n" },
            }),
        )
        .await;

        let sha = "e6ebe7974deb6bb6cc0e2595c8ec31f0c71084b7";
        fs.set_head_for_repo(
            Path::new("/project/.git"),
            &[("src/main.rs", "fn main() {}\n".into())],
            sha,
        );
        fs.set_remote_for_repo(
            Path::new("/project/.git"),
            "origin",
            "https://github.com/zed-industries/zed.git",
        );

        let project = Project::test(fs.clone(), [Path::new("/project")], cx).await;
        project
            .update(cx, |project, cx| project.git_scans_complete(cx))
            .await;

        let worktree_id = project.read_with(cx, |project, cx| {
            project.worktrees(cx).next().unwrap().read(cx).id()
        });
        let project_path = ProjectPath {
            worktree_id,
            path: rel_path("src/main.rs").into(),
        };

        let permalink = project
            .update(cx, |project, cx| {
                project.get_file_permalink(&project_path, cx)
            })
            .await
            .unwrap();

        assert_eq!(
            permalink.as_str(),
            &format!("https://github.com/zed-industries/zed/blob/{sha}/src/main.rs")
        );
        assert!(permalink.fragment().is_none());
    }

    #[gpui::test]
    async fn test_resolve_git_worktree_to_main_repo_ignores_submodule(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            Path::new("/Foo"),
            json!({
                ".git": {
                    "modules": {
                        "Bar": {
                            "HEAD": "ref: refs/heads/main"
                        }
                    }
                },
                "Bar": {
                    ".git": "gitdir: ../.git/modules/Bar\n",
                    "src": { "main.rs": "" }
                }
            }),
        )
        .await;

        // A submodule must not resolve to the superproject's `.git/modules/<name>`;
        // it stays at its own working directory (handled by the caller falling back
        // to the raw path).
        let resolved = resolve_git_worktree_to_main_repo(fs.as_ref(), Path::new("/Foo/Bar")).await;
        assert_eq!(resolved, None);
    }

    #[gpui::test]
    fn test_decode_git_text_windows_1251_one_line_change(cx: &mut TestAppContext) {
        let old_text = "строка один\nстрока два\n";
        let new_text = "строка один\nстрока три\n";
        let (old_bytes, _, _) = encoding_rs::WINDOWS_1251.encode(old_text);
        let (new_bytes, _, _) = encoding_rs::WINDOWS_1251.encode(new_text);

        let decoded_old = decode_git_text(old_bytes.into_owned()).unwrap();
        let decoded_new = decode_git_text(new_bytes.into_owned()).unwrap();
        let buffer = cx.new(|cx| Buffer::local(decoded_new, cx));
        let buffer_snapshot = buffer.read_with(cx, |buffer, _| buffer.snapshot());
        let diff = cx.new(|cx| BufferDiff::new_with_base_text(&decoded_old, &buffer_snapshot, cx));
        let diff = diff.update(cx, |diff, cx| diff.snapshot(cx));
        let hunks = diff.hunks(&buffer_snapshot).collect::<Vec<_>>();
        let [hunk] = hunks.as_slice() else {
            panic!("expected one modified hunk, got {hunks:?}");
        };

        assert_eq!(hunk.range, text::Point::new(1, 0)..text::Point::new(2, 0));
        assert_eq!(
            hunk.diff_base_byte_range,
            old_text.find("строка два").unwrap()..old_text.len()
        );
    }

    #[gpui::test]
    async fn test_open_uncommitted_diff_skips_symlinks(cx: &mut TestAppContext) {
        use util::rel_path::rel_path;

        init_test(cx);

        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            Path::new("/project"),
            json!({
                ".git": {},
                "target.txt": "rule one\nrule two\n",
            }),
        )
        .await;
        fs.insert_symlink("/project/agents.md", PathBuf::from("target.txt"))
            .await;

        fs.set_head_and_index_for_repo(
            Path::new("/project/.git"),
            &[
                // git stores the symlink's target path as the blob for `agents.md`
                ("agents.md", "target.txt".into()),
                ("target.txt", "rule one\n".into()),
            ],
        );

        let project = Project::test(fs.clone(), [Path::new("/project")], cx).await;
        project
            .update(cx, |project, cx| project.git_scans_complete(cx))
            .await;

        let worktree_id = project.read_with(cx, |project, cx| {
            project.worktrees(cx).next().unwrap().read(cx).id()
        });

        // symlink file should not produce a base diff
        let symlink_buffer = project
            .update(cx, |project, cx| {
                project.open_buffer((worktree_id, rel_path("agents.md")), cx)
            })
            .await
            .unwrap();
        let symlink_diff = project
            .update(cx, |project, cx| {
                project.open_uncommitted_diff(symlink_buffer, cx)
            })
            .await
            .unwrap();
        symlink_diff.read_with(cx, |diff, _| {
            assert!(
                !diff.base_text_exists(),
                "symlinked buffer should not have a git diff base"
            );
        });

        // regular file should still produce a base diff
        let regular_buffer = project
            .update(cx, |project, cx| {
                project.open_buffer((worktree_id, rel_path("target.txt")), cx)
            })
            .await
            .unwrap();
        let regular_diff = project
            .update(cx, |project, cx| {
                project.open_uncommitted_diff(regular_buffer, cx)
            })
            .await
            .unwrap();
        regular_diff.read_with(cx, |diff, _| {
            assert!(
                diff.base_text_exists(),
                "regular file should have a git diff base"
            );
        });
    }

    #[gpui::test]
    async fn test_merge_base_status_uses_worktree_contents(cx: &mut TestAppContext) {
        use util::rel_path::rel_path;

        init_test(cx);

        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            Path::new("/project"),
            json!({
                ".git": {},
                "committed.txt": "base\n",
            }),
        )
        .await;
        fs.set_head_and_index_for_repo(
            Path::new("/project/.git"),
            &[("committed.txt", "head\n".into())],
        );
        fs.set_merge_base_content_for_repo(
            Path::new("/project/.git"),
            &[("committed.txt", "base\n".into())],
        );

        let project = Project::test(fs.clone(), [Path::new("/project")], cx).await;
        project
            .update(cx, |project, cx| project.git_scans_complete(cx))
            .await;

        let worktree_id = project.read_with(cx, |project, cx| {
            project.worktrees(cx).next().unwrap().read(cx).id()
        });
        let git_store = project.read_with(cx, |project, _| project.git_store().clone());
        let project_path = (worktree_id, rel_path("committed.txt")).into();
        let buffer = project
            .update(cx, |project, cx| {
                project.open_buffer((worktree_id, rel_path("committed.txt")), cx)
            })
            .await
            .unwrap();
        let buffer_id = buffer.read_with(cx, |buffer, _| buffer.remote_id());

        git_store.read_with(cx, |git_store, cx| {
            assert!(
                git_store
                    .project_path_git_status(&project_path, cx)
                    .is_some_and(|status| status.has_changes())
            );
        });
        cx.update(|cx| {
            SettingsStore::update_global(cx, |settings, cx| {
                settings.update_user_settings(cx, |settings| {
                    settings.git.get_or_insert_default().diff_base =
                        Some(settings::GitDiffBaseSetting::DefaultBranch);
                });
            });
        });
        cx.run_until_parked();

        let display_diff = git_store
            .update(cx, |git_store, cx| {
                git_store.open_display_diff(buffer.clone(), cx)
            })
            .await
            .unwrap();
        display_diff.read_with(cx, |diff, cx| {
            assert_eq!(diff.base_text_string(cx).as_deref(), Some("base\n"));
        });
        git_store.read_with(cx, |git_store, cx| {
            assert_eq!(git_store.display_status_for_buffer_id(buffer_id, cx), None);
            assert_eq!(
                git_store.display_status_for_project_path(&project_path, cx),
                None
            );

            let raw_snapshot = git_store.repo_snapshots(cx).into_values().next().unwrap();
            let display_snapshot = git_store
                .display_repo_snapshots(cx)
                .into_values()
                .next()
                .unwrap();
            assert_eq!(
                raw_snapshot
                    .statuses_by_path
                    .iter()
                    .map(|entry| entry.repo_path.clone())
                    .collect::<Vec<_>>(),
                vec![repo_path("committed.txt")]
            );
            assert_eq!(display_snapshot.statuses_by_path.iter().count(), 0);
        });

        fs.set_merge_base_content_for_repo(
            Path::new("/project/.git"),
            &[("committed.txt", "head\n".into())],
        );
        let repository =
            project.read_with(cx, |project, cx| project.active_repository(cx).unwrap());
        let branches = repository.read_with(cx, |repository, _| {
            repository.snapshot().branch_list.to_vec()
        });
        repository.update(cx, |repository, cx| {
            repository.set_branch_list_for_test(branches, cx)
        });
        cx.run_until_parked();

        git_store.read_with(cx, |git_store, cx| {
            assert!(
                git_store
                    .display_status_for_buffer_id(buffer_id, cx)
                    .is_some_and(|status| status.has_changes())
            );
        });

        let repository_id = repository.read_with(cx, |repository, _| repository.id);
        let display_diff_list = git_store
            .read_with(cx, |git_store, _| {
                git_store.display_diff_for_repo(repository_id)
            })
            .unwrap();
        let weak_display_diff_list = display_diff_list.downgrade();
        drop(display_diff_list);
        drop(display_diff);
        cx.update(|cx| {
            SettingsStore::update_global(cx, |settings, cx| {
                settings.update_user_settings(cx, |settings| {
                    settings.git.get_or_insert_default().diff_base =
                        Some(settings::GitDiffBaseSetting::Head);
                });
            });
        });
        cx.run_until_parked();

        git_store.read_with(cx, |git_store, _| {
            assert!(git_store.display_diff_for_repo(repository_id).is_none());
        });
        assert!(weak_display_diff_list.upgrade().is_none());
    }

    #[gpui::test]
    async fn test_append_pattern_to_ignore_file_creates_and_deduplicates(cx: &mut TestAppContext) {
        let fs: Arc<dyn Fs> = FakeFs::new(cx.executor());
        let path = PathBuf::from("/root/.gitignore");

        // Appending to a non-existent file creates it with a trailing newline.
        super::append_pattern_to_ignore_file(fs.clone(), path.clone(), "build/".to_string())
            .await
            .unwrap();
        assert_eq!(fs.load(&path).await.unwrap(), "build/\n");

        // Appending the same pattern again is a no-op (deduplication).
        super::append_pattern_to_ignore_file(fs.clone(), path.clone(), "build/".to_string())
            .await
            .unwrap();
        assert_eq!(fs.load(&path).await.unwrap(), "build/\n");

        // Appending a distinct pattern adds it with a trailing newline.
        super::append_pattern_to_ignore_file(fs.clone(), path.clone(), "target/".to_string())
            .await
            .unwrap();
        assert_eq!(fs.load(&path).await.unwrap(), "build/\ntarget/\n");
    }

    #[gpui::test]
    async fn test_append_pattern_adds_newline_before_pattern_when_missing(cx: &mut TestAppContext) {
        let fs: Arc<dyn Fs> = FakeFs::new(cx.executor());
        let path = PathBuf::from("/root/.gitignore");

        // Pre-populate the file without a trailing newline.
        fs.save(&path, &text::Rope::from("*.log"), text::LineEnding::Unix)
            .await
            .unwrap();

        // The new pattern must be written on its own line.
        super::append_pattern_to_ignore_file(fs.clone(), path.clone(), "build/".to_string())
            .await
            .unwrap();
        assert_eq!(fs.load(&path).await.unwrap(), "*.log\nbuild/\n");
    }

    #[test]
    fn test_new_worktree_path_uses_posix_style_for_remote_paths() {
        let work_dir = Path::new("/home/user/dev/lsp-tests");
        let directory =
            worktrees_directory_for_repo(work_dir, "../worktrees", PathStyle::Unix).unwrap();
        let directory = PathStyle::Unix.join_path(&directory, "nimble-sky").unwrap();
        let path = PathStyle::Unix.join_path(&directory, "lsp-tests").unwrap();

        assert_eq!(
            path,
            PathBuf::from("/home/user/dev/worktrees/lsp-tests/nimble-sky/lsp-tests")
        );
    }

    fn verify_invariants(repository: &Repository) -> anyhow::Result<()> {
        match &repository.commit_data_handler {
            CommitDataHandlerState::Open(handler) => {
                verify_loading_entries_are_pending(repository, handler)?;
                verify_await_result_loading_entries_have_completion_senders(repository, handler)?;
                verify_pending_requests_are_loading(repository, handler)?;
                verify_completion_senders_are_await_result_loading(repository, handler)?;
                verify_completion_senders_are_pending(handler)?;
                verify_non_await_result_loading_entries_have_no_completion_sender(
                    repository, handler,
                )?;
                verify_loaded_entries_are_not_pending(repository, handler)?;
                verify_loaded_entries_have_no_completion_sender(repository, handler)?;
            }
            CommitDataHandlerState::Closed => {
                verify_closed_handler_invariants(repository)?;
            }
        }

        Ok(())
    }

    fn verify_loading_entries_are_pending(
        repository: &Repository,
        handler: &CommitDataHandler,
    ) -> anyhow::Result<()> {
        for (sha, state) in &repository.commit_data {
            if matches!(state, CommitDataState::Loading(_)) {
                anyhow::ensure!(
                    handler.pending_requests.contains(sha),
                    "loading commit data for {sha} must be tracked in pending_requests"
                );
            }
        }

        Ok(())
    }

    fn verify_await_result_loading_entries_have_completion_senders(
        repository: &Repository,
        handler: &CommitDataHandler,
    ) -> anyhow::Result<()> {
        for (sha, state) in &repository.commit_data {
            if matches!(state, CommitDataState::Loading(Some(_))) {
                anyhow::ensure!(
                    handler.completion_senders.contains_key(sha),
                    "await-result loading commit data for {sha} must have a completion sender"
                );
            }
        }

        Ok(())
    }

    fn verify_pending_requests_are_loading(
        repository: &Repository,
        handler: &CommitDataHandler,
    ) -> anyhow::Result<()> {
        for sha in &handler.pending_requests {
            anyhow::ensure!(
                matches!(
                    repository.commit_data.get(sha),
                    Some(CommitDataState::Loading(_))
                ),
                "pending request for {sha} must correspond to loading commit data"
            );
        }

        Ok(())
    }

    fn verify_completion_senders_are_await_result_loading(
        repository: &Repository,
        handler: &CommitDataHandler,
    ) -> anyhow::Result<()> {
        for sha in handler.completion_senders.keys() {
            anyhow::ensure!(
                matches!(
                    repository.commit_data.get(sha),
                    Some(CommitDataState::Loading(Some(_)))
                ),
                "completion sender for {sha} must correspond to await-result loading commit data"
            );
        }

        Ok(())
    }

    fn verify_completion_senders_are_pending(handler: &CommitDataHandler) -> anyhow::Result<()> {
        for sha in handler.completion_senders.keys() {
            anyhow::ensure!(
                handler.pending_requests.contains(sha),
                "completion sender for {sha} must also be tracked as pending"
            );
        }

        Ok(())
    }

    fn verify_non_await_result_loading_entries_have_no_completion_sender(
        repository: &Repository,
        handler: &CommitDataHandler,
    ) -> anyhow::Result<()> {
        for (sha, state) in &repository.commit_data {
            if matches!(state, CommitDataState::Loading(None)) {
                anyhow::ensure!(
                    !handler.completion_senders.contains_key(sha),
                    "non-await-result loading commit data for {sha} must not have a completion sender"
                );
            }
        }

        Ok(())
    }

    fn verify_loaded_entries_are_not_pending(
        repository: &Repository,
        handler: &CommitDataHandler,
    ) -> anyhow::Result<()> {
        for (sha, state) in &repository.commit_data {
            if matches!(state, CommitDataState::Loaded(_)) {
                anyhow::ensure!(
                    !handler.pending_requests.contains(sha),
                    "loaded commit data for {sha} must not still be pending"
                );
            }
        }

        Ok(())
    }

    fn verify_loaded_entries_have_no_completion_sender(
        repository: &Repository,
        handler: &CommitDataHandler,
    ) -> anyhow::Result<()> {
        for (sha, state) in &repository.commit_data {
            if matches!(state, CommitDataState::Loaded(_)) {
                anyhow::ensure!(
                    !handler.completion_senders.contains_key(sha),
                    "loaded commit data for {sha} must not keep a completion sender"
                );
            }
        }

        Ok(())
    }

    fn verify_closed_handler_invariants(repository: &Repository) -> anyhow::Result<()> {
        for (sha, state) in &repository.commit_data {
            anyhow::ensure!(
                !matches!(state, CommitDataState::Loading(_)),
                "closed handler must not keep loading commit data for {sha}"
            );
        }

        Ok(())
    }

    #[gpui::property_test(config = ProptestConfig {
        cases: 20,
        ..Default::default()
    })]
    async fn test_commit_data_random_invariants(
        #[strategy = any::<u64>()] seed: u64,
        #[strategy = gpui::proptest::collection::vec(0usize..2000, 1..200)] commit_indexes: Vec<
            usize,
        >,
        #[strategy = gpui::proptest::collection::vec(any::<bool>(), 1..200)] await_results: Vec<
            bool,
        >,
        #[strategy = gpui::proptest::collection::vec(0usize..2000, 0..200)] failing_commit_indexes: Vec<
            usize,
        >,
        #[strategy = gpui::proptest::collection::vec(0usize..2000, 0..200)] missing_commit_indexes: Vec<
            usize,
        >,
        cx: &mut TestAppContext,
    ) {
        init_test(cx);
        let mut rng = StdRng::seed_from_u64(seed);

        let commit_shas = (0..2000).map(|_| Oid::random(&mut rng)).collect::<Vec<_>>();
        let failing_shas = failing_commit_indexes
            .into_iter()
            .map(|index| commit_shas[index % commit_shas.len()])
            .collect::<HashSet<_>>();
        let missing_shas = missing_commit_indexes
            .into_iter()
            .map(|index| commit_shas[index % commit_shas.len()])
            .collect::<HashSet<_>>();
        let commit_data = commit_shas
            .iter()
            .filter(|sha| !missing_shas.contains(sha))
            .map(|sha| {
                (
                    CommitData {
                        sha: *sha,
                        parents: SmallVec::new(),
                        author_name: SharedString::from(format!("Author {sha}")),
                        author_email: SharedString::from(format!("{sha}@example.com")),
                        commit_timestamp: rng.random_range(0..10_000),
                        subject: SharedString::from(format!("Subject {sha}")),
                        message: SharedString::from(format!("Subject {sha}\n\nBody for {sha}")),
                    },
                    failing_shas.contains(sha),
                )
            })
            .collect::<Vec<_>>();
        let expected_loaded_shas = commit_indexes
            .iter()
            .map(|index| commit_shas[index % commit_shas.len()])
            .filter(|sha| !failing_shas.contains(sha) && !missing_shas.contains(sha))
            .collect::<HashSet<_>>();

        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            Path::new("/project"),
            json!({
                ".git": {},
                "file.txt": "content",
            }),
        )
        .await;
        fs.set_commit_data(Path::new("/project/.git"), commit_data);

        let project = Project::test(fs.clone(), [Path::new("/project")], cx).await;
        project
            .update(cx, |project, cx| project.git_scans_complete(cx))
            .await;

        let repository = project.read_with(cx, |project, cx| {
            project
                .active_repository(cx)
                .expect("should have a repository")
        });

        cx.update(|cx| {
            cx.observe(&repository, |repo, cx| {
                verify_invariants(repo.read(cx))
                    .context("Invariant weren't held after a cx.notify")
                    .unwrap();
            })
        })
        .detach();

        let mut next_step = 0;
        while next_step < commit_indexes.len() {
            let remaining_steps = commit_indexes.len() - next_step;
            let chunk_size = rng.random_range(1..=remaining_steps.min(16));
            let chunk_end = next_step + chunk_size;

            for step in next_step..chunk_end {
                let sha = commit_shas[commit_indexes[step] % commit_shas.len()];
                let await_result = await_results[step % await_results.len()];

                repository.update(cx, |repository, cx| {
                    repository.fetch_commit_data(sha, await_result, cx);
                    verify_invariants(repository)
                        .with_context(|| {
                            format!(
                                "commit data invariant violation after step {} for sha {}",
                                step + 1,
                                sha,
                            )
                        })
                        .unwrap();
                });
            }

            cx.run_until_parked();
            repository.read_with(cx, |repository, _cx| {
                verify_invariants(repository)
                    .with_context(|| {
                        format!(
                            "commit data invariant violation after draining through step {}",
                            chunk_end,
                        )
                    })
                    .unwrap();
            });

            next_step = chunk_end;
        }

        cx.run_until_parked();
        repository.read_with(cx, |repository, _cx| {
            verify_invariants(repository)
                .with_context(|| "commit data invariant violation after final drain".to_string())
                .unwrap();

            let loaded_shas = repository
                .commit_data
                .iter()
                .filter_map(|(sha, state)| match state {
                    CommitDataState::Loaded(_) => Some(*sha),
                    CommitDataState::Loading(_) => None,
                })
                .collect::<HashSet<_>>();
            let missing_loaded_shas = expected_loaded_shas
                .difference(&loaded_shas)
                .copied()
                .collect::<Vec<_>>();
            let unexpected_loaded_shas = loaded_shas
                .difference(&expected_loaded_shas)
                .copied()
                .collect::<Vec<_>>();
            assert!(
                missing_loaded_shas.is_empty() && unexpected_loaded_shas.is_empty(),
                "loaded commit data SHAs after final drain did not match expectation. missing: {:?}, unexpected: {:?}",
                missing_loaded_shas,
                unexpected_loaded_shas,
            );
        });
    }

    fn repo_paths(paths: &[&str]) -> Vec<RepoPath> {
        paths.iter().map(repo_path).collect()
    }

    #[test]
    fn coalesce_repo_paths_keeps_root_only() {
        let coalesced = GitStore::coalesce_repo_paths(repo_paths(&["", "src", "src/lib.rs"]));

        assert_eq!(coalesced, repo_paths(&[""]));
    }

    #[test]
    fn coalesce_repo_paths_keeps_existing_ancestors() {
        let coalesced = GitStore::coalesce_repo_paths(repo_paths(&[
            "src",
            "src/lib.rs",
            "src/nested/file.rs",
            "tests/test.rs",
        ]));

        assert_eq!(coalesced, repo_paths(&["src", "tests/test.rs"]));
    }

    #[test]
    fn coalesce_repo_paths_does_not_invent_missing_parents() {
        let coalesced = GitStore::coalesce_repo_paths(repo_paths(&[
            "submodule/a.txt",
            "submodule/nested/b.txt",
            "top_level.rs",
        ]));

        assert_eq!(
            coalesced,
            repo_paths(&["submodule/a.txt", "submodule/nested/b.txt", "top_level.rs"])
        );
    }
}

/// This snapshot computes the repository state on the foreground thread while
/// running the git commands on the background thread. We update branch, head,
/// remotes, and worktrees first so the UI can react sooner, then compute file
/// state and emit those events immediately after.
async fn compute_snapshot(
    this: Entity<Repository>,
    backend: Arc<dyn GitRepository>,
    cx: &mut AsyncApp,
) -> RepositorySnapshot {
    log::debug!("starting compute snapshot");

    let (id, work_directory_abs_path, prev_snapshot) = this.update(cx, |this, _| {
        this.paths_needing_status_update.clear();
        (
            this.id,
            this.work_directory_abs_path.clone(),
            this.snapshot.clone(),
        )
    });

    let branches_future = {
        let backend = backend.clone();
        async move { backend.branches().await.log_err().unwrap_or_default() }
    };
    let head_commit_future = {
        let backend = backend.clone();
        async move { backend.show("HEAD".to_string()).await.ok() }
    };
    let worktrees_future = {
        let backend = backend.clone();
        async move { backend.worktrees().await.log_err().unwrap_or_default() }
    };
    let (branches, head_commit, all_worktrees) =
        futures::future::join3(branches_future, head_commit_future, worktrees_future).await;
    log::debug!("fetched branches, head commit, worktrees");

    let BranchesScanResult {
        branches,
        error: branch_list_error,
    } = branches;
    let branch = branches.iter().find(|branch| branch.is_head).cloned();
    let branch_list: Arc<[Branch]> = branches.into();

    let linked_worktrees: Arc<[GitWorktree]> = all_worktrees
        .into_iter()
        .filter(|wt| wt.path != *work_directory_abs_path)
        .collect();

    let mut remote_urls = backend.remote_urls().await;
    let remote_origin_url = remote_urls.remove("origin");
    let remote_upstream_url = remote_urls.remove("upstream");

    log::debug!("fetched remotes");

    let snapshot = this.update(cx, |this, cx| {
        let head_changed =
            branch != this.snapshot.branch || head_commit != this.snapshot.head_commit;
        let branch_list_changed = *branch_list != *this.snapshot.branch_list;
        let branch_list_error_changed = branch_list_error != this.snapshot.branch_list_error;
        let worktrees_changed = *linked_worktrees != *this.snapshot.linked_worktrees;

        this.snapshot = RepositorySnapshot {
            id,
            work_directory_abs_path,
            branch,
            branch_list: branch_list.clone(),
            branch_list_error,
            head_commit,
            remote_origin_url,
            remote_upstream_url,
            linked_worktrees,
            scan_id: prev_snapshot.scan_id + 1,
            ..prev_snapshot
        };

        if head_changed {
            cx.emit(RepositoryEvent::HeadChanged);
        }

        if branch_list_changed || branch_list_error_changed {
            cx.emit(RepositoryEvent::BranchListChanged);
        }

        if worktrees_changed {
            cx.emit(RepositoryEvent::GitWorktreeListChanged);
        }

        this.snapshot.clone()
    });

    let statuses_future = {
        let backend = backend.clone();
        async move {
            backend
                .status(&[RepoPath::from_rel_path(
                    &RelPath::new(".".as_ref(), PathStyle::local()).unwrap(),
                )])
                .await
                .log_err()
                .unwrap_or_default()
        }
    };
    let diff_stats_future = {
        let snapshot = snapshot.clone();
        let backend = backend.clone();
        async move {
            if snapshot.head_commit.is_some() {
                futures::future::join3(
                    backend.diff_stat(DiffStatType::HeadToWorktree, &[]),
                    backend.diff_stat(DiffStatType::HeadToIndex, &[]),
                    backend.diff_stat(DiffStatType::IndexToWorktree, &[]),
                )
                .await
            } else {
                (
                    Ok(status::GitDiffStat::default()),
                    Ok(status::GitDiffStat::default()),
                    Ok(status::GitDiffStat::default()),
                )
            }
        }
    };
    let stash_entries_future = {
        let backend = backend.clone();
        async move { backend.stash_entries().await.log_err().unwrap_or_default() }
    };

    let (statuses, diff_stats, stash_entries) =
        futures::future::join3(statuses_future, diff_stats_future, stash_entries_future).await;
    let (diff_stats, staged_diff_stats, unstaged_diff_stats) = diff_stats;
    let diff_stats = diff_stats.log_err().unwrap_or_default();
    let staged_diff_stats = staged_diff_stats.log_err().unwrap_or_default();
    let unstaged_diff_stats = unstaged_diff_stats.log_err().unwrap_or_default();
    log::debug!("fetched statuses, diff stats, stash entries");

    let diff_stat_map: HashMap<&RepoPath, DiffStat> =
        diff_stats.entries.iter().map(|(p, s)| (p, *s)).collect();
    let staged_diff_stat_map: HashMap<&RepoPath, DiffStat> = staged_diff_stats
        .entries
        .iter()
        .map(|(p, s)| (p, *s))
        .collect();
    let unstaged_diff_stat_map: HashMap<&RepoPath, DiffStat> = unstaged_diff_stats
        .entries
        .iter()
        .map(|(p, s)| (p, *s))
        .collect();
    let mut conflicted_paths = Vec::new();
    let statuses_by_path = SumTree::from_iter(
        statuses.entries.iter().map(|(repo_path, status)| {
            if status.is_conflicted() {
                conflicted_paths.push(repo_path.clone());
            }
            StatusEntry {
                repo_path: repo_path.clone(),
                status: *status,
                diff_stat: diff_stat_map.get(repo_path).copied(),
                staged_diff_stat: staged_diff_stat_map.get(repo_path).copied(),
                unstaged_diff_stat: unstaged_diff_stat_map.get(repo_path).copied(),
            }
        }),
        (),
    );

    let (merge_details, conflicts_changed) = cx
        .background_spawn({
            let backend = backend.clone();
            let mut merge_details = snapshot.merge.clone();
            async move {
                let conflicts_changed = merge_details.update(&backend, conflicted_paths).await;
                (merge_details, conflicts_changed)
            }
        })
        .await;
    log::debug!("new merge details: {merge_details:?}");

    this.update(cx, |this, cx| {
        if conflicts_changed || statuses_by_path != this.snapshot.statuses_by_path {
            cx.emit(RepositoryEvent::StatusesChanged);
        }
        if stash_entries != this.snapshot.stash_entries {
            cx.emit(RepositoryEvent::StashEntriesChanged);
        }

        this.snapshot.scan_id += 1;
        this.snapshot.merge = merge_details;
        this.snapshot.statuses_by_path = statuses_by_path;
        this.snapshot.stash_entries = stash_entries;

        this.snapshot.clone()
    })
}

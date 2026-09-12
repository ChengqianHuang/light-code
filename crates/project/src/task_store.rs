use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context as _;
use collections::HashMap;
use fs::Fs;
use gpui::{App, AsyncApp, Context, Entity, EventEmitter, Task, WeakEntity};
use language::{ContextLocation, ContextProvider as _, LanguageToolchainStore, Location};
use settings::{InvalidSettingsError, SettingsLocation};
use task::{TaskContext, TaskVariables, VariableName};
use text::{BufferId, OffsetRangeExt};
use util::ResultExt;

use crate::{
    BasicContextProvider, Inventory, ProjectEnvironment, buffer_store::BufferStore,
    git_store::GitStore, worktree_store::WorktreeStore,
};

// platform-dependent warning
pub enum TaskStore {
    Functional(StoreState),
    Noop,
}

pub struct StoreState {
    mode: StoreMode,
    task_inventory: Entity<Inventory>,
    buffer_store: WeakEntity<BufferStore>,
    worktree_store: Entity<WorktreeStore>,
    git_store: Entity<GitStore>,
    toolchain_store: Arc<dyn LanguageToolchainStore>,
}

enum StoreMode {
    Local {
        environment: Entity<ProjectEnvironment>,
    },
}

impl EventEmitter<crate::Event> for TaskStore {}

#[derive(Debug)]
pub enum TaskSettingsLocation<'a> {
    Global(&'a Path),
    Worktree(SettingsLocation<'a>),
}

impl TaskStore {
    pub fn local(
        buffer_store: WeakEntity<BufferStore>,
        worktree_store: Entity<WorktreeStore>,
        toolchain_store: Arc<dyn LanguageToolchainStore>,
        environment: Entity<ProjectEnvironment>,
        git_store: Entity<GitStore>,
        cx: &mut Context<Self>,
    ) -> Self {
        Self::Functional(StoreState {
            mode: StoreMode::Local { environment },
            task_inventory: Inventory::new(cx),
            buffer_store,
            git_store,
            toolchain_store,
            worktree_store,
        })
    }

    pub fn task_context_for_location(
        &self,
        captured_variables: TaskVariables,
        location: Location,
        cx: &mut App,
    ) -> Task<anyhow::Result<Option<TaskContext>>> {
        match self {
            TaskStore::Functional(state) => match &state.mode {
                StoreMode::Local { environment } => local_task_context_for_location(
                    state.worktree_store.clone(),
                    state.git_store.clone(),
                    state.toolchain_store.clone(),
                    environment.clone(),
                    captured_variables,
                    location,
                    cx,
                ),
            },
            TaskStore::Noop => Task::ready(Ok(None)),
        }
    }

    pub fn task_inventory(&self) -> Option<&Entity<Inventory>> {
        match self {
            TaskStore::Functional(state) => Some(&state.task_inventory),
            TaskStore::Noop => None,
        }
    }

    pub(super) fn update_user_tasks(
        &self,
        location: TaskSettingsLocation<'_>,
        raw_tasks_json: Option<&str>,
        cx: &mut Context<Self>,
    ) -> Result<(), InvalidSettingsError> {
        let task_inventory = match self {
            TaskStore::Functional(state) => &state.task_inventory,
            TaskStore::Noop => return Ok(()),
        };
        let raw_tasks_json = raw_tasks_json
            .map(|json| json.trim())
            .filter(|json| !json.is_empty());

        task_inventory.update(cx, |inventory, _| {
            inventory.update_file_based_tasks(location, raw_tasks_json)
        })
    }

    pub(super) fn update_user_debug_scenarios(
        &self,
        location: TaskSettingsLocation<'_>,
        raw_tasks_json: Option<&str>,
        cx: &mut Context<Self>,
    ) -> Result<(), InvalidSettingsError> {
        let task_inventory = match self {
            TaskStore::Functional(state) => &state.task_inventory,
            TaskStore::Noop => return Ok(()),
        };
        let raw_tasks_json = raw_tasks_json
            .map(|json| json.trim())
            .filter(|json| !json.is_empty());

        task_inventory.update(cx, |inventory, _| {
            inventory.update_file_based_scenarios(location, raw_tasks_json)
        })
    }
}

fn local_task_context_for_location(
    worktree_store: Entity<WorktreeStore>,
    git_store: Entity<GitStore>,
    toolchain_store: Arc<dyn LanguageToolchainStore>,
    environment: Entity<ProjectEnvironment>,
    captured_variables: TaskVariables,
    location: Location,
    cx: &App,
) -> Task<anyhow::Result<Option<TaskContext>>> {
    let worktree_id = location.buffer.read(cx).file().map(|f| f.worktree_id(cx));
    let worktree_abs_path = worktree_id
        .and_then(|worktree_id| worktree_store.read(cx).worktree_for_id(worktree_id, cx))
        .and_then(|worktree| worktree.read(cx).root_dir());
    let fs = worktree_store.read(cx).fs();

    cx.spawn(async move |cx| {
        let project_env = environment
            .update(cx, |environment, cx| {
                environment.buffer_environment(&location.buffer, &worktree_store, cx)
            })
            .await;

        let mut task_variables = cx
            .update(|cx| {
                combine_task_variables(
                    captured_variables,
                    fs,
                    worktree_store.clone(),
                    location,
                    project_env.clone(),
                    BasicContextProvider::new(worktree_store, git_store),
                    toolchain_store,
                    cx,
                )
            })
            .await?;

        // Remove all custom entries starting with _, as they're not intended for use by the end user.
        task_variables.sweep();

        Ok(Some(TaskContext {
            project_env: project_env.unwrap_or_default(),
            cwd: worktree_abs_path.map(|p| p.to_path_buf()),
            task_variables,
        }))
    })
}

fn worktree_root(
    worktree_store: &Entity<WorktreeStore>,
    location: &Location,
    cx: &mut App,
) -> Option<PathBuf> {
    location
        .buffer
        .read(cx)
        .file()
        .map(|f| f.worktree_id(cx))
        .and_then(|worktree_id| worktree_store.read(cx).worktree_for_id(worktree_id, cx))
        .and_then(|worktree| {
            let worktree = worktree.read(cx);
            if !worktree.is_visible() {
                return None;
            }
            let root_entry = worktree.root_entry()?;
            if !root_entry.is_dir() {
                return None;
            }
            Some(worktree.absolutize(&root_entry.path))
        })
}

fn combine_task_variables(
    mut captured_variables: TaskVariables,
    fs: Option<Arc<dyn Fs>>,
    worktree_store: Entity<WorktreeStore>,
    location: Location,
    project_env: Option<HashMap<String, String>>,
    baseline: BasicContextProvider,
    toolchain_store: Arc<dyn LanguageToolchainStore>,
    cx: &mut App,
) -> Task<anyhow::Result<TaskVariables>> {
    let language_context_provider = location
        .buffer
        .read(cx)
        .language()
        .and_then(|language| language.context_provider());
    cx.spawn(async move |cx| {
        let baseline = cx
            .update(|cx| {
                let worktree_root = worktree_root(&worktree_store, &location, cx);
                baseline.build_context(
                    &captured_variables,
                    ContextLocation {
                        fs: fs.clone(),
                        worktree_root,
                        file_location: &location,
                    },
                    project_env.clone(),
                    toolchain_store.clone(),
                    cx,
                )
            })
            .await
            .context("building basic default context")?;
        captured_variables.extend(baseline);
        if let Some(provider) = language_context_provider {
            captured_variables.extend(
                cx.update(|cx| {
                    let worktree_root = worktree_root(&worktree_store, &location, cx);
                    provider.build_context(
                        &captured_variables,
                        ContextLocation {
                            fs,
                            worktree_root,
                            file_location: &location,
                        },
                        project_env,
                        toolchain_store,
                        cx,
                    )
                })
                .await?,
            );
        }
        Ok(captured_variables)
    })
}

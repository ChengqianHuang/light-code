#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod app_menus;

use std::{future::Future, path::PathBuf, sync::Arc};

use anyhow::Context as _;
use assets::Assets;
use db::kvp::KeyValueStore;
use extension::ExtensionHostProxy;
use fs::{Fs, RealFs};
use git::GitHostingProviderRegistry;
use gpui::{
    App, AppContext as _, Application, AsyncWindowContext, Context, IntoElement, QuitMode, Render,
    ParentElement as _, StatefulInteractiveElement as _, Styled as _, Task,
    TaskExt as _, TitlebarOptions,
    WeakEntity, Window, WindowOptions, px,
};
use http_client::HttpClientWithUrl;
use language::LanguageRegistry;
use node_runtime::{NodeBinaryOptions, NodeRuntime};
use release_channel::AppVersion;
use reqwest_client::ReqwestClient;
use session::{AppSession, Session};
use theme::ThemeRegistry;
use ui::{ButtonCommon as _, Clickable as _, IconButton, IconName, IconSize, Tooltip, h_flex};
use util::ResultExt as _;
use uuid::Uuid;
use workspace::{
    AppState, OpenOptions, Panel, StatusItemView, Workspace, WorkspaceStore,
    item::ItemHandle,
};

fn local_window_options(_: Option<Uuid>, _: &mut App) -> WindowOptions {
    WindowOptions {
        titlebar: Some(TitlebarOptions {
            title: None,
            appears_transparent: true,
            traffic_light_position: Some(gpui::point(px(9.0), px(9.0))),
        }),
        window_min_size: Some(gpui::Size {
            width: px(360.0),
            height: px(240.0),
        }),
        ..Default::default()
    }
}

fn initialize_local_workspaces(cx: &mut App) {
    cx.observe_new(|workspace: &mut Workspace, window, cx| {
        let Some(window) = window else {
            return;
        };

        initialize_local_status_bar(workspace, window, cx);
        let panels_task = load_local_panels(window, cx);
        workspace.set_panels_task(panels_task);
    })
    .detach();
}


struct WorkspaceButtons;

impl Render for WorkspaceButtons {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        h_flex()
            .gap_1()
            .child(
                IconButton::new("status-bar-manage-projects", IconName::FolderOpen)
                    .icon_size(IconSize::Small)
                    .on_click(|_, window, cx| {
                        window.dispatch_action(Box::new(zed_actions::git::Worktree), cx);
                    })
                    .tooltip(|window, cx| {
                        Tooltip::for_action("Manage projects", &zed_actions::git::Worktree, cx)
                    }),
            )
            .child(
                IconButton::new("status-bar-recent-projects", IconName::Clock)
                    .icon_size(IconSize::Small)
                    .on_click(|_, window, cx| {
                        window.dispatch_action(
                            Box::new(zed_actions::OpenRecent::default()),
                            cx,
                        );
                    })
                    .tooltip(|window, cx| {
                        Tooltip::for_action(
                            "Recent projects",
                            &zed_actions::OpenRecent::default(),
                            cx,
                        )
                    }),
            )
    }
}

impl StatusItemView for WorkspaceButtons {
    fn hide_setting(&self, _: &App) -> Option<workspace::HideStatusItem> {
        None
    }

    fn set_active_pane_item(
        &mut self,
        _: Option<&dyn ItemHandle>,
        _: &mut Window,
        _: &mut Context<Self>,
    ) {
    }
}

fn initialize_local_status_bar(
    workspace: &mut Workspace,
    window: &mut Window,
    cx: &mut Context<Workspace>,
) {
    let search = cx.new(|_| search::search_status_button::SearchButton::new());
    let diagnostics = cx.new(|cx| diagnostics::items::DiagnosticIndicator::new(workspace, cx));
    let active_file = cx.new(|_| workspace::active_file_name::ActiveFileName::new());
    let activity = activity_indicator::ActivityIndicator::new(workspace, window, cx);
    let git_blame = cx.new(|_| git_ui::GitBlameStatus::default());
    let merge_conflicts = cx.new(|cx| git_ui::MergeConflictIndicator::new(workspace, cx));
    let encoding = cx.new(|_| encoding_selector::ActiveBufferEncoding::new(workspace));
    let language = cx.new(|_| language_selector::ActiveBufferLanguage::new(workspace));
    let toolchain = cx.new(|cx| toolchain_selector::ActiveToolchain::new(workspace, window, cx));
    let line_ending = cx.new(|_| line_ending_selector::LineEndingIndicator::default());
    let cursor = cx.new(|_| go_to_line::cursor_position::CursorPosition::new(workspace));
    let image_info = cx.new(|_| image_viewer::ImageInfo::new(workspace));
    let vim_mode = cx.new(|cx| vim::ModeIndicator::new(window, cx));
    let pending_keys = cx.new(|cx| which_key::PendingKeystrokesIndicator::new(window, cx));

    workspace.status_bar().update(cx, |status_bar, cx| {
        status_bar.add_left_item(search, window, cx);
        status_bar.add_left_item(diagnostics, window, cx);
        status_bar.add_left_item(active_file, window, cx);
        status_bar.add_left_item(git_blame, window, cx);
        status_bar.add_left_item(merge_conflicts, window, cx);
        status_bar.add_left_item(activity, window, cx);
        // Keep these first so they end up rightmost, in the bottom-right corner.
        status_bar.add_right_item(cx.new(|_| WorkspaceButtons), window, cx);
        status_bar.add_right_item(encoding, window, cx);
        status_bar.add_right_item(language, window, cx);
        status_bar.add_right_item(toolchain, window, cx);
        status_bar.add_right_item(line_ending, window, cx);
        status_bar.add_right_item(cursor, window, cx);
        status_bar.add_right_item(image_info, window, cx);
        status_bar.add_right_item(vim_mode, window, cx);
        status_bar.add_right_item(pending_keys, window, cx);
    });
}

fn load_local_panels(window: &mut Window, cx: &mut Context<Workspace>) -> Task<anyhow::Result<()>> {
    cx.spawn_in(window, async move |workspace, cx| {
        let project_panel = project_panel::ProjectPanel::load(workspace.clone(), cx.clone());
        let outline_panel = outline_panel::OutlinePanel::load(workspace.clone(), cx.clone());
        let terminal_panel =
            terminal_view::terminal_panel::TerminalPanel::load(workspace.clone(), cx.clone());
        let git_panel = git_ui::git_panel::GitPanel::load(workspace.clone(), cx.clone());
        let mut debug_context = cx.clone();
        let debug_panel =
            debugger_ui::debugger_panel::DebugPanel::load(workspace.clone(), &mut debug_context);

        async fn add_panel_when_ready(
            panel_task: impl Future<Output = anyhow::Result<gpui::Entity<impl Panel>>> + 'static,
            workspace: WeakEntity<Workspace>,
            mut cx: AsyncWindowContext,
        ) {
            if let Some(panel) = panel_task
                .await
                .context("failed to load local panel")
                .log_err()
            {
                workspace
                    .update_in(&mut cx, |workspace, window, cx| {
                        workspace.add_panel(panel, window, cx);
                    })
                    .log_err();
            }
        }

        futures::join!(
            add_panel_when_ready(project_panel, workspace.clone(), cx.clone()),
            add_panel_when_ready(outline_panel, workspace.clone(), cx.clone()),
            add_panel_when_ready(terminal_panel, workspace.clone(), cx.clone()),
            add_panel_when_ready(git_panel, workspace.clone(), cx.clone()),
            add_panel_when_ready(debug_panel, workspace.clone(), cx.clone()),
        );

        workspace.update(cx, |workspace, cx| {
            workspace.finish_dock_restoration(cx);
        })?;
        Ok(())
    })
}

fn main() {
    // Spawned by the shell environment capture (`zed --printenv`): print the
    // inherited environment and exit instead of booting the whole editor.
    if std::env::args_os()
        .skip(1)
        .any(|argument| argument == "--printenv")
    {
        util::shell_env::print_env();
        return;
    }

    zlog::init();
    zlog::init_output_stdout();
    ztracing::init();

    for path in [
        paths::config_dir(),
        paths::extensions_dir(),
        paths::languages_dir(),
        paths::debug_adapters_dir(),
        paths::database_dir(),
        paths::logs_dir(),
        paths::temp_dir(),
    ] {
        if let Err(error) = std::fs::create_dir_all(path) {
            eprintln!("failed to create {}: {error}", path.display());
        }
    }

    let platform = gpui_platform::current_platform(false);
    let app = Application::new_inaccessible(platform)
        .with_assets(Assets)
        .with_quit_mode(QuitMode::Explicit);

    let app_database = db::AppDatabase::new();
    let session = app.background_executor().spawn(Session::new(
        Uuid::new_v4().to_string(),
        KeyValueStore::from_app_db(&app_database),
    ));
    let fs = RealFs::new(None, app.background_executor());
    let current_directory = std::env::current_dir().ok();
    let launch_paths: Vec<PathBuf> = std::env::args_os()
        .skip(1)
        .filter(|argument| !argument.to_string_lossy().starts_with('-'))
        .map(PathBuf::from)
        .map(|path| {
            if path.is_absolute() {
                path
            } else if let Some(current_directory) = &current_directory {
                current_directory.join(path)
            } else {
                path
            }
        })
        .collect();

    app.run(move |cx| {
        cx.set_global(app_database);
        menu::init();
        zed_actions::init();
        gpui_tokio::init(cx);
        release_channel::init(AppVersion::load(env!("CARGO_PKG_VERSION"), None, None), cx);
        settings::init(cx);
        zlog_settings::init(cx);
        theme_settings::init(theme::LoadThemes::All(Box::new(Assets)), cx);

        let http_client: Arc<HttpClientWithUrl> = Arc::new(HttpClientWithUrl::new(
            Arc::new(ReqwestClient::new()),
            "https://zed.dev",
            None,
        ));
        cx.set_http_client(http_client.clone());
        <dyn Fs>::set_global(fs.clone(), cx);

        let mut languages = LanguageRegistry::new(cx.background_executor().clone());
        languages.set_language_server_download_dir(paths::languages_dir().clone());
        let languages = Arc::new(languages);
        let node_runtime = NodeRuntime::new(
            http_client.clone(),
            None,
            watch::channel(Some(NodeBinaryOptions {
                allow_path_lookup: true,
                allow_binary_download: true,
                use_paths: None,
            }))
            .1,
        );
        languages::init(languages.clone(), fs.clone(), node_runtime.clone(), cx);

        extension::init(cx);
        let extension_host_proxy = ExtensionHostProxy::global(cx);
        debug_adapter_extension::init(extension_host_proxy.clone(), cx);
        extension_host::init(
            extension_host_proxy.clone(),
            fs.clone(),
            http_client.clone(),
            node_runtime.clone(),
            cx,
        );
        theme_extension::init(
            extension_host_proxy.clone(),
            ThemeRegistry::global(cx),
            cx.background_executor().clone(),
        );

        let workspace_store = cx.new(WorkspaceStore::new);
        language_extension::init(
            language_extension::LspAccess::ViaWorkspaces({
                let workspace_store = workspace_store.clone();
                Arc::new(move |cx: &mut App| {
                    workspace_store.update(cx, |workspace_store, cx| {
                        Ok(workspace_store
                            .workspaces()
                            .filter_map(|workspace| workspace.upgrade())
                            .map(|workspace| workspace.read(cx).project().read(cx).lsp_store())
                            .collect())
                    })
                })
            }),
            extension_host_proxy,
            languages.clone(),
        );
        let session = cx.foreground_executor().block_on(session);
        let app_session = cx.new(|cx| AppSession::new(session, cx));
        let app_state = Arc::new(AppState {
            languages,
            http_client,
            workspace_store,
            fs,
            build_window_options: local_window_options,
            node_runtime,
            session: app_session,
        });
        AppState::set_global(app_state.clone(), cx);
        let menus = app_menus::app_menus(cx);
        cx.set_menus(menus);

        GitHostingProviderRegistry::set_global(Arc::new(GitHostingProviderRegistry::new()), cx);
        git_hosting_providers::init(cx);
        dap_adapters::init(cx);
        snippet_provider::init(cx);
        editor::init(cx);
        debugger_ui::init(cx);
        debugger_tools::init(cx);
        workspace::init(app_state.clone(), cx);
        ui_prompt::init(cx);
        title_bar::init(cx);
        command_palette::init(cx);
        image_viewer::init(cx);
        diagnostics::init(cx);
        go_to_line::init(cx);
        file_finder::init(cx);
        tab_switcher::init(cx);
        outline::init(cx);
        call_hierarchy::init(cx);
        project_symbols::init(cx);
        project_panel::init(cx);
        outline_panel::init(cx);
        tasks_ui::init(cx);
        snippets_ui::init(cx);
        search::init(cx);
        lsp_locations::init(cx);
        vim::init(cx);
        terminal_view::init(cx);
        journal::init(app_state.clone(), cx);
        encoding_selector::init(cx);
        language_selector::init(cx);
        line_ending_selector::init(cx);
        lsp_command_selector::init(cx);
        toolchain_selector::init(cx);
        theme_selector::init(cx);
        settings_profile_selector::init(cx);
        git_ui::init(cx);
        markdown_preview::init(cx);
        tabular_data_preview::init(cx);
        svg_preview::init(cx);
        settings_ui::init(cx);
        keymap_editor::init(cx);
        extensions_ui::init(cx);
        inspector_ui::init(app_state.clone(), cx);
        json_schema_store::init(cx);
        which_key::init(cx);
        initialize_local_workspaces(cx);

        cx.activate(true);
        if launch_paths.is_empty() {
            workspace::open_new(OpenOptions::default(), app_state, cx, |_, _, _| {})
                .detach_and_log_err(cx);
        } else {
            workspace::open_paths(&launch_paths, app_state, OpenOptions::default(), cx)
                .detach_and_log_err(cx);
        }
    });
}

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::sync::Arc;

use assets::Assets;
use db::kvp::KeyValueStore;
use extension::ExtensionHostProxy;
use fs::{Fs, RealFs};
use git::GitHostingProviderRegistry;
use gpui::{AppContext as _, Application, QuitMode, TaskExt as _};
use http_client::HttpClientWithUrl;
use language::LanguageRegistry;
use node_runtime::NodeRuntime;
use release_channel::AppVersion;
use reqwest_client::ReqwestClient;
use session::{AppSession, Session};
use theme::ThemeRegistry;
use uuid::Uuid;
use workspace::{AppState, OpenOptions, WorkspaceStore};

fn main() {
    zlog::init();
    zlog::init_output_stdout();
    ztracing::init();

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
        let node_runtime = NodeRuntime::unavailable();
        languages::init(languages.clone(), fs.clone(), node_runtime.clone(), cx);

        extension::init(cx);
        let extension_host_proxy = ExtensionHostProxy::global(cx);
        extension_host::init(
            extension_host_proxy.clone(),
            fs.clone(),
            http_client.clone(),
            node_runtime.clone(),
            cx,
        );
        theme_extension::init(
            extension_host_proxy,
            ThemeRegistry::global(cx),
            cx.background_executor().clone(),
        );

        let workspace_store = cx.new(WorkspaceStore::new);
        let session = cx.foreground_executor().block_on(session);
        let app_session = cx.new(|cx| AppSession::new(session, cx));
        let app_state = Arc::new(AppState {
            languages,
            http_client,
            workspace_store,
            fs,
            build_window_options: |_, _| Default::default(),
            node_runtime,
            session: app_session,
        });
        AppState::set_global(app_state.clone(), cx);

        GitHostingProviderRegistry::set_global(Arc::new(GitHostingProviderRegistry::new()), cx);
        git_hosting_providers::init(cx);
        editor::init(cx);
        debugger_ui::init(cx);
        debugger_tools::init(cx);
        workspace::init(app_state.clone(), cx);
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
        json_schema_store::init(cx);
        which_key::init(cx);

        cx.activate(true);
        workspace::open_new(OpenOptions::default(), app_state, cx, |_, _, _| {})
            .detach_and_log_err(cx);
    });
}

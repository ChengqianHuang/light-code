use gpui::{App, TaskExt as _};
use workspace::{create_and_open_local_file, with_active_or_new_workspace};
use zed_actions::OpenSettings;

pub fn init(cx: &mut App) {
    cx.on_action(|_: &OpenSettings, cx| {
        with_active_or_new_workspace(cx, |_workspace, window, cx| {
            create_and_open_local_file(paths::settings_file(), window, cx, || {
                settings::initial_user_settings_content().as_ref().into()
            })
            .detach_and_log_err(cx);
        });
    });
}

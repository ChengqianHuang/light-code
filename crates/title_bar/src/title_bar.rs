pub use platform_title_bar::{
    self, DraggedWindowTab, MergeAllWindows, MoveTabToNewWindow, PlatformTitleBar,
    ShowNextWindowTab, ShowPreviousWindowTab,
};

use gpui::{App, Context, Entity, FocusHandle, Focusable, Render, WeakEntity, Window, WindowControlArea};
use ui::prelude::*;
use workspace::Workspace;

pub struct TitleBar {
    focus_handle: FocusHandle,
    workspace: WeakEntity<Workspace>,
}

impl TitleBar {
    fn new(workspace: WeakEntity<Workspace>, cx: &mut Context<Self>) -> Self {
        if let Some(workspace) = workspace.upgrade() {
            cx.observe(&workspace, |_, _, cx| cx.notify()).detach();
        }
        Self {
            focus_handle: cx.focus_handle(),
            workspace,
        }
    }
}

impl Focusable for TitleBar {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for TitleBar {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // With a project open, the first pane's tab bar takes over the top of
        // the window (native Zed style); a separate strip would waste a row.
        // Only empty workspaces keep a slim drag strip.
        let has_worktree = self
            .workspace
            .upgrade()
            .is_some_and(|workspace| {
                workspace
                    .read(cx)
                    .project()
                    .read(cx)
                    .visible_worktrees(cx)
                    .next()
                    .is_some()
            });
        if has_worktree {
            return div().into_any_element();
        }

        let project_name = "Light Code".to_string();

        div()
            .id("welcome_title_bar")
            .h_full()
            .w_full()
            .flex()
            .items_center()
            // The platform title bar container is not mounted in this fork, so
            // this item is what actually paints the strip behind the traffic
            // lights; without a background the window clear color shows
            // through as black.
            .bg(cx.theme().colors().title_bar_background)
            .window_control_area(WindowControlArea::Drag)
            // Leave room for the macOS traffic lights on the left.
            .pl_20()
            .pr_3()
            .child(Label::new(project_name))
            .into_any_element()
    }
}

pub fn init(cx: &mut App) {
    platform_title_bar::PlatformTitleBar::init(cx);
    cx.observe_new(|workspace: &mut Workspace, window, cx| {
        let Some(window) = window else {
            return;
        };
        let workspace_handle = cx.entity().downgrade();
        let item: Entity<TitleBar> = cx.new(|cx| TitleBar::new(workspace_handle, cx));
        workspace.set_titlebar_item(item.into(), window, cx);
    })
    .detach();
}

pub fn restore_banner(_cx: &mut App) {}

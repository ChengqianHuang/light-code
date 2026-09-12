pub use platform_title_bar::{
    self, DraggedWindowTab, MergeAllWindows, MoveTabToNewWindow, PlatformTitleBar,
    ShowNextWindowTab, ShowPreviousWindowTab,
};

use gpui::{App, Context, Entity, FocusHandle, Focusable, Render, WeakEntity, Window};
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
        let project_name = self
            .workspace
            .upgrade()
            .and_then(|workspace| {
                let project = workspace.read(cx).project().clone();
                project
                    .read(cx)
                    .visible_worktrees(cx)
                    .next()
                    .map(|worktree| worktree.read(cx).root_name_str().to_string())
            })
            .unwrap_or_else(|| "Light Code".to_string());

        div()
            .h_full()
            .w_full()
            .flex()
            .items_center()
            .px_3()
            .child(Label::new(project_name))
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

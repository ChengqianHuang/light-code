pub use platform_title_bar::{
    self, DraggedWindowTab, MergeAllWindows, MoveTabToNewWindow, PlatformTitleBar,
    ShowNextWindowTab, ShowPreviousWindowTab,
};

use gpui::{App, Context, Entity, FocusHandle, Focusable, Render, Window};
use ui::prelude::*;
use workspace::Workspace;

pub struct TitleBar {
    focus_handle: FocusHandle,
}

impl TitleBar {
    fn new(cx: &mut Context<Self>) -> Self {
        Self {
            focus_handle: cx.focus_handle(),
        }
    }
}

impl Focusable for TitleBar {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for TitleBar {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div().h_full().w_full()
    }
}

pub fn init(cx: &mut App) {
    platform_title_bar::PlatformTitleBar::init(cx);
    cx.observe_new(|workspace: &mut Workspace, window, cx| {
        let Some(window) = window else {
            return;
        };
        let item: Entity<TitleBar> = cx.new(TitleBar::new);
        workspace.set_titlebar_item(item.into(), window, cx);
    })
    .detach();
}

pub fn restore_banner(_cx: &mut App) {}

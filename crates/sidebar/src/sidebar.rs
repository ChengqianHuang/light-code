use gpui::{App, Context, Entity, EventEmitter, FocusHandle, Focusable, Render, Window, px};
use ui::prelude::*;
use workspace::{
    MultiWorkspace, Sidebar as WorkspaceSidebar, SidebarEvent, SidebarSide, Workspace,
    notifications::NotificationId,
};

gpui::actions!(
    dev,
    [
        /// Shows the local workspace state.
        DumpWorkspaceInfo,
    ]
);

const DEFAULT_WIDTH: f32 = 300.0;
const MIN_WIDTH: f32 = 200.0;
const MAX_WIDTH: f32 = 800.0;

pub struct Sidebar {
    focus_handle: FocusHandle,
    width: f32,
}

impl Sidebar {
    pub fn new(
        _multi_workspace: Entity<MultiWorkspace>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        Self {
            focus_handle: cx.focus_handle(),
            width: DEFAULT_WIDTH,
        }
    }
}

impl WorkspaceSidebar for Sidebar {
    fn width(&self, _cx: &App) -> gpui::Pixels {
        px(self.width)
    }

    fn set_width(&mut self, width: Option<gpui::Pixels>, cx: &mut Context<Self>) {
        self.width = width
            .map(f32::from)
            .unwrap_or(DEFAULT_WIDTH)
            .clamp(MIN_WIDTH, MAX_WIDTH);
        cx.emit(SidebarEvent::SerializeNeeded);
        cx.notify();
    }

    fn has_notifications(&self, _cx: &App) -> bool {
        false
    }

    fn side(&self, _cx: &App) -> SidebarSide {
        SidebarSide::Left
    }
}

impl EventEmitter<SidebarEvent> for Sidebar {}

impl Focusable for Sidebar {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for Sidebar {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div().size_full().p_2().child(Label::new("Local projects"))
    }
}

pub fn dump_workspace_info(
    workspace: &mut Workspace,
    _: &DumpWorkspaceInfo,
    _window: &mut Window,
    cx: &mut Context<Workspace>,
) {
    struct WorkspaceInfoToast;
    let project_count = workspace.project().read(cx).worktrees(cx).count();
    workspace.show_toast(
        workspace::Toast::new(
            NotificationId::unique::<WorkspaceInfoToast>(),
            format!("Local workspace with {project_count} worktree(s)"),
        ),
        cx,
    );
}

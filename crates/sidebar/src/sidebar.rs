use gpui::{
    AnyView, App, Context, CursorStyle, ElementId, Entity, EventEmitter, FocusHandle, Focusable,
    IntoElement, ParentElement, Render, Styled, WeakEntity, Window, px,
};
use project::ProjectGroupKey;
use ui::{
    ButtonCommon as _, Clickable as _, IconName, IconSize, Label, LabelSize, Tooltip, prelude::*,
};
use workspace::{
    MultiWorkspace, OpenMode, Sidebar as WorkspaceSidebar, SidebarEvent, SidebarSide, Workspace,
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
    multi_workspace: WeakEntity<MultiWorkspace>,
    focus_handle: FocusHandle,
    width: f32,
}

impl Sidebar {
    pub fn new(multi_workspace: WeakEntity<MultiWorkspace>, cx: &mut Context<Self>) -> Self {
        if let Some(multi_workspace) = multi_workspace.upgrade() {
            cx.observe(&multi_workspace, |_, _, cx| cx.notify())
                .detach();
        }
        Self {
            multi_workspace,
            focus_handle: cx.focus_handle(),
            width: DEFAULT_WIDTH,
        }
    }

    fn activate_project_group(
        &mut self,
        key: &ProjectGroupKey,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(multi_workspace) = self.multi_workspace.upgrade() else {
            return;
        };
        let existing = multi_workspace
            .read(cx)
            .last_active_workspace_for_group(key, cx)
            .or_else(|| {
                multi_workspace
                    .read(cx)
                    .workspaces()
                    .find(|workspace| {
                        multi_workspace
                            .read(cx)
                            .project_group_key_for_workspace(workspace, cx)
                            == *key
                    })
                    .cloned()
            });
        if let Some(workspace) = existing {
            multi_workspace.update(cx, |multi_workspace, cx| {
                multi_workspace.activate(workspace, None, window, cx);
            });
        } else {
            multi_workspace.update(cx, |multi_workspace, cx| {
                multi_workspace
                    .find_or_create_local_workspace(
                        key.path_list().clone(),
                        Some(key.clone()),
                        None,
                        OpenMode::Activate,
                        None,
                        window,
                        cx,
                    )
                    .detach_and_log_err(cx);
            });
        }
    }

    fn workspace_label(workspace: &Entity<Workspace>, cx: &App) -> String {
        let workspace = workspace.read(cx);
        let names = workspace
            .project()
            .read(cx)
            .visible_worktrees(cx)
            .map(|worktree| worktree.read(cx).root_name_str().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        if names.is_empty() {
            "Empty project".to_string()
        } else {
            names
        }
    }

    fn render_bottom_bar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let multi_workspace = self.multi_workspace.clone();
        h_flex()
            .p_1()
            .gap_1()
            .border_t_1()
            .border_color(cx.theme().colors().border)
            .child(
                IconButton::new("sidebar-add-folder", IconName::FolderAdd)
                    .icon_size(IconSize::Small)
                    .on_click(move |_, _window, cx| {
                        // Adding a folder needs the active workspace context, so
                        // dispatch through the focused workspace rather than
                        // handling it here.
                        if let Some(workspace) = multi_workspace
                            .upgrade()
                            .map(|mw| mw.read(cx).workspace().clone())
                        {
                            workspace.update(cx, |_, cx| {
                                cx.dispatch_action(&workspace::AddFolderToProject);
                            });
                        }
                    })
                    .tooltip(|_, cx| {
                        Tooltip::for_action(
                            "Add Folder to Project…",
                            &workspace::AddFolderToProject,
                            cx,
                        )
                    }),
            )
            .child(div().flex_1())
            .child(
                IconButton::new("sidebar-recent-projects", IconName::Clock)
                    .icon_size(IconSize::Small)
                    .on_click(|_, window, cx| {
                        window.dispatch_action(Box::new(zed_actions::OpenRecent::default()), cx);
                    })
                    .tooltip(|_, cx| {
                        Tooltip::for_action(
                            "Recent projects",
                            &zed_actions::OpenRecent::default(),
                            cx,
                        )
                    }),
            )
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

    fn cycle_project(&mut self, forward: bool, window: &mut Window, cx: &mut Context<Self>) {
        let Some(multi_workspace) = self.multi_workspace.upgrade() else {
            return;
        };
        let groups = multi_workspace.read(cx).project_groups(cx);
        if groups.is_empty() {
            return;
        }
        let active_key = multi_workspace
            .read(cx)
            .project_group_key_for_workspace(multi_workspace.read(cx).workspace(), cx);
        let position = groups.iter().position(|group| group.key == active_key);
        let next = match position {
            Some(position) => {
                if forward {
                    (position + 1) % groups.len()
                } else {
                    (position + groups.len() - 1) % groups.len()
                }
            }
            None => 0,
        };
        let key = groups[next].key.clone();
        self.activate_project_group(&key, window, cx);
    }
}

impl EventEmitter<SidebarEvent> for Sidebar {}

impl Focusable for Sidebar {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for Sidebar {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let Some(multi_workspace) = self.multi_workspace.upgrade() else {
            return div().size_full();
        };
        let multi_workspace = multi_workspace.read(cx);
        let groups = multi_workspace.project_groups(cx);
        let active_workspace = multi_workspace.workspace().clone();
        let active_key = multi_workspace.project_group_key_for_workspace(&active_workspace, cx);
        let active_key = Some(active_key);

        let mut list = v_flex()
            .id("sidebar-project-list")
            .flex_1()
            .overflow_y_scroll();
        for (group_ix, group) in groups.into_iter().enumerate() {
            let is_active_group = Some(&group.key) == active_key.as_ref();
            let name = group.key.display_name(&Default::default()).to_string();
            list = list.child(
                div()
                    .id(ElementId::NamedInteger(
                        "project-group".into(),
                        group_ix as u64,
                    ))
                    .px_2()
                    .py_1()
                    .flex()
                    .items_center()
                    .gap_1()
                    .when(is_active_group, |this| {
                        this.bg(cx.theme().colors().element_active)
                    })
                    .hover(|style| style.bg(cx.theme().colors().element_hover))
                    .on_click({
                        let key = group.key.clone();
                        cx.listener(move |this, _, window, cx| {
                            this.activate_project_group(&key, window, cx);
                        })
                    })
                    .cursor(CursorStyle::PointingHand)
                    .child(
                        div()
                            .when(is_active_group, |this| {
                                this.text_color(cx.theme().colors().text_accent)
                            })
                            .child(Label::new(name).size(LabelSize::Small)),
                    ),
            );

            if group.workspaces.len() > 1 {
                for (workspace_ix, workspace) in group.workspaces.iter().enumerate() {
                    let is_active = *workspace == active_workspace;
                    let label = Self::workspace_label(workspace, cx);
                    list = list.child(
                        div()
                            .id(ElementId::NamedInteger(
                                "workspace-row".into(),
                                (group_ix * 1000 + workspace_ix) as u64,
                            ))
                            .pl_4()
                            .pr_2()
                            .py_0p5()
                            .when(is_active, |this| {
                                this.bg(cx.theme().colors().element_active)
                            })
                            .hover(|style| style.bg(cx.theme().colors().element_hover))
                            .on_click({
                                let workspace = workspace.clone();
                                let multi_workspace = self.multi_workspace.clone();
                                move |_, window, cx| {
                                    if let Some(multi_workspace) = multi_workspace.upgrade() {
                                        multi_workspace.update(cx, |multi_workspace, cx| {
                                            multi_workspace.activate(
                                                workspace.clone(),
                                                None,
                                                window,
                                                cx,
                                            );
                                        });
                                    }
                                }
                            })
                            .cursor(CursorStyle::PointingHand)
                            .child(Label::new(label).size(LabelSize::Small)),
                    );
                }
            }
        }

        v_flex()
            .size_full()
            .bg(cx.theme().colors().panel_background)
            .child(list)
            .child(self.render_bottom_bar(cx))
    }
}

pub fn init(cx: &mut App) {
    cx.observe_new(|workspace: &mut Workspace, window, cx| {
        let Some(window) = window else {
            return;
        };
        let window_handle = window.window_handle();
        let multi_workspace = workspace.multi_workspace().cloned();
        // The workspace is linked to its MultiWorkspace right after creation,
        // so defer until that has happened before registering.
        cx.defer(move |cx| {
            let multi_workspace = match multi_workspace.and_then(|mw| mw.upgrade()) {
                Some(multi_workspace) => multi_workspace,
                None => {
                    let root = window_handle
                        .update(cx, |root: AnyView, _, _| root.downcast::<MultiWorkspace>());
                    match root {
                        Ok(Ok(root)) => root,
                        Err(_) => return,
                        Ok(Err(_)) => return,
                    }
                }
            };
            multi_workspace.update(cx, |multi, cx| {
                if multi.sidebar().is_none() {
                    let sidebar: Entity<Sidebar> =
                        cx.new(|cx| Sidebar::new(multi_workspace.downgrade(), cx));
                    multi.register_sidebar(sidebar, cx);
                }
            });
        });
    })
    .detach();
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

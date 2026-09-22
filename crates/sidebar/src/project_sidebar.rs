use gpui::{
    AnyElement, App, Context, Decorations, Entity, FocusHandle, Focusable, Pixels, Render,
    SharedString, Subscription, TaskExt, WeakEntity, Window, px,
};
use recent_projects::sidebar_recent_projects::SidebarRecentProjects;
use serde::{Deserialize, Serialize};
use settings::Settings as _;
use std::collections::HashMap;
use theme::{ActiveTheme, CLIENT_SIDE_DECORATION_ROUNDING};
use ui::{
    Color, ContextMenu, ContextMenuEntry, Icon, IconButton, IconName, IconSize, KeyBinding, Label,
    LabelSize, PopoverMenu, PopoverMenuHandle, TintColor, Tooltip, prelude::*,
};
use util::ResultExt as _;
use workspace::{
    CloseWindow, MultiWorkspace, MultiWorkspaceEvent, ProjectGroup, ProjectGroupKey,
    Sidebar as WorkspaceSidebar, SidebarSide, ToggleWorkspaceSidebar, WorkspaceSettings,
    sidebar_side_context_menu,
};
use zed_actions::OpenRecent;

const DEFAULT_WIDTH: Pixels = px(300.0);
const MIN_WIDTH: Pixels = px(200.0);
const MAX_WIDTH: Pixels = px(800.0);

#[derive(Default, Serialize, Deserialize)]
struct SerializedSidebar {
    width: Option<f32>,
}

pub struct Sidebar {
    multi_workspace: WeakEntity<MultiWorkspace>,
    width: Pixels,
    focus_handle: FocusHandle,
    recent_projects_popover_handle: PopoverMenuHandle<SidebarRecentProjects>,
    _subscriptions: Vec<Subscription>,
}

impl Sidebar {
    pub fn new(
        multi_workspace: Entity<MultiWorkspace>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let focus_handle = cx.focus_handle();

        let subscription = cx.subscribe_in(
            &multi_workspace,
            window,
            |_, _, _: &MultiWorkspaceEvent, _, cx| cx.notify(),
        );

        Self {
            multi_workspace: multi_workspace.downgrade(),
            width: DEFAULT_WIDTH,
            focus_handle,
            recent_projects_popover_handle: PopoverMenuHandle::default(),
            _subscriptions: vec![subscription],
        }
    }

    fn configured_side(&self, cx: &App) -> SidebarSide {
        WorkspaceSettings::get_global(cx).project_sidebar_side
    }

    fn activate_group(
        multi_workspace: &WeakEntity<MultiWorkspace>,
        key: &ProjectGroupKey,
        window: &mut Window,
        cx: &mut App,
    ) {
        let Some(multi_workspace) = multi_workspace.upgrade() else {
            return;
        };
        let workspace = multi_workspace.read_with(cx, |multi_workspace, cx| {
            multi_workspace
                .last_active_workspace_for_group(key, cx)
                .or_else(|| {
                    multi_workspace
                        .workspaces_for_project_group(key, cx)
                        .into_iter()
                        .next()
                })
        });
        if let Some(workspace) = workspace {
            multi_workspace.update(cx, |multi_workspace, cx| {
                multi_workspace.activate(workspace, None, window, cx);
                multi_workspace.retain_active_workspace(cx);
            });
        }
    }

    fn project_labels(groups: &[ProjectGroup]) -> HashMap<ProjectGroupKey, SharedString> {
        let mut all_paths = groups
            .iter()
            .flat_map(|group| group.key.path_list().paths().iter().cloned())
            .collect::<Vec<_>>();
        all_paths.sort_unstable();
        all_paths.dedup();
        let details =
            util::disambiguate::compute_disambiguation_details(&all_paths, |path, detail| {
                project::path_suffix(path, detail)
            });
        let path_detail_map = all_paths
            .into_iter()
            .zip(details)
            .collect::<HashMap<_, _>>();

        groups
            .iter()
            .map(|group| (group.key.clone(), group.key.display_name(&path_detail_map)))
            .collect()
    }

    fn render_project_menu(
        &self,
        index: usize,
        key: &ProjectGroupKey,
        _cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let multi_workspace = self.multi_workspace.clone();
        let key = key.clone();

        PopoverMenu::new(("project-sidebar-menu", index))
            .trigger(
                IconButton::new(("project-sidebar-menu-trigger", index), IconName::Ellipsis)
                    .icon_size(IconSize::Small)
                    .selected_style(ui::ButtonStyle::Tinted(TintColor::Accent)),
            )
            .menu(move |window, cx| {
                let multi_workspace = multi_workspace.clone();
                let key = key.clone();
                let (group_index, group_count) = multi_workspace
                    .read_with(cx, |multi_workspace, _| {
                        let keys = multi_workspace.project_group_keys();
                        (
                            keys.iter().position(|candidate| candidate == &key),
                            keys.len(),
                        )
                    })
                    .unwrap_or((None, 0));
                let can_move_up = group_index.is_some_and(|index| index > 0);
                let can_move_down = group_index.is_some_and(|index| index + 1 < group_count);

                Some(ContextMenu::build(window, cx, move |menu, _, _| {
                    let open_in_new_window = multi_workspace.clone();
                    let open_key = key.clone();
                    let move_up = multi_workspace.clone();
                    let move_up_key = key.clone();
                    let move_down = multi_workspace.clone();
                    let move_down_key = key.clone();
                    let remove = multi_workspace.clone();
                    let remove_key = key.clone();

                    menu.item(
                        ContextMenuEntry::new("Open Project in New Window")
                            .disabled(group_count < 2)
                            .handler(move |window, cx| {
                                open_in_new_window
                                    .update(cx, |multi_workspace, cx| {
                                        multi_workspace
                                            .open_project_group_in_new_window(&open_key, window, cx)
                                            .detach_and_log_err(cx);
                                    })
                                    .log_err();
                            }),
                    )
                    .separator()
                    .item(
                        ContextMenuEntry::new("Move Up")
                            .disabled(!can_move_up)
                            .handler(move |_, cx| {
                                move_up
                                    .update(cx, |multi_workspace, cx| {
                                        multi_workspace.move_project_group_up(&move_up_key, cx);
                                    })
                                    .log_err();
                            }),
                    )
                    .item(
                        ContextMenuEntry::new("Move Down")
                            .disabled(!can_move_down)
                            .handler(move |_, cx| {
                                move_down
                                    .update(cx, |multi_workspace, cx| {
                                        multi_workspace.move_project_group_down(&move_down_key, cx);
                                    })
                                    .log_err();
                            }),
                    )
                    .separator()
                    .entry("Close Project", None, move |window, cx| {
                        remove
                            .update(cx, |multi_workspace, cx| {
                                multi_workspace
                                    .remove_project_group(&remove_key, window, cx)
                                    .detach_and_log_err(cx);
                            })
                            .log_err();
                    })
                }))
            })
    }

    fn render_project_row(
        &self,
        index: usize,
        group: &ProjectGroup,
        label: SharedString,
        active_key: &ProjectGroupKey,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let is_active = group.key == *active_key;
        let group_name = SharedString::from(format!("project-sidebar-row-{index}"));
        let multi_workspace = self.multi_workspace.clone();
        let key = group.key.clone();

        h_flex()
            .id(("project-sidebar-row", index))
            .group(&group_name)
            .h(px(34.0))
            .mx_1()
            .px_2()
            .gap_2()
            .rounded_md()
            .cursor_pointer()
            .when(is_active, |row| {
                row.bg(cx.theme().colors().element_selected)
            })
            .hover(|style| style.bg(cx.theme().colors().element_hover))
            .child(
                Icon::new(if is_active {
                    IconName::FolderOpen
                } else {
                    IconName::Folder
                })
                .size(IconSize::Small)
                .color(if is_active {
                    Color::Accent
                } else {
                    Color::Muted
                }),
            )
            .child(
                Label::new(label)
                    .size(LabelSize::Small)
                    .truncate()
                    .when(!is_active, |label| label.color(Color::Muted)),
            )
            .child(div().flex_1())
            .child(
                div()
                    .when(!is_active, |element| element.visible_on_hover(&group_name))
                    .on_mouse_down(gpui::MouseButton::Left, |_, _, cx| cx.stop_propagation())
                    .child(self.render_project_menu(index, &group.key, cx)),
            )
            .on_click(move |_, window, cx| {
                Self::activate_group(&multi_workspace, &key, window, cx);
            })
    }

    fn render_recent_projects_button(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let multi_workspace = self.multi_workspace.upgrade();
        let workspace = multi_workspace
            .as_ref()
            .map(|multi_workspace| multi_workspace.read(cx).workspace().downgrade());
        let focus_handle = workspace
            .as_ref()
            .and_then(WeakEntity::upgrade)
            .map(|workspace| workspace.read(cx).focus_handle(cx))
            .unwrap_or_else(|| cx.focus_handle());
        let groups = multi_workspace
            .as_ref()
            .map(|multi_workspace| multi_workspace.read(cx).project_group_keys())
            .unwrap_or_default();

        PopoverMenu::new("sidebar-recent-projects-menu")
            .with_handle(self.recent_projects_popover_handle.clone())
            .menu(move |window, cx| {
                workspace.as_ref().map(|workspace| {
                    SidebarRecentProjects::popover(
                        workspace.clone(),
                        groups.clone(),
                        focus_handle.clone(),
                        window,
                        cx,
                    )
                })
            })
            .trigger_with_tooltip(
                IconButton::new("open-project", IconName::FolderAdd)
                    .icon_size(IconSize::Small)
                    .selected_style(ui::ButtonStyle::Tinted(TintColor::Accent)),
                |_, cx| Tooltip::for_action("Add Project", &OpenRecent::default(), cx),
            )
            .anchor(gpui::Anchor::BottomRight)
    }

    fn render_header(&self, window: &Window, cx: &mut Context<Self>) -> impl IntoElement {
        let on_left = self.configured_side(cx) == SidebarSide::Left;
        let on_right = !on_left;
        let not_fullscreen = !window.is_fullscreen() && !window.is_simple_fullscreen();
        let left_window_controls = !cfg!(target_os = "macos") && not_fullscreen && on_left;
        let right_window_controls = !cfg!(target_os = "macos") && not_fullscreen && on_right;

        h_flex()
            .h(ui::utils::platform_title_bar_height(window))
            .px_2()
            .gap_2()
            .border_b_1()
            .border_color(cx.theme().colors().border)
            .when(left_window_controls, |header| {
                header.children(platform_title_bar::render_left_window_controls(
                    cx.button_layout(),
                    Box::new(CloseWindow),
                    window,
                ))
            })
            .child(
                Icon::new(IconName::Folder)
                    .size(IconSize::Small)
                    .color(Color::Muted),
            )
            .child(Label::new("Projects").size(LabelSize::Small))
            .child(div().flex_1())
            .when(right_window_controls, |header| {
                header.children(platform_title_bar::render_right_window_controls(
                    cx.button_layout(),
                    Box::new(CloseWindow),
                    window,
                ))
            })
    }

    fn render_toggle_button(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let on_right = self.configured_side(cx) == SidebarSide::Right;
        sidebar_side_context_menu("project-sidebar-toggle-menu", cx)
            .anchor(if on_right {
                gpui::Anchor::BottomRight
            } else {
                gpui::Anchor::BottomLeft
            })
            .attach(if on_right {
                gpui::Anchor::TopRight
            } else {
                gpui::Anchor::TopLeft
            })
            .trigger(move |_, _, _| {
                IconButton::new("project-sidebar-close-toggle", IconName::FolderOpen)
                    .icon_size(IconSize::Small)
                    .tooltip(Tooltip::element(move |_, cx| {
                        h_flex()
                            .gap_2()
                            .child(Label::new("Toggle Project Sidebar"))
                            .child(KeyBinding::for_action(&ToggleWorkspaceSidebar, cx))
                            .into_any_element()
                    }))
                    .on_click(|_, window, cx| {
                        if let Some(multi_workspace) = window.root::<MultiWorkspace>().flatten() {
                            multi_workspace.update(cx, |multi_workspace, cx| {
                                multi_workspace.close_sidebar(window, cx);
                            });
                        }
                    })
            })
    }

    fn cycle_project_impl(&self, forward: bool, window: &mut Window, cx: &mut Context<Self>) {
        let Some(multi_workspace) = self.multi_workspace.upgrade() else {
            return;
        };
        let (keys, active_key) = multi_workspace.read_with(cx, |multi_workspace, cx| {
            (
                multi_workspace.project_group_keys(),
                multi_workspace.project_group_key_for_workspace(multi_workspace.workspace(), cx),
            )
        });
        if keys.is_empty() {
            return;
        }
        let active_index = keys.iter().position(|key| key == &active_key).unwrap_or(0);
        let next_index = if forward {
            (active_index + 1) % keys.len()
        } else {
            (active_index + keys.len() - 1) % keys.len()
        };
        Self::activate_group(&self.multi_workspace, &keys[next_index], window, cx);
    }
}

impl WorkspaceSidebar for Sidebar {
    fn width(&self, _cx: &App) -> Pixels {
        self.width
    }

    fn set_width(&mut self, width: Option<Pixels>, cx: &mut Context<Self>) {
        self.width = width.unwrap_or(DEFAULT_WIDTH).clamp(MIN_WIDTH, MAX_WIDTH);
        cx.notify();
    }

    fn has_notifications(&self, _cx: &App) -> bool {
        false
    }

    fn side(&self, cx: &App) -> SidebarSide {
        self.configured_side(cx)
    }

    fn is_threads_list_view_active(&self) -> bool {
        false
    }

    fn cycle_project(&mut self, forward: bool, window: &mut Window, cx: &mut Context<Self>) {
        self.cycle_project_impl(forward, window, cx);
    }

    fn serialized_state(&self, _cx: &App) -> Option<String> {
        serde_json::to_string(&SerializedSidebar {
            width: Some(f32::from(self.width)),
        })
        .log_err()
    }

    fn restore_serialized_state(
        &mut self,
        state: &str,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(serialized) = serde_json::from_str::<SerializedSidebar>(state).log_err()
            && let Some(width) = serialized.width
        {
            self.width = px(width).clamp(MIN_WIDTH, MAX_WIDTH);
            cx.notify();
        }
    }
}

impl gpui::EventEmitter<workspace::SidebarEvent> for Sidebar {}

impl Focusable for Sidebar {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for Sidebar {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let (groups, active_key) = self
            .multi_workspace
            .read_with(cx, |multi_workspace, cx| {
                (
                    multi_workspace.project_groups(cx),
                    multi_workspace
                        .project_group_key_for_workspace(multi_workspace.workspace(), cx),
                )
            })
            .unwrap_or_default();
        let labels = Self::project_labels(&groups);
        let on_left = self.configured_side(cx) == SidebarSide::Left;
        let ui_font = theme_settings::setup_ui_font(window, cx);
        let header = self.render_header(window, cx).into_any_element();
        let rows = groups
            .iter()
            .enumerate()
            .map(|(index, group)| {
                self.render_project_row(
                    index,
                    group,
                    labels.get(&group.key).cloned().unwrap_or_default(),
                    &active_key,
                    cx,
                )
                .into_any_element()
            })
            .collect::<Vec<AnyElement>>();
        let toggle_button = self.render_toggle_button(cx).into_any_element();
        let recent_projects_button = self.render_recent_projects_button(cx).into_any_element();
        let colors = cx.theme().colors();
        let background = colors
            .title_bar_background
            .blend(colors.panel_background.opacity(0.25));

        v_flex()
            .id("project-sidebar")
            .track_focus(&self.focus_handle)
            .font(ui_font)
            .size_full()
            .bg(background)
            .when(on_left, |sidebar| sidebar.border_r_1())
            .when(!on_left, |sidebar| sidebar.border_l_1())
            .border_color(colors.border)
            .map(|sidebar| match window.window_decorations() {
                Decorations::Server => sidebar,
                Decorations::Client { tiling } => sidebar
                    .when(on_left && !(tiling.top || tiling.left), |sidebar| {
                        sidebar.rounded_tl(CLIENT_SIDE_DECORATION_ROUNDING)
                    })
                    .when(on_left && !(tiling.bottom || tiling.left), |sidebar| {
                        sidebar.rounded_bl(CLIENT_SIDE_DECORATION_ROUNDING)
                    })
                    .when(!on_left && !(tiling.top || tiling.right), |sidebar| {
                        sidebar.rounded_tr(CLIENT_SIDE_DECORATION_ROUNDING)
                    })
                    .when(!on_left && !(tiling.bottom || tiling.right), |sidebar| {
                        sidebar.rounded_br(CLIENT_SIDE_DECORATION_ROUNDING)
                    }),
            })
            .child(header)
            .child(
                v_flex()
                    .id("project-sidebar-list")
                    .flex_1()
                    .overflow_y_scroll()
                    .py_1()
                    .when(groups.is_empty(), |list| {
                        list.child(
                            v_flex()
                                .size_full()
                                .items_center()
                                .justify_center()
                                .gap_2()
                                .child(
                                    Icon::new(IconName::FolderOpen)
                                        .size(IconSize::Medium)
                                        .color(Color::Muted),
                                )
                                .child(
                                    Label::new("No projects open")
                                        .size(LabelSize::Small)
                                        .color(Color::Muted),
                                ),
                        )
                    })
                    .children(rows),
            )
            .child(
                h_flex()
                    .p_1()
                    .gap_1()
                    .when(!on_left, |bar| bar.flex_row_reverse())
                    .border_t_1()
                    .border_color(colors.border)
                    .child(toggle_button)
                    .child(div().flex_1())
                    .child(recent_projects_button),
            )
    }
}

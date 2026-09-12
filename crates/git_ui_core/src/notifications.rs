use editor::Editor;
use gpui::{App, Context, Entity, SharedString};
use language::Buffer;
use ui::prelude::*;
use workspace::{Toast, Workspace, notifications::NotificationId};

pub fn open_output(
    operation: impl Into<SharedString>,
    workspace: &mut Workspace,
    output: &str,
    window: &mut Window,
    cx: &mut Context<Workspace>,
) {
    let operation = operation.into();

    let plain_text = terminal::strip_ansi_text(output.as_bytes());

    let buffer = cx.new(|cx| Buffer::local(plain_text.as_str(), cx));
    buffer.update(cx, |buffer, cx| {
        buffer.set_capability(language::Capability::ReadOnly, cx);
    });
    let editor = cx.new(|cx| {
        let mut editor = Editor::for_buffer(buffer, None, window, cx);
        editor.buffer().update(cx, |buffer, cx| {
            buffer.set_title(format!("Output from git {operation}"), cx);
        });
        editor.set_read_only(true);
        editor
    });

    workspace.add_item_to_center(Box::new(editor), window, cx);
}

pub fn show_error_toast(
    workspace: Entity<Workspace>,
    action: impl Into<SharedString>,
    e: anyhow::Error,
    cx: &mut App,
) {
    let action = action.into();
    let message = e.to_string().trim().to_string();
    if message
        .matches(git::repository::REMOTE_CANCELLED_BY_USER)
        .next()
        .is_some()
    { // Hide the cancelled by user message
    } else {
        cx.defer(move |cx| {
            workspace.update(cx, |workspace, cx| {
                struct GitErrorToast;
                workspace.show_toast(
                    Toast::new(
                        NotificationId::unique::<GitErrorToast>(),
                        format!("Git {action} failed: {message}"),
                    ),
                    cx,
                );
            });
        });
    }
}

#[cfg(any())]
fn rpc_error_raw_message_from_chain(error: &anyhow::Error) -> Option<&str> {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<RpcError>().map(RpcError::raw_message))
}

#[cfg(any())]
fn format_git_error_toast_message(error: &anyhow::Error) -> String {
    if let Some(message) = rpc_error_raw_message_from_chain(error) {
        message.trim().to_string()
    } else {
        error.to_string().trim().to_string()
    }
}

#[cfg(all(test, any()))]
mod tests {
    use super::*;

    #[test]
    fn test_format_git_error_toast_message_prefers_raw_rpc_message() {
        let rpc_error = RpcError::from_proto(
            &proto::Error {
                message:
                    "Your local changes to the following files would be overwritten by merge\n"
                        .to_string(),
                code: proto::ErrorCode::Internal as i32,
                tags: Default::default(),
            },
            "Pull",
        );

        let message = format_git_error_toast_message(&rpc_error);
        assert_eq!(
            message,
            "Your local changes to the following files would be overwritten by merge"
        );
    }

    #[test]
    fn test_format_git_error_toast_message_prefers_raw_rpc_message_when_wrapped() {
        let rpc_error = RpcError::from_proto(
            &proto::Error {
                message:
                    "Your local changes to the following files would be overwritten by merge\n"
                        .to_string(),
                code: proto::ErrorCode::Internal as i32,
                tags: Default::default(),
            },
            "Pull",
        );
        let wrapped = rpc_error.context("sending pull request");

        let message = format_git_error_toast_message(&wrapped);
        assert_eq!(
            message,
            "Your local changes to the following files would be overwritten by merge"
        );
    }
}

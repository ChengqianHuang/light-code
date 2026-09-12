> [!IMPORTANT]
> Remove this line to confirm you've reviewed this PR before submitting.

# light-code

AI coding 时代，写代码这件事正在被重新定义。

越来越多的代码由 AI 生成，人的角色从「逐行编写」转向「审查、决策与驾驭」。在这个背景下我们发现：

**人们不再需要一个沉重的全能 IDE，而是极其需要一个轻量、简单的代码编辑器。**

## 为什么选 Zed

我们调研后选择了 [Zed](https://github.com/zed-industries/zed) 作为基础：

- **快**：Rust 编写，GPU 加速渲染，自研 gpui 框架，大文件与低配机器上依然流畅
- **现代**：多语言 LSP、Tree-sitter 语法高亮、内建终端、远程开发，开箱即用
- **有 Agent 基因**：内建 Agent Panel 与 ACP（Agent Client Protocol）生态，天然适配 AI coding 工作流
- **开源**：GPL-3.0-or-later，代码结构清晰，可自由裁剪

## 我们的思路：做减法

Zed 是一个功能全面的编辑器，但全面意味着负担。我们的路线是：

- **移除不需要的代码**——裁掉我们不使用的组件，让仓库更小、构建更快、心智负担更低
- **保留核心体验**——编辑、语言支持、终端、Agent 协作这些主线能力不动摇
- **在此基础上开发特性化功能**——围绕「人与 AI 协作写代码」这一场景做加法，具体特性在规划中

> 项目的裁剪范围与特性路线图会随着开发逐步明确，本文档会持续更新。

## 开发

本项目基于 Zed 源码构建，构建方式与上游一致：

- [macOS 构建指南](./docs/src/development/macos.md)
- [Linux 构建指南](./docs/src/development/linux.md)
- [Windows 构建指南](./docs/src/development/windows.md)

工具链版本由仓库根目录的 `rust-toolchain.toml` 锁定，建议直接使用 `rustup` 管理环境。

## 许可证

本仓库继承 Zed 的许可证体系：主体为 GPL-3.0-or-later，部分组件按其标注采用 Apache-2.0。修改与分发请遵循相应条款。

# SSH 工作台与 Task 授权

CCM 的 SSH 能力分成两层：Files 页的管理员工作台负责连接配置和人工文件浏览；Task 授权只把选定 Profile 的有限能力交给某一个本地 Task。私钥始终由 Manager 后端使用，不会传给浏览器、Task prompt 或 MCP 子进程。

## 管理员使用流程

1. 打开 **Files → SSH workspace → Add profile**。
2. 填写远端地址和用户名，然后上传未加密的 PEM/OpenSSH 私钥；也可继续填写 Manager 本机已有私钥的绝对路径。
3. 探测主机公钥，通过可信渠道核对 SHA-256 指纹后勾选确认并保存。
4. 用 **Test connection** 验证连接；随后可在同页浏览和下载远程文件。
5. 新建 Task 时打开 **SSH access**，选择 Profile，并只勾选需要的 `Run commands`、`Read files`、`Write files`。
6. 已有 Task 可在 Chat 顶栏的 `SSH n` 中修改授权；编辑后必须显式点 **Save SSH grants**。

普通成员只能查看已有授权。Worker、Shared Task 和 Worker 管理副本不允许使用 Manager 本机密钥。

## 授权与失效规则

- `exec`：执行有超时和输出上限的非交互命令，不支持交互 shell 或密码提示。
- `read`：列目录和读取有大小上限的 UTF-8 文本；Files 工作台的下载能力不等于 Task 下载权限。
- `write`：写入最多 1 MB 的 UTF-8 文本；默认拒绝覆盖已有文件，调用方必须显式要求覆盖。
- 每条 Task grant 固定保存授权时的 Profile revision。host、port、username、私钥或固定 host key 变化，或 Profile 被禁用/删除时，旧授权立即失效；管理员需要在 Chat 中核对后重新保存。
- `ccm_ssh` 只暴露有效 grant 所需的工具，并在每次后端操作前重新校验 Task、capability、revision、Profile 状态和 host key。

## 凭据边界

- SSH Profile 数据库只保存私钥路径和公钥指纹，不保存私钥内容；公开 Profile API 只返回脱敏后的路径提示。
- 浏览器上传最多 1 MB，后端先验证私钥格式，再以一次性令牌认领到 `SSH_KEY_STORAGE_DIR`（默认 `~/.ccm/ssh-keys`）；目录为 `0700`、文件为 `0600`。取消会删除待认领文件，保存失败会回滚已认领副本并允许原令牌重试；未认领令牌有效期为 24 小时，Profile 轮换或删除会清理不再引用的 CCM 托管密钥。
- 私钥必须是 Manager 服务用户拥有的普通文件，不得是符号链接或 group/other 可读写文件；加密私钥暂不支持无人值守 Task。
- Files 页折叠区中的 legacy 用户名/密码连接仅保存在浏览器，用于人工文件浏览，不能授权给 Task。
- Secrets 用于把普通密钥值以环境变量注入 Task，与 SSH Profile 无关；不要把 SSH 私钥内容放进 Secrets。
- 主机公钥探测只提供待核对证据，不替代人工通过可信渠道验证指纹。

## API 与执行链路

- 管理 Profile：`/api/ssh-profiles`
- 上传/取消待认领私钥：`POST /api/ssh-profiles/upload-key`、`DELETE /api/ssh-profiles/upload-key/{token}`
- 查看/替换 Task grant：`/api/tasks/{task_id}/ssh-grants`
- MCP 内部操作：`/api/tasks/{task_id}/ssh-access/...`，只接受 Manager 内部服务认证

有有效授权时，Manager 会为 Claude 和 Codex 的普通本地 Task 注入 required `ccm_ssh` MCP。该能力独立于 Codex 主 MCP 开关；如果 required 注入无法得到证明，任务必须在执行前失败，不能静默无 SSH 能力运行。PR Review 等隔离执行链路不会继承 Task SSH 授权。

# pi-token-speed

pi agent 的 footer token 速度监控插件。在 footer 中实时显示模型输出速度：

```
~/Projects/pi-statistic-plugin (main) • my-session
↑12.3k ↓3.4k R10.1k W500 $0.123 34.2%/200k ⚡87/s ▲62/s Σ58/s        (anthropic) glm-4.7 • high
```

- `⚡87/s` — **实时速度**：滑动窗口（最近 5s）输出的 tok/s，流式期间以 accent 色高亮；`~` 前缀（如 `⚡~87/s`）表示 provider 未流式上报 usage，按字符数估算
- `⚡…` — 流式已开始但尚未收到 token（等待首 token）
- `▲408/s` — **上一条消息速度（不含首字）**：`output ÷ (首个token → 结束)`，纯解码速度，**不含任何工具执行时间**
- `▲+339/s` — **上一条消息速度（含首字）**：`output ÷ (消息开始 → 结束)`，TTFT 会摊薄此值
- `t0.3s` — **上一条消息的首字延迟（TTFT）**：`首个token − 消息开始`
- `Σ345/s` — **会话累计平均速度**（不含首字，不含工具时间）

## 聊天记录内的小记

每条 assistant 消息结束后，聊天记录里会附一行小记（不进 LLM 上下文）：

```
▏ 500 tok · model 1.48s · ttft 0.25s · ▲408/s · ▲+339/s
```

每次回复（一次 agent run，可能包含多段模型输出 + 工具调用）结束时，另附一条回复级小记，把时间拆开：

```
▏ ⚡ 2 msgs · 700 tok · model 2.53s · tools 0.30s · wall 2.83s · 277/s incl · 345/s excl
```

- `model` — 纯模型生成时间（各消息流式窗口之和，含各自 TTFT）
- `tools` — `wall − model`：工具执行 + agent 循环开销，**不计入速度**
- `wall` — 本次回复总耗时；纯文本回复的 tools 占比高属正常（循环开销占大头）

小记由 `pi.appendEntry` 持久化 + `pi.registerEntryRenderer` 渲染，`/resume` 后历史小记原样显示。

其余列与默认 footer 一致：`↑input ↓output R/W cache $cost 上下文占比%`，右侧为 model / thinking level / provider。

## 使用

```bash
# 快速试用（不改任何配置）
pi -e /home/ray/Projects/pi-statistic-plugin

# 会话内使用
/tokspeed        # 开 ↔ 关（关=恢复默认 footer）
/tokspeed set    # 设置界面（开关各项 / 选择统计方式）
/tokspeed log    # 每条消息明细表（out/ttft/excl/incl + Σ 平均）
/tokspeed clear  # 清空历史
```

## 设置界面（/tokspeed set）

与内置 /settings 同款的二级选单（基于 SettingsList submenu）：一级为类别，enter 进入二级，esc 返回/关闭；一级右侧实时显示各类摘要：

```
Token speed settings
→ Footer         off · live on 5s   ← 二级：Footer monitor / Live speed ⚡ / Live window (3s/5s/10s)
  Speed metrics  both · ttft · Σ    ← 二级：Last-msg speed (excl/incl/both) / TTFT / Session average
  Chat subtotals msg on · run on    ← 二级：Per-message subtotal / Per-reply subtotal

enter open · esc close
```

设置通过 `appendEntry("tokspeed-settings")` 持久化到当前会话：`/resume` 自动恢复；新会话回到默认值。

`log` 表格示例：

```
#    time   model          out    ttft     excl     incl  note
1    23:30  test-model     500   0.25s    408/s    339/s
2    23:30  test-model     200   0.25s    250/s    190/s
avg ttft 0.25s · Σ excl 346/s · Σ incl 277/s · 2 msgs · 700 out tokens · 1 runs
Σ model 2.53s · Σ tools 0.30s · Σ wall 2.83s
```

默认启用。仅 TUI 模式生效（RPC/print 模式自动跳过）。

## 正式安装（暂缓，按要求未执行）

任选其一：

```bash
# 方式 A：链接到全局扩展目录
ln -s /home/ray/Projects/pi-statistic-plugin ~/.pi/agent/extensions/pi-token-speed

# 方式 B：写入 settings.json
# { "extensions": ["/home/ray/Projects/pi-statistic-plugin"] }
```

## 实现原理（可行性要点）

| 能力 | 机制 |
|------|------|
| 自定义 footer | `ctx.ui.setFooter(factory)`，factory 返回 `{render(width), invalidate, dispose}` |
| 实时 token 数 | `message_update` 事件 → `assistantMessageEvent.partial.usage.output`（Anthropic 流式每帧累加；OpenAI 已带 `include_usage`） |
| 两种速度 | 同一条消息的 `usage.output` 分别除以两段时长：excl = 首 token→结束（纯解码），incl = 消息开始→结束（含 TTFT） |
| 排除工具时长 | 速度只统计 assistant 消息流式窗口；回复级小记额外把 `wall` 拆为 `model` + `tools`，工具耗时一目了然且不参与速度计算 |
| 每条消息持久化 | `pi.appendEntry("tokspeed-msg"/"tokspeed-run")` 写入 session 文件并配 entry renderer 在聊天记录中渲染小记；session 条目本身不存生成时长，这是 `/resume` 后能重建历史的唯一手段 |
| 无流式 usage 的 provider | 退化用 `text_delta/thinking_delta` 字符数 ÷4 估算，显示 `~` 标记（最终记录仍用权威 usage） |
| 流式期间刷新 | factory 闭包持有 `tui`，事件中限流（120ms）调用 `tui.requestRender()` |
| 会话统计 | `ctx.sessionManager.getBranch()` 累计 usage；`ctx.getContextUsage()` 上下文占比；`footerData.getGitBranch()` 分支 |
| 无定时器 | 完全事件驱动，无后台 interval，无资源泄漏；`session_shutdown` 兜底清理 |

注意：

- `setFooter` 会整体替换默认 footer，因此本插件复刻了默认 footer 的核心信息（目录/分支/session 名/token 统计/上下文占比/model/thinking/扩展状态行）
- 每条消息统计通过 custom entry 持久化，`/resume`、`/fork` 后自动恢复历史与 Σ 平均；仅 `/new` 后的新会话从零开始
- TTFT 从 `message_start`（流打开）起算，包含网络 + 排队延迟，略小于真实端到端首字时间
- session 切换时通过重新绑定 `currentCtx` 规避 stale ctx 问题

## 文件

- `index.ts` — 插件主体（单文件扩展）
- `package.json` — pi package 元数据（`pi.extensions` 入口）

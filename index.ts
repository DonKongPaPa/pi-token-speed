/**
 * pi-token-speed — token speed monitor in the pi footer
 *
 * Replaces the default footer with one that adds output speed metrics:
 *   ⚡ live tok/s (sliding window)
 *   ▲ last-message speed excluding TTFT (first token → end)
 *   ▲+ last-message speed including TTFT (message start → end)
 *   t  last-message time-to-first-token
 *   Σ  session average (excluding TTFT)
 * alongside the usual stats (↑input ↓output cache $cost context% model).
 *
 * Per-message records (TTFT, both speeds, tokens, model, aborted flag) are
 * persisted into the session via pi.appendEntry("tokspeed-msg", …), so
 * history and session averages survive /resume. Session files do not store
 * generation durations, so this persistence is what makes reconstruction
 * possible at all.
 *
 * Live samples come from `message_update` streaming events
 * (`partial.usage.output` updates live on most providers); when a provider
 * does not report usage mid-stream, output is estimated from text deltas
 * (~4 chars/token, shown with a "~" marker).
 *
 * Usage:
 *   pi -e ./index.ts           (or install under ~/.pi/agent/extensions/)
 *   /tokspeed                  toggle the monitor on/off
 *   /tokspeed on | off         explicit set
 *   /tokspeed log              per-message stats table
 *   /tokspeed clear            clear in-memory history
 */

import type { ExtensionAPI, ExtensionContext, Theme } from "@earendil-works/pi-coding-agent";
import { getSettingsListTheme } from "@earendil-works/pi-coding-agent";
import type { AssistantMessage } from "@earendil-works/pi-ai";
import { Container, SettingsList, Text, truncateToWidth, visibleWidth, type SettingItem } from "@earendil-works/pi-tui";

/** Live sliding-window length choices (seconds). */
const LIVE_WINDOW_CHOICES = [3, 5, 10];
/** Minimum window span (ms) before a live speed reading is trusted. */
const MIN_SPAN_MS = 300;
/** Rerender throttle while streaming. */
const RENDER_THROTTLE_MS = 120;
/** Custom entry type used for per-message persistence. */
const ENTRY_TYPE = "tokspeed-msg";
/** Custom entry type for per-run (one assistant reply) subtotals. */
const ENTRY_TYPE_RUN = "tokspeed-run";
/** Custom entry type for settings persistence (latest entry wins on restore). */
const ENTRY_TYPE_SETTINGS = "tokspeed-settings";

interface Sample {
	t: number;
	tokens: number;
}

/** Per-message speed record (persisted into the session). */
interface MsgRecord {
	/** Wall-clock time of message end (ms epoch). */
	ts: number;
	/** Model id that produced the message. */
	model: string;
	/** Final output tokens (authoritative, from message usage). */
	output: number;
	/** Time to first token (ms), null if no token growth was observed. */
	ttftMs: number | null;
	/** Generation duration excluding TTFT (first token → end). */
	genMs: number;
	/** Generation duration including TTFT (message start → end). */
	totalMs: number;
	/** output / genMs, tokens per second. */
	tpsExcl: number | null;
	/** output / totalMs, tokens per second. */
	tpsIncl: number | null;
	/** Message ended because the user aborted. */
	aborted: boolean;
}

/**
 * Per-run subtotal (persisted into the session). One run ≈ one assistant
 * reply as the user sees it: it may span several LLM messages separated by
 * tool executions. `modelMs` sums only the assistant-message streaming
 * windows, so `toolMs = wallMs - modelMs` isolates tool-execution time.
 */
interface RunRecord {
	ts: number;
	msgs: number;
	tokens: number;
	/** Sum of per-message durations excluding TTFT. */
	genMs: number;
	/** Sum of per-message durations including TTFT (pure model time). */
	modelMs: number;
	/** agent_start → agent_end wall clock, including tool executions. */
	wallMs: number;
	/** wallMs - modelMs, clamped ≥ 0. */
	toolMs: number;
}

type TuiLike = { requestRender(): void };

/**
 * Display & measurement configuration. Persisted into the session via
 * appendEntry on every change; restored (latest entry wins) on session_start.
 */
interface PluginSettings {
	/** Footer monitor master switch (false restores the default footer). */
	enabled: boolean;
	/** Live speed ⚡ while streaming. */
	showLive: boolean;
	/** Live sliding-window length in seconds. */
	liveWindowSec: number;
	/** Which last-message speed variant(s) to show. */
	speedMode: "excl" | "incl" | "both";
	/** Last-message TTFT `t`. */
	showTtft: boolean;
	/** Session average Σ. */
	showAvg: boolean;
	/** Per-message subtotal line in the chat transcript. */
	showSubtotalMsg: boolean;
	/** Per-run subtotal line in the chat transcript. */
	showSubtotalRun: boolean;
}

function defaultSettings(): PluginSettings {
	return {
		enabled: true,
		showLive: true,
		liveWindowSec: 5,
		speedMode: "both",
		showTtft: true,
		showAvg: true,
		showSubtotalMsg: true,
		showSubtotalRun: true,
	};
}

export default function (pi: ExtensionAPI) {
	// ---- toggle / plumbing ----
	let settings: PluginSettings = defaultSettings();
	let tui: TuiLike | null = null;
	let renderTimer: ReturnType<typeof setTimeout> | null = null;
	/** Latest-bound extension context (session replacement invalidates old ones). */
	let currentCtx: ExtensionContext | undefined;
	let cwd = "";

	// ---- per-message streaming state ----
	let streaming = false;
	let msgStartAt = 0;
	let firstTokenAt = 0;
	let lastSampledTokens = 0;
	let estChars = 0;
	let reportedOutput = 0;
	let samples: Sample[] = [];
	let liveTps: number | null = null;
	let liveIsEstimate = false;

	// ---- history & session aggregates ----
	let history: MsgRecord[] = [];
	let lastMsg: MsgRecord | null = null;
	let aggOutput = 0;
	let aggGenMs = 0; // excluding TTFT
	let aggTotalMs = 0; // including TTFT
	let aggTtftMs = 0;
	let aggCount = 0;

	// ---- per-run (one assistant reply) accumulation ----
	let runActive = false;
	let runWallStart = 0;
	let runModelMs = 0;
	let runGenMs = 0;
	let runTokens = 0;
	let runMsgs = 0;

	// ---- restored per-run aggregates (for the log) ----
	let aggRuns = 0;
	let aggRunModelMs = 0;
	let aggRunToolMs = 0;
	let aggRunWallMs = 0;

	// ------------------------------------------------------------------
	// helpers
	// ------------------------------------------------------------------

	function bind(ctx: ExtensionContext) {
		currentCtx = ctx;
	}

	function requestRender(immediate = false) {
		if (!tui) return;
		if (immediate) {
			if (renderTimer) {
				clearTimeout(renderTimer);
				renderTimer = null;
			}
			tui.requestRender();
			return;
		}
		if (renderTimer) return;
		renderTimer = setTimeout(() => {
			renderTimer = null;
			tui?.requestRender();
		}, RENDER_THROTTLE_MS);
	}

	function beginMessage() {
		streaming = true;
		msgStartAt = performance.now();
		firstTokenAt = 0;
		lastSampledTokens = 0;
		estChars = 0;
		reportedOutput = 0;
		samples = [];
		liveTps = null;
		liveIsEstimate = false;
	}

	function computeLive(now: number): number | null {
		const windowMs = settings.liveWindowSec * 1000;
		if (samples.length < 2) return null;
		const cutoff = now - windowMs;
		let i = 0;
		while (i < samples.length - 1 && samples[i + 1].t <= cutoff) i++;
		if (i > 0) samples = samples.slice(i);
		const first = samples[0]!;
		const last = samples[samples.length - 1]!;
		const dt = last.t - first.t;
		if (dt < MIN_SPAN_MS) return null;
		const dTokens = Math.max(0, last.tokens - first.tokens);
		if (dTokens <= 0) return null;
		return dTokens / (dt / 1000);
	}

	function onTokenProgress(now: number) {
		// Prefer provider-reported cumulative output; fall back to an
		// estimate from streamed characters when the provider only reports
		// usage at the end of the stream.
		const tokens = reportedOutput > 0 ? reportedOutput : Math.round(estChars / 4);
		liveIsEstimate = reportedOutput === 0;
		if (tokens <= lastSampledTokens) {
			requestRender();
			return;
		}
		if (!firstTokenAt) firstTokenAt = now;
		lastSampledTokens = tokens;
		samples.push({ t: now, tokens });
		const cutoff = now - settings.liveWindowSec * 1000 - 1000;
		while (samples.length > 2 && samples[1]!.t <= cutoff) samples.shift();
		liveTps = computeLive(now);
		requestRender();
	}

	function finishMessage(message: AssistantMessage, now: number) {
		const output = message.usage?.output ?? 0;
		if (streaming && msgStartAt > 0 && output > 0) {
			const totalMs = Math.max(1, now - msgStartAt);
			const genMs = firstTokenAt ? Math.max(1, now - firstTokenAt) : 0;
			const ttftMs = firstTokenAt ? firstTokenAt - msgStartAt : null;
			const rec: MsgRecord = {
				ts: Date.now(),
				model: message.model || currentCtx?.model?.id || "?",
				output,
				ttftMs,
				genMs,
				totalMs,
				tpsExcl: genMs > 0 ? output / (genMs / 1000) : null,
				tpsIncl: output / (totalMs / 1000),
				aborted: message.stopReason === "aborted",
			};
			history.push(rec);
			lastMsg = rec;
			aggOutput += output;
			aggTotalMs += totalMs;
			aggCount += 1;
			if (genMs > 0) aggGenMs += genMs;
			if (ttftMs != null) aggTtftMs += ttftMs;
			if (runActive) {
				runModelMs += totalMs;
				runGenMs += genMs;
				runTokens += output;
				runMsgs += 1;
			}
			// Persist so /resume can rebuild history & aggregates.
			try {
				pi.appendEntry(ENTRY_TYPE, rec);
			} catch {
				// Non-fatal (e.g. session not writable); keep in-memory stats.
			}
		}
		streaming = false;
		liveTps = null;
		samples = [];
		requestRender(true);
	}

	function endRun() {
		if (runActive && runMsgs > 0) {
			const wallMs = Math.max(1, performance.now() - runWallStart);
			const rec: RunRecord = {
				ts: Date.now(),
				msgs: runMsgs,
				tokens: runTokens,
				genMs: runGenMs,
				modelMs: runModelMs,
				wallMs,
				toolMs: Math.max(0, wallMs - runModelMs),
			};
			aggRuns += 1;
			aggRunModelMs += rec.modelMs;
			aggRunToolMs += rec.toolMs;
			aggRunWallMs += rec.wallMs;
			try {
				pi.appendEntry(ENTRY_TYPE_RUN, rec);
			} catch {
				// Non-fatal.
			}
		}
		runActive = false;
		runModelMs = 0;
		runGenMs = 0;
		runTokens = 0;
		runMsgs = 0;
	}

	function recordAggregates(rec: MsgRecord) {
		history.push(rec);
		lastMsg = rec;
		aggOutput += rec.output;
		aggTotalMs += rec.totalMs;
		aggCount += 1;
		if (rec.genMs > 0) aggGenMs += rec.genMs;
		if (rec.ttftMs != null) aggTtftMs += rec.ttftMs;
	}

	/** Reset history, then rebuild from persisted per-message/per-run entries. */
	function restoreFromSession(ctx: ExtensionContext) {
		history = [];
		lastMsg = null;
		aggOutput = 0;
		aggGenMs = 0;
		aggTotalMs = 0;
		aggTtftMs = 0;
		aggCount = 0;
		aggRuns = 0;
		aggRunModelMs = 0;
		aggRunToolMs = 0;
		aggRunWallMs = 0;
		settings = defaultSettings();
		let latestSettings: Partial<PluginSettings> | undefined;
		for (const entry of ctx.sessionManager.getEntries()) {
			if (entry.type !== "custom") continue;
			if (entry.customType === ENTRY_TYPE_SETTINGS) {
				const rec = entry.data as Partial<PluginSettings> | undefined;
				if (rec && typeof rec === "object") latestSettings = rec;
			} else if (entry.customType === ENTRY_TYPE) {
				const rec = entry.data as MsgRecord | undefined;
				if (
					rec &&
					typeof rec.output === "number" &&
					typeof rec.totalMs === "number" &&
					rec.output > 0 &&
					rec.totalMs > 0
				) {
					recordAggregates(rec);
				}
			} else if (entry.customType === ENTRY_TYPE_RUN) {
				const rec = entry.data as RunRecord | undefined;
				if (rec && typeof rec.wallMs === "number" && typeof rec.modelMs === "number") {
					aggRuns += 1;
					aggRunModelMs += rec.modelMs;
					aggRunToolMs += rec.toolMs ?? 0;
					aggRunWallMs += rec.wallMs;
				}
			}
		}
		if (latestSettings) {
			settings = { ...defaultSettings(), ...latestSettings };
			if (!LIVE_WINDOW_CHOICES.includes(settings.liveWindowSec)) settings.liveWindowSec = 5;
		}
	}

	function resetSessionState() {
		history = [];
		lastMsg = null;
		aggOutput = 0;
		aggGenMs = 0;
		aggTotalMs = 0;
		aggTtftMs = 0;
		aggCount = 0;
		aggRuns = 0;
		aggRunModelMs = 0;
		aggRunToolMs = 0;
		aggRunWallMs = 0;
		runActive = false;
		runModelMs = 0;
		runGenMs = 0;
		runTokens = 0;
		runMsgs = 0;
		beginMessage();
		streaming = false;
	}

	function stripAnsi(s: string): string {
		// eslint-disable-next-line no-control-regex
		return s.replace(/\x1B\[[0-9;]*m/g, "");
	}

	/** Same thresholds as the default footer, so units look identical (1.0M, 250k, 3.4k…). */
	function formatTokens(count: number): string {
		if (count < 1000) return count.toString();
		if (count < 10000) return `${(count / 1000).toFixed(1)}k`;
		if (count < 1000000) return `${Math.round(count / 1000)}k`;
		if (count < 10000000) return `${(count / 1000000).toFixed(1)}M`;
		return `${Math.round(count / 1000000)}M`;
	}

	function sanitize(text: string): string {
		return text.replace(/[\r\n\t]/g, " ").replace(/ +/g, " ").trim();
	}

	// ------------------------------------------------------------------
	// footer
	// ------------------------------------------------------------------

	function applyFooter(ctx: ExtensionContext) {
		if (ctx.mode !== "tui") return;
		if (!settings.enabled) {
			ctx.ui.setFooter(undefined);
			return;
		}
		ctx.ui.setFooter((t, theme, footerData) => {
			tui = t;
			return {
				invalidate() {},
				dispose() {
					tui = null;
				},
				render(width: number): string[] {
					const ctx = currentCtx;

					// ---- line 1: pwd (branch) • session name ----
					let pwd = cwd || "~";
					const branch = footerData.getGitBranch();
					if (branch) pwd += ` (${branch})`;
					const sessionName = pi.getSessionName();
					if (sessionName) pwd += ` • ${sessionName}`;

					// ---- speed parts ----
					// ⚡  live (accent while streaming)   ▲  last msg excl TTFT
					// ▲+ last msg incl TTFT              t  last msg TTFT
					// Σ  session avg excl TTFT
					// Plain strings: the compose step wraps them in dim, matching the
					// default footer where the whole stats line is dim.
					// Visibility of each metric is controlled by settings.
					const speedParts: string[] = [];
					if (settings.showLive) {
						if (streaming && liveTps != null) {
							speedParts.push(theme.fg("accent", `⚡${liveIsEstimate ? "~" : ""}${liveTps.toFixed(0)}/s`));
						} else if (streaming) {
							speedParts.push(theme.fg("accent", "⚡…"));
						}
					}
					if (lastMsg && settings.speedMode !== "incl" && lastMsg.tpsExcl != null) {
						speedParts.push(`▲${lastMsg.tpsExcl.toFixed(0)}/s`);
					}
					if (lastMsg && settings.speedMode !== "excl" && lastMsg.tpsIncl != null) {
						speedParts.push(`▲+${lastMsg.tpsIncl.toFixed(0)}/s`);
					}
					if (settings.showTtft && lastMsg?.ttftMs != null) {
						speedParts.push(`t${(lastMsg.ttftMs / 1000).toFixed(1)}s`);
					}
					if (settings.showAvg && aggGenMs > 0) {
						const avg = aggOutput / (aggGenMs / 1000);
						speedParts.push(`Σ${avg.toFixed(0)}/s`);
					}

					// ---- default footer stats ----
					const statsParts: string[] = [];
					if (ctx) {
						let input = 0;
						let output = 0;
						let cacheRead = 0;
						let cacheWrite = 0;
						let cost = 0;
						for (const entry of ctx.sessionManager.getBranch()) {
							if (entry.type !== "message") continue;
							const m = entry.message;
							if (m.role === "assistant") {
								input += m.usage.input;
								output += m.usage.output;
								cacheRead += m.usage.cacheRead;
								cacheWrite += m.usage.cacheWrite;
								cost += m.usage.cost.total;
							} else if (m.role === "toolResult" && m.usage) {
								input += m.usage.input;
								output += m.usage.output;
								cacheRead += m.usage.cacheRead;
								cacheWrite += m.usage.cacheWrite;
								cost += m.usage.cost.total;
							}
						}
						if (input) statsParts.push(`↑${formatTokens(input)}`);
						if (output) statsParts.push(`↓${formatTokens(output)}`);
						if (cacheRead) statsParts.push(`R${formatTokens(cacheRead)}`);
						if (cacheWrite) statsParts.push(`W${formatTokens(cacheWrite)}`);
						if (cost) statsParts.push(`$${cost.toFixed(3)}`);

						const usage = ctx.getContextUsage();
						if (usage) {
							const window = formatTokens(usage.contextWindow);
							const pct = usage.percent != null ? `${usage.percent.toFixed(1)}%` : `?`;
							const display = `${pct}/${window}`;
							if ((usage.percent ?? 0) > 90) {
								statsParts.push(theme.fg("error", display));
							} else if ((usage.percent ?? 0) > 70) {
								statsParts.push(theme.fg("warning", display));
							} else {
								statsParts.push(display);
							}
						}
					}
					const allParts = [...statsParts, ...speedParts];
					if (allParts.length === 0) allParts.push(theme.fg("dim", "—"));

					// ---- right side: model (+ provider/thinking like default footer) ----
					const model = ctx?.model;
					let rightSide = model?.id ?? "no-model";
					if (model?.reasoning) {
						const level = pi.getThinkingLevel() || "off";
						rightSide = level === "off" ? `${rightSide} • off` : `${rightSide} • ${level}`;
					}
					if (footerData.getAvailableProviderCount() > 1 && model) {
						rightSide = `(${model.provider}) ${rightSide}`;
					}

					// ---- compose line 2 ----
					// Dim plain parts individually (the default footer dims the whole
					// stats line). Colored parts (ctx%, accent ⚡) keep their own color:
					// an inner reset would clear an outer dim wrapper.
					const styledParts = allParts.map((p) => (p.includes("\x1b[") ? p : theme.fg("dim", p)));
					const leftPlain = allParts.map((p) => stripAnsi(p)).join(" ");
					const rightPlain = stripAnsi(rightSide);
					let statsLine: string;
					const minPad = 2;
					const total = visibleWidth(leftPlain) + minPad + visibleWidth(rightPlain);
					if (total <= width) {
						const pad = " ".repeat(width - visibleWidth(leftPlain) - visibleWidth(rightPlain));
						statsLine = styledParts.join(" ") + theme.fg("dim", pad + rightSide);
					} else {
						statsLine = truncateToWidth(leftPlain, width, "...") + theme.fg("dim", rightPlain);
					}

					const lines = [
						truncateToWidth(theme.fg("dim", pwd), width, theme.fg("dim", "...")),
						statsLine,
					];

					// ---- optional line 3: extension statuses ----
					const statuses = footerData.getExtensionStatuses();
					if (statuses.size > 0) {
						const statusLine = Array.from(statuses.entries())
							.sort(([a], [b]) => a.localeCompare(b))
							.map(([, text]) => sanitize(text))
							.join(" ");
						lines.push(truncateToWidth(statusLine, width, theme.fg("dim", "...")));
					}
					return lines;
				},
			};
		});
	}

	function fmtDur(ms: number): string {
		return ms >= 10000 ? `${(ms / 1000).toFixed(1)}s` : `${(ms / 1000).toFixed(2)}s`;
	}

	// ------------------------------------------------------------------
	// settings persistence & UI
	// ------------------------------------------------------------------

	function persistSettings() {
		try {
			pi.appendEntry(ENTRY_TYPE_SETTINGS, settings);
		} catch {
			// Non-fatal; settings stay in-memory for this session.
		}
	}

	const ON_OFF = ["on", "off"];
	const bool = (v: boolean) => (v ? "on" : "off");

	function applySetting(id: string, newValue: string) {
		switch (id) {
			case "enabled":
				settings.enabled = newValue === "on";
				break;
			case "showLive":
				settings.showLive = newValue === "on";
				break;
			case "liveWindowSec":
				settings.liveWindowSec = Number.parseInt(newValue, 10) || 5;
				break;
			case "speedMode":
				settings.speedMode = newValue as PluginSettings["speedMode"];
				break;
			case "showTtft":
				settings.showTtft = newValue === "on";
				break;
			case "showAvg":
				settings.showAvg = newValue === "on";
				break;
			case "showSubtotalMsg":
				settings.showSubtotalMsg = newValue === "on";
				break;
			case "showSubtotalRun":
				settings.showSubtotalRun = newValue === "on";
				break;
		}
		persistSettings();
		requestRender(true);
	}

	// Fresh-valued item lists per category, rebuilt on each submenu entry.
	function categoryItems(id: string): SettingItem[] {
		switch (id) {
			case "footer":
				return [
					{
						id: "enabled",
						label: "Footer monitor",
						description: "Show the token-speed footer (off restores the default footer)",
						currentValue: bool(settings.enabled),
						values: ON_OFF,
					},
					{
						id: "showLive",
						label: "Live speed ⚡",
						description: "Output tok/s over a sliding window while streaming",
						currentValue: bool(settings.showLive),
						values: ON_OFF,
					},
					{
						id: "liveWindowSec",
						label: "Live window",
						description: "Sliding window length for the live speed",
						currentValue: `${settings.liveWindowSec}s`,
						values: LIVE_WINDOW_CHOICES.map((s) => `${s}s`),
					},
				];
			case "metrics":
				return [
					{
						id: "speedMode",
						label: "Last-msg speed ▲",
						description: "excl = without TTFT · incl = with TTFT",
						currentValue: settings.speedMode,
						values: ["excl", "incl", "both"],
					},
					{
						id: "showTtft",
						label: "TTFT t",
						description: "Time to first token of the last message",
						currentValue: bool(settings.showTtft),
						values: ON_OFF,
					},
					{
						id: "showAvg",
						label: "Session average Σ",
						description: "Cumulative output tok/s since this session was loaded",
						currentValue: bool(settings.showAvg),
						values: ON_OFF,
					},
				];
			case "subtotals":
				return [
					{
						id: "showSubtotalMsg",
						label: "Per-message subtotal",
						description: "Append a stats line after each assistant message in the chat",
						currentValue: bool(settings.showSubtotalMsg),
						values: ON_OFF,
					},
					{
						id: "showSubtotalRun",
						label: "Per-reply subtotal",
						description: "Append model/tools/wall split after each reply (agent run)",
						currentValue: bool(settings.showSubtotalRun),
						values: ON_OFF,
					},
				];
			default:
				return [];
		}
	}

	// Right-side summaries shown on the root category rows.
	function summaryFor(id: string): string {
		switch (id) {
			case "footer":
				return `${bool(settings.enabled)} · live ${bool(settings.showLive)} ${settings.liveWindowSec}s`;
			case "metrics":
				return `${settings.speedMode}${settings.showTtft ? " · ttft" : ""}${settings.showAvg ? " · Σ" : ""}`;
			case "subtotals":
				return `msg ${bool(settings.showSubtotalMsg)} · run ${bool(settings.showSubtotalRun)}`;
			default:
				return "";
		}
	}

	async function openSettings(ctx: ExtensionContext) {
		if (ctx.mode !== "tui") {
			ctx.ui.notify("Settings UI requires TUI mode (use /tokspeed on|off)", "warning");
			return;
		}
		await ctx.ui.custom((_tui, theme, _keybindings, done) => {
			const container = new Container();
			container.addChild(new Text(theme.fg("accent", theme.bold("Token speed settings")), 1, 1));

			// Second-level submenu factory: a titled SettingsList. Esc returns
			// to the root list with the cursor back on the opening category.
			const makeSubmenu =
				(categoryId: string, title: string): NonNullable<SettingItem["submenu"]> =>
				(_currentValue, submenuDone) => {
					const sub = new Container();
					sub.addChild(new Text(theme.fg("accent", theme.bold(title)), 1, 0));
					const subList = new SettingsList(
						categoryItems(categoryId),
						categoryItems(categoryId).length + 2,
						getSettingsListTheme(),
						(itemId, newValue) => {
							applySetting(itemId, newValue);
							rootList.updateValue(categoryId, summaryFor(categoryId));
							if (itemId === "enabled") applyFooter(ctx);
						},
						() => submenuDone(undefined),
					);
					sub.addChild(subList);
					sub.addChild(new Text(theme.fg("dim", "enter/space cycle · esc back"), 0, 0));
					return {
						render: (w: number) => sub.render(w),
						invalidate: () => sub.invalidate(),
						handleInput: (data: string) => subList.handleInput?.(data),
					};
				};

			const rootItems: SettingItem[] = [
				{
					id: "footer",
					label: "Footer",
					description: "Monitor, live speed & window",
					currentValue: summaryFor("footer"),
					submenu: makeSubmenu("footer", "Footer"),
				},
				{
					id: "metrics",
					label: "Speed metrics",
					description: "Speed variants, TTFT & session average",
					currentValue: summaryFor("metrics"),
					submenu: makeSubmenu("metrics", "Speed metrics"),
				},
				{
					id: "subtotals",
					label: "Chat subtotals",
					description: "Stats lines appended in the chat transcript",
					currentValue: summaryFor("subtotals"),
					submenu: makeSubmenu("subtotals", "Chat subtotals"),
				},
			];

			const rootList = new SettingsList(
				rootItems,
				rootItems.length + 2,
				getSettingsListTheme(),
				() => {},
				() => done(undefined),
			);
			container.addChild(rootList);
			container.addChild(new Text(theme.fg("dim", "enter open · esc close"), 0, 1));
			return {
				render: (w: number) => container.render(w),
				invalidate: () => container.invalidate(),
				handleInput: (data: string) => rootList.handleInput?.(data),
			};
		});
	}

	// ------------------------------------------------------------------
	// transcript subtotals (custom entries + renderers)
	// ------------------------------------------------------------------

	// Per-message subtotal, rendered from the persisted tokspeed-msg entry
	// so it also appears for restored sessions. Durations are pure model
	// time: tool execution happens between messages and is never included.
	pi.registerEntryRenderer(ENTRY_TYPE, (entry, _options, theme) => {
		if (!settings.showSubtotalMsg) return undefined;
		const rec = entry.data as MsgRecord | undefined;
		if (!rec || typeof rec.output !== "number" || typeof rec.totalMs !== "number") return undefined;
		const bits = [`${rec.output} tok`, `model ${fmtDur(rec.totalMs)}`];
		if (rec.ttftMs != null) bits.push(`ttft ${(rec.ttftMs / 1000).toFixed(2)}s`);
		if (rec.tpsExcl != null) bits.push(`▲${rec.tpsExcl.toFixed(0)}/s`);
		if (rec.tpsIncl != null) bits.push(`▲+${rec.tpsIncl.toFixed(0)}/s`);
		if (rec.aborted) bits.push("✱ aborted");
		return new Text(theme.fg("dim", `▏ ${bits.join(" · ")}`), 0, 0);
	});

	// Per-run subtotal: separates pure model time from tool-execution time.
	pi.registerEntryRenderer(ENTRY_TYPE_RUN, (entry, _options, theme) => {
		if (!settings.showSubtotalRun) return undefined;
		const rec = entry.data as RunRecord | undefined;
		if (!rec || typeof rec.wallMs !== "number" || typeof rec.modelMs !== "number") return undefined;
		const bits = [
			`${rec.msgs} msg${rec.msgs > 1 ? "s" : ""}`,
			`${rec.tokens} tok`,
			`model ${fmtDur(rec.modelMs)}`,
		];
		if (rec.toolMs > 50) bits.push(`tools ${fmtDur(rec.toolMs)}`);
		bits.push(`wall ${fmtDur(rec.wallMs)}`);
		if (rec.modelMs > 0) bits.push(`${(rec.tokens / (rec.modelMs / 1000)).toFixed(0)}/s incl`);
		if (rec.genMs > 0) bits.push(`${(rec.tokens / (rec.genMs / 1000)).toFixed(0)}/s excl`);
		return new Text(`${theme.fg("accent", "▏ ⚡")} ${theme.fg("dim", bits.join(" · "))}`, 0, 0);
	});

	// ------------------------------------------------------------------
	// per-message log overlay
	// ------------------------------------------------------------------

	function buildLogLines(theme: Theme) {
		const pad = (s: string, n: number) => s.padEnd(n);
		const padN = (s: string, n: number) => s.padStart(n);
		const fmtTps = (v: number | null) => (v != null ? `${v.toFixed(0)}/s` : "—");
		const time = (ts: number) => {
			const d = new Date(ts);
			return `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
		};
		const lines: string[] = [];
		lines.push(theme.fg("accent", theme.bold("Token speed — per message")));
		lines.push("");
		lines.push(
			theme.fg(
				"dim",
				`${pad("#", 5)}${pad("time", 7)}${pad("model", 18)}${padN("out", 8)}${padN("ttft", 8)}${padN("excl", 9)}${padN("incl", 9)}  note`,
			),
		);
		const shown = history.slice(-200);
		shown.forEach((rec, i) => {
			const index = history.length - shown.length + i + 1;
			const note = rec.aborted ? "✱ aborted" : "";
			lines.push(
				`${pad(String(index), 5)}${pad(time(rec.ts), 7)}${pad(rec.model.slice(0, 17), 18)}` +
					`${padN(String(rec.output), 8)}${padN(rec.ttftMs != null ? `${(rec.ttftMs / 1000).toFixed(2)}s` : "—", 8)}` +
					`${padN(fmtTps(rec.tpsExcl), 9)}${padN(fmtTps(rec.tpsIncl), 9)}  ${note}`,
			);
		});
		if (history.length === 0) {
			lines.push(theme.fg("dim", "(no messages recorded yet)"));
		}
		lines.push("");
		const parts: string[] = [];
		if (aggCount > 0) {
			const avgTtft = aggTtftMs / Math.max(1, history.filter((r) => r.ttftMs != null).length);
			parts.push(`avg ttft ${(avgTtft / 1000).toFixed(2)}s`);
			parts.push(`Σ excl ${(aggOutput / (aggGenMs / 1000)).toFixed(0)}/s`);
			parts.push(`Σ incl ${(aggOutput / (aggTotalMs / 1000)).toFixed(0)}/s`);
			parts.push(`${aggCount} msgs`);
			parts.push(`${aggOutput} out tokens`);
		}
		if (aggRuns > 0) {
			parts.push(`${aggRuns} runs`);
		}
		lines.push(theme.fg("dim", parts.join(" · ") || "no data"));
		if (aggRuns > 0) {
			lines.push(
				theme.fg(
					"dim",
					`Σ model ${fmtDur(aggRunModelMs)} · Σ tools ${fmtDur(aggRunToolMs)} · Σ wall ${fmtDur(aggRunWallMs)}`,
				),
			);
		}
		lines.push(theme.fg("dim", "excl = without TTFT (first token → end) · incl = with TTFT (start → end)"));
		return lines;
	}

	async function showLog(ctx: ExtensionContext) {
		if (ctx.mode !== "tui") {
			ctx.ui.notify("/tokspeed log requires TUI mode", "warning");
			return;
		}
		await ctx.ui.custom((_tui, theme, _keybindings, done) => {
			const container = new Container();
			for (const line of buildLogLines(theme)) {
				container.addChild(new Text(line, 0, 0));
			}
			container.addChild(new Text(theme.fg("dim", "press any key to close"), 1, 0));
			return {
				render: (w: number) => container.render(w),
				invalidate: () => container.invalidate(),
				handleInput: () => {
					done(undefined);
				},
			};
		});
	}

	// ------------------------------------------------------------------
	// events
	// ------------------------------------------------------------------

	pi.on("session_start", async (event, ctx) => {
		bind(ctx);
		cwd = ctx.cwd;
		restoreFromSession(ctx);
		beginMessage();
		streaming = false;
		applyFooter(ctx);
	});

	pi.on("agent_start", async (_event, ctx) => {
		bind(ctx);
		runActive = true;
		runWallStart = performance.now();
		runModelMs = 0;
		runGenMs = 0;
		runTokens = 0;
		runMsgs = 0;
	});

	pi.on("agent_end", async (_event, ctx) => {
		bind(ctx);
		endRun();
	});

	pi.on("session_shutdown", async () => {
		tui = null;
		resetSessionState();
	});

	pi.on("message_start", async (event, ctx) => {
		bind(ctx);
		if (event.message.role !== "assistant") return;
		beginMessage();
	});

	pi.on("message_update", async (event, ctx) => {
		bind(ctx);
		if (event.message.role !== "assistant") return;
		const ev = event.assistantMessageEvent;
		const now = performance.now();
		if (ev.type === "text_delta" || ev.type === "thinking_delta" || ev.type === "toolcall_delta") {
			estChars += ev.delta.length;
		}
		if ("partial" in ev) {
			reportedOutput = Math.max(reportedOutput, ev.partial?.usage?.output ?? 0);
		}
		if (!streaming) beginMessage();
		onTokenProgress(now);
	});

	pi.on("message_end", async (event, ctx) => {
		bind(ctx);
		if (event.message.role !== "assistant") return;
		finishMessage(event.message as AssistantMessage, performance.now());
	});

	pi.on("agent_settled", async (_event, ctx) => {
		bind(ctx);
		if (streaming) {
			streaming = false;
			liveTps = null;
			requestRender(true);
		}
	});

	pi.on("model_select", async (event, ctx) => {
		bind(ctx);
		requestRender(true);
	});

	// ------------------------------------------------------------------
	// command
	// ------------------------------------------------------------------

	pi.registerCommand("tokspeed", {
		description:
			"Token speed monitor: toggle footer, open settings, per-message log (/tokspeed set|log|on|off|clear)",
		getArgumentCompletions: (prefix: string) => {
			const items = [
				{ value: "set", label: "set — open the settings dialog" },
				{ value: "on", label: "on — enable footer monitor" },
				{ value: "off", label: "off — restore default footer" },
				{ value: "log", label: "log — per-message stats table" },
				{ value: "clear", label: "clear — reset history" },
			].filter((i) => i.value.startsWith(prefix));
			return items.length > 0 ? items : null;
		},
		handler: async (args, ctx) => {
			const arg = args.trim().toLowerCase();
			if (arg === "log") {
				await showLog(ctx);
				return;
			}
			if (arg === "set") {
				await openSettings(ctx);
				return;
			}
			if (arg === "clear") {
				const hadData = history.length > 0;
				resetSessionState();
				ctx.ui.notify(hadData ? "Token speed history cleared" : "No history to clear", "info");
				return;
			}
			if (arg === "on") settings.enabled = true;
			else if (arg === "off") settings.enabled = false;
			else settings.enabled = !settings.enabled;
			persistSettings();
			applyFooter(ctx);
			ctx.ui.notify(
				settings.enabled
					? "Token speed monitor: on"
					: "Token speed monitor: off (default footer restored)",
				"info",
			);
		},
	});
}

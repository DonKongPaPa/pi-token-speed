/**
 * Runtime smoke test for pi-token-speed.
 * Loads the extension via jiti (same loader pi uses), feeds it fake event
 * sequences, and verifies: live speed, per-message incl/excl-TTFT speeds,
 * TTFT, persistence via appendEntry, restore on session_start, and the
 * /tokspeed log table.
 */
import { createJiti } from "file:///home/ray/.local/share/mise/installs/node/26.7.0/lib/node_modules/@earendil-works/pi-coding-agent/node_modules/jiti/lib/jiti.mjs";

const PI_DIR = "/home/ray/.local/share/mise/installs/node/26.7.0/lib/node_modules/@earendil-works/pi-coding-agent";
// Real pi registers this alias for extension imports; mirror it for the smoke test.
const jiti = createJiti(import.meta.url, {
	alias: { "@earendil-works/pi-coding-agent": `${PI_DIR}/dist/index.js` },
});
const assert = (cond, msg) => { if (!cond) throw new Error("FAIL: " + msg); };

// ---- fake pi API ----
const handlers = new Map();
const commands = new Map();
const entryRenderers = new Map();
const persistedEntries = []; // collect appendEntry calls
const api = {
	on(event, handler) { handlers.set(event, handler); },
	registerCommand(name, opts) { commands.set(name, opts); },
	registerEntryRenderer: (type, renderer) => entryRenderers.set(type, renderer),
	appendEntry: (type, data) => persistedEntries.push({ type: "custom", customType: type, data }),
	getSessionName: () => undefined,
	getThinkingLevel: () => "high",
};

const mod = await jiti.import("/home/ray/Projects/pi-statistic-plugin/index.ts");
// getSettingsListTheme() needs pi's theme system initialized (the real TUI does this at startup)
const { initTheme } = await jiti.import("@earendil-works/pi-coding-agent");
initTheme("dark");
mod.default(api);
console.log("loaded OK; events:", [...handlers.keys()].join(","));
console.log("commands:", [...commands.keys()].join(","));

// ---- fake footer / ui infrastructure ----
let renderCalls = 0;
const theme = { fg: (_c, s) => s, bold: (s) => s };
let lastLines = [];
const tui = { requestRender: () => { queueMicrotask(() => globalThis.__footer?.render(100)); } };

const footerData = {
	getGitBranch: () => "main",
	getExtensionStatuses: () => new Map(),
	getAvailableProviderCount: () => 1,
	onBranchChange: () => () => {},
};

let branchEntries = [];
const mkCtx = (entries) => ({
	mode: "tui",
	cwd: "/home/ray/Projects/pi-statistic-plugin",
	sessionManager: { getEntries: () => entries ?? [], getBranch: () => branchEntries },
	getContextUsage: () => ({ tokens: 68000, contextWindow: 200000, percent: 34.0 }),
	model: { id: "test-model", provider: "test", reasoning: true },
	ui: {
		notify: (msg) => console.log("[notify]", msg),
		setFooter: (factory) => {
			if (!factory) { console.log("[footer] cleared"); return; }
			const comp = factory(tui, theme, footerData);
			comp.render = ((orig) => (w) => { renderCalls++; lastLines = orig(w); return lastLines; })(comp.render.bind(comp));
			globalThis.__footer = comp;
			console.log("[footer] installed");
		},
		custom: async (factory) => {
			const comp = factory(tui, theme, {}, () => {});
			globalThis.__overlayComp = comp;
			globalThis.__render = () => { globalThis.__overlayLines = comp.render(100); return globalThis.__overlayLines; };
			globalThis.__render();
			// resolve immediately; tests drive the retained component via __overlayComp
			return undefined;
		},
	},
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---- simulate session #1 ----
await handlers.get("session_start")({ reason: "startup" }, mkCtx());
const footer = () => globalThis.__footer;

// helper: simulate one streaming message
async function streamMessage({ finalOutput, perDelta, deltas, deltaMs, chars, reportUsage, ttftDelay = 250 }) {
	let usage = { input: 100, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 100, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } };
	const partial = () => ({ role: "assistant", usage, stopReason: "pending" });
	await handlers.get("message_start")({ type: "message_start", message: partial() }, mkCtx());
	await sleep(ttftDelay); // simulate network + model queue latency before first token
	for (let i = 1; i <= deltas; i++) {
		usage = { ...usage, output: reportUsage ? i * perDelta : 0 };
		await handlers.get("message_update")(
			{
				type: "message_update", message: partial(),
				assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "x".repeat(chars), partial: partial() },
			},
			mkCtx(),
		);
		await sleep(deltaMs);
	}
	const endUsage = { ...usage, output: finalOutput, cost: { ...usage.cost, total: 0.01 } };
	await handlers.get("message_end")({ type: "message_end", message: { role: "assistant", usage: endUsage, stopReason: "stop", model: "test-model" } }, mkCtx());
	return endUsage;
}

// ---- message 1: ~500 tokens/s for ~1.26s, usage reported mid-stream ----
console.log("\n== message 1 (reported usage) ==");
await handlers.get("agent_start")({ type: "agent_start" }, mkCtx());
await streamMessage({ finalOutput: 500, perDelta: 25, deltas: 20, deltaMs: 60, chars: 100, reportUsage: true });
footer().invalidate(); footer().render(100);
const l1 = lastLines.join("\n");
console.log(l1);
const m1excl = Number(l1.match(/▲(\d+)\/s/)?.[1]);
const m1incl = Number(l1.match(/▲\+(\d+)\/s/)?.[1]);
const m1ttft = parseFloat(l1.match(/t([\d.]+)s/)?.[1]);
assert(m1excl > 350 && m1excl < 460, `m1 excl speed sane (~410): ${m1excl}`);
assert(m1incl > 250 && m1incl < m1excl, `m1 incl speed < excl (TTFT dilutes): ${m1incl}`);
assert(m1ttft >= 0.2 && m1ttft <= 0.35, `m1 ttft ≈ 0.25s: ${m1ttft}s`);
assert(persistedEntries.length === 1 && persistedEntries[0].customType === "tokspeed-msg", "message 1 persisted");

// ---- message 2: slower, estimate mode (no mid-stream usage) ----
console.log("\n== message 2 (estimate mode) ==");
branchEntries.push({ type: "message", message: { role: "assistant", usage: { input: 100, output: 500, cacheRead: 0, cacheWrite: 0, totalTokens: 600, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0.01 } } } });
await sleep(300); // simulate tool execution between messages (must be excluded from model time)
await streamMessage({ finalOutput: 200, perDelta: 0, deltas: 10, deltaMs: 80, chars: 100, reportUsage: false });
await handlers.get("agent_end")({ type: "agent_end", messages: [] }, mkCtx());
footer().render(100);
const l2 = lastLines.join("\n");
console.log(l2);
assert(persistedEntries.filter((e) => e.customType === "tokspeed-msg").length === 2, "2 messages persisted");

// ---- run subtotal: model time must exclude the simulated tool wait ----
console.log("\n== run subtotal (tool time exclusion) ==");
const runEntry = persistedEntries.find((e) => e.customType === "tokspeed-run");
assert(runEntry, "run subtotal persisted");
console.log("run record:", JSON.stringify(runEntry.data));
assert(runEntry.data.msgs === 2 && runEntry.data.tokens === 700, "run aggregates 2 msgs / 700 tok");
assert(runEntry.data.toolMs > 250, `toolMs isolates the 300ms tool wait: ${runEntry.data.toolMs.toFixed(0)}ms`);
assert(runEntry.data.modelMs < runEntry.data.wallMs, "modelMs < wallMs");
// modelMs ≈ msg1 total (~1.45s) + msg2 total (~1.05s) ≈ 2.5s, NOT ~2.8s (wall)
assert(runEntry.data.modelMs < 2700, `modelMs excludes tool wait: ${runEntry.data.modelMs.toFixed(0)}ms`);

// render both subtotals as they would appear in the transcript

const msgLine = entryRenderers.get("tokspeed-msg")({ data: persistedEntries[0].data }, { expanded: false }, theme).render(100);
const runLine = entryRenderers.get("tokspeed-run")({ data: runEntry.data }, { expanded: false }, theme).render(100);
console.log("msg subtotal :", msgLine.join(""));
console.log("run subtotal :", runLine.join(""));
const d0 = persistedEntries[0].data;
assert(msgLine.join("").includes(`▲${d0.tpsExcl.toFixed(0)}/s`) && msgLine.join("").includes(`▲+${d0.tpsIncl.toFixed(0)}/s`), "msg subtotal shows excl+incl speeds");
assert(runLine.join("").includes("tools") && runLine.join("").includes("wall"), "run subtotal shows tools/wall split");
assert(runLine.join("").includes("excl") && runLine.join("").includes("incl"), "run subtotal shows both speeds");

// ---- log table ----
console.log("\n== /tokspeed log ==");
const cmd = commands.get("tokspeed");
await cmd.handler("log", mkCtx());
console.log(globalThis.__overlayLines.join("\n"));
const ov = globalThis.__overlayLines.join("\n");
assert(ov.includes("excl") && ov.includes("incl") && ov.includes("ttft"), "log table has headers");
assert(ov.includes("Σ excl") && ov.includes("Σ incl"), "log table has session averages");
assert(ov.includes("Σ model") && ov.includes("Σ tools") && ov.includes("Σ wall"), "log shows model/tools/wall totals");
assert((ov.match(/^\s*2\s+/m) !== null), "log table has 2 rows");

// ---- session switch: restore from persisted entries ----
console.log("\n== restore on session_start ==");
branchEntries = [];
renderCalls = 0;
await handlers.get("session_start")({ reason: "resume" }, mkCtx(persistedEntries));
footer().render(100);
const l3 = lastLines.join("\n");
console.log(l3);
const restoredExcl = Number(l3.match(/▲(\d+)\/s/)?.[1]);
const sumLine = l3.match(/Σ(\d+)\/s/);
const expectedLast = Math.round(persistedEntries[1].data.tpsExcl); // last persisted message = msg 2
assert(restoredExcl === expectedLast, `last msg restored from persistence: ${restoredExcl} === ${expectedLast}`);
assert(sumLine !== null, "Σ avg restored");
// log should show restored run totals too
await commands.get("tokspeed").handler("log", mkCtx(persistedEntries));
const ovRestored = globalThis.__overlayLines.join("\n");
assert(ovRestored.includes("1 runs"), "run count restored from persistence");

// ---- settings dialog: two-level menu ----
console.log("\n== /tokspeed set (two-level menu) ==");
const cmdSetPromise = commands.get("tokspeed").handler("set", mkCtx());
let dlg = globalThis.__render().join("\n");
console.log(dlg);
assert(dlg.includes("Token speed settings"), "root title");
assert(dlg.includes("Footer") && dlg.includes("Speed metrics") && dlg.includes("Chat subtotals"), "root categories");
assert(dlg.includes("both · ttft · Σ"), "root summary for metrics");

// enter opens the "Footer" submenu (first row selected)
globalThis.__overlayComp.handleInput("\r");
dlg = globalThis.__render().join("\n");
assert(dlg.includes("Footer monitor") && dlg.includes("Live window"), "submenu items rendered");
console.log("--- Footer submenu ---");
console.log(dlg);

// enter cycles "Footer monitor" on -> off (SettingsList auto-updates + onChange fires)
globalThis.__overlayComp.handleInput("\r");
const offEntry = persistedEntries.filter((e) => e.customType === "tokspeed-settings").pop();
assert(offEntry && offEntry.data.enabled === false, "cycling to off persisted");

// esc returns to root; root summary must reflect the change, cursor back on "footer"
globalThis.__overlayComp.handleInput("\x1b");
dlg = globalThis.__render().join("\n");
console.log("--- after esc back to root ---");
console.log(dlg);
assert(dlg.includes("off · live on 5s"), "root summary updated after submenu change");
assert(!dlg.includes("Footer monitor"), "submenu closed after esc");

// esc again closes the dialog entirely (root SettingsList esc -> onCancel -> done)
globalThis.__overlayComp.handleInput("\x1b");
await cmdSetPromise;
console.log("two-level navigation OK (open -> cycle -> back -> close)");

// restore footer monitor for the restore test below
await commands.get("tokspeed").handler("on", mkCtx());

console.log(`\nPASS: per-message stats, incl/excl TTFT speeds, TTFT, tool-time exclusion, persistence (${persistedEntries.length} entries), restore OK`);

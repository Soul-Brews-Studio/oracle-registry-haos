/**
 * End-to-end against a real broker. Nine checks, each one a behavior the
 * server must keep — not a mock of the transport but the transport itself.
 *
 *   TEST_BROKER=mqtt://127.0.0.1:1883 bun run test
 *
 * The server under test runs as a child process with an isolated STATE_DIR
 * and a fixed OC_NAME, driven over raw JSON-RPC on stdio exactly the way
 * Claude Code drives it.
 */

import { spawn } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import mqtt from "mqtt";

const BROKER = process.env.TEST_BROKER ?? "mqtt://127.0.0.1:1883";
const NAME = "oc-test";

let passed = 0;
let failed = 0;
function check(label: string, ok: boolean, detail = "") {
  if (ok) { passed++; console.log(`  ok ${label}`); }
  else { failed++; console.error(`FAIL ${label}${detail ? ` — ${detail}` : ""}`); }
}
const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ── test mqtt client ────────────────────────────────────────────────────────

const bus = mqtt.connect(BROKER, { clientId: `oc-test-driver-${Date.now()}` });
await new Promise<void>((res, rej) => {
  bus.once("connect", () => res());
  bus.once("error", (e) => rej(new Error(`no broker at ${BROKER}: ${e.message}`)));
});
// Start from a clean slate: clear any retained status from a previous run.
bus.publish(`${NAME}/status`, "", { retain: true });

const seen: { topic: string; payload: string }[] = [];
bus.on("message", (t, p) => seen.push({ topic: t, payload: p.toString() }));
await new Promise<void>((res) => bus.subscribe([`${NAME}/status`, `${NAME}/+/out`], () => res()));

// ── server under test ───────────────────────────────────────────────────────

const stateDir = mkdtempSync(join(tmpdir(), "oc-e2e-"));
const child = spawn("bun", ["run", "--env-file=/dev/null", "server.ts"], {
  cwd: import.meta.dir,
  env: { ...process.env, OC_STATE_DIR: stateDir, OC_NAME: NAME, OC_BROKER: BROKER },
  stdio: ["pipe", "pipe", "pipe"],
});
child.stderr.on("data", (d) => process.env.E2E_VERBOSE && process.stderr.write(d));

const fromServer: any[] = [];
let buf = "";
child.stdout.on("data", (d) => {
  buf += d.toString();
  let nl;
  while ((nl = buf.indexOf("\n")) >= 0) {
    const line = buf.slice(0, nl).trim();
    buf = buf.slice(nl + 1);
    if (line) { try { fromServer.push(JSON.parse(line)); } catch {} }
  }
});
const rpc = (msg: object) => child.stdin.write(JSON.stringify(msg) + "\n");
async function until<T>(pick: () => T | undefined, ms = 5000): Promise<T | undefined> {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    const v = pick();
    if (v !== undefined) return v;
    await wait(50);
  }
  return undefined;
}

// 1. initialize — and the capability that makes this a channel, not a tool server
rpc({ jsonrpc: "2.0", id: 1, method: "initialize", params: { protocolVersion: "2025-06-18", capabilities: {}, clientInfo: { name: "e2e", version: "0" } } });
const init = await until(() => fromServer.find((m) => m.id === 1));
check("initialize handshake", !!init?.result);
check("declares claude/channel capability", !!init?.result?.capabilities?.experimental?.["claude/channel"]);
rpc({ jsonrpc: "2.0", method: "notifications/initialized" });

// 2. retained presence appears after connect
const status = await until(() => seen.find((m) => m.topic === `${NAME}/status` && m.payload.includes('"online":true')));
check("retained status online", !!status && JSON.parse(status!.payload).client === "oracle-channel");

// 3. JSON inbound → channel notification with meta
bus.publish(`${NAME}/room1/in`, JSON.stringify({ text: "hello json", user: "nat", id: "m1" }));
const n1 = await until(() => fromServer.find((m) => m.method === "notifications/claude/channel" && m.params?.content === "hello json"));
check("json message delivered", !!n1);
check("meta carries room and user", n1?.params?.meta?.chat_id === "room1" && n1?.params?.meta?.user === "nat" && n1?.params?.meta?.message_id === "m1");

// 4. bare-string inbound still delivers
bus.publish(`${NAME}/room1/in`, "plain hello");
const n2 = await until(() => fromServer.find((m) => m.method === "notifications/claude/channel" && m.params?.content === "plain hello"));
check("bare string delivered", !!n2);

// 5. wrong-depth topic is not this contract
bus.publish(`${NAME}/a/b/in`, JSON.stringify({ text: "too deep" }));
await wait(700);
check("depth-4 topic ignored", !fromServer.some((m) => m.params?.content === "too deep"));

// 6. tools/list exposes reply
rpc({ jsonrpc: "2.0", id: 2, method: "tools/list" });
const tools = await until(() => fromServer.find((m) => m.id === 2));
check("reply tool listed", tools?.result?.tools?.some((t: any) => t.name === "reply"));

// 7. reply publishes the out envelope
rpc({ jsonrpc: "2.0", id: 3, method: "tools/call", params: { name: "reply", arguments: { chat_id: "room1", text: "pong", reply_to: "m1" } } });
const out = await until(() => seen.find((m) => m.topic === `${NAME}/room1/out`));
const outMsg = out ? JSON.parse(out.payload) : null;
check("reply reaches out topic", outMsg?.type === "msg" && outMsg?.from === "assistant" && outMsg?.text === "pong" && outMsg?.replyTo === "m1");

// 8. reply refuses a chat_id that is not a topic segment
rpc({ jsonrpc: "2.0", id: 4, method: "tools/call", params: { name: "reply", arguments: { chat_id: "#", text: "x" } } });
const bad = await until(() => fromServer.find((m) => m.id === 4));
check("wildcard chat_id refused", bad?.result?.isError === true);

// 9. clean shutdown flips retained presence to offline
child.stdin.end();
const bye = await until(() => seen.find((m) => m.topic === `${NAME}/status` && m.payload.includes('"reason":"shutdown"')), 6000);
check("shutdown publishes retained offline", !!bye && JSON.parse(bye!.payload).online === false);

bus.publish(`${NAME}/status`, "", { retain: true });
bus.end();
child.kill();
console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);

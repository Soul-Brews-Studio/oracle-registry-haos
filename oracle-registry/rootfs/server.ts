/**
 * Oracle Registry — a liveness inventory built on MQTT's Last Will and Testament.
 *
 * The whole point of LWT is that it reports a death nobody was alive to report.
 * An oracle connects declaring a retained will of `offline`, then publishes a
 * retained `online`. If it exits cleanly it clears or overwrites that retain
 * itself; if it is killed, loses power, or drops off the network, the BROKER
 * publishes the will on its behalf. This service is a subscriber to that
 * mechanism — it never invents liveness, it only records what the broker says.
 *
 * Two topics per oracle, both retained, so a registry that restarts rebuilds
 * the entire fleet's state from the broker in one subscribe:
 *
 *   <prefix>/<name>/lwt    "online" | "offline"      (retained; the will)
 *   <prefix>/<name>/meta   {"host":"m5","repo":...}  (retained; identity)
 *
 * Retained is what makes this durable rather than a tail of live traffic. A
 * non-retained scheme would show an empty fleet after any restart of either
 * side, and "I have not heard from anyone" is indistinguishable from "everyone
 * is down" — which is exactly the ambiguity a registry exists to remove.
 */

import { Database } from "bun:sqlite";
import { randomBytes, timingSafeEqual } from "node:crypto";
import mqtt from "mqtt";

const DB_PATH = process.env.OR_DB ?? "/data/registry.db";
const PORT = Number(process.env.OR_PORT ?? 8099);
const BROKER = process.env.OR_BROKER ?? "";
const USERNAME = process.env.OR_USER ?? "";
const PASSWORD = process.env.OR_PASS ?? "";
const PREFIX = (process.env.OR_PREFIX ?? "oracle").replace(/\/+$/, "");
const STALE_MIN = Number(process.env.OR_STALE_MIN ?? 15);
const RETAIN_EVENTS = Number(process.env.OR_RETAIN_EVENTS ?? 5000);

const nowIso = () => new Date().toISOString();

// ── database ────────────────────────────────────────────────────────────────

const db = new Database(DB_PATH, { create: true });
db.exec("PRAGMA journal_mode = WAL");

db.exec(`
CREATE TABLE IF NOT EXISTS oracles (
  name         TEXT PRIMARY KEY,
  state        TEXT NOT NULL,            -- online | offline | unknown
  host         TEXT,
  meta_json    TEXT,
  first_seen   TEXT NOT NULL,
  last_seen    TEXT NOT NULL,            -- last message of ANY kind
  last_online  TEXT,
  last_offline TEXT,
  transitions  INTEGER NOT NULL DEFAULT 0
);

-- Every state change, append-only. The inventory answers "who is up now";
-- this answers "how did we get here", which is the question you actually have
-- at 3am when something is flapping.
CREATE TABLE IF NOT EXISTS events (
  id     INTEGER PRIMARY KEY AUTOINCREMENT,
  name   TEXT NOT NULL,
  state  TEXT NOT NULL,
  at     TEXT NOT NULL,
  source TEXT NOT NULL                   -- lwt | meta | clear
);
CREATE INDEX IF NOT EXISTS events_name_at ON events(name, at DESC);
CREATE INDEX IF NOT EXISTS events_at ON events(at DESC);

CREATE TABLE IF NOT EXISTS api_keys (
  id         TEXT PRIMARY KEY,
  label      TEXT NOT NULL,
  key        TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL,
  last_used  TEXT
);
`);

const q = {
  upsertState: db.query(`
    INSERT INTO oracles (name, state, first_seen, last_seen, last_online, last_offline, transitions)
    VALUES ($name, $state, $now, $now,
            CASE WHEN $state = 'online'  THEN $now END,
            CASE WHEN $state = 'offline' THEN $now END, 0)
    ON CONFLICT(name) DO UPDATE SET
      state        = $state,
      last_seen    = $now,
      last_online  = CASE WHEN $state = 'online'  THEN $now ELSE last_online  END,
      last_offline = CASE WHEN $state = 'offline' THEN $now ELSE last_offline END,
      -- Only count a real change. A broker replaying the same retained value
      -- on reconnect is not a flap, and counting it would make every restart
      -- of THIS service look like fleet instability.
      transitions  = transitions + CASE WHEN oracles.state <> $state THEN 1 ELSE 0 END
  `),
  upsertMeta: db.query(`
    INSERT INTO oracles (name, state, host, meta_json, first_seen, last_seen, transitions)
    VALUES ($name, 'unknown', $host, $meta, $now, $now, 0)
    ON CONFLICT(name) DO UPDATE SET
      host      = $host,
      meta_json = $meta,
      last_seen = $now
  `),
  priorState: db.query<{ state: string }, [string]>(`SELECT state FROM oracles WHERE name = ?`),
  addEvent: db.query(`INSERT INTO events (name, state, at, source) VALUES ($name, $state, $at, $source)`),
  trimEvents: db.query(`
    DELETE FROM events WHERE id NOT IN (SELECT id FROM events ORDER BY id DESC LIMIT $keep)
  `),
  forget: db.query(`DELETE FROM oracles WHERE name = ?`),
  all: db.query(`SELECT * FROM oracles ORDER BY name`),
  one: db.query(`SELECT * FROM oracles WHERE name = ?`),
  events: db.query(`SELECT * FROM events ORDER BY id DESC LIMIT $limit`),
  eventsFor: db.query(`SELECT * FROM events WHERE name = $name ORDER BY id DESC LIMIT $limit`),
  keys: db.query(`SELECT id, label, created_at, last_used FROM api_keys ORDER BY created_at`),
  keyByValue: db.query<{ id: string }, [string]>(`SELECT id FROM api_keys WHERE key = ?`),
  allKeyValues: db.query<{ id: string; key: string }, []>(`SELECT id, key FROM api_keys`),
  touchKey: db.query(`UPDATE api_keys SET last_used = $at WHERE id = $id`),
  mintKey: db.query(`INSERT INTO api_keys (id, label, key, created_at) VALUES ($id, $label, $key, $at)`),
  revokeKey: db.query(`DELETE FROM api_keys WHERE id = ?`),
};

// ── ingestion ───────────────────────────────────────────────────────────────

/** `oracle/beta/lwt` → "beta". Returns null for anything off-shape. */
function nameFrom(topic: string, leaf: string): string | null {
  const parts = topic.split("/");
  if (parts.length !== 3) return null;
  if (parts[0] !== PREFIX || parts[2] !== leaf) return null;
  return parts[1] || null;
}

function recordState(name: string, state: string, source: string) {
  const prior = q.priorState.get(name)?.state ?? null;
  const at = nowIso();
  q.upsertState.run({ $name: name, $state: state, $now: at });
  // An event per CHANGE, not per message — a retained replay on reconnect
  // would otherwise write a row every time this service restarts.
  if (prior !== state) {
    q.addEvent.run({ $name: name, $state: state, $at: at, $source: source });
    q.trimEvents.run({ $keep: RETAIN_EVENTS });
    console.log(`[registry] ${name}: ${prior ?? "(new)"} → ${state}  (${source})`);
  }
}

let mqttStatus: { connected: boolean; error: string | null; since: string | null } = {
  connected: false,
  error: null,
  since: null,
};

function startMqtt() {
  if (!BROKER) {
    mqttStatus.error = "no broker configured";
    console.error("[registry] no broker configured — set it in Configuration");
    return;
  }

  const client = mqtt.connect(BROKER, {
    username: USERNAME || undefined,
    password: PASSWORD || undefined,
    reconnectPeriod: 5000,
    // This service is a watcher; give it a name a human will recognise in the
    // broker's client list rather than a random id.
    clientId: `oracle-registry-${randomBytes(4).toString("hex")}`,
  });

  client.on("connect", () => {
    mqttStatus = { connected: true, error: null, since: nowIso() };
    // Exactly two patterns. Never `#` — on a fleet broker that is ~109 GB/day,
    // a lesson this fleet paid for once already.
    const topics = [`${PREFIX}/+/lwt`, `${PREFIX}/+/meta`];
    client.subscribe(topics, { qos: 1 }, (err) => {
      if (err) console.error("[registry] subscribe failed:", err.message);
      else console.log(`[registry] connected, watching ${topics.join(" and ")}`);
    });
  });

  client.on("message", (topic, payload) => {
    const text = payload.toString().trim();

    const lwtName = nameFrom(topic, "lwt");
    if (lwtName) {
      // An EMPTY retained payload is how a client clears its retain on a clean
      // shutdown. That is a deliberate goodbye, not a malformed message, and it
      // means offline just as much as the will does.
      if (text === "") {
        recordState(lwtName, "offline", "clear");
        return;
      }
      const state = text.toLowerCase();
      if (state === "online" || state === "offline") recordState(lwtName, state, "lwt");
      else console.warn(`[registry] ${lwtName}: ignoring unrecognised lwt payload`);
      return;
    }

    const metaName = nameFrom(topic, "meta");
    if (metaName) {
      if (text === "") return;
      let host: string | null = null;
      let meta = text;
      try {
        const parsed = JSON.parse(text);
        host = typeof parsed.host === "string" ? parsed.host : null;
        meta = JSON.stringify(parsed);
      } catch {
        // Keep it as opaque text rather than dropping it — a badly-formed meta
        // is still evidence that this oracle exists and is talking.
      }
      q.upsertMeta.run({ $name: metaName, $host: host, $meta: meta, $now: nowIso() });
    }
  });

  client.on("error", (err) => {
    mqttStatus.connected = false;
    mqttStatus.error = err.message;
    console.error("[registry] mqtt error:", err.message);
  });
  client.on("close", () => {
    if (mqttStatus.connected) console.warn("[registry] mqtt connection closed");
    mqttStatus.connected = false;
  });
}

// ── auth ────────────────────────────────────────────────────────────────────

/**
 * Ingress requests already passed Home Assistant's login, so the sidebar page
 * needs no key of its own. Home Assistant sets X-Ingress-Path on everything it
 * proxies, and that header cannot be forged from outside because nothing but
 * HA can reach this port — there is no `ports:` mapping in config.yaml.
 */
const viaIngress = (req: Request) => req.headers.has("x-ingress-path");

function keyFromRequest(req: Request): string | null {
  const auth = req.headers.get("authorization");
  if (auth?.startsWith("Bearer ")) return auth.slice(7).trim();
  const x = req.headers.get("x-api-key");
  return x ? x.trim() : null;
}

/** Constant-time compare against every stored key, so a timing signal cannot leak one. */
function validKey(candidate: string): string | null {
  const buf = Buffer.from(candidate);
  for (const row of q.allKeyValues.all()) {
    const stored = Buffer.from(row.key);
    if (stored.length === buf.length && timingSafeEqual(stored, buf)) return row.id;
  }
  return null;
}

function authorize(req: Request): { ok: true; via: string } | { ok: false } {
  if (viaIngress(req)) return { ok: true, via: "ingress" };
  const candidate = keyFromRequest(req);
  if (!candidate) return { ok: false };
  const id = validKey(candidate);
  if (!id) return { ok: false };
  q.touchKey.run({ $at: nowIso(), $id: id });
  return { ok: true, via: "api-key" };
}

// ── shaping ─────────────────────────────────────────────────────────────────

function decorate(row: any) {
  const staleMs = STALE_MIN * 60_000;
  const lastSeen = Date.parse(row.last_seen);
  // "online but we have not heard from it" — the broker still holds the
  // session, so no will has fired, but nothing has arrived either. Worth
  // surfacing separately: it is neither confirmed up nor confirmed down.
  const stale = row.state === "online" && Number.isFinite(lastSeen) && Date.now() - lastSeen > staleMs;
  let meta: unknown = null;
  if (row.meta_json) {
    try { meta = JSON.parse(row.meta_json); } catch { meta = row.meta_json; }
  }
  return {
    name: row.name,
    state: row.state,
    stale,
    host: row.host ?? null,
    meta,
    firstSeen: row.first_seen,
    lastSeen: row.last_seen,
    lastOnline: row.last_online ?? null,
    lastOffline: row.last_offline ?? null,
    transitions: row.transitions,
  };
}

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body, null, 2), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });

const unauthorized = () =>
  json({ error: "unauthorized", message: "Send Authorization: Bearer <api-key>, or open this through the Home Assistant sidebar." }, 401);

// ── http ────────────────────────────────────────────────────────────────────

const web = `${import.meta.dir}/web`;

Bun.serve({
  port: PORT,
  hostname: "0.0.0.0",
  async fetch(req) {
    const url = new URL(req.url);
    // Ingress serves this under /api/hassio_ingress/<token>/…; strip that so
    // one set of route names works through the sidebar and directly alike.
    const path = url.pathname.replace(/^.*\/api\/hassio_ingress\/[^/]+/, "") || "/";

    // Health is deliberately open: a monitor should be able to see that the
    // registry itself is alive without holding a key.
    if (path === "/api/health") {
      return json({
        status: "ok",
        service: "oracle-registry",
        version: process.env.OR_VERSION ?? "0.1.0",
        mqtt: mqttStatus,
        prefix: PREFIX,
        counts: {
          oracles: (q.all.all() as any[]).length,
          online: (q.all.all() as any[]).filter((r) => r.state === "online").length,
        },
      });
    }

    if (path.startsWith("/api/")) {
      const auth = authorize(req);
      if (!auth.ok) return unauthorized();

      if (path === "/api/oracles" && req.method === "GET") {
        const rows = (q.all.all() as any[]).map(decorate);
        return json({
          count: rows.length,
          online: rows.filter((r) => r.state === "online" && !r.stale).length,
          stale: rows.filter((r) => r.stale).length,
          offline: rows.filter((r) => r.state === "offline").length,
          oracles: rows,
        });
      }

      const oneMatch = path.match(/^\/api\/oracles\/([^/]+)$/);
      if (oneMatch) {
        const name = decodeURIComponent(oneMatch[1]);
        const row = q.one.get(name) as any;
        if (!row) return json({ error: "not_found" }, 404);
        if (req.method === "DELETE") {
          // Forgetting is local bookkeeping only. The retained message lives in
          // the BROKER, so a forgotten oracle reappears the moment the registry
          // resubscribes — unless the retain is cleared at the source too.
          q.forget.run(name);
          return json({ forgotten: name, note: "Local record only. Clear the retained topic on the broker to remove it for good." });
        }
        return json({
          oracle: decorate(row),
          events: q.eventsFor.all({ $name: name, $limit: 100 }),
        });
      }

      if (path === "/api/events" && req.method === "GET") {
        const limit = Math.min(Number(url.searchParams.get("limit") ?? 100), 1000);
        return json({ events: q.events.all({ $limit: limit }) });
      }

      // Keys are mintable only from the sidebar — an API key must not be able
      // to mint another one, or a single leak becomes permanent access that
      // survives revoking the key that caused it.
      if (path === "/api/keys") {
        if (req.method === "GET") return json({ keys: q.keys.all() });
        if (req.method === "POST") {
          if (auth.via !== "ingress") {
            return json({ error: "forbidden", message: "Keys can only be minted from the Home Assistant sidebar." }, 403);
          }
          const body = (await req.json().catch(() => ({}))) as any;
          const label = String(body.label ?? "").trim() || "unnamed";
          const key = `ork_${randomBytes(24).toString("hex")}`;
          const id = randomBytes(8).toString("hex");
          q.mintKey.run({ $id: id, $label: label, $key: key, $at: nowIso() });
          // The only time the key is ever returned. It is not recoverable later.
          return json({ id, label, key, note: "Copy it now — this is the only time it is shown." }, 201);
        }
      }

      const keyMatch = path.match(/^\/api\/keys\/([^/]+)$/);
      if (keyMatch && req.method === "DELETE") {
        if (auth.via !== "ingress") return json({ error: "forbidden" }, 403);
        q.revokeKey.run(keyMatch[1]);
        return json({ revoked: keyMatch[1] });
      }

      return json({ error: "not_found" }, 404);
    }

    const file = Bun.file(path === "/" ? `${web}/index.html` : `${web}${path}`);
    if (await file.exists()) return new Response(file);
    return new Response(Bun.file(`${web}/index.html`));
  },
});

console.log(`[registry] listening on 0.0.0.0:${PORT}, prefix "${PREFIX}", db ${DB_PATH}`);
startMqtt();

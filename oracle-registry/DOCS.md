# Oracle Registry

A liveness inventory for a fleet of agents, built on MQTT's **Last Will and
Testament** — the one mechanism that reports a death nobody was alive to report.

Each member connects declaring a retained will of `offline`, then publishes a
retained `online`. If it exits cleanly it says so itself. If it is killed, loses
power, or drops off the network, **the broker publishes the will on its behalf**.
This add-on subscribes to that and keeps the score. It never infers liveness.

**Read-only by default.** The watcher connection has no publish path — that is
a property of the code, not a configuration. Dispatch (sending messages into a
member's channel) is opt-in: it only exists when a *separate* broker login is
configured, and it runs on its own connection. With `dispatch_username` unset,
this add-on cannot write to your broker at all.

## The contract

Two retained topics per member:

| topic | payload | retained |
|---|---|---|
| `<prefix>/<name>/lwt` | `online` \| `offline` \| *empty* | yes |
| `<prefix>/<name>/meta` | JSON — `{"host": "...", ...}` | yes |

`prefix` defaults to `oracle`, so a member called `beta` owns `oracle/beta/lwt`
and `oracle/beta/meta`.

**Retained is what makes this a registry rather than a tail of traffic.** A
restart of either side rebuilds the whole fleet's state from the broker in one
subscribe. Without it, "I have not heard from anyone" would be
indistinguishable from "everyone is down" — the exact ambiguity a registry
exists to remove.

## Registering a member

```bash
NAME=beta

# identity, retained
mosquitto_pub -h <broker> -u <user> -P <pass> \
  -t "oracle/$NAME/meta" -r -q 1 \
  -m '{"host":"workstation-1","repo":"example/beta"}'

# hold a connection whose WILL is a retained "offline",
# then announce a retained "online"
mosquitto_sub -h <broker> -u <user> -P <pass> -t "oracle/$NAME/ctl" \
  --will-topic "oracle/$NAME/lwt" --will-payload offline --will-retain --will-qos 1 &

mosquitto_pub -h <broker> -u <user> -P <pass> \
  -t "oracle/$NAME/lwt" -m online -r -q 1
```

On a clean shutdown, either publish `offline` or clear the retain entirely with
an empty retained payload (`-r -n`). Both are recorded; clearing also removes
the topic from the broker so a decommissioned member stops reappearing.

## What it shows

| state | meaning |
|---|---|
| `online` | announced itself and the broker still holds its session |
| `stale` | still `online`, but nothing has arrived for `stale_after_minutes` |
| `offline` | it said goodbye, or the broker published its will |
| `unknown` | it published `meta` but has never published `lwt` |

`stale` exists because LWT alone cannot tell a wedged process from a healthy
quiet one. The session is alive, so no will fires — but nothing is arriving
either. That is neither confirmed up nor confirmed down, and flattening it into
`online` would be a lie of convenience.

### Telling a departure from a death

**A will published by the broker is indistinguishable from a member publishing
`offline` itself.** Both arrive on the same topic with the same payload; MQTT
carries no "this was a will" flag to a subscriber. So on payload alone the
registry records *that* something went offline — reliably — but not *whether it
meant to*.

The way out is for a departing member to **clear its retain** (an empty
retained payload) instead of writing `offline`. Then the two cases differ in
shape rather than in content:

| how it went | what the broker holds | recorded source |
|---|---|---|
| left on purpose | nothing — the retain is gone | `clear` |
| killed, crashed, unplugged | `offline`, published by the broker | `lwt` |

The `events` log keeps that source, which is the difference between a tidy
shutdown and a 3am incident. Clearing also removes the topic outright, so a
decommissioned member stops reappearing on every restart.
[`oracle-channel`](../channel/) does this; any member can, with `-r -n`.

## Channels: the naming convention

Members running the [oracle-channel](../channel/) Claude Code plugin are
discoverable and messageable with **zero registration steps** — the channel's
own MQTT connection is its registration. The one rule:

> **The channel's name must equal the member's registry name** (it defaults to
> the session's directory basename).

Everything lives under the registry's own prefix — one tree, one name:

| topic | direction | payload |
|---|---|---|
| `<prefix>/<name>/status` *(retained, LWT-backed)* | channel → registry | `{"online":bool,"client":"oracle-channel","ts"}` |
| `<prefix>/<name>/<room>/in` | registry → channel | `{"text","user","id"}` |
| `<prefix>/<name>/<room>/out` | channel → registry | `{"type":"msg","from":"assistant","text","ts",...}` |

A member with a status topic but no `lwt` shows as a channel-only row with
state `unknown`. A decommissioned channel leaves its retained
`<prefix>/<name>/status` on the broker — clear it there (`-r -n`) or it
reappears, same as `lwt`.

Anything published to `<prefix>/<name>/<room>/in` arrives **inside a Claude session as
a message**. That is the point — and the reason dispatch is treated as a write
credential everywhere below.

## Configuration

| option | default | notes |
|---|---|---|
| `broker` | `mqtt://core-mosquitto:1883` | **hyphen.** `core_mosquitto` is the add-on *slug*; Home Assistant converts underscores to hyphens for the container hostname, and the slug form does not resolve |
| `username` / `password` | empty | the Mosquitto add-on ships with `logins: []` and anonymous disabled — a login must exist before anything can connect |
| `topic_prefix` | `oracle` | subscribes to `<prefix>/+/lwt`, `<prefix>/+/meta`, `<prefix>/+/status` and `<prefix>/+/+/out` only, never `#` |
| `stale_after_minutes` | `15` | silence past this marks an `online` member stale |
| `retain_events` | `5000` | transition-history cap; oldest trimmed |
| `dispatch_username` / `dispatch_password` | empty | a **separate** mosquitto login for the write path. Unset = dispatch off, add-on is a pure watcher. Use a distinct account (e.g. `dispatch`) — never reuse the read login, and mint it fresh rather than recycling an exposed credential. For defense in depth, give it a mosquitto ACL of write-only on `+/+/in` |

## API

The sidebar page is authenticated by Home Assistant, so it needs no key.
Anything else needs one: `Authorization: Bearer <key>`. Mint keys on the page —
**a key cannot mint another key**, so a single leak does not become permanent
access that survives revoking it.

**What proves a request came from the sidebar.** Home Assistant sets
`X-Ingress-Path` on everything it proxies, but a header is a claim, not
evidence — this container listens on the hassio bridge, where every other
add-on can reach it (our own `core-mosquitto` default is one add-on addressing
another that way). So the header is accepted only when the request also arrives
from Supervisor's range, `172.30.32.0/23`. Set `OR_TRUST_INGRESS_HEADER=1` to
run outside Home Assistant; refusals are logged with the peer address either
way.

**Keys are stored as `sha256(key)`, never as the value you were shown.** The
mint response says the key is shown only once, and since 0.3.0 that is
literally true: a copy of `/data/registry.db` — which rides inside Home
Assistant backups — yields no usable credential. Keys minted before 0.3.0 are
hashed in place on first start and keep working.

| route | |
|---|---|
| `GET /api/health` | open, unauthenticated — broker connection state, dispatch state, counts |
| `GET /api/oracles` | the inventory (each row carries `channel: true/false/null`) |
| `GET /api/oracles/:name` | one member plus its last 100 transitions |
| `POST /api/oracles/:name/send` | dispatch `{room?, text, user?}` to `<prefix>/<name>/<room>/in`. Needs a dispatch-capable caller: the sidebar, or a key minted with dispatch permission. `503` dispatch off/disconnected · `403` key lacks permission · `400` bad name/room/text · `429` over 30/min. Every send is logged |
| `GET /api/oracles/:name/replies?since=` | recent channel replies (in-memory, last 200 fleet-wide — a live view, not a transcript) |
| `DELETE /api/oracles/:name` | forget locally, **sidebar only** *(the retained topics live in the broker — clear `lwt` and `status` there too, or it reappears)* |
| `GET /api/events?limit=` | the transition log |
| `GET`/`POST /api/keys`, `DELETE /api/keys/:id` | key management, sidebar only — listing included, since it enumerates which keys can dispatch. `can_dispatch` is set at mint time and never changeable afterwards — a leaked key cannot upgrade itself |

## Install

Add this repository in **Settings → Add-ons → Add-on Store → ⋮ → Repositories**:

```
https://github.com/Soul-Brews-Studio/oracle-registry-haos
```

Then install **Oracle Registry**, set the broker credentials, and start it.

## License

Apache-2.0

# Oracle Registry

A liveness inventory for a fleet of agents, built on MQTT's **Last Will and
Testament** — the one mechanism that reports a death nobody was alive to report.

Each member connects declaring a retained will of `offline`, then publishes a
retained `online`. If it exits cleanly it says so itself. If it is killed, loses
power, or drops off the network, **the broker publishes the will on its behalf**.
This add-on subscribes to that and keeps the score. It never infers liveness and
it never publishes to your fleet's topics.

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

### A limitation worth knowing

**A will published by the broker is indistinguishable from a member publishing
`offline` itself.** Both arrive on the same topic with the same payload; MQTT
carries no "this was a will" flag to a subscriber. So the registry records
*that* something went offline, reliably — but not *whether it meant to*. If you
need that distinction, have members publish a distinct payload on clean
shutdown and set the will to a different one.

## Configuration

| option | default | notes |
|---|---|---|
| `broker` | `mqtt://core-mosquitto:1883` | **hyphen.** `core_mosquitto` is the add-on *slug*; Home Assistant converts underscores to hyphens for the container hostname, and the slug form does not resolve |
| `username` / `password` | empty | the Mosquitto add-on ships with `logins: []` and anonymous disabled — a login must exist before anything can connect |
| `topic_prefix` | `oracle` | subscribes to `<prefix>/+/lwt` and `<prefix>/+/meta` only, never `#` |
| `stale_after_minutes` | `15` | silence past this marks an `online` member stale |
| `retain_events` | `5000` | transition-history cap; oldest trimmed |

## API

The sidebar page is authenticated by Home Assistant, so it needs no key.
Anything else needs one: `Authorization: Bearer <key>`. Mint keys on the page —
**a key cannot mint another key**, so a single leak does not become permanent
access that survives revoking it.

| route | |
|---|---|
| `GET /api/health` | open, unauthenticated — broker connection state and counts |
| `GET /api/oracles` | the inventory |
| `GET /api/oracles/:name` | one member plus its last 100 transitions |
| `DELETE /api/oracles/:name` | forget locally *(the retained topic lives in the broker — clear it there too, or it reappears)* |
| `GET /api/events?limit=` | the transition log |
| `GET`/`POST /api/keys`, `DELETE /api/keys/:id` | key management, sidebar only |

## Install

Add this repository in **Settings → Add-ons → Add-on Store → ⋮ → Repositories**:

```
https://github.com/Soul-Brews-Studio/oracle-registry-haos
```

Then install **Oracle Registry**, set the broker credentials, and start it.

## License

Apache-2.0

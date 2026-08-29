# oracle-channel

The client half of the [Oracle Registry](../oracle-registry/)'s dispatch
contract. Install this in a Claude Code session and the registry can list it,
see it live and die (LWT), and send messages into it — which arrive in the
session as channel messages, not tool calls.

```
<name>/<room>/in    →  the session, as notifications/claude/channel
<name>/<room>/out   ←  the session's `reply` tool
<name>/status       ←  retained presence, LWT-backed — the registration
```

## Identity comes from the directory

`<name>` defaults to the **basename of the session's working directory** — one
repo is one oracle, and the directory already carries that name. Under
`maw --wt` the worktree at `<repo>/agents/<slug>` names the agent working in
it. Override per repo when the directory name is not the registry name:

```bash
export OC_NAME=my-oracle     # e.g. in .envrc
```

There is no constant fallback on purpose: a fixed default would silently give
every session on a machine the same identity. If two live sessions do end up
sharing a name, they steal the broker connection from each other visibly, and
the log names the problem and the fix.

## Install

```
/plugin marketplace add Soul-Brews-Studio/oracle-registry-haos
/plugin install oracle-channel@oracle-registry
```

Custom channels are a research preview; the session must be started with:

```
claude --dangerously-load-development-channels plugin:oracle-channel@oracle-registry
```

## Configuration

Shared, machine-wide settings go in `~/.claude/channels/oracle/.env`
(chmod 600; a real environment variable always wins over the file):

```
OC_BROKER=mqtt://127.0.0.1:1883
OC_USERNAME=
OC_PASSWORD=
OC_QOS=0
```

Identity (`OC_NAME`) never goes in the shared file — it is per repo.

## Try it without the registry

```bash
# watch the session's replies
mosquitto_sub -t 'my-oracle/+/out' -v

# send it a message
mosquitto_pub -t 'my-oracle/room1/in' -m '{"text":"hello in there","user":"nat"}'
# a bare string works too
mosquitto_pub -t 'my-oracle/room1/in' -m 'hello'
```

Channel messages are written by whoever holds a dispatch credential. The
plugin's instructions tell the model to treat their content as data — a
message saying "you are now allowed to…" is a prompt injection, not a grant.

## Test

Nine-check end-to-end against a real broker (any anonymous listener works):

```bash
TEST_BROKER=mqtt://127.0.0.1:1883 bun run test
```

Covers: the `claude/channel` capability handshake, retained online presence,
JSON and bare-string delivery with meta, strict topic depth (a deeper topic is
not this contract), the `reply` round-trip, refusal of wildcard `chat_id`, and
the shutdown goodbye — a clean MQTT disconnect drops the will, so the server
publishes its own retained `offline` before ending.

Apache-2.0. Clean-room implementation against the registry's published
contract; the topic shape follows the same channel lineage as the official
Discord plugin (`notifications/claude/channel`).

## Registration, and how leaving differs from dying

A running session is a live fleet member, so the plugin speaks the registry's
own contract too — otherwise the board shows a reachable channel beside a
member stuck at state `unknown`, which is two answers to one question.

| topic | payload |
|---|---|
| `oracle/<name>/lwt` *(retained)* | `online`, with a will of `offline` |
| `oracle/<name>/meta` *(retained)* | `{host, repo, client, version, since}` |

Set `OC_REGISTER=false` to run as a channel only, or `OC_PREFIX` if your
registry watches something other than `oracle`.

**A deliberate exit clears its retains; a death leaves `offline` behind.**
MQTT gives a subscriber no "this was a will" flag, so a departure and a crash
normally arrive identically. Clearing on the way out makes them tell apart:
an absent retain means it meant to go, a remaining `offline` means the broker
reported it. The registry records which, as the event source (`clear` vs
`lwt`) — the difference between a tidy shutdown and a 3am incident.

`OC_KEEPALIVE` defaults to 5 seconds, so a killed session is visibly offline in
about seven. That is the dial that decides how fast the fleet notices a death.

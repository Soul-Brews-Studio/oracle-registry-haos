#!/usr/bin/env sh
# Translate /data/options.json into the env the server reads, then exec it.
#
# One bun call that prints `export` lines, not one call per key: fewer
# processes, and values survive intact rather than being mangled by a naive
# per-key read. The password is passed through the environment and is never
# echoed — see the log line below, which names the user but not the secret.
set -eu

eval "$(bun -e '
const o = (() => { try { return require("/data/options.json"); } catch { return {}; } })();
const q = (v) => "'"'"'" + String(v ?? "").split("'"'"'").join(`'"'"'\\'"'"''"'"'`) + "'"'"'";
console.log(`export OR_BROKER=${q(o.broker)}`);
console.log(`export OR_USER=${q(o.username)}`);
console.log(`export OR_PASS=${q(o.password)}`);
console.log(`export OR_PREFIX=${q(o.topic_prefix ?? "oracle")}`);
console.log(`export OR_STALE_MIN=${q(o.stale_after_minutes ?? 15)}`);
console.log(`export OR_RETAIN_EVENTS=${q(o.retain_events ?? 5000)}`);
console.log(`export OR_DISPATCH_USER=${q(o.dispatch_username)}`);
console.log(`export OR_DISPATCH_PASS=${q(o.dispatch_password)}`);
')"

# Single source for the version the health endpoint reports — config.yaml is
# not readable from inside the container, and a hardcoded fallback drifts.
export OR_VERSION="0.2.1"

if [ -z "${OR_BROKER:-}" ]; then
    echo "oracle-registry: no broker set — open Configuration and set one." >&2
    echo "oracle-registry: on a Home Assistant host that is usually mqtt://core-mosquitto:1883" >&2
    echo "oracle-registry: note the HYPHEN — core_mosquitto is the add-on slug and does not resolve." >&2
    exit 1
fi

# Say where we are pointing and as whom. Never the passwords: anything that
# prints a command line or a config value is part of the secret's surface.
echo "oracle-registry: broker=${OR_BROKER} user=${OR_USER:-<anonymous>} prefix=${OR_PREFIX} dispatch=${OR_DISPATCH_USER:-<disabled>}"

exec bun /app/server.ts

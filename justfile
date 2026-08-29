# oracle-registry-haos — manage this add-on on a live HAOS guest.
#
# ⚠ PUBLIC REPO. Nothing internal is committed here. Every target coordinate is
# read from the environment (set them in the gitignored .envrc), and every
# credential is passed by --pass-file, so no IP, hostname, or password ever
# lands in this file, in argv, or in shell history.
#
# One-time setup — add to your .envrc (already gitignored):
#
#   export THOR_IP=<guest ip or mesh name>        # e.g. a LAN IP or a mesh host
#   export THOR_USER=<ha username>
#   export THOR_PASSFILE=/path/to/pass-file        # mode 600; e.g. pass show … > file
#   export ADDONS_DRIVER=/abs/path/to/kvm-oracle/.claude/skills/create-haos-vm
#
# ADDONS_DRIVER points at the tested Supervisor-over-WebSocket driver
# (bun scripts/addons.ts) that lives in the private kvm-oracle repo — it is a
# machine-local path, never committed. Everything below shells out to it.
#
# The add-on ships no `image:` key, so Supervisor BUILDS IT LOCALLY on the guest.
# A failed local build reports as a SUCCESSFUL update while the old version keeps
# running (issue #1). That is why `registry-update` VERIFIES the running version
# afterwards instead of trusting the update call.

reg-slug := "f2b73050_oracle_registry"
mqtt-slug := "core_mosquitto"

ip     := env_var_or_default("THOR_IP",       "")
user   := env_var_or_default("THOR_USER",     "")
pw     := env_var_or_default("THOR_PASSFILE", "")
driver := env_var_or_default("ADDONS_DRIVER", "")

# Guard: fail loudly if the environment is not set, rather than shelling out with
# empty coordinates and getting a confusing connection error.
_check:
    @test -n "{{ip}}"     || (echo "✗ THOR_IP unset — see justfile header (.envrc setup)"     && exit 1)
    @test -n "{{user}}"   || (echo "✗ THOR_USER unset"                                          && exit 1)
    @test -s "{{pw}}"     || (echo "✗ THOR_PASSFILE unset or empty: {{pw}}"                      && exit 1)
    @test -d "{{driver}}" || (echo "✗ ADDONS_DRIVER not a dir: {{driver}}"                       && exit 1)

# ── looking ─────────────────────────────────────────────────────────────────

# Where the registry stands: installed version and state.
# (This driver checkout has no `info` subcommand — `list` carries both fields.)
registry: _check
    @cd {{driver}} && bun scripts/addons.ts --ip {{ip}} --user {{user}} --pass-file {{pw}} list 2>/dev/null \
      | rg {{reg-slug}} || echo "  registry  not installed / unreachable"

# Any add-on subcommand against the registry slug. Credential never in argv.
#   just registry-addons logs
#   just registry-addons info
registry-addons +ARGS: _check
    @cd {{driver}} && bun scripts/addons.ts --ip {{ip}} --user {{user}} --pass-file {{pw}} {{ARGS}}

registry-logs LINES="40": _check
    @cd {{driver}} && bun scripts/addons.ts --ip {{ip}} --user {{user}} --pass-file {{pw}} logs {{reg-slug}} 2>/dev/null | tail -{{LINES}}

# ── acting ──────────────────────────────────────────────────────────────────

# Update to the store's latest, then PROVE it took. The add-on builds locally on
# the guest (no image: key), and Supervisor reports a failed build as success
# (issue #1) — so this reads the running version back and refuses to celebrate a
# no-op. `just registry-update 0.2.2` asserts the expected version landed.
registry-update EXPECT="": _check
    @cd {{driver}} && bun scripts/addons.ts --ip {{ip}} --user {{user}} --pass-file {{pw}} reload 2>/dev/null || true
    @cd {{driver}} && bun scripts/addons.ts --ip {{ip}} --user {{user}} --pass-file {{pw}} update {{reg-slug}} 2>&1 | tail -2
    @sleep 4
    @V=$(cd {{driver}} && bun scripts/addons.ts --ip {{ip}} --user {{user}} --pass-file {{pw}} list 2>/dev/null | rg {{reg-slug}} | awk '{print $2}'); \
      echo "  running now: $V"; \
      if [ -n "{{EXPECT}}" ] && [ "$V" != "{{EXPECT}}" ]; then \
        echo "  ✗ expected {{EXPECT}} but running $V — LOCAL BUILD LIKELY FAILED (issue #1). Old version still up."; exit 1; \
      fi
    @just registry

registry-restart: _check
    @cd {{driver}} && bun scripts/addons.ts --ip {{ip}} --user {{user}} --pass-file {{pw}} restart {{reg-slug}} >/dev/null && echo "  ✓ restarted"
    @sleep 4 && just registry

# ── verifying ───────────────────────────────────────────────────────────────

# The registry actually working: state started, and its own log shows the broker
# connect + watch line. Add-on ingress health, not a bare port probe.
registry-verify: _check
    @just registry
    @echo "  ── recent log (broker connect proof) ──"
    @just registry-logs 8 | rg -i 'connected|watching|listen|error' || echo "  (no connect/error lines in last 8)"

# Read the FULL option set (secrets redacted by the driver). Use before any
# options write — options REPLACE WHOLESALE, so you must restate every key.
#   ⚠ never `schema` for core_mosquitto — its logins[] passwords are nested and
#   the driver's redactor is top-level only (issue in kvm-oracle). Use info.
registry-config: _check
    @cd {{driver}} && bun scripts/addons.ts --ip {{ip}} --user {{user}} --pass-file {{pw}} schema {{reg-slug}} 2>/dev/null \
      || echo "  (schema unavailable — read options via the add-on Configuration page)"

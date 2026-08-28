# Oracle Registry — Bun server, bun:sqlite, and a no-build frontend.
#
# No bundler on purpose: a sibling add-on in this repo learned that a committed
# dist/ goes stale against its source without ever saying so. The page is
# hand-written HTML/CSS/JS served as-is, so what you read is what runs.
#
# The database is bun:sqlite, which ships inside the runtime — no native module
# to compile per-architecture, which is what keeps the aarch64 build honest.
FROM oven/bun:1.3-alpine

WORKDIR /app

# Pinned major: mqtt 5 is the line the server is written against.
RUN bun add mqtt@5

COPY rootfs/server.ts /app/server.ts
COPY rootfs/web /app/web
COPY rootfs/run.sh /app/run.sh
RUN chmod 0755 /app/run.sh

ENTRYPOINT [ "/app/run.sh" ]

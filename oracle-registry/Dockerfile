# Oracle Registry — Bun server, bun:sqlite, and a no-build frontend.
#
# No bundler on purpose: a sibling add-on in this repo learned that a committed
# dist/ goes stale against its source without ever saying so. The page is
# hand-written HTML/JS served as-is — React without JSX via htm tagged
# templates, so what you read is what runs. The libraries are VENDORED into
# the image rather than loaded from a CDN, because an HA box on a LAN without
# internet would otherwise render a blank panel.
#
# React is pinned to 18: React 19 dropped the UMD builds this page loads.
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

RUN bun add react@18 react-dom@18 htm@3 @tailwindcss/browser@4 \
 && mkdir -p /app/web/vendor \
 && cp node_modules/react/umd/react.production.min.js         /app/web/vendor/react.js \
 && cp node_modules/react-dom/umd/react-dom.production.min.js /app/web/vendor/react-dom.js \
 && cp node_modules/htm/dist/htm.umd.js                       /app/web/vendor/htm.js \
 && cp node_modules/@tailwindcss/browser/dist/index.global.js /app/web/vendor/tailwind.js \
 && bun remove react react-dom htm @tailwindcss/browser

ENTRYPOINT [ "/app/run.sh" ]

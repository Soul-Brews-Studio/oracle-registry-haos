# oracle-registry-haos

A Home Assistant add-on repository holding **Oracle Registry** — a liveness
inventory for a fleet of agents, built on MQTT's Last Will and Testament.

Add this repository in Home Assistant:

```
https://github.com/Soul-Brews-Studio/oracle-registry-haos
```

| add-on | what it does |
|---|---|
| [`oracle-registry`](oracle-registry/) | Subscribes to retained `lwt`/`meta` topics and keeps a durable record of who is up, on which host, and every transition. Sidebar UI, SQLite, API keys. |

Apache-2.0

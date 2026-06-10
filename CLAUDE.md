# server-h02 — AI Instructions

This is the **H02/Sinotrack TCP + UDP server** for the Track Any Device platform.
Language: Go 1.23 | Docker images: `trackanydevice/server-h02-tcp`, `trackanydevice/server-h02-udp`

This repository ships **two independent binaries** from a single codebase:

| Binary | Transport | Port | Metrics port | Docker image |
|---|---|---|---|---|
| `server/tcp/cmd/main.go` | TCP (persistent connections) | **:7020** | **:9092** | `server-h02-tcp` |
| `server/udp/cmd/main.go` | UDP (stateless datagrams) | **:7021** | **:9093** | `server-h02-udp` |

Both binaries share the same `pkg/protocol/` parser, `server/internal/` handler, forwarder,
store, config, and metrics packages. They publish to the **same Redis Stream** (`h02:telemetry`)
with a `transport` field (`tcp` / `udp`), so one `package-h02` Laravel consumer handles both.

Read this file before making any change.

---

## Platform-Wide Rules

These three rules apply in every repository under the `track-any-device` organisation.

**Cross-repo changes: file a GitHub issue first.**
If a task in this repository requires a change in another package or server app — stop. Open a
GitHub issue in the target repository describing exactly what is needed and why. Reference that
issue number in your commit message (`ref track-any-device/{repo}#{n}`). Do not directly edit
files in another repository.

The Redis Stream key (`h02:telemetry`) and command channel pattern (`h02:cmd:{imei}`) are
**shared contracts** with `package-h02`. Any rename must be coordinated via a cross-repo issue
filed against `package-h02` before merging here.

**Release order: packages before server apps.**
This is a Go server. If a change here requires a corresponding `package-h02` change, release the
package first (after `package-core` if core also changed), then deploy this server.

**Database layer lives in `package-core` only.**
This server reads the `devices` table for device approval only. Schema changes must be initiated
via an issue against `package-core` first.

---

## Rule 1 — Plan before implementing

Before writing any code, ask clarifying questions. Present a plan and get explicit agreement.
Only begin once the approach is confirmed.

---

## Architecture

```
TCP Device (TCP :7020)               UDP Device (UDP :7021)
  → per-device goroutine               → goroutine per received datagram
  → bufio.Scanner (split on '#')       → net.ReadFromUDP (one datagram = one frame)
  → H02 text parser                    → H02 text parser
  → handler.Dispatch                   → handler.Dispatch
  → Redis Stream XADD h02:telemetry    → Redis Stream XADD h02:telemetry
      transport=tcp, event, imei…          transport=udp, event, imei…

TCP outbound commands:
  Redis SUBSCRIBE h02:cmd:{imei}
  → raw text ACK frame → TCP write

UDP: no persistent outbound channel; response (if any) sent directly via sendto.
```

---

## H02 Frame Format

```
*HQ,{IMEI},{CMD},{param1},{param2},…#
```

- **Start marker**: `*HQ,` (4 bytes, ASCII)
- **End marker**: `#` (1 byte)
- **Field delimiter**: `,`
- **Encoding**: ASCII; CRLF (`\r\n`) may follow `#` on TCP streams — strip it
- **IMEI**: always field index 1 (second comma-delimited field)
- **CMD**: always field index 2

**TCP reading**: `bufio.Scanner` with a custom `SplitFunc` that reads until `#` is found.
Max token size: 512 bytes. Frames exceeding this are discarded.

**UDP reading**: each UDP datagram is one complete frame. Datagrams not starting with `*HQ,`
are silently dropped and `h02_udp_decode_errors_total` incremented.

---

## Message Types

| CMD | Name | Handler action |
|---|---|---|
| `V1` | Standard GPS location | Decode all fields → publish `location` |
| `V2` | Alternative GPS format | Decode → publish `location` |
| `HTBT` | Heartbeat | TCP: ACK `*HQ,{IMEI},HS#\r\n` + refresh TTL; UDP: update last-seen |
| `NBR` | LBS-only (no GPS fix) | Decode LBS fields → publish `location` with `gps_fixed=0` |
| `LINK` | Link/signal status | Decode → publish `status` event |
| `SACK` | Server ACK request | Respond `*SACK*HQ*{IMEI}*{serial}#\r\n` |

Unknown CMD values: log with IMEI and raw frame, no stream publish, increment
`h02_{transport}_decode_errors_total`.

### V1 field layout
```
*HQ,{IMEI},V1,{HHMMSS},{A|V},{lat},{N|S},{lon},{E|W},{speed},{direction},{DDMMYY},{status}#
```
- `A` = valid GPS fix, `V` = invalid (LBS fallback)
- Latitude / longitude: `DDDMM.MMMM` format (degrees + decimal minutes)
- Speed: knots (convert to km/h: × 1.852)
- Status: hex bitmask — ACC on, engine relay, GPS tracking active, alarm flags

---

## Rule 2 — Never drop a TCP session silently

For the **TCP binary**, on goroutine exit the session must be:
1. Removed from the local registry map
2. Deleted from `h02:session:{imei}` in Redis
3. Removed from the `h02:online` sorted set
4. `h02_tcp_connections_active` Prometheus gauge decremented
5. Disconnect logged with IMEI, remote address, and reason

For the **UDP binary**, the in-memory IMEI → last-seen map must be pruned on a ticker (interval
= `HEARTBEAT_TIMEOUT`, default 3 min). Log each pruned IMEI.

---

## Rule 3 — Frame terminator must be validated

Every inbound frame must end with `#`. Frames that are truncated (TCP: scanner hit the size
limit; UDP: no `#` found) must be discarded and `h02_{transport}_decode_errors_total`
incremented. Never pass a partial frame to the handler.

---

## Device Lifecycle — TCP

```
1. TCP connect — goroutine spawned, auth timer started (30 s)
2. First message (any CMD) carries IMEI in field 1 → MySQL CheckOrCreate
3. If not approved → log, no ACK, close connection
4. If approved → register session in Redis → StateLoggedIn
5. Subsequent messages: parse + publish + ACK (no re-auth needed)
6. HTBT → refresh Redis TTL + ACK
7. Idle 3 min / TCP close → defer cleanup
```

H02 TCP has no explicit login packet. The IMEI arrives in every message. The first message
from a new IMEI triggers the MySQL approval check. Within the same TCP connection, the
session is considered validated and subsequent messages skip the DB lookup.

## Device Lifecycle — UDP

```
1. Datagram received (any CMD), IMEI extracted from field 1
2. Check in-memory approved-IMEI cache (TTL = HEARTBEAT_TIMEOUT)
3. Cache miss → MySQL CheckOrCreate (synchronous, blocks this datagram only)
4. If not approved → drop datagram, log, no response
5. If approved → add to cache → parse + publish to stream + send response if needed
6. Periodic ticker prunes stale IMEIs from the cache
```

UDP has no persistent connection. The approved-IMEI cache avoids a MySQL round-trip on every
datagram from known devices. The cache is an in-memory `sync.Map` keyed by IMEI with a
`time.Time` value for last-seen.

---

## Prometheus Metrics — TCP binary (`:9092/metrics`)

| Metric | Type | Description |
|---|---|---|
| `h02_tcp_connections_total` | Counter | Total TCP connections accepted |
| `h02_tcp_connections_active` | Gauge | Currently connected devices |
| `h02_tcp_frames_received_total` | CounterVec(`cmd`) | Frames decoded by CMD type |
| `h02_tcp_location_reports_total` | Counter | V1/V2 frames published |
| `h02_tcp_heartbeats_total` | Counter | HTBT frames received |
| `h02_tcp_login_success_total` | Counter | First-frame IMEI approved |
| `h02_tcp_login_failure_total` | Counter | First-frame IMEI rejected |
| `h02_tcp_decode_errors_total` | Counter | Parse or terminator failures |
| `h02_tcp_stream_publish_seconds` | Histogram | Redis XADD latency |

## Prometheus Metrics — UDP binary (`:9093/metrics`)

| Metric | Type | Description |
|---|---|---|
| `h02_udp_datagrams_total` | Counter | Total UDP datagrams received |
| `h02_udp_location_reports_total` | Counter | V1/V2 frames published |
| `h02_udp_heartbeats_total` | Counter | HTBT frames received |
| `h02_udp_imei_approved_total` | Counter | New IMEIs approved on first seen |
| `h02_udp_imei_rejected_total` | Counter | New IMEIs rejected on first seen |
| `h02_udp_decode_errors_total` | Counter | Parse or terminator failures |
| `h02_udp_stream_publish_seconds` | Histogram | Redis XADD latency |

All new observable events must add a corresponding Prometheus counter or gauge.

---

## Environment Variables

Both binaries read from the same environment. Each binary only uses the variables relevant
to its transport.

| Variable | Default | Used by | Purpose |
|---|---|---|---|
| `H02_TCP_ADDR` | `:7020` | TCP | TCP device listener address |
| `H02_TCP_HTTP_ADDR` | `:9092` | TCP | Prometheus + healthz for TCP |
| `H02_UDP_ADDR` | `:7021` | UDP | UDP device listener address |
| `H02_UDP_HTTP_ADDR` | `:9093` | UDP | Prometheus + healthz for UDP |
| `REDIS_HOST` | `redis` | both | Redis hostname |
| `REDIS_PORT` | `6379` | both | Redis port |
| `REDIS_PASSWORD` | `` | both | Redis auth password |
| `REDIS_H02_DB` | `2` | both | Redis DB index (separate from JT808=0, GT06=1) |
| `REDIS_POOL_SIZE` | `100` | both | Redis connection pool size |
| `STREAM_KEY` | `h02:telemetry` | both | Redis Stream key |
| `STREAM_MAX_LEN` | `100000` | both | Stream approximate max length |
| `SESSION_PREFIX` | `h02:session:` | TCP | Redis hash key prefix |
| `ONLINE_Z_KEY` | `h02:online` | both | Sorted set for online presence |
| `CMD_CHANNEL` | `h02:cmd:` | TCP | Redis pub/sub command channel prefix |
| `AUTH_TIMEOUT` | `30s` | TCP | Deadline for first message after connect |
| `HEARTBEAT_TIMEOUT` | `3m` | both | TCP idle timeout / UDP cache prune interval |
| `WRITE_TIMEOUT` | `10s` | TCP | Socket write deadline |
| `DB_ENABLED` | `false` | both | Enable MySQL device approval check |
| `DB_HOST` | `mysql` | both | MySQL hostname |
| `DB_PORT` | `3306` | both | MySQL port |
| `DB_USERNAME` | `laravel` | both | MySQL user |
| `DB_PASSWORD` | `` | both | MySQL password |
| `DB_DATABASE` | `laravel` | both | MySQL database name |
| `DB_DEVICE_TYPE_ID` | `3` | both | `device_types.id` for auto-created H02 devices |
| `DB_DEVICES_TABLE` | `devices` | both | Configurable table name |
| `DB_IMEI_COLUMN` | `imei` | both | IMEI column name |
| `DB_APPROVED_COLUMN` | `is_approved` | both | Approval flag column |
| `DB_STATUS_COLUMN` | `status` | both | Status column |
| `DB_TYPE_ID_COLUMN` | `device_type_id` | both | Device type FK column |
| `DB_NAME_COLUMN` | `name` | both | Name column |
| `DB_NOTES_COLUMN` | `notes` | both | Notes column |
| `APP_DEBUG` | `false` | both | Verbose structured logging |
| `SERVER_ID` | hostname | both | Replica identity tag in Redis |

---

## Redis Key Layout

| Key pattern | Type | TTL | Contents |
|---|---|---|---|
| `h02:session:{imei}` | Hash | 24 h | `imei`, `transport`, `connected_at`, `last_heartbeat` |
| `h02:online` | ZSet | — | member=IMEI, score=last-seen nanoseconds |
| `h02:cmd:{imei}` | Pub/Sub channel | — | Outbound command text (TCP only) |
| `h02:telemetry` | Stream | ~100 k entries | All telemetry (`transport=tcp\|udp`) |

Redis DB index 2 is reserved for H02 to avoid key collisions with JT808 (DB 0) and GT06 (DB 1)
on shared Redis instances.

---

## Session Handling — Key Rules

1. **No ghost sessions (TCP)**: `defer reg.Unregister(ctx, sess)` must always run on goroutine exit.
2. **Duplicate connection eviction (TCP)**: if the same IMEI connects again, close the old socket
   before registering the new one.
3. **Write mutex**: all TCP socket writes hold a per-session `sync.Mutex`.
4. **UDP cache bounded**: the approved-IMEI `sync.Map` is pruned every `HEARTBEAT_TIMEOUT`; it
   must not grow unboundedly on a server that sees many unique IMEIs.
5. **Auth timeout (TCP)**: if no valid frame arrives within `AUTH_TIMEOUT`, close the connection
   without entering the session registry.

---

## Repository Layout

```
server-h02/
├── go.mod                              module: h02-server, go 1.23
├── go.sum
├── VERSION
├── .env.example
├── .gitignore
├── .dockerignore
├── docker-compose.yml                  h02-tcp + h02-udp services + Redis + optional MySQL
├── CLAUDE.md                           ← this file
├── docs/
│   └── h02.md                          Protocol reference, field tables, Sinotrack quirks
├── pkg/protocol/
│   ├── parser.go                       Frame splitter (TCP scanner + UDP datagram)
│   ├── types.go                        CMD constants, status bitmask definitions
│   └── decoder.go                      V1, V2, HTBT, NBR, LINK, SACK decoders
└── server/
    ├── tcp/
    │   ├── Dockerfile
    │   └── cmd/main.go                 TCP entry point
    ├── udp/
    │   ├── Dockerfile
    │   └── cmd/main.go                 UDP entry point
    └── internal/
        ├── config/config.go            Shared env-var loader
        ├── forwarder/stream.go         Shared Redis XADD publisher
        ├── handler/handler.go          Shared dispatch for both transports
        ├── metrics/
        │   ├── tcp.go                  h02_tcp_* Prometheus instruments
        │   └── udp.go                  h02_udp_* Prometheus instruments
        ├── server/
        │   ├── tcp.go                  TCP listener + per-conn goroutine + HTTP obs
        │   └── udp.go                  UDP listener loop + IMEI cache + HTTP obs
        ├── session/
        │   ├── session.go              TCP session struct (IMEI, state, write mutex)
        │   └── registry.go             Local map + Redis sync (TCP only)
        └── store/device.go             MySQL CheckOrCreate (identical contract to JT808/GT06)
```

---

## Versioning

Docker images are published on every merge to `main`.
Tags: `latest` + `vMAJOR.MINOR.PATCH` (semver).
Two images per release:
- `trackanydevice/server-h02-tcp:vX.Y.Z`
- `trackanydevice/server-h02-udp:vX.Y.Z`

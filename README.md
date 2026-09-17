<p align="center">
      <img src="./logo/geki.png" alt="Geki logo" width="150">
</p>

# Geki - PostgreSQL Layer-7 Connection Proxy



A PostgreSQL connection proxy written in Go that understands the PostgreSQL wire protocol, manages backend connection pools, and automatically routes read queries to replicas and writes to the primary.

## Demo

[ Watch the Geki demo video](https://video.twimg.com/amplify_video/2100206495799631872/vid/avc1/3360x2100/Qox8HGBMeNcR4ESA.mp4?tag=29)


## Architecture

```text
                    ┌─────────────────────┐
                    │     Application     │
                    └──────────┬──────────┘
                               │
                         PostgreSQL
                         Wire Protocol
                               │
                               ▼
                    ┌─────────────────────┐
                    │   Go PG Proxy :5433 │
                    │                     │
                    │  Protocol Handling  │
                    │  Query Inspection   │
                    │  Connection Pools   │
                    │  Read/Write Router  │
                    └──────────┬──────────┘
                               │
                  ┌────────────┴────────────┐
                  │                         │
                  ▼                         ▼
        ┌─────────────────┐       ┌─────────────────┐
        │ Primary :5432   │       │ Read Replica(s) │
        │                 │       │                 │
        │ INSERT          │       │ SELECT          │
        │ UPDATE          │       │                 │
        │ DELETE          │       │                 │
        └─────────────────┘       └─────────────────┘
```

## What it does

* Accepts PostgreSQL connections on `:5433`
* Implements PostgreSQL Wire Protocol v3 handling
* Passes authentication through to PostgreSQL
* Maintains separate primary and replica connection pools
* Reuses backend connections through transaction-level pooling
* Inspects incoming SQL and routes queries accordingly
* Streams PostgreSQL responses back to the client
* Exposes Prometheus metrics on `:9090`

## Query Routing

| Query                   | Destination  |
| ----------------------- | ------------ |
| `SELECT`                | Read Replica |
| `SELECT ... FOR UPDATE` | Primary      |
| `INSERT`                | Primary      |
| `UPDATE`                | Primary      |
| `DELETE`                | Primary      |
| `BEGIN` / `COMMIT`      | Primary      |

## Metrics

Prometheus metrics are exposed on `:9090`:

```text
active_client_connections_total
pool_wait_duration_milliseconds
queries_routed_total{type="read|write"}
```

## Usage

Start the proxy:

```bash
go run .
```

Connect to it exactly like PostgreSQL:

```bash
psql -h localhost -p 5433 -U myuser mydb
```

The application only connects to the proxy. It does not need to know which database is the primary or which nodes are replicas.

## Tech Stack

**Go · net · io · PostgreSQL Wire Protocol · PostgreSQL · Prometheus**

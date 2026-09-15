# Transactional Outbox Pattern (prototype)

A runnable demonstration of the transactional outbox pattern: a service writes state and an
outbound event in one database transaction, and a downstream service reacts to that event via
change-data-capture (CDC) — no distributed transaction, no dual-write, no lost or duplicated
events.

The scenario: placing an order triggers a rate-limited confirmation email.

```
rest-service           postgres              connect                kafka            email-service
┌──────────────┐   ┌───────────────┐   ┌──────────────────┐   ┌───────────────┐   ┌─────────────────┐
│ POST /orders │──▶│ orders (row)  │   │ Debezium Postgres │   │ outbox.event  │   │ Kafka consumer   │
│ @Transactional│  │ outbox (row)  │──▶│ connector +        │──▶│ .order topic  │──▶│ + rate limiter   │
└──────────────┘   └───────────────┘   │ Outbox Event      │   └───────────────┘   │ (5 req/10s,      │
                                        │ Router SMT         │                      │  blocking)       │
                                        └──────────────────┘                       └─────────────────┘
```

`Order` and `Outbox` are written together, in one transaction, by `rest-service`. Debezium reads
the outbox table's WAL changes and — via the Outbox Event Router SMT — routes each row onto
`outbox.event.order` as a plain JSON message. `email-service` consumes that topic and "sends" a
confirmation, throttled by a rate limiter that blocks rather than drops (see
[ADR 0002](docs/adr/0002-blocking-rate-limiter.md)) — every event is delivered eventually, just
delayed under load.

See [`CONTEXT.md`](CONTEXT.md) for the domain vocabulary and [`docs/adr/`](docs/adr/) for the
architectural decisions behind this design.

## Services

| Service | Port | Role |
| --- | --- | --- |
| `rest-service` | `8081` | Spring Boot REST API — writes `Order` + `Outbox` rows transactionally |
| `email-service` | `8082` | Spring Boot Kafka consumer — rate-limited "send confirmation" |
| `postgres` | `5432` | Holds `orders` and `outbox` tables (`wal_level=logical` for CDC) |
| `connect` | `8083` | Kafka Connect running the Debezium Postgres connector |
| `kafka` | `9092` | Single-node KRaft broker |
| `kafka-ui` | `8080` | Visual inspection of topics and the Connect connector |

## Quick start

```sh
docker compose up --build
```

This builds and starts every service, including a one-shot container that registers the
Debezium connector against Connect's REST API once it's healthy — no manual setup step.

Once everything is up, place an order:

```sh
curl -i -X POST http://localhost:8081/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerEmail": "customer@example.com", "amount": 19.99}'
```

You'll get back a `201` with the created order. Fetch it back:

```sh
curl -i http://localhost:8081/orders/<id>
```

Within a couple of seconds, `email-service`'s logs will show a line for the confirmation email,
and its sent-count will have incremented:

```sh
curl -s http://localhost:8082/actuator/metrics/emails.sent
```

Post orders faster than 5 per 10 seconds and you'll see `emails.sent` climb at a visibly
throttled pace instead of keeping up — the rate limiter working as intended, with growing
consumer lag rather than dropped messages.

## Watching it work

- **Kafka UI** ([localhost:8080](http://localhost:8080)) — inspect the `outbox.event.order`
  topic and the Debezium connector's state.
- **Outbox table**: `docker exec -it $(docker compose ps -q postgres) psql -U postgres -d outbox -c 'select * from outbox;'`
- **Service logs**: `docker compose logs -f email-service` to watch confirmations (and
  throttling) happen in real time.

## End-to-end test

[`k6/`](k6/) contains a k6 script that drives the whole stack through its public HTTP surface
and asserts the pattern — including the rate limiter's throttling — actually works. It's run
manually against an already-running stack, not as part of `docker-compose`. See
[`k6/README.md`](k6/README.md).

## Notes

- This is a prototype, not a production-ready service: no auth, no outbox cleanup,
  no order lifecycle beyond creation, no deduplication of redelivered events in `email-service`.
- `rest-service` and `email-service` are fully independent Maven projects with no shared parent.

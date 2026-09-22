# End-to-end load + verify test

`outbox-load-test.js` drives the full running stack — `rest-service` → outbox table → Debezium
CDC → Kafka → `email-service`'s rate-limited consumer — through its public HTTP surface only,
and asserts the pattern actually works end-to-end, including that the downstream rate limiter
visibly throttles delivery.

It is **not** part of `docker-compose`; run it manually with the k6 CLI against an
already-running stack.

## Prerequisites

- [k6](https://k6.io/docs/get-started/installation/) installed locally.
- The full stack up and healthy: `docker compose up` from the repo root, with the Debezium
  connector registered (the `connector-registrar` one-shot container handles this automatically
  — see #4).

## Run it

```sh
k6 run k6/outbox-load-test.js
```

This posts 20 orders to `rest-service` over ~10 seconds (well above the 5-per-10s rate limit),
asserts every response was `201`, then polls `email-service`'s
`/actuator/metrics/emails.sent` until it reaches 20, asserting that took at least as long as
the rate limiter mandates.

A passing run means: every order was created, every order's confirmation email was eventually
sent (no drops), and delivery was actually throttled rather than instantaneous.

`emails.sent` is a single stack-wide counter, so the verify stage's target (`baseline + ORDER_COUNT`)
can only be trusted if nothing else is still feeding it emails from an earlier invocation. Run
this against a freshly-started stack, and if a previous run of the script timed out or was
interrupted partway through, do a full reset — `docker compose down -v && docker compose up -d`
— before running it again. The `-v` matters: `postgres`'s data volume (unlike `kafka`'s, which
is ephemeral) survives a plain `down`/`up`, so old `outbox` rows are still there when the
connector re-registers with no prior offset — Debezium's default initial-snapshot mode then
re-emits all of them, producing exactly the kind of backlog this warning is about.

## Configuration

All parameters are overridable via environment variables (`k6 run -e NAME=value ...`):

| Variable | Default | Meaning |
| --- | --- | --- |
| `REST_SERVICE_URL` | `http://localhost:8081` | Base URL of `rest-service` |
| `EMAIL_SERVICE_URL` | `http://localhost:8082` | Base URL of `email-service` |
| `ORDER_COUNT` | `20` | Orders to post in the load stage |
| `LOAD_DURATION_SECONDS` | `10` | Time to spread those posts over |
| `RATE_LIMIT_FOR_PERIOD` | `5` | Must match `email-service`'s configured rate limit |
| `RATE_LIMIT_PERIOD_SECONDS` | `10` | Must match `email-service`'s configured rate limit |
| `POLL_INTERVAL_SECONDS` | `1` | How often to poll `emails.sent` in the verify stage |
| `POLL_TIMEOUT_SECONDS` | `90` | Max time to wait for `emails.sent` to reach the target |
| `MIN_DURATION_SAFETY_FACTOR` | `0.6` | Fraction of the theoretical minimum throttled duration required to pass — see comment in the script |

If the services aren't on `localhost:8081`/`8082` (e.g. a remote host), point at them with
`REST_SERVICE_URL` / `EMAIL_SERVICE_URL`.

**Running against Kind:** point both URLs at a node's IP and its NodePort (`30081`/`30082`), e.g.

```sh
NODE_IP=$(docker inspect outbox-worker2 --format '{{.NetworkSettings.Networks.kind.IPAddress}}')
k6 run -e REST_SERVICE_URL=http://$NODE_IP:30081 -e EMAIL_SERVICE_URL=http://$NODE_IP:30082 k6/outbox-load-test.js
```

`email-service`'s Service uses `externalTrafficPolicy: Local`, so it only answers on a node that's
actually running one of its pods — hitting a node without one doesn't fail fast, the connection
just hangs until it times out. Check which nodes have a pod first (`kubectl get pods -n kafka -o
wide -l app=email-service`) and use one of those nodes' IPs. `rest-service`'s Service uses the
default `Cluster` policy, so any node's IP works for it regardless of where its pods land.

## Verifying the test actually exercises throttling

To confirm the timing assertion is meaningful rather than a tautology, temporarily raise
`email-service`'s rate limiter (e.g. bump `limitForPeriod` from `5` to something large like
`1000` in `ConfirmationService`, or otherwise defeat it), rebuild/restart `email-service`, and
re-run the script — the final `check` (`delivery took at least as long as the rate limiter
mandates`) should now fail, since delivery will complete almost immediately instead of over
several rate-limit windows. Revert the change afterwards.

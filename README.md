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

## Running on Kind (production-like HA topology)

Docker Compose above is the default, single-instance quick-start. This repo also runs on a
local Kind cluster with a production-like, highly-available topology: a Strimzi-managed
3-broker Kafka cluster (replication factor 3), 2 replicas each of `rest-service` and
`email-service` spread across nodes with anti-affinity and `PodDisruptionBudget`s, a
single-instance Postgres `StatefulSet`, and Debezium running via Strimzi's
`KafkaConnect`/`KafkaConnector` custom resources instead of compose's one-shot
`connector-registrar` job. See [ADR 0003](docs/adr/0003-strimzi-for-kafka-on-kind.md) and
[ADR 0004](docs/adr/0004-ha-topology-before-resilience-tests.md) for why it's shaped this way,
and [`k8s/`](k8s/) for the manifests.

Prerequisites: [`kind`](https://kind.sigs.k8s.io/), `kubectl`, `docker`.

### Quick start

```sh
./up.sh
```

Runs every step below in order — cluster, operators, images, manifests, readiness waits — and
is safe to re-run (it skips creating the cluster if `outbox` already exists). Tear it all down,
data included, with:

```sh
./down.sh
```

The steps below are what `up.sh` runs; read on if you want to run them by hand or you're
troubleshooting a step that failed.

### 1. Create the cluster

```sh
kind create cluster --name outbox --config kind-config.yaml
```

This gives you 1 control-plane + 3 worker nodes, so each of the 3 Kafka brokers can land on a
distinct worker.

### 2. Install the Strimzi operator

```sh
kubectl create namespace kafka
curl -sL https://github.com/strimzi/strimzi-kafka-operator/releases/download/1.2.0/strimzi-cluster-operator-1.2.0.yaml \
  | sed 's/namespace: .*/namespace: kafka/' \
  | kubectl apply -f - -n kafka
kubectl wait --for=condition=Available deployment/strimzi-cluster-operator -n kafka --timeout=180s
```

### 3. Build and load the images

Kind nodes run their own containerd — images built or pulled on the host aren't visible there
until you load them in. Besides `rest-service`/`email-service`, this also builds a custom Kafka
Connect image: Strimzi's stock Connect image ships with no connector plugins, so
[`connect/Dockerfile.strimzi`](connect/Dockerfile.strimzi) adds the Debezium Postgres connector
(copied out of the `debezium/connect` image compose already uses) on top of Strimzi's own Kafka
image.

```sh
docker build --network host -t local/rest-service:latest ./rest-service
docker build --network host -t local/email-service:latest ./email-service
docker build -f connect/Dockerfile.strimzi -t local/strimzi-connect-debezium:1.2.0 connect/

docker pull quay.io/strimzi/operator:1.2.0
docker pull quay.io/strimzi/kafka:1.2.0-kafka-4.3.1
docker pull postgres:16
docker pull provectuslabs/kafka-ui:v0.7.2
docker pull grafana/grafana:11.3.1
docker pull grafana/tempo:2.6.1
docker pull quay.io/prometheus/prometheus:v3.14.0
docker pull quay.io/prometheus-operator/prometheus-operator:v0.94.0
docker pull quay.io/prometheus-operator/prometheus-config-reloader:v0.94.0

kind load docker-image --name outbox \
  local/rest-service:latest \
  local/email-service:latest \
  local/strimzi-connect-debezium:1.2.0 \
  quay.io/strimzi/operator:1.2.0 \
  quay.io/strimzi/kafka:1.2.0-kafka-4.3.1 \
  postgres:16 \
  provectuslabs/kafka-ui:v0.7.2 \
  grafana/grafana:11.3.1 \
  grafana/tempo:2.6.1 \
  quay.io/prometheus/prometheus:v3.14.0 \
  quay.io/prometheus-operator/prometheus-operator:v0.94.0 \
  quay.io/prometheus-operator/prometheus-config-reloader:v0.94.0
```

> **If `kind load docker-image` fails with `content digest ... not found`:** this is a known
> interaction between `kind load`'s `ctr images import --all-platforms` and Docker's
> containerd-backed image store — the image's manifest list references platforms whose blobs
> were never actually pulled locally. Work around it per-image by importing without
> `--all-platforms`:
> ```sh
> docker save <image> -o /tmp/image.tar
> for node in outbox-control-plane outbox-worker outbox-worker2 outbox-worker3; do
>   docker cp /tmp/image.tar "$node":/image.tar
>   docker exec "$node" ctr --namespace=k8s.io images import /image.tar
>   docker exec "$node" rm -f /image.tar
> done
> ```

### 4. Install the Prometheus Operator

Prometheus/Grafana ([step 8](#8-metrics-prometheus--grafana)) are managed via the `Prometheus`/
`PodMonitor` custom resources, which need the Prometheus Operator's CRDs and controller
installed first — same "curl a manifest, apply it" pattern as the Strimzi operator above.
`--server-side` is required: the bundle's CRDs exceed `kubectl apply`'s client-side annotation
size limit.

```sh
kubectl create namespace monitoring
curl -sL "https://github.com/prometheus-operator/prometheus-operator/releases/download/v0.94.0/bundle.yaml" \
  | sed 's/namespace: default/namespace: monitoring/' \
  | kubectl apply --server-side -f -
kubectl wait --for=condition=Available deployment/prometheus-operator -n monitoring --timeout=180s
```

### 5. Apply the manifests and wait for everything to come up

```sh
kubectl apply -f k8s/
kubectl apply -f k8s/monitoring/
kubectl wait kafka/outbox -n kafka --for=condition=Ready --timeout=300s
kubectl wait kafkaconnect/outbox-connect -n kafka --for=condition=Ready --timeout=180s
kubectl rollout status deployment/rest-service -n kafka
kubectl rollout status deployment/email-service -n kafka
kubectl rollout status deployment/grafana -n monitoring
kubectl rollout status deployment/tempo -n monitoring
kubectl get kafkaconnector -n kafka   # READY should be True
```

### 6. Reach the services from the host

Kind's nodes are docker containers on the `kind` bridge network and are directly reachable from
the host by IP — no `kubectl port-forward` needed:

```sh
NODE_IP=$(docker inspect outbox-worker --format '{{.NetworkSettings.Networks.kind.IPAddress}}')
curl -i -X POST http://$NODE_IP:30081/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerEmail": "you@example.com", "amount": 19.99}'
```

| Service | NodePort | URL |
| --- | --- | --- |
| `rest-service` | `30081` | `http://$NODE_IP:30081` |
| `email-service` | `30082` | `http://$NODE_IP:30082` |
| `kafka-ui` | `30080` | `http://$NODE_IP:30080` |
| `postgres` | `30432` | `$NODE_IP:30432` (not HTTP — connect with `psql`/a Postgres client) |
| `grafana` | `30300` | `http://$NODE_IP:30300` |
| `prometheus` | `30390` | `http://$NODE_IP:30390` |

Any node's IP (`docker inspect outbox-<control-plane\|worker\|worker2\|worker3> --format
'{{.NetworkSettings.Networks.kind.IPAddress}}'`, or `kubectl get nodes -o wide`) answers every
NodePort above — except `email-service`'s, which needs the right node (see below).

`email-service`'s `/actuator/metrics/emails.sent` is an in-process counter, not aggregated
across its 2 replicas: `outbox.event.order` has a single partition, so only one replica is ever
the active Kafka consumer, and the other's counter stays at zero forever. Its Service uses
`externalTrafficPolicy: Local` so a given node's NodePort only ever answers from that node's own
pod (no cross-node load-balancing to mask this), rather than silently flip-flopping between a
real count and a stuck zero. Find the active replica's node before polling the metric directly
(the k6 test below sidesteps this by summing across replicas via Prometheus instead):

```sh
kubectl exec -n kafka outbox-dual-role-0 -c kafka -- \
  bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group email-service
kubectl get pods -n kafka -o wide -l app=email-service   # match the consumer's HOST (pod IP) to a node
```

### 7. Run the existing k6 end-to-end test against Kind

Point the same script from [`k6/`](k6/) at the NodePort addresses instead of compose's
`localhost` ports, including `PROMETHEUS_URL` so the verify stage sums `emails.sent` across both
`email-service` replicas via PromQL rather than polling one pod's counter directly (see
[`k6/README.md`](k6/README.md#configuration) for why that matters):

```sh
k6 run \
  -e REST_SERVICE_URL=http://<any-node-ip>:30081 \
  -e EMAIL_SERVICE_URL=http://<any-node-ip>:30082 \
  -e PROMETHEUS_URL=http://<any-node-ip>:30390 \
  k6/outbox-load-test.js
```

`EMAIL_SERVICE_URL` doesn't need to be the active replica's node here — with `PROMETHEUS_URL`
set, the script never polls it for the `emails.sent` count, so any node's IP works.

A passing run means the same thing it does on compose: every order created, every confirmation
eventually sent with no drops, and delivery visibly throttled.

### 8. Metrics: Prometheus + Grafana

[`k8s/monitoring/`](k8s/monitoring/) adds a Prometheus + Grafana stack, in its own `monitoring`
namespace, that makes the HA topology's actual behavior visible instead of just "up/down": Kafka
broker/Connect/consumer-lag metrics via Strimzi's built-in `strimziMetricsReporter` and
`kafkaExporter` (see `k8s/02-kafka.yaml`, `k8s/03-kafka-connect.yaml`), and `rest-service`/
`email-service` metrics via Micrometer's `/actuator/prometheus`.

Open Grafana at `http://<any-node-ip>:30300` — no login required (anonymous Admin access, this
being a prototype with no auth anywhere else either). A dashboard called **Outbox Pattern in
Action** is auto-provisioned on first boot, with three panels:

- **Orders created vs. confirmation emails sent** — the rate limiter's throttling curve: place
  orders faster than 5/10s (e.g. via the k6 script above) and watch the two lines diverge.
- **`outbox.event.order` consumer lag** — climbs under load, drains once the rate limiter catches
  up.
- **Kafka partition leadership by broker** — which of the 3 brokers is leading which partitions.

Prometheus's own UI (targets/graph pages, useful for checking scrape health directly) is at
`http://<any-node-ip>:30390`.

### 9. Distributed tracing: Tempo

[`k8s/monitoring/03-tempo.yaml`](k8s/monitoring/03-tempo.yaml) adds Grafana Tempo (single-binary,
local disk storage) to the `monitoring` namespace as the trace backend for the one hop metrics
can't show: the CDC relay between `rest-service` writing the `outbox` row and `email-service`
consuming the routed Kafka message. `rest-service` and `email-service` both export traces via
Spring Boot's Micrometer Tracing + OTLP exporter, and Debezium's EventRouter SMT carries the
trace across the CDC hop by routing the `outbox.trace_context` column onto the Kafka message as a
`traceparent` header — no custom SMT, no log-correlation hack (see
[ADR 0005](docs/adr/0005-trace-context-through-outbox.md)).

To view a trace: place an order, then open Grafana ([`http://<any-node-ip>:30300`](#8-metrics-prometheus--grafana)),
go to **Explore**, pick the **Tempo** datasource, and search by service name (`rest-service` or
`email-service`) or by trace ID (logged by both services alongside every span). A single trace
shows the `POST /orders` request span in `rest-service`, then a `receive` span in `email-service`
for the same trace ID — proof the CDC hop didn't break trace continuity, even though Debezium
itself never touched an OpenTelemetry SDK.

### Cleanup

```sh
./down.sh
```

Equivalent to `kind delete cluster --name outbox` directly.

## Notes

- This is a prototype, not a production-ready service: no auth, no outbox cleanup,
  no order lifecycle beyond creation, no deduplication of redelivered events in `email-service`.
- `rest-service` and `email-service` are fully independent Maven projects with no shared parent.

k6/outbox-load-test.js polls a metric that only lives on one of two email-service replicas

`email-service` runs 2 replicas, but `outbox.event.order` has only 1 partition, so only one
replica's consumer ever gets a partition assignment and does the work — the `emails.sent`
Micrometer counter only increments on that pod's JVM.

`k6/outbox-load-test.js` asserts `emails.sent` reaches the expected count by polling
`${EMAIL_SERVICE_URL}/actuator/metrics/emails.sent`, where `EMAIL_SERVICE_URL` is the
Kind NodePort Service (`:30082`). The Service load-balances across both replicas, so the
poll has roughly even odds of landing on the idle pod (counter stuck at 0) for the whole
test, making the check fail even though the pipeline is working correctly.

Reproduced on 2026-09-22: 20 orders posted, all 20 confirmation emails sent and correctly
rate-limited (5 per ~10s, visible in the busy pod's logs), but k6 reported
`emails.sent only reached 0/20 after 90s` because it happened to keep polling the pod
that never got a partition.

Fix should make the assertion resilient to which replica answers — e.g. sum the counter
across replicas via Prometheus/PromQL instead of hitting a single pod's actuator endpoint
through the load-balanced Service, or otherwise stop relying on per-pod JVM state for an
end-to-end check.

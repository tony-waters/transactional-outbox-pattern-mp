
I want to create a prototype to demonstrate the transactional outbox pattern, built around a system that sends an email notification when an order is received.

there are 2 main services: 'rest-service' and 'email-service':

**rest-service** is a spring boot REST api. it writes to the order table and the outbox table in a single transaction.
**email-service** is a Spring Boot application. it pretends to send a customer confirmation email for each order. It has a rate-limiter (resilience4j) of 5 requests every 10 seconds.

**postgres** is used as the database to hold the order table and the outbox table.
**debezium** is used for Change Data Capture - feeds the outbox order details to kafka. 
**apache kafka** is used as a queue for the email notification service.

Create both services in this repo, with their own pom.
make everything run in docker-compose.
include k6 tests to demonstrate it works.

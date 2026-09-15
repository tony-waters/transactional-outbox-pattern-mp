package com.example.outbox.rest.order;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OutboxWriteTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void writesAnOutboxRowInDebeziumsExpectedShape() {
        CreateOrderRequest request = new CreateOrderRequest("customer@example.com", new BigDecimal("19.99"));

        ResponseEntity<OrderResponse> createResponse = restTemplate.postForEntity("/orders", request, OrderResponse.class);
        assertThat(createResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        OrderResponse created = createResponse.getBody();
        assertThat(created).isNotNull();

        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT aggregatetype, aggregateid, type, payload FROM outbox WHERE aggregateid = ?",
                created.id().toString());

        assertThat(row.get("aggregatetype")).isEqualTo("order");
        assertThat(row.get("aggregateid")).isEqualTo(created.id().toString());
        assertThat(row.get("type")).isEqualTo("OrderCreated");
        assertThat(row.get("payload").toString())
                .contains(created.id().toString())
                .contains("customer@example.com")
                .contains("19.99");
    }

    @Test
    void noOutboxEventTypeOtherThanOrderCreatedIsEverWritten() {
        CreateOrderRequest request = new CreateOrderRequest("another@example.com", new BigDecimal("5.00"));
        restTemplate.postForEntity("/orders", request, OrderResponse.class);

        Long distinctTypes = jdbcTemplate.queryForObject("SELECT COUNT(DISTINCT type) FROM outbox", Long.class);
        assertThat(distinctTypes).isEqualTo(1L);

        String theOnlyType = jdbcTemplate.queryForObject("SELECT DISTINCT type FROM outbox", String.class);
        assertThat(theOnlyType).isEqualTo("OrderCreated");
    }
}

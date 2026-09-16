package com.example.outbox.rest.order;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final Tracer tracer;
    private final Propagator propagator;

    public OrderService(OrderRepository orderRepository, OutboxEventRepository outboxEventRepository,
                         ObjectMapper objectMapper, Tracer tracer, Propagator propagator) {
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
        this.tracer = tracer;
        this.propagator = propagator;
    }

    @Transactional
    public Order createOrder(CreateOrderRequest request) {
        Order order = new Order(UUID.randomUUID(), request.customerEmail(), request.amount(), Instant.now());
        orderRepository.save(order);

        OutboxEvent event = new OutboxEvent(UUID.randomUUID(), "order", order.getId().toString(), "OrderCreated",
                writePayload(order), currentTraceParent());
        outboxEventRepository.save(event);

        return order;
    }

    // Carries the current request's trace into the outbox row so it survives the CDC hop
    // (see ADR 0005) — delegates to the registered Propagator rather than hand-formatting a
    // W3C traceparent, so this keeps working if the propagation format ever changes.
    private String currentTraceParent() {
        Span currentSpan = tracer.currentSpan();
        if (currentSpan == null) {
            return null;
        }
        Map<String, String> carrier = new HashMap<>();
        propagator.inject(currentSpan.context(), carrier, Map::put);
        return carrier.get("traceparent");
    }

    public Optional<Order> findOrder(UUID id) {
        return orderRepository.findById(id);
    }

    private String writePayload(Order order) {
        try {
            return objectMapper.writeValueAsString(OrderCreatedPayload.from(order));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize OrderCreated payload for order " + order.getId(), e);
        }
    }
}

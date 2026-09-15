package com.example.outbox.rest.order;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orderRepository, OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Order createOrder(CreateOrderRequest request) {
        Order order = new Order(UUID.randomUUID(), request.customerEmail(), request.amount(), Instant.now());
        orderRepository.save(order);

        OutboxEvent event = new OutboxEvent(UUID.randomUUID(), "order", order.getId().toString(), "OrderCreated", writePayload(order));
        outboxEventRepository.save(event);

        return order;
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

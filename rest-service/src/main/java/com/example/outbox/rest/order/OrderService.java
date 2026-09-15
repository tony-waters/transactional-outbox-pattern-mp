package com.example.outbox.rest.order;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
public class OrderService {

    private final OrderRepository orderRepository;

    public OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @Transactional
    public Order createOrder(CreateOrderRequest request) {
        Order order = new Order(UUID.randomUUID(), request.customerEmail(), request.amount(), Instant.now());
        return orderRepository.save(order);
    }

    public Optional<Order> findOrder(UUID id) {
        return orderRepository.findById(id);
    }
}

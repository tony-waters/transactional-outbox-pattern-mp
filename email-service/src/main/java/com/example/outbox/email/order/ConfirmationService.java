package com.example.outbox.email.order;

import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
class ConfirmationService {

    private static final Logger log = LoggerFactory.getLogger(ConfirmationService.class);

    // resilience4j has no literal "block forever" option, so a very long timeout stands in for it.
    private static final Duration EFFECTIVELY_UNBOUNDED = Duration.ofDays(365);

    // Blocks the calling (consumer) thread until a permit frees up, rather than rejecting,
    // retrying or dead-lettering on rate-limit breach (see ADR 0002).
    private final RateLimiter rateLimiter = RateLimiter.of("emailSender", RateLimiterConfig.custom()
            .limitForPeriod(5)
            .limitRefreshPeriod(Duration.ofSeconds(10))
            .timeoutDuration(EFFECTIVELY_UNBOUNDED)
            .build());

    private final Counter emailsSent;

    ConfirmationService(MeterRegistry meterRegistry) {
        this.emailsSent = Counter.builder("emails.sent").register(meterRegistry);
    }

    void send(OrderCreatedEvent event) {
        rateLimiter.acquirePermission();
        log.info("Sent confirmation email to {} for order {} ({})", event.customerEmail(), event.orderId(), event.amount());
        emailsSent.increment();
    }
}

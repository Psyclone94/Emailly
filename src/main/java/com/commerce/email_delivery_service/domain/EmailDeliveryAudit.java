package com.commerce.email_delivery_service.domain;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "email_delivery_audit", schema = "email")
public class EmailDeliveryAudit {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "customer_id", nullable = false)
    private UUID customerId;

    @Column(name = "rabbit_queue", nullable = false, length = 100)
    private String rabbitQueue;

    // the AMQP messageId, set by order-service's OutboxPublisher to the outbox event's UUID -
    // unique-constrained here, this is what makes redelivery detection possible
    @Column(name = "rabbit_message_id", nullable = false, unique = true)
    private UUID rabbitMessageId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private DeliveryStatus status;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    @Column(name = "last_error", columnDefinition = "text")
    private String lastError;

    @CreationTimestamp
    @Column(name = "received_at", updatable = false)
    private OffsetDateTime receivedAt;

    @Column(name = "sent_at")
    private OffsetDateTime sentAt;

    protected EmailDeliveryAudit() {
        // JPA
    }

    public EmailDeliveryAudit(UUID id, UUID orderId, UUID customerId, String rabbitQueue,
                              UUID rabbitMessageId, DeliveryStatus status, int attemptCount) {
        this.id = id;
        this.orderId = orderId;
        this.customerId = customerId;
        this.rabbitQueue = rabbitQueue;
        this.rabbitMessageId = rabbitMessageId;
        this.status = status;
        this.attemptCount = attemptCount;
    }

    public UUID getId() { return id; }
    public UUID getOrderId() { return orderId; }
    public UUID getCustomerId() { return customerId; }
    public String getRabbitQueue() { return rabbitQueue; }
    public UUID getRabbitMessageId() { return rabbitMessageId; }
    public DeliveryStatus getStatus() { return status; }
    public int getAttemptCount() { return attemptCount; }
    public String getLastError() { return lastError; }
    public OffsetDateTime getReceivedAt() { return receivedAt; }
    public OffsetDateTime getSentAt() { return sentAt; }

    public void markSent() {
        this.status = DeliveryStatus.SENT;
        this.sentAt = OffsetDateTime.now();
    }

    public void updateAttemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
    }

    public void markFailed(String error, int attemptCount) {
        this.status = DeliveryStatus.FAILED;
        this.lastError = truncate(error);
        this.attemptCount = attemptCount;
    }

    public void markDlq(String error) {
        this.status = DeliveryStatus.DLQ;
        this.lastError = truncate(error);
    }

    private String truncate(String s) {
        if (s == null) return null;
        return s.length() > 2000 ? s.substring(0, 2000) : s;
    }
}
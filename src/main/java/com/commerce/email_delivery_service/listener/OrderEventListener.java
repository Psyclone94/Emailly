package com.commerce.email_delivery_service.listener;

import com.commerce.email_delivery_service.config.RabbitTopologyConstants;
import com.commerce.email_delivery_service.exception.PoisonMessageException;
import com.commerce.email_delivery_service.exception.RetryableProcessingException;
import com.commerce.email_delivery_service.service.EmailDeliveryOrchestrator;
import com.commerce.email_delivery_service.service.RetryPublisher;
import com.fasterxml.jackson.databind.JsonNode;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final EmailDeliveryOrchestrator orchestrator;
    private final RetryPublisher retryPublisher;

    public OrderEventListener(EmailDeliveryOrchestrator orchestrator, RetryPublisher retryPublisher) {
        this.orchestrator = orchestrator;
        this.retryPublisher = retryPublisher;
    }

    @RabbitListener(queues = {
            "commerce.order.events.q0", "commerce.order.events.q1", "commerce.order.events.q2",
            "commerce.order.events.q3", "commerce.order.events.q4", "commerce.order.events.q5"
    })
    public void handle(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        String queueName = message.getMessageProperties().getConsumerQueue();
        UUID rabbitMessageId = parseMessageId(message);
        int attemptCount = readAttemptCount(message);

        try {
            process(message, queueName, rabbitMessageId, attemptCount);
            channel.basicAck(deliveryTag, false);

        } catch (PoisonMessageException e) {
            log.error("Poison message on {} (id={}): {}", queueName, rabbitMessageId, e.getMessage());
            if (rabbitMessageId != null) {
                orchestrator.markDlq(rabbitMessageId, e.getMessage());
            }
            retryPublisher.routeToDlq(message.getBody(), message.getMessageProperties(), e.getMessage());
            channel.basicAck(deliveryTag, false); // we've handled it via DLQ publish, remove from main queue

        } catch (RetryableProcessingException e) {
            log.warn("Retryable failure on {} (id={}, attempt={}): {}",
                    queueName, rabbitMessageId, attemptCount, e.getMessage(), e);

            String detailedError = e.getMessage() +
                    (e.getCause() != null ? " | cause: " + e.getCause().getMessage() : "");

            if (rabbitMessageId != null) {
                orchestrator.markFailed(rabbitMessageId, detailedError, attemptCount);
            }

            if (attemptCount < RabbitTopologyConstants.MAX_RETRY_TIERS) {
                retryPublisher.republishToNextTier(queueName, message.getBody(),
                        message.getMessageProperties(), attemptCount);
            } else {
                String reason = "Exhausted " + RabbitTopologyConstants.MAX_RETRY_TIERS
                        + " retries: " + detailedError;
                if (rabbitMessageId != null) {
                    orchestrator.markDlq(rabbitMessageId, reason);
                }
                retryPublisher.routeToDlq(message.getBody(), message.getMessageProperties(), reason);
            }
            channel.basicAck(deliveryTag, false); // we've explicitly handled the next step ourselves

        } catch (Exception e) {
            // Unexpected/infra failure (e.g. DB unreachable when writing the audit row) -
            // we CAN'T reliably run our own retry logic here since it depends on the DB.
            // Fall back to nack: the queue's static x-dead-letter-exchange config routes
            // this to tier-0 retry automatically, acting as a safety net.
            log.error("Unexpected error processing message on {} (id={}), falling back to nack",
                    queueName, rabbitMessageId, e);
            channel.basicNack(deliveryTag, false, false);
        }
    }

    private void process(Message message, String queueName, UUID rabbitMessageId, int attemptCount) {
        if (rabbitMessageId != null && orchestrator.isDuplicateDelivery(rabbitMessageId)) {
            log.info("Duplicate delivery detected for message {}, already handled - skipping", rabbitMessageId);
            return;
        }

        JsonNode event = orchestrator.parseEvent(message.getBody());
        UUID orderId = UUID.fromString(event.get("orderId").asText());
        UUID customerId = UUID.fromString(event.get("customerId").asText());
        JsonNode items = event.get("items");
        String attachmentKey = event.hasNonNull("attachmentKey") ? event.get("attachmentKey").asText() : null;

        if (rabbitMessageId != null) {
            orchestrator.recordReceived(rabbitMessageId, orderId, customerId, queueName, attemptCount);
        }

        orchestrator.sendEmail(orderId, customerId, items, attachmentKey);

        if (rabbitMessageId != null) {
            orchestrator.markSent(rabbitMessageId);
        }
    }

    private UUID parseMessageId(Message message) {
        String id = message.getMessageProperties().getMessageId();
        if (id == null) {
            log.warn("Message arrived with no messageId set - idempotency dedup will not work for this message");
            return null;
        }
        try {
            return UUID.fromString(id);
        } catch (IllegalArgumentException e) {
            log.warn("Message arrived with non-UUID messageId '{}' - idempotency dedup will not work", id);
            return null;
        }
    }

    private int readAttemptCount(Message message) {
        Object header = message.getMessageProperties().getHeaders().get("x-attempt-count");
        if (header instanceof Integer i) {
            return i;
        }
        return 0; // no header means first delivery
    }
}
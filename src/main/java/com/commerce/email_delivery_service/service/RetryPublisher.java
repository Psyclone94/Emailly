package com.commerce.email_delivery_service.service;

import com.commerce.email_delivery_service.config.RabbitTopologyConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class RetryPublisher {

    private static final long CONFIRM_TIMEOUT_MS = 5_000;
    private final RabbitTemplate rabbitTemplate;

    /**
     * Republishes to the correct retry-tier queue based on the CURRENT attempt count.
     * attemptCount 0 (first delivery, no prior retries) -> tier 0 (1s)
     * attemptCount 1 -> tier 1 (5s)
     * attemptCount 2 -> tier 2 (15s)
     */
    public void republishToNextTier(String originQueueName, byte[] body, MessageProperties originalProps,
                                    int currentAttemptCount) {
        int queueIndex = RabbitTopologyConstants.extractQueueIndex(originQueueName);
        String routingKey = RabbitTopologyConstants.retryRoutingKey(queueIndex, currentAttemptCount);

        MessageProperties newProps = cloneBaseProps(originalProps);
        newProps.setHeader("x-attempt-count", currentAttemptCount + 1);

        Message retryMessage = new Message(body, newProps);
        CorrelationData correlationData = new CorrelationData(newProps.getMessageId() + "-retry-" + currentAttemptCount);

        rabbitTemplate.invoke(operations -> {
            operations.send(RabbitTopologyConstants.RETRY_EXCHANGE, routingKey, retryMessage, correlationData);
            operations.waitForConfirmsOrDie(CONFIRM_TIMEOUT_MS);
            return null;
        });

        log.warn("Message {} republished to retry tier {} (queue {}), attempt {} -> {}",
                originalProps.getMessageId(), currentAttemptCount, originQueueName, currentAttemptCount, currentAttemptCount + 1);
    }

    public void routeToDlq(byte[] body, MessageProperties originalProps, String reason) {
        MessageProperties newProps = cloneBaseProps(originalProps);
        newProps.setHeader("x-dlq-reason", reason);

        Message dlqMessage = new Message(body, newProps);
        CorrelationData correlationData = new CorrelationData(newProps.getMessageId() + "-dlq");

        rabbitTemplate.invoke(operations -> {
            // published via default exchange with routing key = queue name, routing directly to DLQ
            operations.send("", RabbitTopologyConstants.DLQ_NAME, dlqMessage, correlationData);
            operations.waitForConfirmsOrDie(CONFIRM_TIMEOUT_MS);
            return null;
        });

        log.error("Message {} routed to DLQ: {}", originalProps.getMessageId(), reason);
    }

    private MessageProperties cloneBaseProps(MessageProperties original) {
        MessageProperties copy = new MessageProperties();
        copy.setContentType(original.getContentType());
        copy.setMessageId(original.getMessageId());
        return copy;
    }
}
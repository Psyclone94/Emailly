package com.commerce.email_delivery_service.service;

import com.commerce.email_delivery_service.config.RabbitTopologyConstants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.GetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Pulls messages off the DLQ and gives them a fresh shot at the main retry ladder, safely.
 *
 * Safety guarantees:
 *  - Manual ack (basicGet with autoAck=false) - a message is only permanently removed from
 *    the DLQ after we've confirmed it was successfully republished. If the republish fails,
 *    times out, or this process crashes mid-operation, the message is nack'd back into the
 *    DLQ unchanged - it can NEVER be silently lost.
 *  - x-redrive-count header caps this at MAX_REDRIVE_ATTEMPTS. A message with a permanently
 *    broken root cause (e.g. genuinely malformed data) doesn't bounce between DLQ and main
 *    queue forever - after the cap, it sits in the DLQ flagged for actual human attention,
 *    rather than the system quietly masking a real problem forever.
 *  - Unparseable messages (can't even extract customerId for routing) are left in the DLQ
 *    rather than guessed at - a bad routing key would just misroute the message elsewhere.
 */
@Component
public class DlqRedriveService {

    private static final Logger log = LoggerFactory.getLogger(DlqRedriveService.class);
    private static final long CONFIRM_TIMEOUT_MS = 5_000;
    private static final int AUTO_REDRIVE_BATCH_SIZE = 10;

    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;

    public DlqRedriveService(RabbitTemplate rabbitTemplate, ObjectMapper objectMapper) {
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Manually triggered via REST - e.g. after an operator has confirmed the underlying
     * issue (SMTP outage, etc.) is fixed and wants to flush the backlog immediately.
     */
    public RedriveResult redrive(int maxMessages) {
        return rabbitTemplate.execute(channel -> {
            channel.confirmSelect(); // enable publisher confirms on this channel for the republish step

            int redriven = 0, exhausted = 0, poison = 0;

            for (int i = 0; i < maxMessages; i++) {
                GetResponse response = channel.basicGet(RabbitTopologyConstants.DLQ_NAME, false); // manual ack
                if (response == null) {
                    break; // DLQ empty, nothing more to do
                }

                long deliveryTag = response.getEnvelope().getDeliveryTag();
                byte[] body = response.getBody();
                AMQP.BasicProperties originalProps = response.getProps();

                try {
                    int redriveCount = readIntHeader(originalProps, "x-redrive-count", 0);

                    if (redriveCount >= RabbitTopologyConstants.MAX_REDRIVE_ATTEMPTS) {
                        channel.basicNack(deliveryTag, false, true); // back into DLQ, unchanged
                        exhausted++;
                        log.warn("Message {} exhausted {} redrive attempts, needs manual investigation",
                                originalProps.getMessageId(), RabbitTopologyConstants.MAX_REDRIVE_ATTEMPTS);
                        continue;
                    }

                    String customerId;
                    try {
                        JsonNode json = objectMapper.readTree(body);
                        customerId = json.get("customerId").asText();
                    } catch (Exception parseEx) {
                        channel.basicNack(deliveryTag, false, true); // can't route it safely, leave for inspection
                        poison++;
                        log.error("Message {} in DLQ has unparseable body, cannot redrive - needs manual fix",
                                originalProps.getMessageId());
                        continue;
                    }

                    Map<String, Object> headers = new HashMap<>();
                    headers.put("x-attempt-count", 0); // fresh full retry ladder - assume root cause is fixed
                    headers.put("x-redrive-count", redriveCount + 1);

                    AMQP.BasicProperties newProps = new AMQP.BasicProperties.Builder()
                            .contentType(originalProps.getContentType())
                            .messageId(originalProps.getMessageId())
                            .headers(headers)
                            .build();

                    channel.basicPublish(RabbitTopologyConstants.MAIN_EXCHANGE, customerId, true, newProps, body);
                    boolean confirmed = channel.waitForConfirms(CONFIRM_TIMEOUT_MS);

                    if (confirmed) {
                        channel.basicAck(deliveryTag, false); // only NOW is it safe to remove from DLQ
                        redriven++;
                        log.info("Message {} redriven from DLQ (attempt {}/{})", originalProps.getMessageId(),
                                redriveCount + 1, RabbitTopologyConstants.MAX_REDRIVE_ATTEMPTS);
                    } else {
                        channel.basicNack(deliveryTag, false, true); // publish not confirmed - keep in DLQ, don't lose it
                        log.error("Redrive publish not confirmed for message {}, left in DLQ",
                                originalProps.getMessageId());
                    }

                } catch (Exception e) {
                    // anything unexpected mid-redrive - the safest thing is always to put it back
                    log.error("Unexpected error redriving message, returning to DLQ", e);
                    channel.basicNack(deliveryTag, false, true);
                }
            }

            return new RedriveResult(redriven, exhausted, poison);
        });
    }

    /**
     * Self-healing background pass - small batch, long interval. This is what makes the
     * system "smart enough to send it out on its own" for transient outages (SMTP blip,
     * momentary MinIO hiccup) without needing an operator to notice and trigger it manually.
     * The MAX_REDRIVE_ATTEMPTS cap is what keeps this safe - it won't mask a permanently
     * broken message by silently cycling it forever.
     */
    @Scheduled(fixedDelay = 120_000)
    public void autoRedrive() {
        RedriveResult result = redrive(AUTO_REDRIVE_BATCH_SIZE);
        if (result.redriven() > 0 || result.exhausted() > 0 || result.poison() > 0) {
            log.info("Auto-redrive cycle: {} redriven, {} exhausted (manual fix needed), {} poison (manual fix needed)",
                    result.redriven(), result.exhausted(), result.poison());
        }
    }

    private int readIntHeader(AMQP.BasicProperties props, String headerName, int defaultValue) {
        if (props.getHeaders() == null) return defaultValue;
        Object value = props.getHeaders().get(headerName);
        return value instanceof Integer i ? i : defaultValue;
    }
}

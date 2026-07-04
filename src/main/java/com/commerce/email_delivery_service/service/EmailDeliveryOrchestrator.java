package com.commerce.email_delivery_service.service;

import com.commerce.email_delivery_service.domain.DeliveryStatus;
import com.commerce.email_delivery_service.domain.EmailDeliveryAudit;
import com.commerce.email_delivery_service.exception.PoisonMessageException;
import com.commerce.email_delivery_service.repository.EmailDeliveryAuditRepository;
import com.commerce.email_delivery_service.storage.AttachmentFetchService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.util.UUID;

@Service
public class EmailDeliveryOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(EmailDeliveryOrchestrator.class);

    private final EmailDeliveryAuditRepository auditRepository;
    private final AttachmentFetchService attachmentFetchService;
    private final EmailSendService emailSendService;
    private final ObjectMapper objectMapper;
    private final AuditWriter auditWriter;

    public EmailDeliveryOrchestrator(EmailDeliveryAuditRepository auditRepository,
                                     AttachmentFetchService attachmentFetchService,
                                     EmailSendService emailSendService,
                                     ObjectMapper objectMapper,
                                     AuditWriter auditWriter) {
        this.auditRepository = auditRepository;
        this.attachmentFetchService = attachmentFetchService;
        this.emailSendService = emailSendService;
        this.objectMapper = objectMapper;
        this.auditWriter = auditWriter;
    }

    /**
     * @return true if this message was a duplicate delivery already handled - caller should
     * just ack and stop, no further processing needed.
     */
    @Transactional
    public boolean isDuplicateDelivery(UUID rabbitMessageId) {
        return auditRepository.findByRabbitMessageId(rabbitMessageId)
                .filter(a -> a.getStatus() == DeliveryStatus.SENT || a.getStatus() == DeliveryStatus.DLQ)
                .isPresent();
    }

    /**
     * Parses and validates the message body. Malformed JSON is poison - not worth retrying.
     */
    public JsonNode parseEvent(byte[] body) {
        try {
            JsonNode node = objectMapper.readTree(body);
            if (!node.hasNonNull("orderId") || !node.hasNonNull("customerId")) {
                throw new PoisonMessageException("Event missing required fields (orderId/customerId)");
            }
            return node;
        } catch (IOException e) {
            throw new PoisonMessageException("Malformed event JSON, cannot parse", e);
        }
    }

    public EmailDeliveryAudit recordReceived(UUID rabbitMessageId, UUID orderId, UUID customerId,
                                             String queueName, int attemptCount) {
        // check first: on a retry, the row from the original delivery already exists -
        // update its attempt_count in place rather than trying (and failing) another insert
        var existing = auditRepository.findByRabbitMessageId(rabbitMessageId);
        if (existing.isPresent()) {
            EmailDeliveryAudit audit = existing.get();
            audit.updateAttemptCount(attemptCount);
            return auditRepository.save(audit);
        }

        EmailDeliveryAudit audit = new EmailDeliveryAudit(
                UUID.randomUUID(), orderId, customerId, queueName, rabbitMessageId,
                DeliveryStatus.RECEIVED, attemptCount);
        try {
            // separate bean call - goes through Spring's proxy, gets its own real transaction
            // that commits or rolls back completely independent of this method's context
            return auditWriter.insert(audit);
        } catch (DataIntegrityViolationException e) {
            // genuine race: another thread/instance inserted between our check above and this
            // insert attempt (shouldn't happen with single-active-consumer, but cheap insurance)
            // - this runs in a FRESH transaction, unaffected by the rolled-back insert attempt
            return auditRepository.findByRabbitMessageId(rabbitMessageId).orElseThrow(() -> e);
        }
    }

    public void sendEmail(UUID orderId, UUID customerId, JsonNode items, String attachmentKey) {
        if (attachmentKey == null) {
            emailSendService.sendOrderConfirmation(orderId, customerId, items.toString(), null, null);
            return;
        }

        try (ResponseInputStream<GetObjectResponse> attachmentStream = attachmentFetchService.fetch(attachmentKey)) {
            String filename = attachmentKey.substring(attachmentKey.lastIndexOf('/') + 1);

            // 1. Read the network stream entirely into memory before it closes
            byte[] attachmentBytes = attachmentStream.readAllBytes();

            // 2. Wrap the memory buffer back into an InputStream for EmailSendService
            try (java.io.ByteArrayInputStream memoryStream = new java.io.ByteArrayInputStream(attachmentBytes)) {
                emailSendService.sendOrderConfirmation(orderId, customerId, items.toString(), filename, memoryStream);
            }
        } catch (IOException e) {
            throw new com.commerce.email_delivery_service.exception.RetryableProcessingException(
                    "Failed to process attachment stream for order " + orderId, e);
        }
    }
    @Transactional
    public void markSent(UUID rabbitMessageId) {
        auditRepository.findByRabbitMessageId(rabbitMessageId).ifPresent(a -> {
            a.markSent();
            auditRepository.save(a);
        });
    }

    @Transactional
    public void markFailed(UUID rabbitMessageId, String error, int attemptCount) {
        auditRepository.findByRabbitMessageId(rabbitMessageId).ifPresent(a -> {
            a.markFailed(error, attemptCount);
            auditRepository.save(a);
        });
    }

    @Transactional
    public void markDlq(UUID rabbitMessageId, String error) {
        auditRepository.findByRabbitMessageId(rabbitMessageId).ifPresent(a -> {
            a.markDlq(error);
            auditRepository.save(a);
        });
    }
}
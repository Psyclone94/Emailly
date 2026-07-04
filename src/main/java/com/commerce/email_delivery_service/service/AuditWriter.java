package com.commerce.email_delivery_service.service;

import com.commerce.email_delivery_service.domain.EmailDeliveryAudit;
import com.commerce.email_delivery_service.repository.EmailDeliveryAuditRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Deliberately a SEPARATE bean from EmailDeliveryOrchestrator, not just a separate method.
 * Postgres aborts the ENTIRE transaction the instant any statement fails - catching the
 * exception in Java doesn't help if the fallback query runs on that same poisoned
 * transaction/connection. Putting the insert in its own bean means Spring's transactional
 * proxy commits/rolls back THIS transaction independently and fully before the exception
 * ever reaches the caller - so the caller's subsequent fallback query gets a clean, fresh
 * transaction instead of inheriting a doomed one.
 */
@Component
public class AuditWriter {

    private final EmailDeliveryAuditRepository auditRepository;

    public AuditWriter(EmailDeliveryAuditRepository auditRepository) {
        this.auditRepository = auditRepository;
    }

    @Transactional
    public EmailDeliveryAudit insert(EmailDeliveryAudit audit) {
        return auditRepository.saveAndFlush(audit);
    }
}

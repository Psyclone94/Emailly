package com.commerce.email_delivery_service.repository;


import com.commerce.email_delivery_service.domain.EmailDeliveryAudit;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface EmailDeliveryAuditRepository extends JpaRepository<EmailDeliveryAudit, UUID> {

    Optional<EmailDeliveryAudit> findByRabbitMessageId(UUID rabbitMessageId);
}

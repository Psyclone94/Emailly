package com.commerce.email_delivery_service.controller;

import com.commerce.email_delivery_service.config.RabbitTopologyConstants;
import com.commerce.email_delivery_service.service.DlqRedriveService;
import com.commerce.email_delivery_service.service.RedriveResult;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Properties;

@RestController
@RequestMapping("/api/v1/dlq")
public class DlqController {

    private final DlqRedriveService dlqRedriveService;
    private final RabbitAdmin rabbitAdmin;

    public DlqController(DlqRedriveService dlqRedriveService, RabbitAdmin rabbitAdmin) {
        this.dlqRedriveService = dlqRedriveService;
        this.rabbitAdmin = rabbitAdmin;
    }

    @GetMapping("/count")
    public Map<String, Object> count() {
        Properties props = rabbitAdmin.getQueueProperties(RabbitTopologyConstants.DLQ_NAME);
        int messageCount = props != null
                ? (Integer) props.get(RabbitAdmin.QUEUE_MESSAGE_COUNT)
                : 0;
        return Map.of("queue", RabbitTopologyConstants.DLQ_NAME, "messageCount", messageCount);
    }

    /**
     * Manually triggers a redrive pass. Use this right after fixing whatever caused messages
     * to land in the DLQ (SMTP outage resolved, MinIO back up, etc.) to flush the backlog
     * immediately, rather than waiting for the next scheduled auto-redrive cycle.
     */
    @PostMapping("/redrive")
    public RedriveResult redrive(@RequestParam(defaultValue = "20") int maxMessages) {
        return dlqRedriveService.redrive(maxMessages);
    }
}

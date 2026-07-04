package com.commerce.email_delivery_service.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {

    private static final Logger log = LoggerFactory.getLogger(RabbitConfig.class);

    // This service doesn't declare topology (order-service already did), but RabbitAdmin
    // is still needed here as a dependency of nothing in particular - kept for symmetry
    // and in case this service ever needs to declare something of its own.
    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        return new RabbitAdmin(connectionFactory);
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMandatory(true);
        template.setReturnsCallback(returned -> log.error(
                "Retry/DLQ publish unroutable! exchange={} routingKey={} replyCode={} replyText={}",
                returned.getExchange(), returned.getRoutingKey(),
                returned.getReplyCode(), returned.getReplyText()));
        template.setConfirmCallback((correlationData, ack, cause) -> {
            if (!ack) {
                log.error("Broker NACKed retry/DLQ publish for correlationId={} cause={}",
                        correlationData != null ? correlationData.getId() : "unknown", cause);
            }
        });
        return template;
    }
}

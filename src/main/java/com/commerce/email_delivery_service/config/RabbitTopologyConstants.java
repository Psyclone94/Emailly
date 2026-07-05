package com.commerce.email_delivery_service.config;

public final class RabbitTopologyConstants {

    private RabbitTopologyConstants() {}

    public static final String MAIN_EXCHANGE = "commerce.order.events";
    public static final String RETRY_EXCHANGE = "commerce.order.events.retry";
    public static final String DLQ_NAME = "commerce.order.events.DLQ";
    public static final int QUEUE_COUNT = 6;
    public static final int MAX_RETRY_TIERS = 3; // 1s, 5s, 15s
    public static final int MAX_REDRIVE_ATTEMPTS = 3; // caps auto-redrive loops on a permanently-broken message

    public static String mainQueueName(int index) {
        return "commerce.order.events.q" + index;
    }

    public static String retryRoutingKey(int index, int tier) {
        return "retry.q" + index + ".t" + tier;
    }

    /** Extracts the queue index (0-5) from a queue name like "commerce.order.events.q3". */
    public static int extractQueueIndex(String queueName) {
        String suffix = queueName.substring(queueName.lastIndexOf('.') + 1); // "q3"
        return Integer.parseInt(suffix.substring(1)); // "3" -> 3
    }
}

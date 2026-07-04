package com.commerce.email_delivery_service.exception;

/**
 * Thrown for failures that will NEVER succeed on retry - malformed JSON, attachment key
 * that returns a genuine 404 (the file doesn't exist, retrying won't make it appear).
 * These go straight to the DLQ on first failure, no point burning 3 retry cycles on them.
 */
public class PoisonMessageException extends RuntimeException {
    public PoisonMessageException(String message) {
        super(message);
    }

    public PoisonMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}

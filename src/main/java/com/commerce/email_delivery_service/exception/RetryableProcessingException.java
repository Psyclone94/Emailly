package com.commerce.email_delivery_service.exception;

public class RetryableProcessingException extends RuntimeException {
    public RetryableProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}


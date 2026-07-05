package com.commerce.email_delivery_service.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<Map<String, Object>> handleResponseStatus(ResponseStatusException e) {
        return errorResponse(HttpStatus.valueOf(e.getStatusCode().value()), "BAD_REQUEST",
                e.getReason() != null ? e.getReason() : "Bad request");
    }

    // SMTP down/unreachable at the moment of an ad-hoc send request - the caller gets a clear
    // signal to retry, rather than a generic 500
    @ExceptionHandler(RetryableProcessingException.class)
    public ResponseEntity<Map<String, Object>> handleRetryable(RetryableProcessingException e) {
        log.error("Email send failed: {}", e.getMessage());
        return errorResponse(HttpStatus.BAD_GATEWAY, "EMAIL_SEND_FAILED",
                "Could not send email, please retry");
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleUnexpected(Exception e) {
        log.error("Unexpected error", e);
        return errorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR",
                "An unexpected error occurred");
    }

    private ResponseEntity<Map<String, Object>> errorResponse(HttpStatus status, String code, String message) {
        return ResponseEntity.status(status).body(Map.of(
                "code", code,
                "message", message,
                "timestamp", Instant.now().toString()
        ));
    }
}

package com.commerce.email_delivery_service.service;

public record RedriveResult(int redriven, int exhausted, int poison) {
}

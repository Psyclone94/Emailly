package com.commerce.email_delivery_service.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public record SendEmailRequest(
        @NotEmpty List<@Email String> to,
        List<@Email String> cc,
        List<@Email String> bcc,
        String from,          // optional - falls back to a configured default if null
        @NotBlank String subject,
        @NotBlank String body,
        boolean html           // false = plain text, true = body is rendered as HTML
) {
}
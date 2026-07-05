package com.commerce.email_delivery_service.controller;

import com.commerce.email_delivery_service.dto.SendEmailRequest;
import com.commerce.email_delivery_service.service.EmailSendService;
import com.commerce.email_delivery_service.service.EmailSendService.EmailAttachmentInput;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Generic email send endpoint - independent of the order pipeline. Useful for anything that
 * doesn't originate from an order event: manual notifications, ops alerts, ad-hoc customer
 * communication, etc.
 */
@RestController
@RequestMapping("/api/v1/emails")
public class EmailController {

    private final EmailSendService emailSendService;
    private final ObjectMapper objectMapper;
    private final Validator validator;

    public EmailController(EmailSendService emailSendService, ObjectMapper objectMapper, Validator validator) {
        this.emailSendService = emailSendService;
        this.objectMapper = objectMapper;
        this.validator = validator;
    }

    @PostMapping(consumes = "multipart/form-data")
    public ResponseEntity<Map<String, String>> sendEmail(
            // bound as raw String, not SendEmailRequest directly - same reasoning as
            // OrderController: Swagger UI doesn't reliably tag this part as application/json,
            // which makes Spring's content-negotiated @RequestPart binding reject it
            @RequestPart("email") String emailJson,
            @RequestPart(value = "attachments", required = false) List<MultipartFile> attachments) {

        SendEmailRequest request = parseAndValidate(emailJson);
        List<EmailAttachmentInput> attachmentInputs = readAttachments(attachments);

        emailSendService.sendCustomEmail(
                request.to(), request.cc(), request.bcc(), request.from(),
                request.subject(), request.body(), request.html(), attachmentInputs);

        return ResponseEntity.ok(Map.of("status", "SENT"));
    }

    private SendEmailRequest parseAndValidate(String emailJson) {
        SendEmailRequest request;
        try {
            request = objectMapper.readValue(emailJson, SendEmailRequest.class);
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Malformed 'email' JSON part: " + e.getMessage());
        }

        Set<ConstraintViolation<SendEmailRequest>> violations = validator.validate(request);
        if (!violations.isEmpty()) {
            String detail = violations.stream()
                    .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                    .reduce((a, b) -> a + "; " + b)
                    .orElse("Validation failed");
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, detail);
        }
        return request;
    }

    private List<EmailAttachmentInput> readAttachments(List<MultipartFile> attachments) {
        if (attachments == null || attachments.isEmpty()) {
            return List.of();
        }
        List<EmailAttachmentInput> result = new ArrayList<>();
        for (MultipartFile file : attachments) {
            if (file.isEmpty()) continue;
            try {
                // fully read into memory here, at the boundary - same principle as the
                // order-confirmation attachment fix, avoid passing live streams around
                result.add(new EmailAttachmentInput(file.getOriginalFilename(), file.getBytes()));
            } catch (IOException e) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        "Failed to read attachment: " + file.getOriginalFilename());
            }
        }
        return result;
    }
}

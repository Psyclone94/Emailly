package com.commerce.email_delivery_service.service;

import com.commerce.email_delivery_service.exception.RetryableProcessingException;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class EmailSendService {

    private final JavaMailSender mailSender;

    /**
     * @param attachmentStream nullable - only present if the order had an attachment
     */
    public void sendOrderConfirmation(UUID orderId, UUID customerId, String itemsSummary,
                                      String attachmentFilename, InputStream attachmentStream) {
        try {
            MimeMessage mimeMessage = mailSender.createMimeMessage();

            // multipart=true allows attachments; UTF-8 ensures character encoding safety
            MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, true, "UTF-8");
            helper.setTo(customerId + "@example-customer.test"); // placeholder - real lookup TBD
            helper.setFrom("orders@commerce.test");
            helper.setSubject("Order Confirmation - " + orderId);
            helper.setText("Thanks for your order!\n\nItems: " + itemsSummary, false);

            if (attachmentStream != null && attachmentFilename != null) {
                helper.addAttachment(attachmentFilename, () -> attachmentStream);
            }

            mailSender.send(mimeMessage);
        } catch (MailException | MessagingException e) {
            throw new RetryableProcessingException("Failed to send order confirmation email for order " + orderId, e);
        }
    }
}

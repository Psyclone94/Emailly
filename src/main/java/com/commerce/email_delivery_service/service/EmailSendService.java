package com.commerce.email_delivery_service.service;

import com.commerce.email_delivery_service.exception.RetryableProcessingException;
import jakarta.mail.internet.MimeMessage;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.List;
import java.util.UUID;

@Service
public class EmailSendService {

    private static final String DEFAULT_FROM = "orders@commerce.test";

    private final JavaMailSender mailSender;

    public EmailSendService(JavaMailSender mailSender) {
        this.mailSender = mailSender;
    }

    /**
     * @param attachmentBytes nullable - only present if the order had an attachment. Passed as
     *                        an already-fully-read byte[], NOT a live stream - see the class
     *                        comment history for why (a live InputStream closing before
     *                        JavaMail actually reads it causes a very confusing IOException).
     */
    public void sendOrderConfirmation(UUID orderId, UUID customerId, String itemsSummary,
                                      String attachmentFilename, byte[] attachmentBytes) {
        try {
            MimeMessage mimeMessage = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, true, "UTF-8");

            helper.setTo(customerId + "@example-customer.test"); // placeholder - real lookup TBD
            helper.setFrom(DEFAULT_FROM);
            helper.setSubject("Order Confirmation - " + orderId);
            helper.setText("Thanks for your order!\n\nItems: " + itemsSummary, false);

            if (attachmentBytes != null && attachmentFilename != null) {
                helper.addAttachment(attachmentFilename, new ByteArrayResource(attachmentBytes));
            }

            mailSender.send(mimeMessage);
        } catch (MailException | jakarta.mail.MessagingException e) {
            throw new RetryableProcessingException("Failed to send order confirmation email for order " + orderId, e);
        }
    }

    /**
     * Generic send - used by the ad-hoc /api/v1/emails endpoint. Same byte[]-not-stream
     * principle for attachments as above.
     */
    public void sendCustomEmail(List<String> to, List<String> cc, List<String> bcc, String from,
                                String subject, String body, boolean html,
                                List<EmailAttachmentInput> attachments) {
        try {
            MimeMessage mimeMessage = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, true, "UTF-8");

            helper.setTo(to.toArray(new String[0]));
            if (cc != null && !cc.isEmpty()) {
                helper.setCc(cc.toArray(new String[0]));
            }
            if (bcc != null && !bcc.isEmpty()) {
                helper.setBcc(bcc.toArray(new String[0]));
            }
            helper.setFrom(from != null && !from.isBlank() ? from : DEFAULT_FROM);
            helper.setSubject(subject);
            helper.setText(body, html);

            if (attachments != null) {
                for (EmailAttachmentInput attachment : attachments) {
                    helper.addAttachment(attachment.filename(), new ByteArrayResource(attachment.bytes()));
                }
            }

            mailSender.send(mimeMessage);
        } catch (MailException | jakarta.mail.MessagingException e) {
            throw new RetryableProcessingException("Failed to send custom email: " + subject, e);
        }
    }

    public record EmailAttachmentInput(String filename, byte[] bytes) {
    }
}

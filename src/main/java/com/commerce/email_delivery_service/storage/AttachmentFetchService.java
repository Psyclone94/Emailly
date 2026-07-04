package com.commerce.email_delivery_service.storage;

import com.commerce.email_delivery_service.config.S3Config;
import com.commerce.email_delivery_service.exception.PoisonMessageException;
import com.commerce.email_delivery_service.exception.RetryableProcessingException;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

@Service
public class AttachmentFetchService {

    private final S3Client s3Client;
    private final S3Config.S3Properties s3Properties;

    public AttachmentFetchService(S3Client s3Client, S3Config.S3Properties s3Properties) {
        this.s3Client = s3Client;
        this.s3Properties = s3Properties;
    }

    public ResponseInputStream<GetObjectResponse> fetch(String objectKey) {
        try {
            return s3Client.getObject(GetObjectRequest.builder()
                    .bucket(s3Properties.bucket()) // 👈 Fixed: .getBucket() changed to .bucket()
                    .key(objectKey)
                    .build());
        } catch (NoSuchKeyException e) {
            // the object genuinely doesn't exist - retrying won't change that, this is poison
            throw new PoisonMessageException("Attachment not found in storage: " + objectKey, e);
        } catch (S3Exception e) {
            // anything else (timeout, connection refused, 5xx from MinIO) is worth retrying
            throw new RetryableProcessingException("Failed to fetch attachment " + objectKey, e);
        }
    }
}
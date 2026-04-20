package com.spectralogic.integrations;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobItem;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Random;

public class CloudUtils {
    static final String localstackEndpoint = "http://localhost:4566";
    public static S3Client createLocalStackClient() {
        try {
            return S3Client.builder()
                    .region(Region.US_EAST_1) // Region can be any valid region for LocalStack
                    .credentialsProvider(AnonymousCredentialsProvider.create()) // Anonymous credentials for localstack
                    .endpointOverride(URI.create(localstackEndpoint))
                    .serviceConfiguration(S3Configuration.builder()
                            .pathStyleAccessEnabled(true) // Crucial for LocalStack
                            .build())
                    .httpClientBuilder(ApacheHttpClient.builder())
                    .build();

        } catch (Exception e) {
            System.err.println("Error deleting object: " + e.getMessage());

        }
        return null;
    }

    public static void deleteAllBuckets(S3Client s3Client) {
        // 1. List all buckets
        ListBucketsResponse listBucketsResponse = s3Client.listBuckets();
        List<Bucket> buckets = listBucketsResponse.buckets();

        System.out.println("Found " + buckets.size() + " buckets to delete.");

        for (Bucket bucket : buckets) {
            String bucketName = bucket.name();
            try {
                // 2. S3 buckets must be empty before deletion
                emptyBucket(s3Client, bucketName);

                // 3. Delete the bucket
                DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                        .bucket(bucketName)
                        .build();

                s3Client.deleteBucket(deleteBucketRequest);
                System.out.println("Deleted bucket: " + bucketName);

            } catch (S3Exception e) {
                System.err.println("Failed to delete bucket " + bucketName + ": " + e.awsErrorDetails().errorMessage());
            }
        }
    }

    private static void emptyBucket(S3Client s3Client, String bucketName) {
        // List all objects in the bucket
        ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build();

        ListObjectsV2Response listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest);

        for (S3Object s3Object : listObjectsResponse.contents()) {
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Object.key())
                    .build();
            s3Client.deleteObject(deleteObjectRequest);
        }
    }
    public static void deleteObject(S3Client s3Client, String bucketName) {
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build();

        ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
        List<S3Object> objects = listResponse.contents();

        if (objects.isEmpty()) {
            System.out.println("The bucket is empty. No objects to delete.");
            return;
        }


       Optional<S3Object> lastObject = objects.stream()
                .max(Comparator.comparing(S3Object::lastModified));

        if (lastObject.isPresent()) {
            String key = lastObject.get().key();
            System.out.println("Found last modified object: " + key +
                    " (Modified: " + lastObject.get().lastModified() + ")");


            DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            s3Client.deleteObject(deleteRequest);

            System.out.println("Successfully deleted object: " + key);
        }
    }

    public static String reduceSizeS3Object(S3Client s3Client, String bucketName) {
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build();

        ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
        List<S3Object> objects = listResponse.contents();

        if (objects.isEmpty()) {
            System.out.println("The bucket is empty. No objects to delete.");
            return "";
        }


        Optional<S3Object> lastObject = objects.stream()
                .max(Comparator.comparing(S3Object::lastModified));

        if (lastObject.isPresent()) {
            String key = lastObject.get().key();
            System.out.println("Found last modified object: " + key +
                    " (Modified: " + lastObject.get().lastModified() + ")");


            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentType("text/plain")
                    .build();
            String newContent = "This content has been corrupted.";
            s3Client.putObject(putObjectRequest, RequestBody.fromString(newContent));

            System.out.println("Successfully updated: " + key + " in bucket: " + bucketName);
            return key;
        }
        return "";
    }

    public static String corruptS3ObjectSameSize(S3Client s3Client, String bucketName) {
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build();

        ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
        List<S3Object> objects = listResponse.contents();

        if (objects.isEmpty()) {
            System.out.println("The bucket is empty.");
            return "";
        }

        Optional<S3Object> lastObject = objects.stream()
                .max(Comparator.comparing(S3Object::lastModified));

        if (lastObject.isPresent()) {
            String key = lastObject.get().key();
            try {
                // 1. Get the existing object's metadata to determine its exact size
                HeadObjectRequest headRequest = HeadObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build();

                HeadObjectResponse headResponse = s3Client.headObject(headRequest);
                long originalSize = headResponse.contentLength();

                System.out.println("Original size of " + key + ": " + originalSize + " bytes.");

                // 2. Generate "corrupt" data of the exact same size
                // For small objects, we can use a byte array.
                // For very large objects, use a custom InputStream to avoid OutOfMemory.
                byte[] junkData = new byte[(int) originalSize];
                new Random().nextBytes(junkData);

                // 3. Overwrite the object with the junk data
                PutObjectRequest putRequest = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .contentType(headResponse.contentType())
                        .build();

                s3Client.putObject(putRequest, RequestBody.fromBytes(junkData));

                System.out.println("Successfully corrupted " + key + " while maintaining size: " + originalSize);
                return key;


            } catch (S3Exception e) {
                System.err.println("Error accessing S3: " + e.awsErrorDetails().errorMessage());
                return "";
            }
        }
        return "";
    }

    public static String prependS3ObjectPadding(S3Client s3Client, String bucketName) {
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build();

        ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
        List<S3Object> objects = listResponse.contents();

        if (objects.isEmpty()) {
            System.out.println("The bucket is empty.");
            return "";
        }

        Optional<S3Object> lastObject = objects.stream()
                .max(Comparator.comparing(S3Object::lastModified));

        if (lastObject.isPresent()) {
            String key = lastObject.get().key();

            // 1. Download existing content
            GetObjectRequest getRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(getRequest);
            byte[] originalData = objectBytes.asByteArray();

            // 2. Create the prefix padding (e.g., 1KB of 'A's)
            String prefix = "--- START OF CORRUPTED DATA ---\n" + "A".repeat(1024);
            byte[] prefixBytes = prefix.getBytes();

            // 3. Allocate new array and SWAP order
            byte[] combinedData = new byte[prefixBytes.length + originalData.length];

            // Copy prefix to the START
            System.arraycopy(prefixBytes, 0, combinedData, 0, prefixBytes.length);

            // Copy original data AFTER the prefix
            System.arraycopy(originalData, 0, combinedData, prefixBytes.length, originalData.length);

            // 4. Upload the modified object
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentType(objectBytes.response().contentType())
                    .build();

            s3Client.putObject(putObjectRequest, RequestBody.fromBytes(combinedData));

            System.out.println("Successfully prepended padding to: " + key);
            return key;
        }
        return "";
    }

    public static String increaseS3ObjectSizeEnd(S3Client s3Client, String bucketName) {
        // 1. Find the latest objects
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build();

        ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
        List<S3Object> objects = listResponse.contents();

        if (objects.isEmpty()) {
            System.out.println("The bucket is empty. No objects to modify.");
            return "";
        }

        // 2. Identify the last modified object
        Optional<S3Object> lastObject = objects.stream()
                .max(Comparator.comparing(S3Object::lastModified));

        if (lastObject.isPresent()) {
            String key = lastObject.get().key();
            long originalSize = lastObject.get().size();

            // 3. Download existing content
            GetObjectRequest getRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(getRequest);
            byte[] data = objectBytes.asByteArray();

            // 4. Create "padding" to increase size (e.g., adding 1KB of extra data)
            String padding = "\n--- ADDITIONAL DATA TO INCREASE SIZE ---\n" + "X".repeat(1024);
            byte[] paddingBytes = padding.getBytes();

            byte[] combinedData = new byte[data.length + paddingBytes.length];
            System.arraycopy(data, 0, combinedData, 0, data.length);
            System.arraycopy(paddingBytes, 0, combinedData, data.length, paddingBytes.length);

            // 5. Upload the larger object
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentType(objectBytes.response().contentType())
                    .build();

            s3Client.putObject(putObjectRequest, RequestBody.fromBytes(combinedData));

            System.out.println("Successfully increased size for: " + key);
            System.out.println("Original size: " + originalSize + " bytes. New size: " + combinedData.length + " bytes.");

            return key;
        }
        return "";
    }

    public static BlobServiceClient createAzuriteClient() {
        String connectionString = String.format(
                "DefaultEndpointsProtocol=http;" +
                        "AccountName=devstoreaccount1;" +
                        "AccountKey=Ss0sk4dZsuH0Cji92F1Ye2kuoEhv+mmYCLfLzGrdw0A1zQagbiBBbnHJNiALudX5nXXZkc4lxT0nFREbg8lpAQ==;" +
                        "BlobEndpoint=http://127.0.0.1:%d/devstoreaccount1;",
                10000
        );

        return new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();
    }

    public static void deleteObject(BlobContainerClient containerClient) {
        BlobItem firstBlob = containerClient.listBlobs().stream()
                .findFirst()
                .orElse(null);

        if (firstBlob != null) {
            String blobName = firstBlob.getName();
            containerClient.getBlobClient(blobName).delete();
            System.out.println("Successfully deleted the first blob found: " + blobName);
        } else {
            System.out.println("No blobs found in the container to delete.");
        }

    }

    public static void deleteAllContainers(BlobServiceClient blobServiceClient) {
        System.out.println("Starting cleanup: Deleting all containers...");

        // listBlobContainers returns a PagedIterable of all containers
        for (BlobContainerItem containerItem : blobServiceClient.listBlobContainers()) {
            String containerName = containerItem.getName();
            try {
                blobServiceClient.deleteBlobContainer(containerName);
                System.out.println("Deleted container: " + containerName);
            } catch (Exception e) {
                System.err.println("Failed to delete container " + containerName + ": " + e.getMessage());
            }
        }

        System.out.println("Cleanup finished.");
    }

}

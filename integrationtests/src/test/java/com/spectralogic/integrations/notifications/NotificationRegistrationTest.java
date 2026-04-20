package com.spectralogic.integrations.notifications;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.commands.spectrads3.notifications.*;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.networking.FailedRequestException;
import com.spectralogic.integrations.TestUtils;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static org.junit.jupiter.api.Assertions.*;


public class NotificationRegistrationTest {
    private final static Logger LOG = Logger.getLogger(NotificationRegistrationTest.class);

    private static final String BUCKET_NAME = "notification-registrations";
    private static final String NOTIFICATION_ENDPOINT = "http://localhost:9999/notification";

    private Ds3Client client;

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        client = TestUtils.setTestParams();
        if (client != null) {
            TestUtils.cleanSetUp(client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            cleanupBuckets(client, BUCKET_NAME);
            cleanupAllNotificationRegistrations(client);
            client.close();
        }
    }

    private void cleanupAllNotificationRegistrations(Ds3Client client) throws IOException {
        for (AzureTargetFailureNotificationRegistration r :
                client.getAzureTargetFailureNotificationRegistrationsSpectraS3(
                        new GetAzureTargetFailureNotificationRegistrationsSpectraS3Request())
                        .getAzureTargetFailureNotificationRegistrationListResult()
                        .getAzureTargetFailureNotificationRegistrations()) {
            client.deleteAzureTargetFailureNotificationRegistrationSpectraS3(
                    new DeleteAzureTargetFailureNotificationRegistrationSpectraS3Request(r.getId().toString()));
        }
        for (BucketChangesNotificationRegistration r :
                client.getBucketChangesNotificationRegistrationsSpectraS3(
                        new GetBucketChangesNotificationRegistrationsSpectraS3Request())
                        .getBucketChangesNotificationRegistrationListResult()
                        .getBucketChangesNotificationRegistrations()) {
            client.deleteBucketChangesNotificationRegistrationSpectraS3(
                    new DeleteBucketChangesNotificationRegistrationSpectraS3Request(r.getId().toString()));
        }
        for (Ds3TargetFailureNotificationRegistration r :
                client.getDs3TargetFailureNotificationRegistrationsSpectraS3(
                        new GetDs3TargetFailureNotificationRegistrationsSpectraS3Request())
                        .getDs3TargetFailureNotificationRegistrationListResult()
                        .getDs3TargetFailureNotificationRegistrations()) {
            client.deleteDs3TargetFailureNotificationRegistrationSpectraS3(
                    new DeleteDs3TargetFailureNotificationRegistrationSpectraS3Request(r.getId()));
        }
        for (JobCompletedNotificationRegistration r :
                client.getJobCompletedNotificationRegistrationsSpectraS3(
                        new GetJobCompletedNotificationRegistrationsSpectraS3Request())
                        .getJobCompletedNotificationRegistrationListResult()
                        .getJobCompletedNotificationRegistrations()) {
            client.deleteJobCompletedNotificationRegistrationSpectraS3(
                    new DeleteJobCompletedNotificationRegistrationSpectraS3Request(r.getId()));
        }
        for (JobCreatedNotificationRegistration r :
                client.getJobCreatedNotificationRegistrationsSpectraS3(
                        new GetJobCreatedNotificationRegistrationsSpectraS3Request())
                        .getJobCreatedNotificationRegistrationListResult()
                        .getJobCreatedNotificationRegistrations()) {
            client.deleteJobCreatedNotificationRegistrationSpectraS3(
                    new DeleteJobCreatedNotificationRegistrationSpectraS3Request(r.getId()));
        }
        for (JobCreationFailedNotificationRegistration r :
                client.getJobCreationFailedNotificationRegistrationsSpectraS3(
                        new GetJobCreationFailedNotificationRegistrationsSpectraS3Request())
                        .getJobCreationFailedNotificationRegistrationListResult()
                        .getJobCreationFailedNotificationRegistrations()) {
            client.deleteJobCreationFailedNotificationRegistrationSpectraS3(
                    new DeleteJobCreationFailedNotificationRegistrationSpectraS3Request(r.getId()));
        }
        for (S3ObjectCachedNotificationRegistration r :
                client.getObjectCachedNotificationRegistrationsSpectraS3(
                        new GetObjectCachedNotificationRegistrationsSpectraS3Request())
                        .getS3ObjectCachedNotificationRegistrationListResult()
                        .getS3ObjectCachedNotificationRegistrations()) {
            client.deleteObjectCachedNotificationRegistrationSpectraS3(
                    new DeleteObjectCachedNotificationRegistrationSpectraS3Request(r.getId()));
        }
        for (S3ObjectLostNotificationRegistration r :
                client.getObjectLostNotificationRegistrationsSpectraS3(
                        new GetObjectLostNotificationRegistrationsSpectraS3Request())
                        .getS3ObjectLostNotificationRegistrationListResult()
                        .getS3ObjectLostNotificationRegistrations()) {
            client.deleteObjectLostNotificationRegistrationSpectraS3(
                    new DeleteObjectLostNotificationRegistrationSpectraS3Request(r.getId()));
        }
        for (S3ObjectPersistedNotificationRegistration r :
                client.getObjectPersistedNotificationRegistrationsSpectraS3(
                        new GetObjectPersistedNotificationRegistrationsSpectraS3Request())
                        .getS3ObjectPersistedNotificationRegistrationListResult()
                        .getS3ObjectPersistedNotificationRegistrations()) {
            client.deleteObjectPersistedNotificationRegistrationSpectraS3(
                    new DeleteObjectPersistedNotificationRegistrationSpectraS3Request(r.getId()));
        }
        for (PoolFailureNotificationRegistration r :
                client.getPoolFailureNotificationRegistrationsSpectraS3(
                        new GetPoolFailureNotificationRegistrationsSpectraS3Request())
                        .getPoolFailureNotificationRegistrationListResult()
                        .getPoolFailureNotificationRegistrations()) {
            client.deletePoolFailureNotificationRegistrationSpectraS3(
                    new DeletePoolFailureNotificationRegistrationSpectraS3Request(r.getId()));
        }
        for (S3TargetFailureNotificationRegistration r :
                client.getS3TargetFailureNotificationRegistrationsSpectraS3(
                        new GetS3TargetFailureNotificationRegistrationsSpectraS3Request())
                        .getS3TargetFailureNotificationRegistrationListResult()
                        .getS3TargetFailureNotificationRegistrations()) {
            client.deleteS3TargetFailureNotificationRegistrationSpectraS3(
                    new DeleteS3TargetFailureNotificationRegistrationSpectraS3Request(r.getId().toString()));
        }
        for (StorageDomainFailureNotificationRegistration r :
                client.getStorageDomainFailureNotificationRegistrationsSpectraS3(
                        new GetStorageDomainFailureNotificationRegistrationsSpectraS3Request())
                        .getStorageDomainFailureNotificationRegistrationListResult()
                        .getStorageDomainFailureNotificationRegistrations()) {
            client.deleteStorageDomainFailureNotificationRegistrationSpectraS3(
                    new DeleteStorageDomainFailureNotificationRegistrationSpectraS3Request(r.getId()));
        }
        for (SystemFailureNotificationRegistration r :
                client.getSystemFailureNotificationRegistrationsSpectraS3(
                        new GetSystemFailureNotificationRegistrationsSpectraS3Request())
                        .getSystemFailureNotificationRegistrationListResult()
                        .getSystemFailureNotificationRegistrations()) {
            client.deleteSystemFailureNotificationRegistrationSpectraS3(
                    new DeleteSystemFailureNotificationRegistrationSpectraS3Request(r.getId()));
        }
        for (TapeFailureNotificationRegistration r :
                client.getTapeFailureNotificationRegistrationsSpectraS3(
                        new GetTapeFailureNotificationRegistrationsSpectraS3Request())
                        .getTapeFailureNotificationRegistrationListResult()
                        .getTapeFailureNotificationRegistrations()) {
            client.deleteTapeFailureNotificationRegistrationSpectraS3(
                    new DeleteTapeFailureNotificationRegistrationSpectraS3Request(r.getId()));
        }
        for (TapePartitionFailureNotificationRegistration r :
                client.getTapePartitionFailureNotificationRegistrationsSpectraS3(
                        new GetTapePartitionFailureNotificationRegistrationsSpectraS3Request())
                        .getTapePartitionFailureNotificationRegistrationListResult()
                        .getTapePartitionFailureNotificationRegistrations()) {
            client.deleteTapePartitionFailureNotificationRegistrationSpectraS3(
                    new DeleteTapePartitionFailureNotificationRegistrationSpectraS3Request(r.getId()));
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testCreateAzureTargetFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testCreateAzureTargetFailureNotificationRegistration");

        final PutAzureTargetFailureNotificationRegistrationSpectraS3Response response =
                client.putAzureTargetFailureNotificationRegistrationSpectraS3(
                        new PutAzureTargetFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        assertNotNull(response.getAzureTargetFailureNotificationRegistrationResult().getId(),
                "Registration should return a valid ID");

        final int count = client.getAzureTargetFailureNotificationRegistrationsSpectraS3(
                new GetAzureTargetFailureNotificationRegistrationsSpectraS3Request())
                .getAzureTargetFailureNotificationRegistrationListResult()
                .getAzureTargetFailureNotificationRegistrations().size();
        assertEquals(1, count, "There should be exactly 1 azure target failure notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testCreateBucketChangesNotificationRegistration() throws IOException {
        LOG.info("Starting test: testCreateBucketChangesNotificationRegistration");

        final PutBucketChangesNotificationRegistrationSpectraS3Response response =
                client.putBucketChangesNotificationRegistrationSpectraS3(
                        new PutBucketChangesNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        assertNotNull(response.getBucketChangesNotificationRegistrationResult().getId(),
                "Registration should return a valid ID");

        final int count = client.getBucketChangesNotificationRegistrationsSpectraS3(
                new GetBucketChangesNotificationRegistrationsSpectraS3Request())
                .getBucketChangesNotificationRegistrationListResult()
                .getBucketChangesNotificationRegistrations().size();
        assertEquals(1, count, "There should be exactly 1 bucket changes notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testCreateDs3TargetFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testCreateDs3TargetFailureNotificationRegistration");

        final PutDs3TargetFailureNotificationRegistrationSpectraS3Response response =
                client.putDs3TargetFailureNotificationRegistrationSpectraS3(
                        new PutDs3TargetFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        assertNotNull(response.getDs3TargetFailureNotificationRegistrationResult().getId(),
                "Registration should return a valid ID");

        final int count = client.getDs3TargetFailureNotificationRegistrationsSpectraS3(
                new GetDs3TargetFailureNotificationRegistrationsSpectraS3Request())
                .getDs3TargetFailureNotificationRegistrationListResult()
                .getDs3TargetFailureNotificationRegistrations().size();
        assertEquals(1, count, "There should be exactly 1 ds3 target failure notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testCreateJobCompletedNotificationRegistration() throws IOException {
        LOG.info("Starting test: testCreateJobCompletedNotificationRegistration");

        final PutJobCompletedNotificationRegistrationSpectraS3Response response =
                client.putJobCompletedNotificationRegistrationSpectraS3(
                        new PutJobCompletedNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        assertNotNull(response.getJobCompletedNotificationRegistrationResult().getId(),
                "Registration should return a valid ID");

        final int count = client.getJobCompletedNotificationRegistrationsSpectraS3(
                new GetJobCompletedNotificationRegistrationsSpectraS3Request())
                .getJobCompletedNotificationRegistrationListResult()
                .getJobCompletedNotificationRegistrations().size();
        assertEquals(1, count, "There should be exactly 1 job completed notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testCreateJobCreatedNotificationRegistration() throws IOException {
        LOG.info("Starting test: testCreateJobCreatedNotificationRegistration");

        final PutJobCreatedNotificationRegistrationSpectraS3Response response =
                client.putJobCreatedNotificationRegistrationSpectraS3(
                        new PutJobCreatedNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        assertNotNull(response.getJobCreatedNotificationRegistrationResult().getId(),
                "Registration should return a valid ID");

        final int count = client.getJobCreatedNotificationRegistrationsSpectraS3(
                new GetJobCreatedNotificationRegistrationsSpectraS3Request())
                .getJobCreatedNotificationRegistrationListResult()
                .getJobCreatedNotificationRegistrations().size();
        assertEquals(1, count, "There should be exactly 1 job created notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testCreateJobCreationFailedNotificationRegistration() throws IOException {
        LOG.info("Starting test: testCreateJobCreationFailedNotificationRegistration");

        final PutJobCreationFailedNotificationRegistrationSpectraS3Response response =
                client.putJobCreationFailedNotificationRegistrationSpectraS3(
                        new PutJobCreationFailedNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        assertNotNull(response.getJobCreationFailedNotificationRegistrationResult().getId(),
                "Registration should return a valid ID");

        final int count = client.getJobCreationFailedNotificationRegistrationsSpectraS3(
                new GetJobCreationFailedNotificationRegistrationsSpectraS3Request())
                .getJobCreationFailedNotificationRegistrationListResult()
                .getJobCreationFailedNotificationRegistrations().size();
        assertEquals(1, count, "There should be exactly 1 job creation failed notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testCreateObjectCachedNotificationRegistration() throws IOException {
        LOG.info("Starting test: testCreateObjectCachedNotificationRegistration");

        final PutObjectCachedNotificationRegistrationSpectraS3Response response =
                client.putObjectCachedNotificationRegistrationSpectraS3(
                        new PutObjectCachedNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        assertNotNull(response.getS3ObjectCachedNotificationRegistrationResult().getId(),
                "Registration should return a valid ID");

        final int count = client.getObjectCachedNotificationRegistrationsSpectraS3(
                new GetObjectCachedNotificationRegistrationsSpectraS3Request())
                .getS3ObjectCachedNotificationRegistrationListResult()
                .getS3ObjectCachedNotificationRegistrations().size();
        assertEquals(1, count, "There should be exactly 1 object cached notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testCreateObjectLostNotificationRegistration() throws IOException {
        LOG.info("Starting test: testCreateObjectLostNotificationRegistration");

        final PutObjectLostNotificationRegistrationSpectraS3Response response =
                client.putObjectLostNotificationRegistrationSpectraS3(
                        new PutObjectLostNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        assertNotNull(response.getS3ObjectLostNotificationRegistrationResult().getId(),
                "Registration should return a valid ID");

        final int count = client.getObjectLostNotificationRegistrationsSpectraS3(
                new GetObjectLostNotificationRegistrationsSpectraS3Request())
                .getS3ObjectLostNotificationRegistrationListResult()
                .getS3ObjectLostNotificationRegistrations().size();
        assertEquals(1, count, "There should be exactly 1 object lost notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testCreateObjectPersistedNotificationRegistration() throws IOException {
        LOG.info("Starting test: testCreateObjectPersistedNotificationRegistration");

        final PutObjectPersistedNotificationRegistrationSpectraS3Response response =
                client.putObjectPersistedNotificationRegistrationSpectraS3(
                        new PutObjectPersistedNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        assertNotNull(response.getS3ObjectPersistedNotificationRegistrationResult().getId(),
                "Registration should return a valid ID");

        final int count = client.getObjectPersistedNotificationRegistrationsSpectraS3(
                new GetObjectPersistedNotificationRegistrationsSpectraS3Request())
                .getS3ObjectPersistedNotificationRegistrationListResult()
                .getS3ObjectPersistedNotificationRegistrations().size();
        assertEquals(1, count, "There should be exactly 1 object persisted notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testCreatePoolFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testCreatePoolFailureNotificationRegistration");

        final PutPoolFailureNotificationRegistrationSpectraS3Response response =
                client.putPoolFailureNotificationRegistrationSpectraS3(
                        new PutPoolFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        assertNotNull(response.getPoolFailureNotificationRegistrationResult().getId(),
                "Registration should return a valid ID");

        final int count = client.getPoolFailureNotificationRegistrationsSpectraS3(
                new GetPoolFailureNotificationRegistrationsSpectraS3Request())
                .getPoolFailureNotificationRegistrationListResult()
                .getPoolFailureNotificationRegistrations().size();
        assertEquals(1, count, "There should be exactly 1 pool failure notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testCreateS3TargetFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testCreateS3TargetFailureNotificationRegistration");

        final PutS3TargetFailureNotificationRegistrationSpectraS3Response response =
                client.putS3TargetFailureNotificationRegistrationSpectraS3(
                        new PutS3TargetFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        assertNotNull(response.getS3TargetFailureNotificationRegistrationResult().getId(),
                "Registration should return a valid ID");

        final int count = client.getS3TargetFailureNotificationRegistrationsSpectraS3(
                new GetS3TargetFailureNotificationRegistrationsSpectraS3Request())
                .getS3TargetFailureNotificationRegistrationListResult()
                .getS3TargetFailureNotificationRegistrations().size();
        assertEquals(1, count, "There should be exactly 1 s3 target failure notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testCreateStorageDomainFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testCreateStorageDomainFailureNotificationRegistration");

        final PutStorageDomainFailureNotificationRegistrationSpectraS3Response response =
                client.putStorageDomainFailureNotificationRegistrationSpectraS3(
                        new PutStorageDomainFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        assertNotNull(response.getStorageDomainFailureNotificationRegistrationResult().getId(),
                "Registration should return a valid ID");

        final int count = client.getStorageDomainFailureNotificationRegistrationsSpectraS3(
                new GetStorageDomainFailureNotificationRegistrationsSpectraS3Request())
                .getStorageDomainFailureNotificationRegistrationListResult()
                .getStorageDomainFailureNotificationRegistrations().size();
        assertEquals(1, count, "There should be exactly 1 storage domain failure notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testCreateSystemFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testCreateSystemFailureNotificationRegistration");

        final PutSystemFailureNotificationRegistrationSpectraS3Response response =
                client.putSystemFailureNotificationRegistrationSpectraS3(
                        new PutSystemFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        assertNotNull(response.getSystemFailureNotificationRegistrationResult().getId(),
                "Registration should return a valid ID");

        final int count = client.getSystemFailureNotificationRegistrationsSpectraS3(
                new GetSystemFailureNotificationRegistrationsSpectraS3Request())
                .getSystemFailureNotificationRegistrationListResult()
                .getSystemFailureNotificationRegistrations().size();
        assertEquals(1, count, "There should be exactly 1 system failure notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testCreateTapeFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testCreateTapeFailureNotificationRegistration");

        final PutTapeFailureNotificationRegistrationSpectraS3Response response =
                client.putTapeFailureNotificationRegistrationSpectraS3(
                        new PutTapeFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        assertNotNull(response.getTapeFailureNotificationRegistrationResult().getId(),
                "Registration should return a valid ID");

        final int count = client.getTapeFailureNotificationRegistrationsSpectraS3(
                new GetTapeFailureNotificationRegistrationsSpectraS3Request())
                .getTapeFailureNotificationRegistrationListResult()
                .getTapeFailureNotificationRegistrations().size();
        assertEquals(1, count, "There should be exactly 1 tape failure notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testCreateTapePartitionFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testCreateTapePartitionFailureNotificationRegistration");

        final PutTapePartitionFailureNotificationRegistrationSpectraS3Response response =
                client.putTapePartitionFailureNotificationRegistrationSpectraS3(
                        new PutTapePartitionFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        assertNotNull(response.getTapePartitionFailureNotificationRegistrationResult().getId(),
                "Registration should return a valid ID");

        final int count = client.getTapePartitionFailureNotificationRegistrationsSpectraS3(
                new GetTapePartitionFailureNotificationRegistrationsSpectraS3Request())
                .getTapePartitionFailureNotificationRegistrationListResult()
                .getTapePartitionFailureNotificationRegistrations().size();
        assertEquals(1, count, "There should be exactly 1 tape partition failure notification registration");
    }

    // --- Delete notification registration tests ---

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testDeleteJobCompletedNotificationRegistration() throws IOException {
        LOG.info("Starting test: testDeleteJobCompletedNotificationRegistration");

        final UUID regId = client.putJobCompletedNotificationRegistrationSpectraS3(
                new PutJobCompletedNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getJobCompletedNotificationRegistrationResult().getId();

        client.deleteJobCompletedNotificationRegistrationSpectraS3(
                new DeleteJobCompletedNotificationRegistrationSpectraS3Request(regId));

        final int count = client.getJobCompletedNotificationRegistrationsSpectraS3(
                new GetJobCompletedNotificationRegistrationsSpectraS3Request())
                .getJobCompletedNotificationRegistrationListResult()
                .getJobCompletedNotificationRegistrations().size();
        assertEquals(0, count, "There should be 0 job completed notification registrations after deletion");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testDeleteJobCreatedNotificationRegistration() throws IOException {
        LOG.info("Starting test: testDeleteJobCreatedNotificationRegistration");

        final UUID regId = client.putJobCreatedNotificationRegistrationSpectraS3(
                new PutJobCreatedNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getJobCreatedNotificationRegistrationResult().getId();

        client.deleteJobCreatedNotificationRegistrationSpectraS3(
                new DeleteJobCreatedNotificationRegistrationSpectraS3Request(regId));

        final int count = client.getJobCreatedNotificationRegistrationsSpectraS3(
                new GetJobCreatedNotificationRegistrationsSpectraS3Request())
                .getJobCreatedNotificationRegistrationListResult()
                .getJobCreatedNotificationRegistrations().size();
        assertEquals(0, count, "There should be 0 job created notification registrations after deletion");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testDeleteObjectCachedNotificationRegistration() throws IOException {
        LOG.info("Starting test: testDeleteObjectCachedNotificationRegistration");

        final UUID regId = client.putObjectCachedNotificationRegistrationSpectraS3(
                new PutObjectCachedNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getS3ObjectCachedNotificationRegistrationResult().getId();

        client.deleteObjectCachedNotificationRegistrationSpectraS3(
                new DeleteObjectCachedNotificationRegistrationSpectraS3Request(regId));

        final int count = client.getObjectCachedNotificationRegistrationsSpectraS3(
                new GetObjectCachedNotificationRegistrationsSpectraS3Request())
                .getS3ObjectCachedNotificationRegistrationListResult()
                .getS3ObjectCachedNotificationRegistrations().size();
        assertEquals(0, count, "There should be 0 object cached notification registrations after deletion");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testDeleteObjectLostNotificationRegistration() throws IOException {
        LOG.info("Starting test: testDeleteObjectLostNotificationRegistration");

        final UUID regId = client.putObjectLostNotificationRegistrationSpectraS3(
                new PutObjectLostNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getS3ObjectLostNotificationRegistrationResult().getId();

        client.deleteObjectLostNotificationRegistrationSpectraS3(
                new DeleteObjectLostNotificationRegistrationSpectraS3Request(regId));

        final int count = client.getObjectLostNotificationRegistrationsSpectraS3(
                new GetObjectLostNotificationRegistrationsSpectraS3Request())
                .getS3ObjectLostNotificationRegistrationListResult()
                .getS3ObjectLostNotificationRegistrations().size();
        assertEquals(0, count, "There should be 0 object lost notification registrations after deletion");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testDeleteObjectPersistedNotificationRegistration() throws IOException {
        LOG.info("Starting test: testDeleteObjectPersistedNotificationRegistration");

        final UUID regId = client.putObjectPersistedNotificationRegistrationSpectraS3(
                new PutObjectPersistedNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getS3ObjectPersistedNotificationRegistrationResult().getId();

        client.deleteObjectPersistedNotificationRegistrationSpectraS3(
                new DeleteObjectPersistedNotificationRegistrationSpectraS3Request(regId));

        final int count = client.getObjectPersistedNotificationRegistrationsSpectraS3(
                new GetObjectPersistedNotificationRegistrationsSpectraS3Request())
                .getS3ObjectPersistedNotificationRegistrationListResult()
                .getS3ObjectPersistedNotificationRegistrations().size();
        assertEquals(0, count, "There should be 0 object persisted notification registrations after deletion");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testDeleteStorageDomainFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testDeleteStorageDomainFailureNotificationRegistration");

        final UUID regId = client.putStorageDomainFailureNotificationRegistrationSpectraS3(
                new PutStorageDomainFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getStorageDomainFailureNotificationRegistrationResult().getId();

        client.deleteStorageDomainFailureNotificationRegistrationSpectraS3(
                new DeleteStorageDomainFailureNotificationRegistrationSpectraS3Request(regId));

        final int count = client.getStorageDomainFailureNotificationRegistrationsSpectraS3(
                new GetStorageDomainFailureNotificationRegistrationsSpectraS3Request())
                .getStorageDomainFailureNotificationRegistrationListResult()
                .getStorageDomainFailureNotificationRegistrations().size();
        assertEquals(0, count, "There should be 0 storage domain failure notification registrations after deletion");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testDeleteTapeFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testDeleteTapeFailureNotificationRegistration");

        final UUID regId = client.putTapeFailureNotificationRegistrationSpectraS3(
                new PutTapeFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getTapeFailureNotificationRegistrationResult().getId();

        client.deleteTapeFailureNotificationRegistrationSpectraS3(
                new DeleteTapeFailureNotificationRegistrationSpectraS3Request(regId));

        final int count = client.getTapeFailureNotificationRegistrationsSpectraS3(
                new GetTapeFailureNotificationRegistrationsSpectraS3Request())
                .getTapeFailureNotificationRegistrationListResult()
                .getTapeFailureNotificationRegistrations().size();
        assertEquals(0, count, "There should be 0 tape failure notification registrations after deletion");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testDeleteTapePartitionFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testDeleteTapePartitionFailureNotificationRegistration");

        final UUID regId = client.putTapePartitionFailureNotificationRegistrationSpectraS3(
                new PutTapePartitionFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getTapePartitionFailureNotificationRegistrationResult().getId();

        client.deleteTapePartitionFailureNotificationRegistrationSpectraS3(
                new DeleteTapePartitionFailureNotificationRegistrationSpectraS3Request(regId));

        final int count = client.getTapePartitionFailureNotificationRegistrationsSpectraS3(
                new GetTapePartitionFailureNotificationRegistrationsSpectraS3Request())
                .getTapePartitionFailureNotificationRegistrationListResult()
                .getTapePartitionFailureNotificationRegistrations().size();
        assertEquals(0, count, "There should be 0 tape partition failure notification registrations after deletion");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testDeletePoolFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testDeletePoolFailureNotificationRegistration");

        final UUID regId = client.putPoolFailureNotificationRegistrationSpectraS3(
                new PutPoolFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getPoolFailureNotificationRegistrationResult().getId();

        client.deletePoolFailureNotificationRegistrationSpectraS3(
                new DeletePoolFailureNotificationRegistrationSpectraS3Request(regId));

        final int count = client.getPoolFailureNotificationRegistrationsSpectraS3(
                new GetPoolFailureNotificationRegistrationsSpectraS3Request())
                .getPoolFailureNotificationRegistrationListResult()
                .getPoolFailureNotificationRegistrations().size();
        assertEquals(0, count, "There should be 0 pool failure notification registrations after deletion");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testDeleteSystemFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testDeleteSystemFailureNotificationRegistration");

        final UUID regId = client.putSystemFailureNotificationRegistrationSpectraS3(
                new PutSystemFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getSystemFailureNotificationRegistrationResult().getId();

        client.deleteSystemFailureNotificationRegistrationSpectraS3(
                new DeleteSystemFailureNotificationRegistrationSpectraS3Request(regId));

        final int count = client.getSystemFailureNotificationRegistrationsSpectraS3(
                new GetSystemFailureNotificationRegistrationsSpectraS3Request())
                .getSystemFailureNotificationRegistrationListResult()
                .getSystemFailureNotificationRegistrations().size();
        assertEquals(0, count, "There should be 0 system failure notification registrations after deletion");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testDeleteDs3TargetFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testDeleteDs3TargetFailureNotificationRegistration");

        final UUID regId = client.putDs3TargetFailureNotificationRegistrationSpectraS3(
                new PutDs3TargetFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getDs3TargetFailureNotificationRegistrationResult().getId();

        client.deleteDs3TargetFailureNotificationRegistrationSpectraS3(
                new DeleteDs3TargetFailureNotificationRegistrationSpectraS3Request(regId));

        final int count = client.getDs3TargetFailureNotificationRegistrationsSpectraS3(
                new GetDs3TargetFailureNotificationRegistrationsSpectraS3Request())
                .getDs3TargetFailureNotificationRegistrationListResult()
                .getDs3TargetFailureNotificationRegistrations().size();
        assertEquals(0, count, "There should be 0 ds3 target failure notification registrations after deletion");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testDeleteS3TargetFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testDeleteS3TargetFailureNotificationRegistration");

        final UUID regId = client.putS3TargetFailureNotificationRegistrationSpectraS3(
                new PutS3TargetFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getS3TargetFailureNotificationRegistrationResult().getId();

        client.deleteS3TargetFailureNotificationRegistrationSpectraS3(
                new DeleteS3TargetFailureNotificationRegistrationSpectraS3Request(regId.toString()));

        final int count = client.getS3TargetFailureNotificationRegistrationsSpectraS3(
                new GetS3TargetFailureNotificationRegistrationsSpectraS3Request())
                .getS3TargetFailureNotificationRegistrationListResult()
                .getS3TargetFailureNotificationRegistrations().size();
        assertEquals(0, count, "There should be 0 s3 target failure notification registrations after deletion");
    }

    // --- Fail to delete non-existent notification registration tests ---

    private static final UUID NON_EXISTENT_ID = UUID.randomUUID();

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testFailToDeleteNonExistentJobCompletedNotificationRegistration() {
        LOG.info("Starting test: testFailToDeleteNonExistentJobCompletedNotificationRegistration");
        try {
            client.deleteJobCompletedNotificationRegistrationSpectraS3(
                    new DeleteJobCompletedNotificationRegistrationSpectraS3Request(NON_EXISTENT_ID));
            fail("Expected FailedRequestException with 404");
        } catch (final FailedRequestException e) {
            assertEquals(404, e.getStatusCode(), "Deleting non-existent registration should return 404");
        } catch (final IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testFailToDeleteNonExistentJobCreatedNotificationRegistration() {
        LOG.info("Starting test: testFailToDeleteNonExistentJobCreatedNotificationRegistration");
        try {
            client.deleteJobCreatedNotificationRegistrationSpectraS3(
                    new DeleteJobCreatedNotificationRegistrationSpectraS3Request(NON_EXISTENT_ID));
            fail("Expected FailedRequestException with 404");
        } catch (final FailedRequestException e) {
            assertEquals(404, e.getStatusCode(), "Deleting non-existent registration should return 404");
        } catch (final IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testFailToDeleteNonExistentObjectCachedNotificationRegistration() {
        LOG.info("Starting test: testFailToDeleteNonExistentObjectCachedNotificationRegistration");
        try {
            client.deleteObjectCachedNotificationRegistrationSpectraS3(
                    new DeleteObjectCachedNotificationRegistrationSpectraS3Request(NON_EXISTENT_ID));
            fail("Expected FailedRequestException with 404");
        } catch (final FailedRequestException e) {
            assertEquals(404, e.getStatusCode(), "Deleting non-existent registration should return 404");
        } catch (final IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testFailToDeleteNonExistentObjectLostNotificationRegistration() {
        LOG.info("Starting test: testFailToDeleteNonExistentObjectLostNotificationRegistration");
        try {
            client.deleteObjectLostNotificationRegistrationSpectraS3(
                    new DeleteObjectLostNotificationRegistrationSpectraS3Request(NON_EXISTENT_ID));
            fail("Expected FailedRequestException with 404");
        } catch (final FailedRequestException e) {
            assertEquals(404, e.getStatusCode(), "Deleting non-existent registration should return 404");
        } catch (final IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testFailToDeleteNonExistentObjectPersistedNotificationRegistration() {
        LOG.info("Starting test: testFailToDeleteNonExistentObjectPersistedNotificationRegistration");
        try {
            client.deleteObjectPersistedNotificationRegistrationSpectraS3(
                    new DeleteObjectPersistedNotificationRegistrationSpectraS3Request(NON_EXISTENT_ID));
            fail("Expected FailedRequestException with 404");
        } catch (final FailedRequestException e) {
            assertEquals(404, e.getStatusCode(), "Deleting non-existent registration should return 404");
        } catch (final IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testFailToDeleteNonExistentStorageDomainFailureNotificationRegistration() {
        LOG.info("Starting test: testFailToDeleteNonExistentStorageDomainFailureNotificationRegistration");
        try {
            client.deleteStorageDomainFailureNotificationRegistrationSpectraS3(
                    new DeleteStorageDomainFailureNotificationRegistrationSpectraS3Request(NON_EXISTENT_ID));
            fail("Expected FailedRequestException with 404");
        } catch (final FailedRequestException e) {
            assertEquals(404, e.getStatusCode(), "Deleting non-existent registration should return 404");
        } catch (final IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testFailToDeleteNonExistentTapeFailureNotificationRegistration() {
        LOG.info("Starting test: testFailToDeleteNonExistentTapeFailureNotificationRegistration");
        try {
            client.deleteTapeFailureNotificationRegistrationSpectraS3(
                    new DeleteTapeFailureNotificationRegistrationSpectraS3Request(NON_EXISTENT_ID));
            fail("Expected FailedRequestException with 404");
        } catch (final FailedRequestException e) {
            assertEquals(404, e.getStatusCode(), "Deleting non-existent registration should return 404");
        } catch (final IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testFailToDeleteNonExistentTapePartitionFailureNotificationRegistration() {
        LOG.info("Starting test: testFailToDeleteNonExistentTapePartitionFailureNotificationRegistration");
        try {
            client.deleteTapePartitionFailureNotificationRegistrationSpectraS3(
                    new DeleteTapePartitionFailureNotificationRegistrationSpectraS3Request(NON_EXISTENT_ID));
            fail("Expected FailedRequestException with 404");
        } catch (final FailedRequestException e) {
            assertEquals(404, e.getStatusCode(), "Deleting non-existent registration should return 404");
        } catch (final IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testFailToDeleteNonExistentPoolFailureNotificationRegistration() {
        LOG.info("Starting test: testFailToDeleteNonExistentPoolFailureNotificationRegistration");
        try {
            client.deletePoolFailureNotificationRegistrationSpectraS3(
                    new DeletePoolFailureNotificationRegistrationSpectraS3Request(NON_EXISTENT_ID));
            fail("Expected FailedRequestException with 404");
        } catch (final FailedRequestException e) {
            assertEquals(404, e.getStatusCode(), "Deleting non-existent registration should return 404");
        } catch (final IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testFailToDeleteNonExistentSystemFailureNotificationRegistration() {
        LOG.info("Starting test: testFailToDeleteNonExistentSystemFailureNotificationRegistration");
        try {
            client.deleteSystemFailureNotificationRegistrationSpectraS3(
                    new DeleteSystemFailureNotificationRegistrationSpectraS3Request(NON_EXISTENT_ID));
            fail("Expected FailedRequestException with 404");
        } catch (final FailedRequestException e) {
            assertEquals(404, e.getStatusCode(), "Deleting non-existent registration should return 404");
        } catch (final IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testFailToDeleteNonExistentDs3TargetFailureNotificationRegistration() {
        LOG.info("Starting test: testFailToDeleteNonExistentDs3TargetFailureNotificationRegistration");
        try {
            client.deleteDs3TargetFailureNotificationRegistrationSpectraS3(
                    new DeleteDs3TargetFailureNotificationRegistrationSpectraS3Request(NON_EXISTENT_ID));
            fail("Expected FailedRequestException with 404");
        } catch (final FailedRequestException e) {
            assertEquals(404, e.getStatusCode(), "Deleting non-existent registration should return 404");
        } catch (final IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testFailToDeleteNonExistentS3TargetFailureNotificationRegistration() {
        LOG.info("Starting test: testFailToDeleteNonExistentS3TargetFailureNotificationRegistration");
        try {
            client.deleteS3TargetFailureNotificationRegistrationSpectraS3(
                    new DeleteS3TargetFailureNotificationRegistrationSpectraS3Request(NON_EXISTENT_ID.toString()));
            fail("Expected FailedRequestException with 404");
        } catch (final FailedRequestException e) {
            assertEquals(404, e.getStatusCode(), "Deleting non-existent registration should return 404");
        } catch (final IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }
}

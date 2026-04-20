package com.spectralogic.integrations.notifications;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.commands.spectrads3.notifications.*;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.integrations.TestUtils;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static org.junit.jupiter.api.Assertions.*;


public class GetNotificationRegistrationTest {
    private final static Logger LOG = Logger.getLogger(GetNotificationRegistrationTest.class);

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
        for (StorageDomainFailureNotificationRegistration r :
                client.getStorageDomainFailureNotificationRegistrationsSpectraS3(
                        new GetStorageDomainFailureNotificationRegistrationsSpectraS3Request())
                        .getStorageDomainFailureNotificationRegistrationListResult()
                        .getStorageDomainFailureNotificationRegistrations()) {
            client.deleteStorageDomainFailureNotificationRegistrationSpectraS3(
                    new DeleteStorageDomainFailureNotificationRegistrationSpectraS3Request(r.getId()));
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
        for (PoolFailureNotificationRegistration r :
                client.getPoolFailureNotificationRegistrationsSpectraS3(
                        new GetPoolFailureNotificationRegistrationsSpectraS3Request())
                        .getPoolFailureNotificationRegistrationListResult()
                        .getPoolFailureNotificationRegistrations()) {
            client.deletePoolFailureNotificationRegistrationSpectraS3(
                    new DeletePoolFailureNotificationRegistrationSpectraS3Request(r.getId()));
        }
        for (SystemFailureNotificationRegistration r :
                client.getSystemFailureNotificationRegistrationsSpectraS3(
                        new GetSystemFailureNotificationRegistrationsSpectraS3Request())
                        .getSystemFailureNotificationRegistrationListResult()
                        .getSystemFailureNotificationRegistrations()) {
            client.deleteSystemFailureNotificationRegistrationSpectraS3(
                    new DeleteSystemFailureNotificationRegistrationSpectraS3Request(r.getId()));
        }
        for (S3TargetFailureNotificationRegistration r :
                client.getS3TargetFailureNotificationRegistrationsSpectraS3(
                        new GetS3TargetFailureNotificationRegistrationsSpectraS3Request())
                        .getS3TargetFailureNotificationRegistrationListResult()
                        .getS3TargetFailureNotificationRegistrations()) {
            client.deleteS3TargetFailureNotificationRegistrationSpectraS3(
                    new DeleteS3TargetFailureNotificationRegistrationSpectraS3Request(r.getId().toString()));
        }
        for (Ds3TargetFailureNotificationRegistration r :
                client.getDs3TargetFailureNotificationRegistrationsSpectraS3(
                        new GetDs3TargetFailureNotificationRegistrationsSpectraS3Request())
                        .getDs3TargetFailureNotificationRegistrationListResult()
                        .getDs3TargetFailureNotificationRegistrations()) {
            client.deleteDs3TargetFailureNotificationRegistrationSpectraS3(
                    new DeleteDs3TargetFailureNotificationRegistrationSpectraS3Request(r.getId()));
        }
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
        for (JobCreationFailedNotificationRegistration r :
                client.getJobCreationFailedNotificationRegistrationsSpectraS3(
                        new GetJobCreationFailedNotificationRegistrationsSpectraS3Request())
                        .getJobCreationFailedNotificationRegistrationListResult()
                        .getJobCreationFailedNotificationRegistrations()) {
            client.deleteJobCreationFailedNotificationRegistrationSpectraS3(
                    new DeleteJobCreationFailedNotificationRegistrationSpectraS3Request(r.getId()));
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetJobCompletedNotificationRegistration() throws IOException {
        LOG.info("Starting test: testGetJobCompletedNotificationRegistration");

        final UUID regId = client.putJobCompletedNotificationRegistrationSpectraS3(
                new PutJobCompletedNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getJobCompletedNotificationRegistrationResult().getId();

        final JobCompletedNotificationRegistration result =
                client.getJobCompletedNotificationRegistrationSpectraS3(
                        new GetJobCompletedNotificationRegistrationSpectraS3Request(regId))
                        .getJobCompletedNotificationRegistrationResult();

        assertNotNull(result, "GET should return the registration");
        assertEquals(regId, result.getId(), "Returned registration ID should match");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetJobCreatedNotificationRegistration() throws IOException {
        LOG.info("Starting test: testGetJobCreatedNotificationRegistration");

        final UUID regId = client.putJobCreatedNotificationRegistrationSpectraS3(
                new PutJobCreatedNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getJobCreatedNotificationRegistrationResult().getId();

        final JobCreatedNotificationRegistration result =
                client.getJobCreatedNotificationRegistrationSpectraS3(
                        new GetJobCreatedNotificationRegistrationSpectraS3Request(regId))
                        .getJobCreatedNotificationRegistrationResult();

        assertNotNull(result, "GET should return the registration");
        assertEquals(regId, result.getId(), "Returned registration ID should match");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetObjectCachedNotificationRegistration() throws IOException {
        LOG.info("Starting test: testGetObjectCachedNotificationRegistration");

        final UUID regId = client.putObjectCachedNotificationRegistrationSpectraS3(
                new PutObjectCachedNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getS3ObjectCachedNotificationRegistrationResult().getId();

        final S3ObjectCachedNotificationRegistration result =
                client.getObjectCachedNotificationRegistrationSpectraS3(
                        new GetObjectCachedNotificationRegistrationSpectraS3Request(regId))
                        .getS3ObjectCachedNotificationRegistrationResult();

        assertNotNull(result, "GET should return the registration");
        assertEquals(regId, result.getId(), "Returned registration ID should match");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetObjectLostNotificationRegistration() throws IOException {
        LOG.info("Starting test: testGetObjectLostNotificationRegistration");

        final UUID regId = client.putObjectLostNotificationRegistrationSpectraS3(
                new PutObjectLostNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getS3ObjectLostNotificationRegistrationResult().getId();

        final S3ObjectLostNotificationRegistration result =
                client.getObjectLostNotificationRegistrationSpectraS3(
                        new GetObjectLostNotificationRegistrationSpectraS3Request(regId))
                        .getS3ObjectLostNotificationRegistrationResult();

        assertNotNull(result, "GET should return the registration");
        assertEquals(regId, result.getId(), "Returned registration ID should match");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetObjectPersistedNotificationRegistration() throws IOException {
        LOG.info("Starting test: testGetObjectPersistedNotificationRegistration");

        final UUID regId = client.putObjectPersistedNotificationRegistrationSpectraS3(
                new PutObjectPersistedNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getS3ObjectPersistedNotificationRegistrationResult().getId();

        final S3ObjectPersistedNotificationRegistration result =
                client.getObjectPersistedNotificationRegistrationSpectraS3(
                        new GetObjectPersistedNotificationRegistrationSpectraS3Request(regId))
                        .getS3ObjectPersistedNotificationRegistrationResult();

        assertNotNull(result, "GET should return the registration");
        assertEquals(regId, result.getId(), "Returned registration ID should match");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetStorageDomainFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testGetStorageDomainFailureNotificationRegistration");

        final UUID regId = client.putStorageDomainFailureNotificationRegistrationSpectraS3(
                new PutStorageDomainFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getStorageDomainFailureNotificationRegistrationResult().getId();

        final StorageDomainFailureNotificationRegistration result =
                client.getStorageDomainFailureNotificationRegistrationSpectraS3(
                        new GetStorageDomainFailureNotificationRegistrationSpectraS3Request(regId))
                        .getStorageDomainFailureNotificationRegistrationResult();

        assertNotNull(result, "GET should return the registration");
        assertEquals(regId, result.getId(), "Returned registration ID should match");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetTapeFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testGetTapeFailureNotificationRegistration");

        final UUID regId = client.putTapeFailureNotificationRegistrationSpectraS3(
                new PutTapeFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getTapeFailureNotificationRegistrationResult().getId();

        final TapeFailureNotificationRegistration result =
                client.getTapeFailureNotificationRegistrationSpectraS3(
                        new GetTapeFailureNotificationRegistrationSpectraS3Request(regId))
                        .getTapeFailureNotificationRegistrationResult();

        assertNotNull(result, "GET should return the registration");
        assertEquals(regId, result.getId(), "Returned registration ID should match");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetTapePartitionFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testGetTapePartitionFailureNotificationRegistration");

        final UUID regId = client.putTapePartitionFailureNotificationRegistrationSpectraS3(
                new PutTapePartitionFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getTapePartitionFailureNotificationRegistrationResult().getId();

        final TapePartitionFailureNotificationRegistration result =
                client.getTapePartitionFailureNotificationRegistrationSpectraS3(
                        new GetTapePartitionFailureNotificationRegistrationSpectraS3Request(regId))
                        .getTapePartitionFailureNotificationRegistrationResult();

        assertNotNull(result, "GET should return the registration");
        assertEquals(regId, result.getId(), "Returned registration ID should match");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetPoolFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testGetPoolFailureNotificationRegistration");

        final UUID regId = client.putPoolFailureNotificationRegistrationSpectraS3(
                new PutPoolFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getPoolFailureNotificationRegistrationResult().getId();

        final PoolFailureNotificationRegistration result =
                client.getPoolFailureNotificationRegistrationSpectraS3(
                        new GetPoolFailureNotificationRegistrationSpectraS3Request(regId))
                        .getPoolFailureNotificationRegistrationResult();

        assertNotNull(result, "GET should return the registration");
        assertEquals(regId, result.getId(), "Returned registration ID should match");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetSystemFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testGetSystemFailureNotificationRegistration");

        final UUID regId = client.putSystemFailureNotificationRegistrationSpectraS3(
                new PutSystemFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getSystemFailureNotificationRegistrationResult().getId();

        final SystemFailureNotificationRegistration result =
                client.getSystemFailureNotificationRegistrationSpectraS3(
                        new GetSystemFailureNotificationRegistrationSpectraS3Request(regId))
                        .getSystemFailureNotificationRegistrationResult();

        assertNotNull(result, "GET should return the registration");
        assertEquals(regId, result.getId(), "Returned registration ID should match");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetS3TargetFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testGetS3TargetFailureNotificationRegistration");

        final UUID regId = client.putS3TargetFailureNotificationRegistrationSpectraS3(
                new PutS3TargetFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getS3TargetFailureNotificationRegistrationResult().getId();

        final S3TargetFailureNotificationRegistration result =
                client.getS3TargetFailureNotificationRegistrationSpectraS3(
                        new GetS3TargetFailureNotificationRegistrationSpectraS3Request(regId.toString()))
                        .getS3TargetFailureNotificationRegistrationResult();

        assertNotNull(result, "GET should return the registration");
        assertEquals(regId, result.getId(), "Returned registration ID should match");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetDs3TargetFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testGetDs3TargetFailureNotificationRegistration");

        final UUID regId = client.putDs3TargetFailureNotificationRegistrationSpectraS3(
                new PutDs3TargetFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getDs3TargetFailureNotificationRegistrationResult().getId();

        final Ds3TargetFailureNotificationRegistration result =
                client.getDs3TargetFailureNotificationRegistrationSpectraS3(
                        new GetDs3TargetFailureNotificationRegistrationSpectraS3Request(regId))
                        .getDs3TargetFailureNotificationRegistrationResult();

        assertNotNull(result, "GET should return the registration");
        assertEquals(regId, result.getId(), "Returned registration ID should match");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetAzureTargetFailureNotificationRegistration() throws IOException {
        LOG.info("Starting test: testGetAzureTargetFailureNotificationRegistration");

        final UUID regId = client.putAzureTargetFailureNotificationRegistrationSpectraS3(
                new PutAzureTargetFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT))
                .getAzureTargetFailureNotificationRegistrationResult().getId();

        final AzureTargetFailureNotificationRegistration result =
                client.getAzureTargetFailureNotificationRegistrationSpectraS3(
                        new GetAzureTargetFailureNotificationRegistrationSpectraS3Request(regId.toString()))
                        .getAzureTargetFailureNotificationRegistrationResult();

        assertNotNull(result, "GET should return the registration");
        assertEquals(regId, result.getId(), "Returned registration ID should match");
    }

    // --- GET all notification registrations tests ---

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetAllJobCompletedNotificationRegistrations() throws IOException {
        LOG.info("Starting test: testGetAllJobCompletedNotificationRegistrations");

        client.putJobCompletedNotificationRegistrationSpectraS3(
                new PutJobCompletedNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        final int count = client.getJobCompletedNotificationRegistrationsSpectraS3(
                new GetJobCompletedNotificationRegistrationsSpectraS3Request())
                .getJobCompletedNotificationRegistrationListResult()
                .getJobCompletedNotificationRegistrations().size();
        assertTrue(count >= 1, "GET all should return at least 1 job completed notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetAllJobCreatedNotificationRegistrations() throws IOException {
        LOG.info("Starting test: testGetAllJobCreatedNotificationRegistrations");

        client.putJobCreatedNotificationRegistrationSpectraS3(
                new PutJobCreatedNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        final int count = client.getJobCreatedNotificationRegistrationsSpectraS3(
                new GetJobCreatedNotificationRegistrationsSpectraS3Request())
                .getJobCreatedNotificationRegistrationListResult()
                .getJobCreatedNotificationRegistrations().size();
        assertTrue(count >= 1, "GET all should return at least 1 job created notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetAllObjectCachedNotificationRegistrations() throws IOException {
        LOG.info("Starting test: testGetAllObjectCachedNotificationRegistrations");

        client.putObjectCachedNotificationRegistrationSpectraS3(
                new PutObjectCachedNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        final int count = client.getObjectCachedNotificationRegistrationsSpectraS3(
                new GetObjectCachedNotificationRegistrationsSpectraS3Request())
                .getS3ObjectCachedNotificationRegistrationListResult()
                .getS3ObjectCachedNotificationRegistrations().size();
        assertTrue(count >= 1, "GET all should return at least 1 object cached notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetAllObjectLostNotificationRegistrations() throws IOException {
        LOG.info("Starting test: testGetAllObjectLostNotificationRegistrations");

        client.putObjectLostNotificationRegistrationSpectraS3(
                new PutObjectLostNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        final int count = client.getObjectLostNotificationRegistrationsSpectraS3(
                new GetObjectLostNotificationRegistrationsSpectraS3Request())
                .getS3ObjectLostNotificationRegistrationListResult()
                .getS3ObjectLostNotificationRegistrations().size();
        assertTrue(count >= 1, "GET all should return at least 1 object lost notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetAllObjectPersistedNotificationRegistrations() throws IOException {
        LOG.info("Starting test: testGetAllObjectPersistedNotificationRegistrations");

        client.putObjectPersistedNotificationRegistrationSpectraS3(
                new PutObjectPersistedNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        final int count = client.getObjectPersistedNotificationRegistrationsSpectraS3(
                new GetObjectPersistedNotificationRegistrationsSpectraS3Request())
                .getS3ObjectPersistedNotificationRegistrationListResult()
                .getS3ObjectPersistedNotificationRegistrations().size();
        assertTrue(count >= 1, "GET all should return at least 1 object persisted notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetAllStorageDomainFailureNotificationRegistrations() throws IOException {
        LOG.info("Starting test: testGetAllStorageDomainFailureNotificationRegistrations");

        client.putStorageDomainFailureNotificationRegistrationSpectraS3(
                new PutStorageDomainFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        final int count = client.getStorageDomainFailureNotificationRegistrationsSpectraS3(
                new GetStorageDomainFailureNotificationRegistrationsSpectraS3Request())
                .getStorageDomainFailureNotificationRegistrationListResult()
                .getStorageDomainFailureNotificationRegistrations().size();
        assertTrue(count >= 1, "GET all should return at least 1 storage domain failure notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetAllTapeFailureNotificationRegistrations() throws IOException {
        LOG.info("Starting test: testGetAllTapeFailureNotificationRegistrations");

        client.putTapeFailureNotificationRegistrationSpectraS3(
                new PutTapeFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        final int count = client.getTapeFailureNotificationRegistrationsSpectraS3(
                new GetTapeFailureNotificationRegistrationsSpectraS3Request())
                .getTapeFailureNotificationRegistrationListResult()
                .getTapeFailureNotificationRegistrations().size();
        assertTrue(count >= 1, "GET all should return at least 1 tape failure notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetAllTapePartitionFailureNotificationRegistrations() throws IOException {
        LOG.info("Starting test: testGetAllTapePartitionFailureNotificationRegistrations");

        client.putTapePartitionFailureNotificationRegistrationSpectraS3(
                new PutTapePartitionFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        final int count = client.getTapePartitionFailureNotificationRegistrationsSpectraS3(
                new GetTapePartitionFailureNotificationRegistrationsSpectraS3Request())
                .getTapePartitionFailureNotificationRegistrationListResult()
                .getTapePartitionFailureNotificationRegistrations().size();
        assertTrue(count >= 1, "GET all should return at least 1 tape partition failure notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetAllPoolFailureNotificationRegistrations() throws IOException {
        LOG.info("Starting test: testGetAllPoolFailureNotificationRegistrations");

        client.putPoolFailureNotificationRegistrationSpectraS3(
                new PutPoolFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        final int count = client.getPoolFailureNotificationRegistrationsSpectraS3(
                new GetPoolFailureNotificationRegistrationsSpectraS3Request())
                .getPoolFailureNotificationRegistrationListResult()
                .getPoolFailureNotificationRegistrations().size();
        assertTrue(count >= 1, "GET all should return at least 1 pool failure notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetAllSystemFailureNotificationRegistrations() throws IOException {
        LOG.info("Starting test: testGetAllSystemFailureNotificationRegistrations");

        client.putSystemFailureNotificationRegistrationSpectraS3(
                new PutSystemFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        final int count = client.getSystemFailureNotificationRegistrationsSpectraS3(
                new GetSystemFailureNotificationRegistrationsSpectraS3Request())
                .getSystemFailureNotificationRegistrationListResult()
                .getSystemFailureNotificationRegistrations().size();
        assertTrue(count >= 1, "GET all should return at least 1 system failure notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetAllS3TargetFailureNotificationRegistrations() throws IOException {
        LOG.info("Starting test: testGetAllS3TargetFailureNotificationRegistrations");

        client.putS3TargetFailureNotificationRegistrationSpectraS3(
                new PutS3TargetFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        final int count = client.getS3TargetFailureNotificationRegistrationsSpectraS3(
                new GetS3TargetFailureNotificationRegistrationsSpectraS3Request())
                .getS3TargetFailureNotificationRegistrationListResult()
                .getS3TargetFailureNotificationRegistrations().size();
        assertTrue(count >= 1, "GET all should return at least 1 s3 target failure notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetAllDs3TargetFailureNotificationRegistrations() throws IOException {
        LOG.info("Starting test: testGetAllDs3TargetFailureNotificationRegistrations");

        client.putDs3TargetFailureNotificationRegistrationSpectraS3(
                new PutDs3TargetFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        final int count = client.getDs3TargetFailureNotificationRegistrationsSpectraS3(
                new GetDs3TargetFailureNotificationRegistrationsSpectraS3Request())
                .getDs3TargetFailureNotificationRegistrationListResult()
                .getDs3TargetFailureNotificationRegistrations().size();
        assertTrue(count >= 1, "GET all should return at least 1 ds3 target failure notification registration");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testGetAllAzureTargetFailureNotificationRegistrations() throws IOException {
        LOG.info("Starting test: testGetAllAzureTargetFailureNotificationRegistrations");

        client.putAzureTargetFailureNotificationRegistrationSpectraS3(
                new PutAzureTargetFailureNotificationRegistrationSpectraS3Request(NOTIFICATION_ENDPOINT));

        final int count = client.getAzureTargetFailureNotificationRegistrationsSpectraS3(
                new GetAzureTargetFailureNotificationRegistrationsSpectraS3Request())
                .getAzureTargetFailureNotificationRegistrationListResult()
                .getAzureTargetFailureNotificationRegistrations().size();
        assertTrue(count >= 1, "GET all should return at least 1 azure target failure notification registration");
    }
}

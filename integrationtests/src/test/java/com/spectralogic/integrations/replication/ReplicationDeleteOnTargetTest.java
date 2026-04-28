package com.spectralogic.integrations.replication;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.commands.DeleteObjectRequest;
import com.spectralogic.ds3client.commands.GetBucketRequest;
import com.spectralogic.ds3client.commands.GetBucketResponse;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.models.common.Credentials;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.*;
import static com.spectralogic.integrations.TestConstants.*;
import static com.spectralogic.integrations.TestUtils.cleanSetUp;
import static com.spectralogic.integrations.TestUtils.creds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReplicationDeleteOnTargetTest {
    private final static Logger LOG = Logger.getLogger(ReplicationDeleteOnTargetTest.class);

    private static final String BUCKET_NAME = "replication_delete_target_bucket";
    private static final String INPUT_DIR = "testFiles";
    private static final String OBJECT_NAME = "beowulf.txt";

    private Ds3Client clientA;
    private Ds3Client clientB;
    private final Credentials remoteCreds = new Credentials(authId, secretKey);

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        creds = new Credentials(authId, secretKey);
        clientA = Ds3ClientBuilder.create("http://localhost:8081", creds).build();
        clientB = Ds3ClientBuilder.create("http://localhost:8082", remoteCreds).build();

        clearDs3ReplicationRules(clientA, DATA_POLICY_TAPE_SINGLE_COPY_NAME);
        clearDs3Targets(clientA);
        cleanSetUp(clientA);

        clearDs3ReplicationRules(clientB, DATA_POLICY_TAPE_SINGLE_COPY_NAME);
        cleanSetUp(clientB);
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (clientA != null) {
            cleanupAllBuckets(clientA);
            clearDs3ReplicationRules(clientA, DATA_POLICY_TAPE_SINGLE_COPY_NAME);
            cleanSetUp(clientA);
            clientA.close();
        }
        if (clientB != null) {
            cleanupAllBuckets(clientB);
            clearDs3ReplicationRules(clientB, DATA_POLICY_TAPE_SINGLE_COPY_NAME);
            cleanSetUp(clientB);
            clientB.close();
        }
    }

    @Test
    public void testDeleteOnTargetThenQueryFullDetailsOnSource() throws Exception {
        LOG.info("Starting test: ReplicationDeleteOnTargetTest");
        final Ds3Target target = registerDockerDs3Target(clientA, authId, secretKey, null);
        configureSourceWithReplicationToTarget(target);
        configureTarget();

        final Ds3ClientHelpers helperA = Ds3ClientHelpers.wrap(clientA);
        helperA.ensureBucketExists(BUCKET_NAME);

        final URL testFilesUrl = getClass().getClassLoader().getResource(INPUT_DIR);
        if (testFilesUrl == null) {
            throw new RuntimeException("Could not find " + INPUT_DIR + " directory in resources.");
        }
        final Path inputPath = Paths.get(testFilesUrl.toURI());
        final long objectSize = java.nio.file.Files.size(inputPath.resolve(OBJECT_NAME));

        final Ds3Object sourceObject = new Ds3Object(OBJECT_NAME, objectSize);
        final Ds3ClientHelpers.Job putJob = helperA.startWriteJob(
                BUCKET_NAME, Collections.singletonList(sourceObject));
        putJob.transfer(new FileObjectPutter(inputPath));
        final UUID putJobId = putJob.getJobId();
        addJobName(clientA, "ReplicationDeleteOnTargetTest_PUT", putJobId);

        waitForJobsToComplete(clientA);
        LOG.info("BP-A PUT job " + putJobId + " complete");

        waitForJobsToComplete(clientB);
        LOG.info("BP-B replication PUT job complete");

        assertEquals(1, getBlobCountOnTape(clientA, BUCKET_NAME),
                "Object should be persisted to tape on BP-A");
        assertEquals(1, getBlobCountOnTape(clientB, BUCKET_NAME),
                "Object should be persisted to tape on BP-B");

        reclaimCache(clientA);
        reclaimCache(clientB);

        clientB.deleteObject(new DeleteObjectRequest(BUCKET_NAME, OBJECT_NAME));
        LOG.info("Deleted " + OBJECT_NAME + " from " + BUCKET_NAME + " on BP-B");

        final GetBucketResponse bucketAResponse = clientA.getBucket(new GetBucketRequest(BUCKET_NAME));
        assertEquals(1, bucketAResponse.getListBucketResult().getObjects().size(),
                "BP-A should still list the object since deletes do not replicate from target to source");

        final GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response fullDetails =
                clientA.getPhysicalPlacementForObjectsWithFullDetailsSpectraS3(
                        new GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request(
                                BUCKET_NAME,
                                Collections.singletonList(new Ds3Object(OBJECT_NAME, objectSize))));

        final List<BulkObject> details = fullDetails.getBulkObjectListResult().getObjects();
        assertEquals(1, details.size(), "Full details should describe exactly one object");
        final BulkObject detail = details.get(0);
        assertNotNull(detail.getPhysicalPlacement(), "Physical placement must be present");
        assertEquals(1, detail.getPhysicalPlacement().getTapes().size(),
                "BP-A should still report the object on tape after BP-B deletes its copy");
        VerifyDs3TargetSpectraS3Request verifyDs3TargetSpectraS3Request = new VerifyDs3TargetSpectraS3Request(target.getName());
        verifyDs3TargetSpectraS3Request.withFullDetails(true);
        VerifyDs3TargetSpectraS3Response verifyResp = clientA.verifyDs3TargetSpectraS3(verifyDs3TargetSpectraS3Request);

        GetActiveJobsSpectraS3Request getActiveJobsSpectraS3Request = new GetActiveJobsSpectraS3Request();
        GetActiveJobsSpectraS3Response activeJobs = clientA.getActiveJobsSpectraS3(getActiveJobsSpectraS3Request);
        System.out.println("Waiting for IOM jobs to be created ....");
        while (activeJobs.getActiveJobListResult().getActiveJobs().size() < 2) {
            Thread.sleep(1000);
            activeJobs = clientA.getActiveJobsSpectraS3(getActiveJobsSpectraS3Request);
        }
        System.out.println("IOM jobs created. Waiting for IOM jobs to be completed ....");
        waitForJobsToComplete(clientA);
    }

    private void configureSourceWithReplicationToTarget(Ds3Target target) throws IOException, SQLException {
        final DataPolicy singleCopyTapeDP = findDataPolicy(clientA, DATA_POLICY_TAPE_SINGLE_COPY_NAME);


        final PutDs3DataReplicationRuleSpectraS3Request putRule =
                new PutDs3DataReplicationRuleSpectraS3Request(
                        singleCopyTapeDP.getId(), target.getId(), DataReplicationRuleType.PERMANENT);
        putRule.withReplicateDeletes(false);
        clientA.putDs3DataReplicationRuleSpectraS3(putRule);
        clientA.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId)
                .withDefaultDataPolicyId(singleCopyTapeDP.getId()));
    }

    private void configureTarget() throws IOException {
        getTapesReady(clientB);
        cleanupAllBuckets(clientB);
        final DataPolicy singleCopyTapeDP = findDataPolicy(clientB, DATA_POLICY_TAPE_SINGLE_COPY_NAME);
        clientB.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId)
                .withDefaultDataPolicyId(singleCopyTapeDP.getId()));
    }

    private static DataPolicy findDataPolicy(final Ds3Client client, final String name) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3(new GetDataPoliciesSpectraS3Request());
        final List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Assertions.assertTrue(dataPolicies.size() > 1);
        final Optional<DataPolicy> match = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(name)).findFirst();
        Assertions.assertTrue(match.isPresent(), "Data policy " + name + " not found");
        return match.get();
    }




}

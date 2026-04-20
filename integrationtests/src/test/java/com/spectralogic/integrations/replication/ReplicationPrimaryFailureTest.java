package com.spectralogic.integrations.replication;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.commands.GetBucketRequest;
import com.spectralogic.ds3client.commands.GetBucketResponse;
import com.spectralogic.ds3client.commands.GetObjectRequest;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectGetter;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.models.common.Credentials;
import com.spectralogic.integrations.TestUtils;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ApiHelpers.getBlobCountOnTape;
import static com.spectralogic.integrations.Ds3ApiHelpers.reclaimCache;
import static com.spectralogic.integrations.Ds3ReplicationUtils.*;
import static com.spectralogic.integrations.TestConstants.*;

import static com.spectralogic.integrations.TestUtils.cleanSetUp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ReplicationPrimaryFailureTest {
    final String bucketName = "single_copy_bucket";
    String inputPath = "testFiles";

    private final static Logger LOG = Logger.getLogger( ReplicationPrimaryFailureTest.class );
    public Ds3Client client;
    Ds3Client remote_client;
    Credentials remote_creds = new Credentials(authId, secretKey);
    DataPolicy newPolicy;

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        Credentials creds = new Credentials(authId, secretKey);
        client = Ds3ClientBuilder.create("http://localhost:8081", creds).build();
        remote_client = Ds3ClientBuilder.create("http://localhost:8082", remote_creds).build();
        if (client != null) {
            cleanupBuckets(client, bucketName);
            clearDs3ReplicationRules(client );
            clearDs3Targets(client);

            TestUtils.cleanSetUp(client);
        }
        if (remote_client != null) {
            cleanupBuckets(remote_client, bucketName);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            getTapesReady( client);
            clearDs3ReplicationRules(client, DATA_POLICY_TAPE_REPLICATION_COPY_NAME );
            cleanupAllBuckets(client);
            cleanSetUp(client);
            client.close();
        }
        if (remote_client != null) {
            cleanupAllBuckets(remote_client);
            clearDs3ReplicationRules(remote_client );
            cleanSetUp(remote_client);
        }
    }

    public DataPolicy updateUserDataPolicy(Ds3Client client) throws IOException, SQLException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Assertions.assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> dataPolicy = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_REPLICATION_COPY_NAME)).findFirst();

        if (!dataPolicy.isPresent()) {
             newPolicy = createDataPolicy(client, DATA_POLICY_TAPE_REPLICATION_COPY_NAME);
        } else {
            newPolicy = dataPolicy.get();
        }
        Ds3Target target = registerDockerDs3Target(client,authId, secretKey, null);
        UUID tapeStorageDomainId = getStorageDomainId(client,STORAGE_DOMAIN_TAPE_SINGLE_COPY_NAME );
        PutDataPersistenceRuleSpectraS3Request putDataPersistenceRuleSpectraS3RequestTape =
                new PutDataPersistenceRuleSpectraS3Request(newPolicy.getId(), DataIsolationLevel.BUCKET_ISOLATED, tapeStorageDomainId, DataPersistenceRuleType.PERMANENT);
        client.putDataPersistenceRuleSpectraS3(putDataPersistenceRuleSpectraS3RequestTape);
        PutDs3DataReplicationRuleSpectraS3Request putDs3DataReplicationRuleSpectraS3Request = new PutDs3DataReplicationRuleSpectraS3Request(newPolicy.getId(), target.getId(), DataReplicationRuleType.PERMANENT);
        putDs3DataReplicationRuleSpectraS3Request.withReplicateDeletes(false);
        client.putDs3DataReplicationRuleSpectraS3(putDs3DataReplicationRuleSpectraS3Request);
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(newPolicy.getId() ));
        return newPolicy;
    }

    private void updateUserDataPolicyRemote() throws IOException {
        //Remote client
        try  {
            getTapesReady(remote_client);
            final GetDataPoliciesSpectraS3Response responsePolicy =
                    remote_client.getDataPoliciesSpectraS3(new GetDataPoliciesSpectraS3Request());
            List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
            Optional<DataPolicy> dataPolicy = dataPolicies.stream()
                    .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();


            remote_client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(dataPolicy.get().getId()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    @Test
    public void testPutJobToDs3target() throws IOException, InterruptedException {
        LOG.info("Starting test: ReplicationPrimaryFailureTest using docker" );
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
            final Ds3ClientHelpers remote_helper = Ds3ClientHelpers.wrap(remote_client);
            DataPolicy dataPolicy= updateUserDataPolicy(client);
            updateUserDataPolicyRemote();
            reclaimCache(client);
            // Make sure that the bucket exists, if it does not this will create it
            helper.ensureBucketExists(bucketName, dataPolicy.getId());

            // Get the testFiles folder as a resource
            final URL testFilesUrl = getClass().getClassLoader().getResource(inputPath);
            if (testFilesUrl == null) {
                throw new RuntimeException("Could not find testFiles directory in resources.");
            }
            final Path inputPath = Paths.get(testFilesUrl.toURI());
            final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(inputPath);


            final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);
            job.transfer(new FileObjectPutter(inputPath));
            UUID jobId = job.getJobId();
            addJobName(client, "ReplicationTargetDs3Test", jobId);

            GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();
            GetActiveJobsSpectraS3Response activeJobsResponse = client.getActiveJobsSpectraS3( request );

            Optional<UUID> filteredId = activeJobsResponse.getActiveJobListResult().getActiveJobs().stream()
                    .filter(model -> model.getId().equals(jobId)).findFirst().map(ActiveJob::getId) ;


            while (filteredId.isPresent()) {
                activeJobsResponse = client.getActiveJobsSpectraS3( request );
                filteredId = activeJobsResponse.getActiveJobListResult().getActiveJobs().stream()
                        .filter(model -> model.getId().equals(jobId)).findFirst().map(ActiveJob::getId) ;
                TestUtil.sleep(1000);


            }

            System.out.println("Docker-ReplicationTargetS3Test: Completed PUT: ReplicationTargetS3Test" );
            Optional<UUID> completedId = getCompletedJobs(client).stream()
                    .filter(model -> model.getId().equals(jobId)).findFirst().map(CompletedJob::getId) ;
            assertNotNull(completedId.get());
            assertEquals(5, getBlobCountOnTape(client, bucketName));

            final GetBucketResponse response = client.getBucket(new GetBucketRequest(bucketName));
            assertEquals(5, response.getListBucketResult().getObjects().size());
            final List<Ds3Object> objectList = new ArrayList<>();
            for (final Contents contents : response.getListBucketResult().getObjects()) {
                objectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
            }
            GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request = new GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request( bucketName, objectList );
            GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response = client.getPhysicalPlacementForObjectsWithFullDetailsSpectraS3(getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request);
            List<BulkObject> details =  getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response.getBulkObjectListResult().getObjects();
            for (BulkObject detail: details) {
                assertEquals(1, detail.getPhysicalPlacement().getTapes().size() );
                assertEquals(1, detail.getPhysicalPlacement().getDs3Targets().size() );
            }


            reclaimCache(client);

            GetTapePartitionsSpectraS3Request getTapePartitionsSpectraS3Request = new GetTapePartitionsSpectraS3Request();
            GetTapePartitionsSpectraS3Response  partitionResp = client.getTapePartitionsSpectraS3(getTapePartitionsSpectraS3Request);
            List<TapePartition> partitions = partitionResp.getTapePartitionListResult().getTapePartitions();
            for (TapePartition partition : partitions) {
                ModifyTapePartitionSpectraS3Request modifyTapePartitionSpectraS3Request = new ModifyTapePartitionSpectraS3Request(partition.getName());
                modifyTapePartitionSpectraS3Request.withQuiesced(Quiesced.PENDING);
                client.modifyTapePartitionSpectraS3(modifyTapePartitionSpectraS3Request);
            }

            Path path = Paths.get("output-with-cache.txt");
            FileChannel channelWithCache = FileChannel.open(
                    path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE);
            GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, response.getListBucketResult().getObjects().get(0).getKey(), channelWithCache);
            getObjectRequest.withOffset(0);
            client.getObject(getObjectRequest);
            activeJobsResponse = client.getActiveJobsSpectraS3( request );
            assertEquals(2, getCompletedJobs(client).size(), "There should be one active job created.");


            final List<Ds3Object> filesToGet = new ArrayList<>();

            // Specify the object that you want to get and the range from that object you want to retrieve
            filesToGet.add(new Ds3Object("beowulf.txt"));


            // You can also mix regular object gets with partial object gets
            filesToGet.add(new Ds3Object("ulysses.txt"));

            // When the helper function writes the data to a file it will write it in the sorted over of the Ranges
            // where the range with the lowest starting offset is first.  Any ranges that overlap will be consolidated
            // into a single range, and all the ranges will be written to the same file.

            final Ds3ClientHelpers.Job readJob = helper.startReadJob(bucketName, filesToGet);
            UUID readJobId = readJob.getJobId();
            final Path outputPath2 = Paths.get("output");

            readJob.transfer(new FileObjectGetter(outputPath2));

            LOG.info("Read job completed." +  readJobId);

            LOG.info("Read job completed." +  readJobId);

        } catch (final Exception e) {
            LOG.error("Test ReplicationTargetS3Test failed", e);
            Assertions.fail( "Test ReplicationTargetS3Test failed: " + e.getMessage());
        }

    }
}

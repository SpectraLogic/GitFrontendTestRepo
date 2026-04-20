package com.spectralogic.integrations.iom;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.GetBucketRequest;
import com.spectralogic.ds3client.commands.GetBucketResponse;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.integrations.TestUtils;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Tag("LocalDevelopment")
@Tag("iomtest")
public class IOMPoolTest {
    private final static Logger LOG = Logger.getLogger( IOMPoolTest.class );
    private Ds3Client client;

    public static void clearPersistenceRule(Ds3Client client, UUID dataPolicyId) throws IOException {
        DeleteDataPersistenceRuleSpectraS3Request deleteDataPersistenceRuleSpectraS3Request =
                new DeleteDataPersistenceRuleSpectraS3Request(dataPolicyId);
        client.deleteDataPersistenceRuleSpectraS3(deleteDataPersistenceRuleSpectraS3Request);
    }
    public void addTapeCopyTarget(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        Assertions.assertTrue(responsePolicy.getDataPolicyListResult().getDataPolicies().size() > 1);
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Optional<DataPolicy> singleCopyPoolDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_POOL_NAME)).findFirst();

        List<StorageDomain> storageDomains = getStorageDomains(client);
        Optional<StorageDomain> tapeFirstCopySD = storageDomains.stream()
                .filter(sd -> sd.getName().equals(STORAGE_DOMAIN_TAPE_SINGLE_COPY_NAME)).findFirst();
        UUID storageDomainId = tapeFirstCopySD.get().getId();
        PutDataPersistenceRuleSpectraS3Request singleCopyRequest = new PutDataPersistenceRuleSpectraS3Request(singleCopyPoolDP.get().getId(), DataIsolationLevel.STANDARD,storageDomainId, DataPersistenceRuleType.PERMANENT);
        client.putDataPersistenceRuleSpectraS3(singleCopyRequest);
    }

    public void deletePreExistingTarget(Ds3Client client) throws IOException {
        GetDataPersistenceRulesSpectraS3Response rules = client.getDataPersistenceRulesSpectraS3(new GetDataPersistenceRulesSpectraS3Request());
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        assertTrue(responsePolicy.getDataPolicyListResult().getDataPolicies().size() > 1);
        List<StorageDomain> storageDomains = getStorageDomains(client);
        Optional<StorageDomain> tapeSecondSD = storageDomains.stream()
                .filter(sd -> sd.getName().equals(STORAGE_DOMAIN_TAPE_SINGLE_COPY_NAME)).findFirst();
        UUID storageDomainId = tapeSecondSD.get().getId();
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_POOL_NAME)).findFirst();
        Optional<StorageDomain> tapeFirstCopySD = storageDomains.stream()
                .filter(sd -> sd.getName().equals(STORAGE_DOMAIN_TAPE_DUAL_COPY_NAME)).findFirst();
        UUID storageDomainIdSecond = tapeFirstCopySD.map(StorageDomain::getId).orElse(null);
        if (singleCopyTapeDP.isPresent()) {
            List<DataPersistenceRule> dpPersistenceRule = rules.getDataPersistenceRuleListResult().getDataPersistenceRules().stream()
                    .filter(rule -> (rule.getStorageDomainId().equals(storageDomainId) && rule.getDataPolicyId().equals(singleCopyTapeDP.get().getId()))).collect(Collectors.toList());
            if (!dpPersistenceRule.isEmpty()) {
                System.out.println("Deleting first persistence rule");
                client.deleteDataPersistenceRuleSpectraS3(new DeleteDataPersistenceRuleSpectraS3Request(dpPersistenceRule.get(0).getId()));
            }
            if (storageDomainIdSecond != null ) {
                List<DataPersistenceRule> dpPersistenceRule2 = rules.getDataPersistenceRuleListResult().getDataPersistenceRules().stream()
                        .filter(rule -> (rule.getStorageDomainId().equals(storageDomainIdSecond) && rule.getDataPolicyId().equals(singleCopyTapeDP.get().getId()))).collect(Collectors.toList());
                if (!dpPersistenceRule2.isEmpty()) {
                    System.out.println("Deleting second persistence rule");
                    client.deleteDataPersistenceRuleSpectraS3(new DeleteDataPersistenceRuleSpectraS3Request(dpPersistenceRule2.get(0).getId()));
                }
            }
        }


    }

    public void addSecondTapeCopyTarget(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        Assertions.assertTrue(responsePolicy.getDataPolicyListResult().getDataPolicies().size() > 1);
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Optional<DataPolicy> singleCopyPoolDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_POOL_NAME)).findFirst();

        List<StorageDomain> storageDomains = getStorageDomains(client);
        Optional<StorageDomain> tapeFirstCopySD = storageDomains.stream()
                .filter(sd -> sd.getName().equals(STORAGE_DOMAIN_TAPE_DUAL_COPY_NAME)).findFirst();
        UUID storageDomainId = tapeFirstCopySD.get().getId();
        PutDataPersistenceRuleSpectraS3Request singleCopyRequest = new PutDataPersistenceRuleSpectraS3Request(singleCopyPoolDP.get().getId(), DataIsolationLevel.STANDARD,storageDomainId, DataPersistenceRuleType.PERMANENT);
        client.putDataPersistenceRuleSpectraS3(singleCopyRequest);
    }

    public static void updateUserDataPolicyPool(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();

        GetStorageDomainsSpectraS3Request storageDomainsSpectraS3Request = new GetStorageDomainsSpectraS3Request();
        GetStorageDomainsSpectraS3Response storageDomainsSpectraS3Response = client.getStorageDomainsSpectraS3(storageDomainsSpectraS3Request);



        Assertions.assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> poolDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_POOL_NAME)).findFirst();
        if (poolDP.isEmpty()) {
            poolDP = Optional.ofNullable(createDataPolicy(client, DATA_POLICY_POOL_NAME));
        }
        clearPersistenceRules(client, poolDP.get().getId());
        GetDataPersistenceRulesSpectraS3Request getDataPersistenceRulesSpectraS3Request =
                new GetDataPersistenceRulesSpectraS3Request();
        getDataPersistenceRulesSpectraS3Request.withDataPolicyId(poolDP.get().getId());
        GetDataPersistenceRulesSpectraS3Response resp = client.getDataPersistenceRulesSpectraS3(getDataPersistenceRulesSpectraS3Request);


        UUID storageDomainId = getStorageDomainId(client, STORAGE_DOMAIN_POOL_NAME);
        PutDataPersistenceRuleSpectraS3Request putDataPersistenceRuleSpectraS3Request =
                new PutDataPersistenceRuleSpectraS3Request(poolDP.get().getId(), DataIsolationLevel.BUCKET_ISOLATED, storageDomainId, DataPersistenceRuleType.PERMANENT);
        PutDataPersistenceRuleSpectraS3Response rep = client.putDataPersistenceRuleSpectraS3(putDataPersistenceRuleSpectraS3Request);
        modifyUser(client, poolDP.get());
    }

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
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }

    @Test
    public void testIOMPool() throws IOException, InterruptedException {
        final String bucketName = "pool_iom_bucket";
        String inputPath = "testFiles";
        LOG.info("Starting test : testIOMPool" );
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
            deletePreExistingTarget(client);
            PoolPartition poolPartition = findCreatePoolsPartition(client,"pool-partition");


            updateUserDataPolicyPool(client);
            helper.ensureBucketExists(bucketName);
            final URL testFilesUrl = getClass().getClassLoader().getResource(inputPath);
            if (testFilesUrl == null) {
                throw new RuntimeException("Could not find testFiles directory in resources.");
            }
            final Path inputPathFiles = Paths.get(testFilesUrl.toURI());
            final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(inputPathFiles);

            final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);

            // Start the write job using an Object Putter that will read the files
            // from the local file system.
            job.transfer(new FileObjectPutter(inputPathFiles));

            UUID currentJobId = job.getJobId();
            addJobName(client, "IOMPoolTest", currentJobId);

            GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();
            GetActiveJobsSpectraS3Response activeJobsResponse = client.getActiveJobsSpectraS3( request );

            Optional<UUID> filteredId = activeJobsResponse.getActiveJobListResult().getActiveJobs().stream()
                    .filter(model -> model.getId().equals(currentJobId)).findFirst().map(ActiveJob::getId) ;


            while (!filteredId.isEmpty()) {
                activeJobsResponse = client.getActiveJobsSpectraS3( request );
                filteredId = activeJobsResponse.getActiveJobListResult().getActiveJobs().stream()
                        .filter(model -> model.getId().equals(currentJobId)).findFirst().map(ActiveJob::getId) ;
                TestUtil.sleep(1000);
            }

            System.out.println("IOMPoolTest: Job is complete.");
            Assertions.assertEquals(5, getBlobCountOnPool(client, poolPartition.getId()));

            LOG.info("Adding Tape data replication rule to create IOM jobs....");
            addTapeCopyTarget(client);
            LOG.info("Waiting for the creation of IOM jobs ....");
            while (client.getActiveJobsSpectraS3( request ).getActiveJobListResult().getActiveJobs().isEmpty()) {
                if (getCompletedJobs(client).size() == 3) {
                    break;
                }
                TestUtil.sleep(1000);
            }

            LOG.info("IOM jobs created....");
            while (!client.getActiveJobsSpectraS3( request ).getActiveJobListResult().getActiveJobs().isEmpty()) {
                if (getCompletedJobs(client).size() == 3) {
                    break;
                }
                TestUtil.sleep(1000);
            }
            LOG.info("IOM jobs completed....");
            List<CompletedJob> completedJobs = getCompletedJobs(client);
            boolean hasIOMGetJob = completedJobs.stream()
                    .anyMatch(completedJob -> completedJob.getName().contains("IOM GET Job"));
            boolean hasIOMPutJob = completedJobs.stream()
                    .anyMatch(completedJob -> completedJob.getName().contains("IOM PUT Job"));
            Assertions.assertTrue(hasIOMGetJob);
            Assertions.assertTrue(hasIOMPutJob);

            final GetBucketResponse response = client.getBucket(new GetBucketRequest(bucketName));
            Assertions.assertEquals(5, response.getListBucketResult().getObjects().size());

            final List<Ds3Object> objectList = new ArrayList<>();
            for (final Contents contents : response.getListBucketResult().getObjects()) {
                objectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
            }
            GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request = new GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request( bucketName, objectList );
            GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response = client.getPhysicalPlacementForObjectsWithFullDetailsSpectraS3(getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request);
            List<BulkObject> details =  getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response.getBulkObjectListResult().getObjects();
            for (BulkObject detail: details) {
                Assertions.assertFalse(detail.getPhysicalPlacement().getTapes().isEmpty(), "Tape copies should be equal or greater than 1");
                Assertions.assertFalse(detail.getPhysicalPlacement().getPools().isEmpty(), "Pool copies should be equal or greater than 1");
            }
            addSecondTapeCopyTarget(client);
            LOG.info("Waiting for the creation of IOM jobs ....");
            while (client.getActiveJobsSpectraS3( request ).getActiveJobListResult().getActiveJobs().isEmpty()) {
                if (getCompletedJobs(client).size() == 5) {
                    break;
                }
                TestUtil.sleep(1000);
            }

            LOG.info("IOM jobs created....");
            while (!client.getActiveJobsSpectraS3( request ).getActiveJobListResult().getActiveJobs().isEmpty()) {
                if (getCompletedJobs(client).size() == 5) {
                    break;
                }
                TestUtil.sleep(1000);
            }
            LOG.info("IOM jobs completed....");
            completedJobs = getCompletedJobs(client);
            hasIOMGetJob = completedJobs.stream()
                    .anyMatch(completedJob -> completedJob.getName().contains("IOM GET Job"));
            hasIOMPutJob = completedJobs.stream()
                    .anyMatch(completedJob -> completedJob.getName().contains("IOM PUT Job"));
            Assertions.assertTrue(hasIOMGetJob);
            Assertions.assertTrue(hasIOMPutJob);

            getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request = new GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request( bucketName, objectList );
            getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response = client.getPhysicalPlacementForObjectsWithFullDetailsSpectraS3(getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request);
            details =  getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response.getBulkObjectListResult().getObjects();
            for (BulkObject detail: details) {
                Assertions.assertEquals(2, detail.getPhysicalPlacement().getTapes().size(), "Tape copies should be 2");
                Assertions.assertEquals(1, detail.getPhysicalPlacement().getPools().size(), "Pool copies should be equal to 1");
            }

        } catch (IOException | SQLException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}

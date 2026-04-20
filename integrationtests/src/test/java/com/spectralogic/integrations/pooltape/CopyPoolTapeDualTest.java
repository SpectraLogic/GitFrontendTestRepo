package com.spectralogic.integrations.pooltape;

import com.spectralogic.ds3client.Ds3Client;
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
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.TestConstants.*;

@Order(4)

@Tag("LocalDevelopment")
public class CopyPoolTapeDualTest {
    private final static Logger LOG = Logger.getLogger( CopyPoolTapeDualTest.class );
    private Ds3Client client;

    public static void updateUserDataPolicyPool(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Assertions.assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> poolDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_HYBRID_DUAL_COPY_NAME)).findFirst();
        if (poolDP.isEmpty()) {
            poolDP = Optional.ofNullable(createDataPolicy(client, DATA_POLICY_HYBRID_DUAL_COPY_NAME));
        }
        clearPersistenceRules(client, poolDP.get().getId());
        UUID poolStorageDomainId = getStorageDomainId(client, STORAGE_DOMAIN_POOL_NAME);
        UUID tapeStorageDomainId = getStorageDomainId(client,STORAGE_DOMAIN_TAPE_SINGLE_COPY_NAME );
        UUID tapeDualCopyStorageDomainId = getStorageDomainId(client, STORAGE_DOMAIN_TAPE_DUAL_COPY_NAME);

        GetDataPersistenceRulesSpectraS3Request getDataPersistenceRulesSpectraS3Request =
                new GetDataPersistenceRulesSpectraS3Request();
        getDataPersistenceRulesSpectraS3Request.withDataPolicyId(poolDP.get().getId());
        GetDataPersistenceRulesSpectraS3Response resp = client.getDataPersistenceRulesSpectraS3(getDataPersistenceRulesSpectraS3Request);
        if (resp.getDataPersistenceRuleListResult().getDataPersistenceRules().isEmpty()) {
            PutDataPersistenceRuleSpectraS3Request putDataPersistenceRuleSpectraS3RequestPool =
                    new PutDataPersistenceRuleSpectraS3Request(poolDP.get().getId(), DataIsolationLevel.BUCKET_ISOLATED, poolStorageDomainId, DataPersistenceRuleType.PERMANENT);
            client.putDataPersistenceRuleSpectraS3(putDataPersistenceRuleSpectraS3RequestPool);
            PutDataPersistenceRuleSpectraS3Request putDataPersistenceRuleSpectraS3RequestTape =
                    new PutDataPersistenceRuleSpectraS3Request(poolDP.get().getId(), DataIsolationLevel.BUCKET_ISOLATED, tapeStorageDomainId, DataPersistenceRuleType.PERMANENT);
            client.putDataPersistenceRuleSpectraS3(putDataPersistenceRuleSpectraS3RequestTape);
            PutDataPersistenceRuleSpectraS3Request putDataPersistenceRuleSpectraS3RequestDualCopyTape =
                    new PutDataPersistenceRuleSpectraS3Request(poolDP.get().getId(), DataIsolationLevel.BUCKET_ISOLATED, tapeDualCopyStorageDomainId, DataPersistenceRuleType.PERMANENT);
            client.putDataPersistenceRuleSpectraS3(putDataPersistenceRuleSpectraS3RequestDualCopyTape);
        } else if (resp.getDataPersistenceRuleListResult().getDataPersistenceRules().size() <3 ) {
            DataPersistenceRule dataPersistenceRule = resp.getDataPersistenceRuleListResult().getDataPersistenceRules().get(0);
            if (!dataPersistenceRule.getStorageDomainId().equals(poolStorageDomainId)) {
                PutDataPersistenceRuleSpectraS3Request putDataPersistenceRuleSpectraS3Request =
                        new PutDataPersistenceRuleSpectraS3Request(poolDP.get().getId(), DataIsolationLevel.BUCKET_ISOLATED, poolStorageDomainId, DataPersistenceRuleType.PERMANENT);
                client.putDataPersistenceRuleSpectraS3(putDataPersistenceRuleSpectraS3Request);
            } else if (!dataPersistenceRule.getStorageDomainId().equals(tapeStorageDomainId)){
                PutDataPersistenceRuleSpectraS3Request putDataPersistenceRuleSpectraS3Request =
                        new PutDataPersistenceRuleSpectraS3Request(poolDP.get().getId(), DataIsolationLevel.BUCKET_ISOLATED, tapeStorageDomainId, DataPersistenceRuleType.PERMANENT);
                client.putDataPersistenceRuleSpectraS3(putDataPersistenceRuleSpectraS3Request);
            }else if (!dataPersistenceRule.getStorageDomainId().equals(tapeDualCopyStorageDomainId)){
                PutDataPersistenceRuleSpectraS3Request putDataPersistenceRuleSpectraS3RequestDualCopyTape =
                        new PutDataPersistenceRuleSpectraS3Request(poolDP.get().getId(), DataIsolationLevel.BUCKET_ISOLATED, tapeDualCopyStorageDomainId, DataPersistenceRuleType.PERMANENT);
                client.putDataPersistenceRuleSpectraS3(putDataPersistenceRuleSpectraS3RequestDualCopyTape);
            }

        } else {
            Assertions.assertEquals(3,resp.getDataPersistenceRuleListResult().getDataPersistenceRules().size(),"There should be two data persistence rule for the data policy");
        }
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
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    public void testSingleCopyOnPoolAndDualCopyTape() throws IOException, InterruptedException {
        final String bucketName = "pool_tape_dual_copy_bucket";
        String inputPath = "testFiles";
        LOG.info("Starting test : PoolCopyTapeCopyTest");
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
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
            addJobName(client, "PoolCopyTapeCopyTest", currentJobId);

            waitForJobsToComplete(client);


            System.out.println("PoolCopyTapeCopyTest: Job is complete.");
            Assertions.assertEquals(5, getBlobCountOnPool(client, poolPartition.getId()));
            Assertions.assertEquals(10, getBlobCountOnTape(client, bucketName));
        } catch (IOException | SQLException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}

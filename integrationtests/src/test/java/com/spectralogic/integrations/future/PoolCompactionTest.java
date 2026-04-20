package com.spectralogic.integrations.future;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.integrations.TestUtils;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.DatabaseUtils.updatePoolPartition;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ApiHelpers.modifyUser;
import static com.spectralogic.integrations.TestConstants.DATA_POLICY_POOL_NAME;
import static com.spectralogic.integrations.TestConstants.STORAGE_DOMAIN_POOL_NAME;

@Tag("LocalDevelopment")
@Tag("iomtest")
@Tag("pooltest")
//https://jira.spectralogic.com/browse/EMPROD-3966
public class PoolCompactionTest {
    private final static Logger LOG = Logger.getLogger( PoolCompactionTest.class );
    private static Ds3Client client;

    public static void updateUserDataPolicyPool(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();

        Assertions.assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> poolDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_POOL_NAME)).findFirst();
        if (poolDP.isEmpty()) {
            poolDP = Optional.ofNullable(createDataPolicy(client, DATA_POLICY_POOL_NAME));
        }

        clearPersistenceRules(client, poolDP.get().getId());
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

    //@Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testSingleCopyPool() throws IOException, InterruptedException {
        final String bucketName = "pool_copy_bucket";
        String inputPath = "testFiles";
        String poolPartitionName = "pool-partition";
        LOG.info("Starting test : PoolCompactionTest" );
        try {

            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

            PoolPartition poolPartition = findCreatePoolsPartition(client, poolPartitionName);

            GetPoolsSpectraS3Request getPoolsSpectraS3Request = new GetPoolsSpectraS3Request();
            GetPoolsSpectraS3Response resp = client.getPoolsSpectraS3(getPoolsSpectraS3Request);


            GetPoolPartitionsSpectraS3Request partitionsSpectraS3Request = new GetPoolPartitionsSpectraS3Request();
            GetPoolPartitionsSpectraS3Response poolPartitionResp = client.getPoolPartitionsSpectraS3(partitionsSpectraS3Request);

            PoolPartition partition = poolPartitionResp.getPoolPartitionListResult().getPoolPartitions().stream()
                    .filter(dp -> dp.getName().equals(poolPartitionName)).findFirst()
                    .orElseThrow(() -> new NoSuchElementException("No pool partition found with name: " + poolPartitionName));
            updatePoolPartition(resp.getPoolListResult().getPools().get(0).getGuid(), partition.getId().toString());
            updatePoolPartition(resp.getPoolListResult().getPools().get(1).getGuid(), partition.getId().toString());

            ModifyPoolSpectraS3Request modifyPoolSpectraS3Request = new ModifyPoolSpectraS3Request(resp.getPoolListResult().getPools().get(0).getGuid());
            modifyPoolSpectraS3Request.withQuiesced(Quiesced.PENDING);
            client.modifyPoolSpectraS3(modifyPoolSpectraS3Request);

            GetPoolSpectraS3Request getPoolSpectraS3Request2 = new GetPoolSpectraS3Request(resp.getPoolListResult().getPools().get(0).getName());
            GetPoolSpectraS3Response poolResp = client.getPoolSpectraS3(getPoolSpectraS3Request2);
            while (poolResp.getPoolResult().getQuiesced() != Quiesced.YES) {
                LOG.info("Waiting for pool to be quiesced...");
                Thread.sleep(1000);
                poolResp = client.getPoolSpectraS3(getPoolSpectraS3Request2);
            }
            updateUserDataPolicyPool(client);
            CompactPoolSpectraS3Request compactPoolSpectraS3Request = new CompactPoolSpectraS3Request(resp.getPoolListResult().getPools().get(0).getGuid());
            client.compactPoolSpectraS3(compactPoolSpectraS3Request);

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
            addJobName(client, "CopyPoolTest", currentJobId);

            isJobCompleted(client, currentJobId);

            LOG.info("CopyPoolTest job completed." +  currentJobId);
            Assertions.assertEquals(5, getBlobCountOnPool(client, poolPartition.getId()));
        } catch (IOException | SQLException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}

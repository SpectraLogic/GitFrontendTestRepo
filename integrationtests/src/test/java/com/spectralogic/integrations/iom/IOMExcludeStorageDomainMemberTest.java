/**
 * This test excludes storage domain member after PUT. This will trigger IOM jobs.
 */
package com.spectralogic.integrations.iom;

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
import java.util.*;
import java.util.stream.Collectors;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;
@Tag("LocalDevelopment")
@Tag("iomtest")
public class IOMExcludeStorageDomainMemberTest {
    private final static Logger LOG = Logger.getLogger( IOMExcludeStorageDomainMemberTest.class );
    static final String bucketName = "iom_exclude_bucket";
    static UUID newPersistenceRuleId;
    String inputPath = "testFiles";
    private Ds3Client client;

    public static void configureStorageDomain(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<StorageDomain> storageDomains = getStorageDomains(client);
        Optional<StorageDomain> tapeFirstSD = storageDomains.stream()
                .filter(sd -> sd.getName().equals("Tape Second Copy")).findFirst();
        UUID storageDomainId = tapeFirstSD.get().getId();
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();
        PutDataPersistenceRuleSpectraS3Request putDs3DataPolicySpectraS3Request = new PutDataPersistenceRuleSpectraS3Request(singleCopyTapeDP.get().getId(),DataIsolationLevel.STANDARD,storageDomainId, DataPersistenceRuleType.PERMANENT);
        PutDataPersistenceRuleSpectraS3Response resp= client.putDataPersistenceRuleSpectraS3(putDs3DataPolicySpectraS3Request);
        newPersistenceRuleId = resp.getDataPersistenceRuleResult().getId();
    }

    public static DataPersistenceRule checkDataPersistenceRule(Ds3Client client) throws IOException {
        List<StorageDomain> storageDomains = getStorageDomains(client);
        Optional<StorageDomain> tapeSecondSD = storageDomains.stream()
                .filter(sd -> sd.getName().equals("Tape Second Copy")).findFirst();
        UUID storageDomainId = tapeSecondSD.get().getId();

        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();
        final GetDataPersistenceRulesSpectraS3Response response = client.getDataPersistenceRulesSpectraS3(new GetDataPersistenceRulesSpectraS3Request());
        List<DataPersistenceRule> rules = response.getDataPersistenceRuleListResult().getDataPersistenceRules();
        List<DataPersistenceRule> filteredRules = rules.stream().filter(rule -> (rule.getStorageDomainId().equals(storageDomainId) && rule.getDataPolicyId().equals(singleCopyTapeDP.get().getId()))).collect(Collectors.toList());
        if(!filteredRules.isEmpty()){
            return filteredRules.get(0);
        }
        return null;
    }

    /**
     * This function excludes storage domain member.
     * @param client
     * @throws IOException
     */
    public static Map<String, Object> excludeStorageDomainMember(Ds3Client client ) throws IOException {
        Map<String, Object> map = new HashMap<>();
        List<StorageDomain> all = getStorageDomains(client);
        Optional<StorageDomain> dualCopyTapeSD = all.stream()
                .filter(dp -> dp.getName().equals("Tape Second Copy")).findFirst();

        StorageDomain sd = dualCopyTapeSD.get();
        List<StorageDomainMember> allMembers = getStorageDomainMembers(client);
        GetTapesSpectraS3Request getTapesSpectraS3Request = new GetTapesSpectraS3Request();
        GetTapesSpectraS3Response getTapesSpectraS3Response = client.getTapesSpectraS3( getTapesSpectraS3Request);
        List<StorageDomainMember>  validMembers = allMembers.stream().filter(sdm -> sdm.getStorageDomainId().equals(sd.getId())).toList();
        GetBucketsSpectraS3Request rr = new GetBucketsSpectraS3Request();
        GetBucketsSpectraS3Response response = client.getBucketsSpectraS3(rr);

        Optional<UUID> bucketId = response.getBucketListResult().getBuckets().stream()
                .filter(model -> model.getName().equals(bucketName)).findFirst().map(Bucket::getId);

        for (Tape tape : getTapesSpectraS3Response.getTapeListResult().getTapes()) {

            if (tape.getBucketId() !=null && tape.getBucketId().equals(bucketId.get())) {
               if (validMembers.stream().anyMatch(sdm -> sdm.getId().equals( tape.getStorageDomainMemberId()))){
                    ModifyStorageDomainMemberSpectraS3Request modifyStorageDomainMemberSpectraS3Request = new ModifyStorageDomainMemberSpectraS3Request(tape.getStorageDomainMemberId().toString());

                    modifyStorageDomainMemberSpectraS3Request.withState(StorageDomainMemberState.EXCLUSION_IN_PROGRESS);
                    ModifyStorageDomainMemberSpectraS3Response modifiedMember = client.modifyStorageDomainMemberSpectraS3(modifyStorageDomainMemberSpectraS3Request);

                    map.put("storageDomainMemberId", validMembers.get(0).getId() );
                    map.put("tapePartitionId", modifiedMember.getStorageDomainMemberResult().getTapePartitionId() );
                    map.put("tapeType", modifiedMember.getStorageDomainMemberResult().getTapeType() );
                    return map;
                }
            }

        }
        return map;
    }

    /**
     * This function adds the storage domain member excluded in
     * @param client
     * @param map
     * @throws IOException
     */
    public static void revertBackStorageDomainMember(Ds3Client client, Map<String, Object> map) throws IOException {
        UUID storageDomainId =  getStorageDomainId(client, "Tape Second Copy" );
        List<StorageDomainMember> allMembers = getStorageDomainMembers(client);;
        List<StorageDomainMember>  validMembers = allMembers.stream().filter(sdm -> sdm.getState().equals(StorageDomainMemberState.EXCLUSION_IN_PROGRESS) && sdm.getStorageDomainId().equals(storageDomainId)).toList() ;
        if (!validMembers.isEmpty()) {
            ModifyStorageDomainMemberSpectraS3Request modifyStorageDomainMemberSpectraS3Request = new ModifyStorageDomainMemberSpectraS3Request(validMembers.get(0).getId().toString());
            modifyStorageDomainMemberSpectraS3Request.withState(StorageDomainMemberState.NORMAL);
            ModifyStorageDomainMemberSpectraS3Response modifiedResp = client.modifyStorageDomainMemberSpectraS3(modifyStorageDomainMemberSpectraS3Request);

        } else {
            PutTapeStorageDomainMemberSpectraS3Request req = new PutTapeStorageDomainMemberSpectraS3Request( storageDomainId, (UUID) map.get("tapePartitionId" ), String.valueOf(map.get("tapeType")));
            PutTapeStorageDomainMemberSpectraS3Response resp = client.putTapeStorageDomainMemberSpectraS3(req);
        }
    }

    public void updateUserDataPolicy(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> dualCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_DUAL_COPY_NAME)).findFirst();
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(dualCopyTapeDP.get().getId() ));
    }

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        client = TestUtils.setTestParams();
        if (client != null) {
            TestUtils.cleanSetUp(client);
            DataPersistenceRule rule = checkDataPersistenceRule(client);
            if ( rule!= null) {
                client.deleteDataPersistenceRuleSpectraS3(new DeleteDataPersistenceRuleSpectraS3Request(rule.getId()));
            }
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
    public void testPutJobToTapes() throws IOException, InterruptedException {
        LOG.info("Starting test: IOMExcludeStorageDomainMemberTest:");
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
            updateUserDataPolicy(client);

            configureStorageDomain(client);


            helper.ensureBucketExists(bucketName);
            final URL testFilesUrl = getClass().getClassLoader().getResource(inputPath);
            if (testFilesUrl == null) {
                throw new RuntimeException("Could not find testFiles directory in resources.");
            }
            final Path inputPath = Paths.get(testFilesUrl.toURI());
            final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(inputPath);

            LOG.info("Starting write to bucket " + bucketName + " with " +  objects.toString());
            final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);
            job.transfer(new FileObjectPutter(inputPath));

            UUID currentJobId = job.getJobId();
            addJobName(client, "IOMExcludeStorageDomainMemberTest", currentJobId);

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

            System.out.println("IOMExcludeStorageDomainMemberTest: Job is complete.");

            Optional<UUID> completedId = getCompletedJobs(client).stream()
                    .filter(model -> model.getId().equals(currentJobId)).findFirst().map(CompletedJob::getId) ;
            assertNotNull(completedId.get());
            assertEquals(10, getBlobCountOnTape(client, bucketName));
            LOG.info("Put job completed. Excluding storage domain member...");
            Map<String, Object> map = excludeStorageDomainMember(client);

            LOG.info("Put job completed. Waiting for IOM job creation...");
            while (client.getActiveJobsSpectraS3( request ).getActiveJobListResult().getActiveJobs().isEmpty()) {
                if (getCompletedJobs(client).size() == 3) {
                    break;
                }
                TestUtil.sleep( 1000);
            }
            LOG.info("IOM Jobs created. Waiting for IOM jobs to be completed.");
            while (!client.getActiveJobsSpectraS3( request ).getActiveJobListResult().getActiveJobs().isEmpty()) {
                if (getCompletedJobs(client).size() == 3) {
                    break;
                }
                TestUtil.sleep( 1000);
            }
            LOG.info("IOM Jobs completed.");
            List<CompletedJob> completedJobs = getCompletedJobs(client);
            boolean hasIOMGetJob = completedJobs.stream()
                    .anyMatch(completedJob -> completedJob.getName().contains("IOM GET Job"));
            boolean hasIOMPutJob = completedJobs.stream()
                    .anyMatch(completedJob -> completedJob.getName().contains("IOM PUT Job"));
            assertTrue(hasIOMGetJob);
            assertTrue(hasIOMPutJob);
            revertBackStorageDomainMember(client, map);
            DeleteDataPersistenceRuleSpectraS3Request deleteDataPersistenceRuleSpectraS3Request = new DeleteDataPersistenceRuleSpectraS3Request(newPersistenceRuleId);
            client.deleteDataPersistenceRuleSpectraS3(deleteDataPersistenceRuleSpectraS3Request);
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}

package com.spectralogic.integrations;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.*;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.spectralogic.integrations.DatabaseUtils.*;
import static com.spectralogic.integrations.TestConstants.*;
import static com.spectralogic.integrations.TestConstants.authId;



public class Ds3ApiHelpers {

    public static void modifyUser(Ds3Client client, DataPolicy dataPolicy) throws IOException {
        if (BP_USED !=null && BP_USED.equals("true") && ds3AccessKey != null && ds3SecretKey != null) {
            client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(ds3AccessKey).withDefaultDataPolicyId(dataPolicy.getId() ));
        } else {
            client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(dataPolicy.getId() ));
        }
    }

    public static PoolPartition findCreatePoolsPartition(Ds3Client client, String name) throws SQLException, IOException {
        String runsOnBP = System.getenv("BP_USED");
        PoolPartition poolPartition = null;
        if (runsOnBP != null  && runsOnBP.equals("true")) {
            GetPoolPartitionsSpectraS3Request partitionsSpectraS3Request = new GetPoolPartitionsSpectraS3Request();
            GetPoolPartitionsSpectraS3Response poolPartitionResp = client.getPoolPartitionsSpectraS3(partitionsSpectraS3Request);
            if (poolPartitionResp.getPoolPartitionListResult().getPoolPartitions().size() > 0) {
                return poolPartitionResp.getPoolPartitionListResult().getPoolPartitions().get(0);
            }
            return null;
        } else {

            try {
                poolPartition = getPoolsPartition(client, name);
            } catch ( NoSuchElementException e ) {
                createPoolsPartition(client, name);
                poolPartition = getPoolsPartition(client, name);
            } catch(Exception e) {
                throw new RuntimeException("Failed to create or retrieve pool partition", e);
            }
        }
        return poolPartition;
    }
    public static void checkTapesUntilNormal(Ds3Client client) {
        try {
            GetTapesSpectraS3Request request = new GetTapesSpectraS3Request();
            GetTapesSpectraS3Response response = client.getTapesSpectraS3(request);
            List<Tape> tapes = response.getTapeListResult().getTapes();


            List<Tape> states = tapes.stream().filter(tape -> !(tape.getState().equals(TapeState.NORMAL))).toList();

           if (!states.isEmpty() ) {
                Thread.sleep(2000);
                checkTapesUntilNormal(client);
            } else {
                System.out.println("All tapes are in NORMAL state.");
            }
            /*long normalTapesCount = tapes.stream()
                    // 1. Filter: Keep only the tapes that match the condition (TapeState.NORMAL)
                    .filter(tape -> tape.getState() == TapeState.NORMAL)
                    // 2. Count: Return the number of elements remaining in the stream
                    .count();
            if (normalTapesCount < 0) {
                Thread.sleep(2000);
                checkTapesUntilNormal(client);
            }*/

        } catch (Exception e) {
            throw new RuntimeException("Error checking tape state", e);
        }
    }

    public static void quiescePartitions(Ds3Client client) throws IOException {
        GetTapePartitionsSpectraS3Response response = client.getTapePartitionsSpectraS3(new GetTapePartitionsSpectraS3Request());
        List<TapePartition> partitions = response.getTapePartitionListResult().getTapePartitions();
        int index = 0;
        for (final com.spectralogic.ds3client.models.TapePartition partition : partitions) {
            ModifyTapePartitionSpectraS3Request req = new ModifyTapePartitionSpectraS3Request(partition.getName());
            if (!partition.getQuiesced().equals(Quiesced.YES)) {
                req.withQuiesced(Quiesced.PENDING);
                client.modifyTapePartitionSpectraS3(req);
            }
        }
    }

    public static void keepPartitionsWriteOnly(Ds3Client client) throws IOException {
        GetTapePartitionsSpectraS3Response response = client.getTapePartitionsSpectraS3(new GetTapePartitionsSpectraS3Request());
        List<TapePartition> partitions = response.getTapePartitionListResult().getTapePartitions();

        GetTapeDrivesSpectraS3Request drivesReq = new GetTapeDrivesSpectraS3Request();
        GetTapeDrivesSpectraS3Response drivesResp = client.getTapeDrivesSpectraS3(drivesReq);
        List<TapeDrive> drives = drivesResp.getTapeDriveListResult().getTapeDrives();
        for (final com.spectralogic.ds3client.models.TapePartition partition : partitions) {
            ModifyTapePartitionSpectraS3Request req = new ModifyTapePartitionSpectraS3Request(partition.getName());
            if (!partition.getQuiesced().equals(Quiesced.YES)) {

                final List<TapeDrive> availableTapeDrives = drives
                        .stream()
                        .filter(drive -> drive.getPartitionId().equals(partition.getId()))
                        .collect(Collectors.toList());

                GetTapeDriveSpectraS3Request getTapeDriveSpectraS3Request = new GetTapeDriveSpectraS3Request(availableTapeDrives.get(0).getId());
                GetTapeDriveSpectraS3Response tapeDriveResponse = client.getTapeDriveSpectraS3(getTapeDriveSpectraS3Request);

                ModifyTapeDriveSpectraS3Request driveReq = new ModifyTapeDriveSpectraS3Request(availableTapeDrives.get(0).getId());
                driveReq.withQuiesced(Quiesced.PENDING);
                tapeDriveResponse = client.getTapeDriveSpectraS3(getTapeDriveSpectraS3Request);
                client.modifyTapeDriveSpectraS3(driveReq);
                while (!tapeDriveResponse.getTapeDriveResult().getQuiesced().equals(Quiesced.YES)) {
                   TestUtil.sleep(1000);
                    tapeDriveResponse = client.getTapeDriveSpectraS3(getTapeDriveSpectraS3Request);
                }
                for (int i=1; i < availableTapeDrives.size(); i++) {
                    driveReq = new ModifyTapeDriveSpectraS3Request(availableTapeDrives.get(i).getId());
                    driveReq.withReservedTaskType(ReservedTaskType.WRITE);
                    client.modifyTapeDriveSpectraS3(driveReq);

                }
            }

        }
    }

    public static void revertModifiedPartitions(Ds3Client client) throws IOException {
        GetTapePartitionsSpectraS3Response response = client.getTapePartitionsSpectraS3(new GetTapePartitionsSpectraS3Request());
        List<TapePartition> partitions = response.getTapePartitionListResult().getTapePartitions();

        GetTapeDrivesSpectraS3Request drivesReq = new GetTapeDrivesSpectraS3Request();
        GetTapeDrivesSpectraS3Response drivesResp = client.getTapeDrivesSpectraS3(drivesReq);
        List<TapeDrive> drives = drivesResp.getTapeDriveListResult().getTapeDrives();

        for (final com.spectralogic.ds3client.models.TapePartition partition : partitions) {
            ModifyTapePartitionSpectraS3Request req = new ModifyTapePartitionSpectraS3Request(partition.getName());
            if (!partition.getQuiesced().equals(Quiesced.YES)) {
                final List<TapeDrive> availableTapeDrives = drives
                        .stream()
                        .filter(drive -> drive.getPartitionId() == partition.getId())
                        .collect(Collectors.toList());

                for (int i=0; i < availableTapeDrives.size(); i++) {
                    ModifyTapeDriveSpectraS3Request driveReq = new ModifyTapeDriveSpectraS3Request(availableTapeDrives.get(i).getId());
                    driveReq.withQuiesced(Quiesced.NO);
                    driveReq.withReservedTaskType(ReservedTaskType.ANY);
                    client.modifyTapeDriveSpectraS3(driveReq);

                }
            }

        }
    }

    public static void getTapesReady(Ds3Client client) throws IOException {
        GetTapePartitionsSpectraS3Response response = client.getTapePartitionsSpectraS3(new GetTapePartitionsSpectraS3Request());
        List<TapePartition> partitions = response.getTapePartitionListResult().getTapePartitions();
        for (final com.spectralogic.ds3client.models.TapePartition partition : partitions) {
            ModifyTapePartitionSpectraS3Request req = new ModifyTapePartitionSpectraS3Request(partition.getName());
            if (!partition.getQuiesced().equals(Quiesced.NO)) {
                req.withQuiesced(Quiesced.NO);
                client.modifyTapePartitionSpectraS3(req);
            }

        }
        GetTapeDrivesSpectraS3Request drivesReq = new GetTapeDrivesSpectraS3Request();
        GetTapeDrivesSpectraS3Response drivesResp = client.getTapeDrivesSpectraS3(drivesReq);
        List<TapeDrive> drives = drivesResp.getTapeDriveListResult().getTapeDrives();
        for (final com.spectralogic.ds3client.models.TapeDrive drive : drives) {
            if (!drive.getQuiesced().equals(Quiesced.NO)) {
                ModifyTapeDriveSpectraS3Request req = new ModifyTapeDriveSpectraS3Request(drive.getId());
                req.withQuiesced(Quiesced.NO);
                client.modifyTapeDriveSpectraS3(req);
            }

        }
        FormatAllTapesSpectraS3Request formatAllTapesSpectraS3Request = new FormatAllTapesSpectraS3Request();
        client.formatAllTapesSpectraS3(formatAllTapesSpectraS3Request);
        checkTapesUntilNormal(client);
    }

    public static int getBlobCountOnTape(Ds3Client client, String bucketName ) throws IOException {
        GetBucketsSpectraS3Request rr = new GetBucketsSpectraS3Request();
        GetBucketsSpectraS3Response response = client.getBucketsSpectraS3(rr);
        Optional<UUID> bucketId = response.getBucketListResult().getBuckets().stream()
                .filter(model -> model.getName().equals(bucketName)).findFirst().map(Bucket::getId);


        GetTapesSpectraS3Request getTapesSpectraS3Request = new GetTapesSpectraS3Request();
        GetTapesSpectraS3Response getTapesSpectraS3Response = client.getTapesSpectraS3( getTapesSpectraS3Request);
        int blobCount = 0;
        for (Tape tape : getTapesSpectraS3Response.getTapeListResult().getTapes()) {
            UUID tapeId = tape.getId();
            if (tape.getBucketId() !=null && tape.getBucketId().equals(bucketId.get())) {
                GetBlobsOnTapeSpectraS3Request getBlobsOnTapeSpectraS3Request = new GetBlobsOnTapeSpectraS3Request(tapeId);
                GetBlobsOnTapeSpectraS3Response getBlobsOnTapeSpectraS3Response = client.getBlobsOnTapeSpectraS3(getBlobsOnTapeSpectraS3Request);
                blobCount += getBlobsOnTapeSpectraS3Response.getBulkObjectListResult().getObjects().size();
            }

        }
        return blobCount;
    }

    public static BulkObject getBlobTape(Ds3Client client, UUID tapeId, UUID blobId) throws IOException, JSONException {
        GetBlobsOnTapeSpectraS3Request request = new GetBlobsOnTapeSpectraS3Request(tapeId);
        GetBlobsOnTapeSpectraS3Response response = client.getBlobsOnTapeSpectraS3(request);
        Optional<BulkObject> bulkObject = response.getBulkObjectListResult().getObjects().stream()
                .filter(model -> model.getId().equals(blobId)).findFirst();

        return bulkObject.get();
    }

    public static String getBlobPersistence( Ds3Client client,UUID blobId) throws IOException, JSONException {
        JSONArray blobIds = new JSONArray();
        blobIds.put(blobId);
        JSONObject json = new JSONObject();
        json.put("blobIds", blobIds);

        GetBlobPersistenceSpectraS3Request dd = new
                GetBlobPersistenceSpectraS3Request(json.toString());
        GetBlobPersistenceSpectraS3Response rp = client.getBlobPersistenceSpectraS3(dd);
        return rp.getStringResult();
    }



    public static List<DataPolicy> getAllDataPolicies(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        return responsePolicy.getDataPolicyListResult().getDataPolicies();

    }

    public static void cleanupBuckets (Ds3Client client, String bucketName) throws IOException {
        HeadBucketResponse headBucketResponse = client.headBucket(new HeadBucketRequest(bucketName));
        if (headBucketResponse.getStatus() == HeadBucketResponse.Status.DOESNTEXIST) {
            return;
        }
        final GetBucketResponse response = client.getBucket(new GetBucketRequest(bucketName));
        for (final Contents contents : response.getListBucketResult().getObjects()) {
            client.deleteObject(new DeleteObjectRequest(bucketName, contents.getKey()));
        }
        final DeleteBucketSpectraS3Request delrequest =
                new DeleteBucketSpectraS3Request( bucketName );
        delrequest.withForce( true );
        client.deleteBucketSpectraS3( delrequest );
    }

    public static void cleanupAllBuckets (Ds3Client client) throws IOException {
        GetBucketsSpectraS3Request request = new GetBucketsSpectraS3Request();
        final GetBucketsSpectraS3Response response = client.getBucketsSpectraS3(request);
        response.getBucketListResult().getBuckets().forEach( b -> {
            if (b.getName().contains("spectra-") ||   (b.getName().contains("Spectra-")) ){
                return;
            }
            try {
                final GetBucketResponse bucketResponse = client.getBucket(new GetBucketRequest(b.getName()));
                for (final Contents contents : bucketResponse.getListBucketResult().getObjects()  ) {
                    client.deleteObject(new DeleteObjectRequest(b.getName(), contents.getKey()));
                }
                final DeleteBucketSpectraS3Request delrequest =
                        new DeleteBucketSpectraS3Request( b.getName() );
                delrequest.withForce( true );
                client.deleteBucketSpectraS3( delrequest );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        });
    }

    public static void cleanupAllObjects (Ds3Client client, String bucket) throws IOException {
        try {
            final GetBucketResponse bucketResponse = client.getBucket(new GetBucketRequest(bucket));
            for (final Contents contents : bucketResponse.getListBucketResult().getObjects()  ) {
                client.deleteObject(new DeleteObjectRequest(bucket, contents.getKey()));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    public static void updateGroupMember(UUID groupId, UUID userId) {
        try {
            Connection connection = getTestDatabaseConnection();
            checkAndUpdateGroup(groupId, userId, connection);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static int getBlobsPersistence(Ds3Client client) throws IOException {
        GetTapesSpectraS3Request getTapesSpectraS3Request = new GetTapesSpectraS3Request();
        GetTapesSpectraS3Response getTapesSpectraS3Response = client.getTapesSpectraS3( getTapesSpectraS3Request);
        UUID tapeId = getTapesSpectraS3Response.getTapeListResult().getTapes().get(0).getId();
        GetBlobsOnTapeSpectraS3Request getBlobsOnTapeSpectraS3Request = new GetBlobsOnTapeSpectraS3Request(tapeId);
        GetBlobsOnTapeSpectraS3Response getBlobsOnTapeSpectraS3Response = client.getBlobsOnTapeSpectraS3(getBlobsOnTapeSpectraS3Request);
        return getBlobsOnTapeSpectraS3Response.getBulkObjectListResult().getObjects().size();
    }

    public static List<StorageDomain> getStorageDomains(Ds3Client client) throws IOException {
        GetStorageDomainsSpectraS3Request getStorageDomainsSpectraS3Request = new GetStorageDomainsSpectraS3Request();
        GetStorageDomainsSpectraS3Response getStorageDomainsSpectraS3Response = client.getStorageDomainsSpectraS3( getStorageDomainsSpectraS3Request);
        return getStorageDomainsSpectraS3Response.getStorageDomainListResult().getStorageDomains();
    }



    public static List<StorageDomainMember> getStorageDomainMembers(Ds3Client client) throws IOException {
        GetStorageDomainMembersSpectraS3Request getStorageDomainMembersSpectraS3Request = new GetStorageDomainMembersSpectraS3Request();
        GetStorageDomainMembersSpectraS3Response resp = client.getStorageDomainMembersSpectraS3(getStorageDomainMembersSpectraS3Request);
        return resp.getStorageDomainMemberListResult().getStorageDomainMembers();
    }

    public static void clearAllCompletedJobs(Ds3Client client) throws IOException {
        ClearAllCompletedJobsSpectraS3Request clearAllCompletedJobsSpectraS3Request = new ClearAllCompletedJobsSpectraS3Request();
        client.clearAllCompletedJobsSpectraS3(clearAllCompletedJobsSpectraS3Request);
    }


    public static UUID getStorageDomainId (Ds3Client client, String storageDomainName) throws IOException {
        List<StorageDomain> all = getStorageDomains(client);
        Optional<StorageDomain> dualCopyTapeSD = all.stream()
                .filter(dp -> dp.getName().equals(storageDomainName)).findFirst();

        StorageDomain sd = dualCopyTapeSD.get();
        return sd.getId();
    }

    public static List<TapePartition> getTapePartitions(Ds3Client client ) throws IOException {
        GetTapePartitionsSpectraS3Request partitionsSpectraS3Request = new GetTapePartitionsSpectraS3Request();
        GetTapePartitionsSpectraS3Response partitionsResponse = client.getTapePartitionsSpectraS3(partitionsSpectraS3Request);
        List<TapePartition> partitions = partitionsResponse.getTapePartitionListResult().getTapePartitions();
        return partitions;
    }

    public static List<CompletedJob> getCompletedJobs(Ds3Client client) throws IOException {
        GetCompletedJobsSpectraS3Request getCompletedJobsSpectraS3Request = new GetCompletedJobsSpectraS3Request();
        GetCompletedJobsSpectraS3Response getCompletedJobsSpectraS3Response = client.getCompletedJobsSpectraS3(getCompletedJobsSpectraS3Request);
        return getCompletedJobsSpectraS3Response.getCompletedJobListResult().getCompletedJobs();
    }

    public static int getBlobCountOnPool(Ds3Client client, UUID partitionId ) throws IOException {
        GetStorageDomainMembersSpectraS3Request getStorageDomainMembersSpectraS3Request = new GetStorageDomainMembersSpectraS3Request();
        GetStorageDomainMembersSpectraS3Response getStorageDomainMembersSpectraS3Response = client.getStorageDomainMembersSpectraS3(getStorageDomainMembersSpectraS3Request);
        Optional<StorageDomainMember> member = getStorageDomainMembersSpectraS3Response.getStorageDomainMemberListResult().getStorageDomainMembers().stream()
                .filter(model -> model.getPoolPartitionId() !=null && model.getPoolPartitionId().equals(partitionId)).findFirst();

        GetDataPoliciesSpectraS3Request getDataPoliciesSpectraS3Request = new GetDataPoliciesSpectraS3Request();
        GetDataPoliciesSpectraS3Response getDataPoliciesSpectraS3Response = client.getDataPoliciesSpectraS3(getDataPoliciesSpectraS3Request);
        GetStorageDomainsSpectraS3Request storageDomainRequest = new GetStorageDomainsSpectraS3Request();
        GetStorageDomainsSpectraS3Response storageDomainResponse = client.getStorageDomainsSpectraS3(storageDomainRequest);
        GetPoolsSpectraS3Request getPoolsSpectraS3Request = new GetPoolsSpectraS3Request();
        GetPoolsSpectraS3Response getPoolsSpectraS3Response = client.getPoolsSpectraS3( getPoolsSpectraS3Request);
        int blobCount = 0;
        for (Pool pool : getPoolsSpectraS3Response.getPoolListResult().getPools()) {
            if (pool.getPartitionId() !=null && pool.getPartitionId().equals(member.get().getPoolPartitionId()) && pool.getQuiesced().equals(Quiesced.NO)) {
                GetBlobsOnPoolSpectraS3Request getBlobsOnPoolSpectraS3Request = new GetBlobsOnPoolSpectraS3Request(pool.getName());
                GetBlobsOnPoolSpectraS3Response getBlobsOnPoolSpectraS3Response = client.getBlobsOnPoolSpectraS3(getBlobsOnPoolSpectraS3Request);
                blobCount += getBlobsOnPoolSpectraS3Response.getBulkObjectListResult().getObjects().size();
                return blobCount;
            }

        }
        return blobCount;
    }


    public static void clearPersistenceRules(Ds3Client client,  UUID dataPolicyId) throws IOException {
        GetDataPersistenceRulesSpectraS3Request persistenceReq = new GetDataPersistenceRulesSpectraS3Request();
        persistenceReq.withDataPolicyId(dataPolicyId);
        GetDataPersistenceRulesSpectraS3Response persistenceResponse = client.getDataPersistenceRulesSpectraS3(persistenceReq);
        List<DataPersistenceRule> persistenceRules = persistenceResponse.getDataPersistenceRuleListResult().getDataPersistenceRules();
        for (DataPersistenceRule rule: persistenceRules) {
            DeleteDataPersistenceRuleSpectraS3Request req = new DeleteDataPersistenceRuleSpectraS3Request(rule.getId());
            client.deleteDataPersistenceRuleSpectraS3(req);
        }
    }

    public static void reclaimCache(Ds3Client client) throws IOException {
        ForceFullCacheReclaimSpectraS3Request forceFullCacheReclaimRequest = new ForceFullCacheReclaimSpectraS3Request();
        client.forceFullCacheReclaimSpectraS3(forceFullCacheReclaimRequest);
    }


    public static PoolPartition createPoolsPartition(Ds3Client client, String partitionName) throws IOException, SQLException {
        PutPoolPartitionSpectraS3Request putPoolPartitionSpectraS3Request = new PutPoolPartitionSpectraS3Request(partitionName,PoolType.NEARLINE);
        PutPoolPartitionSpectraS3Response partitionResp = client.putPoolPartitionSpectraS3(putPoolPartitionSpectraS3Request);
        createStorageDomain( client,STORAGE_DOMAIN_POOL_NAME);
        addPoolStorageDomainMember(client, STORAGE_DOMAIN_POOL_NAME, partitionResp.getPoolPartitionResult().getId().toString());
        return partitionResp.getPoolPartitionResult();
    }

    public static PoolPartition getPoolsPartition(Ds3Client client, String partitionName) throws IOException, SQLException {
        GetPoolsSpectraS3Request getPoolsSpectraS3Request = new GetPoolsSpectraS3Request();
        GetPoolsSpectraS3Response resp = client.getPoolsSpectraS3(getPoolsSpectraS3Request);

        //unquiesce pools

        for (Pool pool: resp.getPoolListResult().getPools()) {
            if (pool.getQuiesced().equals(Quiesced.YES)) {
                ModifyPoolSpectraS3Request req = new ModifyPoolSpectraS3Request(pool.getName());
                req.withQuiesced(Quiesced.PENDING);
                client.modifyPoolSpectraS3(req);
                Pool pooldetails = client.getPoolSpectraS3(new GetPoolSpectraS3Request(pool.getName())).getPoolResult();
                while (pooldetails.getQuiesced().equals(Quiesced.YES)) {
                    TestUtil.sleep(1000);
                    pooldetails = client.getPoolSpectraS3(new GetPoolSpectraS3Request(pool.getName())).getPoolResult();
                }
            }
        }

        GetPoolPartitionsSpectraS3Request partitionsSpectraS3Request = new GetPoolPartitionsSpectraS3Request();
        GetPoolPartitionsSpectraS3Response poolPartitionResp = client.getPoolPartitionsSpectraS3(partitionsSpectraS3Request);

        PoolPartition partition = poolPartitionResp.getPoolPartitionListResult().getPoolPartitions().stream()
                .filter(dp -> dp.getName().equals(partitionName)).findFirst()
                .orElseThrow(() -> new NoSuchElementException("No pool partition found with name: " + partitionName));
        updatePoolPartition(resp.getPoolListResult().getPools().get(0).getGuid(), partition.getId().toString());

        return partition;
    }

    public static void addPoolStorageDomainMember(Ds3Client client, String storageDomainName, String poolPartionId) throws IOException {
        UUID storageDomainId = getStorageDomainId(client, storageDomainName);
        PutPoolStorageDomainMemberSpectraS3Request putPoolStorageDomainMemberSpectraS3Request =
                new PutPoolStorageDomainMemberSpectraS3Request(UUID.fromString(poolPartionId), storageDomainId);
        client.putPoolStorageDomainMemberSpectraS3(putPoolStorageDomainMemberSpectraS3Request);

    }

    public static void createStorageDomain(Ds3Client client, String storageDomainName) throws IOException {
        GetStorageDomainsSpectraS3Request getStorageDomainsSpectraS3Request = new GetStorageDomainsSpectraS3Request();
        GetStorageDomainsSpectraS3Response getStorageDomainsSpectraS3Response = client.getStorageDomainsSpectraS3( getStorageDomainsSpectraS3Request);
        Optional<StorageDomain> foundSD = getStorageDomainsSpectraS3Response.getStorageDomainListResult().getStorageDomains().stream()
                .filter(dp -> dp.getName().equals(storageDomainName)).findFirst();
        if (foundSD.isPresent()) {
            return;
        }
        PutStorageDomainSpectraS3Request putStorageDomainSpectraS3Request = new PutStorageDomainSpectraS3Request(storageDomainName);
        putStorageDomainSpectraS3Request.withWriteOptimization(WriteOptimization.PERFORMANCE);
        PutStorageDomainSpectraS3Response resp = client.putStorageDomainSpectraS3(putStorageDomainSpectraS3Request);
        PutDataPolicySpectraS3Request putDataPolicySpectraS3Request = new PutDataPolicySpectraS3Request(DATA_POLICY_POOL_NAME);
    }


    public static DataPolicy createDataPolicy(Ds3Client client, String datapolicyName) throws IOException {
        GetDataPoliciesSpectraS3Request getDataPoliciesSpectraS3Request = new GetDataPoliciesSpectraS3Request();
        GetDataPoliciesSpectraS3Response dataPoliciesResp = client.getDataPoliciesSpectraS3(getDataPoliciesSpectraS3Request);
        Optional<DataPolicy> foundDP = dataPoliciesResp.getDataPolicyListResult().getDataPolicies().stream()
                .filter(dp -> dp.getName().equals(datapolicyName)).findFirst();
        if (foundDP.isPresent()) {
            return foundDP.get();
        }
        PutDataPolicySpectraS3Request putDataPolicySpectraS3Request = new PutDataPolicySpectraS3Request(datapolicyName);
        PutDataPolicySpectraS3Response resp = client.putDataPolicySpectraS3(putDataPolicySpectraS3Request);
        return resp.getDataPolicyResult();
    }

    public static void deleteExistingPools(Ds3Client client, String partitionName) throws IOException {
        DeletePoolPartitionSpectraS3Request deletePoolPartitionSpectraS3Request = new DeletePoolPartitionSpectraS3Request(partitionName);
        client.deletePoolPartitionSpectraS3(deletePoolPartitionSpectraS3Request);
    }


    public static void addJobName (Ds3Client client, String jobName, UUID jobId) throws IOException {
        ModifyJobSpectraS3Request modifyJobSpectraS3Request = new ModifyJobSpectraS3Request(jobId);
        modifyJobSpectraS3Request.withName(jobName);
        client.modifyJobSpectraS3(modifyJobSpectraS3Request);
    }

    public static void clearAllJobs(Ds3Client client) throws IOException {
        CancelAllJobsSpectraS3Request clearAllJobsSpectraS3Request = new CancelAllJobsSpectraS3Request();
        client.cancelAllJobsSpectraS3(clearAllJobsSpectraS3Request);
        clearAllCompletedJobs(client);
    }

    public static List<BlobStoreTaskInformation> getTasks(Ds3Client client) throws IOException {
        GetDataPlannerBlobStoreTasksSpectraS3Request getTasksSpectraS3Request = new GetDataPlannerBlobStoreTasksSpectraS3Request();
        getTasksSpectraS3Request.withFullDetails(true);
        GetDataPlannerBlobStoreTasksSpectraS3Response response = client.getDataPlannerBlobStoreTasksSpectraS3(getTasksSpectraS3Request);
        return response.getBlobStoreTasksInformationResult().getTasks();
    }

    public static void deleteObjects(Ds3Client cleint, String bucketName, List<String> objects) throws IOException {
        DeleteObjectsRequest deleteReq = new DeleteObjectsRequest(bucketName, objects);
        cleint.deleteObjects(deleteReq);
    }

    public static boolean isJobCompleted(Ds3Client client, UUID jobId) throws IOException {
        GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();
        GetActiveJobsSpectraS3Response activeJobsResponse = client.getActiveJobsSpectraS3( request );

        Optional<UUID> filteredId = activeJobsResponse.getActiveJobListResult().getActiveJobs().stream()
                .filter(model -> model.getId().equals(jobId)).findFirst().map(ActiveJob::getId) ;


        while (!filteredId.isEmpty()) {
            activeJobsResponse = client.getActiveJobsSpectraS3( request );
            filteredId = activeJobsResponse.getActiveJobListResult().getActiveJobs().stream()
                    .filter(model -> model.getId().equals(jobId)).findFirst().map(ActiveJob::getId) ;

            TestUtil.sleep(1000);
        }

        GetCompletedJobSpectraS3Request getCompletedJobSpectraS3Request = new GetCompletedJobSpectraS3Request(jobId.toString());
        GetCompletedJobSpectraS3Response completedJobResp = client.getCompletedJobSpectraS3(getCompletedJobSpectraS3Request);
        return completedJobResp.getCompletedJobResult().getId().equals(jobId);

    }

    public static void waitForJobsToComplete(Ds3Client client) throws IOException {
        GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();
        GetActiveJobsSpectraS3Response activeJobsResponse = client.getActiveJobsSpectraS3( request );

        List<ActiveJob> activeJobs = activeJobsResponse.getActiveJobListResult().getActiveJobs();


        while (!activeJobs.isEmpty()) {
            activeJobsResponse = client.getActiveJobsSpectraS3( request );
            activeJobs = activeJobsResponse.getActiveJobListResult().getActiveJobs();
            TestUtil.sleep(1000);

        }
    }

    public static void waitForCancellation(Ds3Client client, UUID jobId) throws IOException {
        // Wait until the job is no longer in the active jobs list
        GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();
        while (true) {
            GetActiveJobsSpectraS3Response response = client.getActiveJobsSpectraS3(request);
            boolean stillActive = response.getActiveJobListResult().getActiveJobs().stream()
                    .anyMatch(job -> job.getId().equals(jobId));
            if (!stillActive) {
                break;
            }
            TestUtil.sleep(2000);
        }
        // Wait until the cancelled job record appears, confirming cleanup is complete
        while (true) {
            try {
                client.getCanceledJobSpectraS3(new GetCanceledJobSpectraS3Request(jobId.toString()));
                break;
            } catch (final IOException ex) {
                TestUtil.sleep(2000);
            }
        }
    }

    public static void toggleIOM(Ds3Client client, boolean value) throws IOException {
        ModifyDataPathBackendSpectraS3Request modifyDataPathBackendSpectraS3Request = new ModifyDataPathBackendSpectraS3Request();
        modifyDataPathBackendSpectraS3Request.withIomEnabled(value);
        client.modifyDataPathBackendSpectraS3(modifyDataPathBackendSpectraS3Request);
    }


}

package com.spectralogic.s3.common.persistencetarget;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.shared.PoolObservable;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeRole;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.platform.persistencetarget.PersistenceTargetUtil;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class PersistenceTargetUtil_Test {

    @Test
    public void testFilterForWritablePools() {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);

        mockDaoDriver.createABMConfigSingleCopyOnPool();
        final StorageDomain storageDomain = mockDaoDriver.attainOneAndOnly(StorageDomain.class);
        final StorageDomainMember storageDomainMember = mockDaoDriver.attainOneAndOnly(StorageDomainMember.class);
        final PoolPartition poolPartition = mockDaoDriver.attain(PoolPartition.class, storageDomainMember.getPoolPartitionId());
        final PoolPartition otherPartition = mockDaoDriver.createPoolPartition(PoolType.ONLINE, "other-partition");

        final Pool availablePool1 = mockDaoDriver.createPool(poolPartition.getId(), PoolState.NORMAL);
        final Pool availablePool2 = mockDaoDriver.createPool(poolPartition.getId(), PoolState.NORMAL);
        final Pool quiescedPool = mockDaoDriver.createPool(poolPartition.getId(), PoolState.NORMAL);
        final Pool unlinkedPool = mockDaoDriver.createPool(otherPartition.getId(), PoolState.NORMAL);

        mockDaoDriver.updateBean(quiescedPool.setQuiesced(Quiesced.YES), Pool.QUIESCED);
        WhereClause filter = PersistenceTargetUtil.filterForWritablePools(
            null, // no bucket isolation
            storageDomain.getId(),
            1000L, // bytes to write
            null, // no unavailable pools
            true  // unallocated only
        );

        final Map<UUID, Pool> filteredPools = dbSupport.getServiceManager()
            .getRetriever(Pool.class)
            .retrieveAll(filter)
            .toMap();

        assertEquals(2, filteredPools.size(), "Should return exactly 2 available pools");
        assertTrue(filteredPools.containsKey(availablePool1.getId()), "Should include first available pool");
        assertTrue(filteredPools.containsKey(availablePool2.getId()), "Should include second available pool");
        assertFalse(filteredPools.containsKey(quiescedPool.getId()), "Should not include quiesced pool");
        assertFalse(filteredPools.containsKey(unlinkedPool.getId()), "Should not include pool from unlinked partition");

        final Pool lostPool = mockDaoDriver.createPool(poolPartition.getId(), PoolState.LOST);
        filter = PersistenceTargetUtil.filterForWritablePools(
            null,
            storageDomain.getId(),
            0L, // test with 0 bytes
            Set.of(availablePool1.getId()), // mark availablePool1 as unavailable
            true
        );

        Map<UUID, Pool> filtered = dbSupport.getServiceManager()
            .getRetriever(Pool.class)
            .retrieveAll(filter)
            .toMap();

        assertEquals(1, filtered.size(), "Should return only 1 pool");
        assertFalse(filtered.containsKey(availablePool1.getId()), "Should not include pool marked as unavailable");
        assertTrue(filtered.containsKey(availablePool2.getId()), "Should include second available pool");
        assertFalse(filtered.containsKey(lostPool.getId()), "Should not include LOST pool");

        final Pool smallCapacityPool = mockDaoDriver.createPool(poolPartition.getId(), PoolState.NORMAL);
        mockDaoDriver.updateBean(
            smallCapacityPool.setAvailableCapacity(500L),
            PoolObservable.AVAILABLE_CAPACITY
        );
        final Pool largeCapacityPool = mockDaoDriver.createPool(poolPartition.getId(), PoolState.NORMAL);
        mockDaoDriver.updateBean(
            largeCapacityPool.setAvailableCapacity(2000L),
            PoolObservable.AVAILABLE_CAPACITY
        );

        filter = PersistenceTargetUtil.filterForWritablePools(
            null,
            storageDomain.getId(),
            1000L,
            null,
            true
        );

        filtered = dbSupport.getServiceManager()
            .getRetriever(Pool.class)
            .retrieveAll(filter)
            .toMap();

        assertFalse(filtered.containsKey(smallCapacityPool.getId()),
            "Should not include pool with insufficient capacity (500 bytes < 1000 bytes required)");
        assertTrue(filtered.containsKey(largeCapacityPool.getId()),
            "Should include pool with sufficient capacity (2000 bytes >= 1000 bytes required)");

        filter = PersistenceTargetUtil.filterForWritablePools(
            null,
            storageDomain.getId(),
            400L,
            null,
            true
        );

        filtered = dbSupport.getServiceManager()
            .getRetriever(Pool.class)
            .retrieveAll(filter)
            .toMap();

        assertTrue(filtered.containsKey(smallCapacityPool.getId()),
            "Should include pool with sufficient capacity (500 bytes >= 400 bytes required)");
        assertTrue(filtered.containsKey(largeCapacityPool.getId()),
            "Should include pool with sufficient capacity (2000 bytes >= 400 bytes required)");
    }

    @Test
    public void testFilterForWritablePoolsBucketIsolation() {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);

        mockDaoDriver.createABMConfigSingleCopyOnPool();
        final StorageDomain storageDomain = mockDaoDriver.attainOneAndOnly(StorageDomain.class);
        final StorageDomainMember storageDomainMember = mockDaoDriver.attainOneAndOnly(StorageDomainMember.class);
        final PoolPartition poolPartition = mockDaoDriver.attain(PoolPartition.class, storageDomainMember.getPoolPartitionId());

        final Pool poolForBucket1 = mockDaoDriver.createPool(poolPartition.getId(), PoolState.NORMAL);
        final Pool poolForBucket2 = mockDaoDriver.createPool(poolPartition.getId(), PoolState.NORMAL);
        final Pool unallocatedPool = mockDaoDriver.createPool(poolPartition.getId(), PoolState.NORMAL);

        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly(DataPolicy.class);
        final DataPersistenceRule rule = mockDaoDriver.attainOneAndOnly(DataPersistenceRule.class);
        mockDaoDriver.updateBean(
            rule.setIsolationLevel(DataIsolationLevel.BUCKET_ISOLATED),
            DataPersistenceRule.ISOLATION_LEVEL
        );

        final Bucket bucket1 = mockDaoDriver.createBucket(null, dataPolicy.getId(), "bucket1");
        final Bucket bucket2 = mockDaoDriver.createBucket(null, dataPolicy.getId(), "bucket2");

        mockDaoDriver.updateBean(
            poolForBucket1
                .setBucketId(bucket1.getId())
                .setStorageDomainMemberId(storageDomainMember.getId())
                .setAssignedToStorageDomain(true),
            PersistenceTarget.BUCKET_ID,
            PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
            PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN
        );

        mockDaoDriver.updateBean(
            poolForBucket2
                .setBucketId(bucket2.getId())
                .setStorageDomainMemberId(storageDomainMember.getId())
                .setAssignedToStorageDomain(true),
            PersistenceTarget.BUCKET_ID,
            PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
            PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN
        );

        WhereClause filter = PersistenceTargetUtil.filterForWritablePools(
            null, // no bucket isolation
            storageDomain.getId(),
            100L,
            null,
            true // unallocated only
        );

        Map<UUID, Pool> filtered = dbSupport.getServiceManager()
            .getRetriever(Pool.class)
            .retrieveAll(filter)
            .toMap();

        assertEquals(1, filtered.size(), "Without bucket ID, should return only unallocated pool");
        assertTrue(filtered.containsKey(unallocatedPool.getId()), "Should include unallocated pool");
        assertFalse(filtered.containsKey(poolForBucket1.getId()), "Should not include pool allocated to bucket1");
        assertFalse(filtered.containsKey(poolForBucket2.getId()), "Should not include pool allocated to bucket2");

        final UUID isolatedBucketId1 = PersistenceTargetUtil.getIsolatedBucketId(
            bucket1.getId(),
            storageDomain.getId(),
            dbSupport.getServiceManager()
        );
        filter = PersistenceTargetUtil.filterForWritablePools(
            isolatedBucketId1,
            storageDomain.getId(),
            100L,
            null,
            false // allocated pools
        );

        filtered = dbSupport.getServiceManager()
            .getRetriever(Pool.class)
            .retrieveAll(filter)
            .toMap();

        assertEquals(1, filtered.size(), "With bucket1 ID and unallocated=false, should return only bucket1's pool");
        assertTrue(filtered.containsKey(poolForBucket1.getId()), "Should include pool allocated to bucket1");
        assertFalse(filtered.containsKey(poolForBucket2.getId()), "Should not include pool allocated to bucket2");
        assertFalse(filtered.containsKey(unallocatedPool.getId()), "Should not include unallocated pool");

        final UUID isolatedBucketId2 = PersistenceTargetUtil.getIsolatedBucketId(
            bucket2.getId(),
            storageDomain.getId(),
            dbSupport.getServiceManager()
        );
        filter = PersistenceTargetUtil.filterForWritablePools(
            isolatedBucketId2,
            storageDomain.getId(),
            100L,
            null,
            false // allocated pools
        );

        filtered = dbSupport.getServiceManager()
            .getRetriever(Pool.class)
            .retrieveAll(filter)
            .toMap();

        assertEquals(1, filtered.size(), "With bucket2 ID and unallocated=false, should return only bucket2's pool");
        assertTrue(filtered.containsKey(poolForBucket2.getId()), "Should include pool allocated to bucket2");
        assertFalse(filtered.containsKey(poolForBucket1.getId()), "Should not include pool allocated to bucket1");
        assertFalse(filtered.containsKey(unallocatedPool.getId()), "Should not include unallocated pool");

        filter = PersistenceTargetUtil.filterForWritablePools(
            isolatedBucketId1,
            storageDomain.getId(),
            100L,
            null,
            true // unallocated only
        );

        filtered = dbSupport.getServiceManager()
            .getRetriever(Pool.class)
            .retrieveAll(filter)
            .toMap();

        assertEquals(1, filtered.size(), "With bucket1 ID and unallocated=true, should return only unallocated pool");
        assertTrue(filtered.containsKey(unallocatedPool.getId()), "Should include unallocated pool");
        assertFalse(filtered.containsKey(poolForBucket1.getId()), "Should not include pool allocated to bucket1");
        assertFalse(filtered.containsKey(poolForBucket2.getId()), "Should not include pool allocated to bucket2");
    }

    @Test
    public void testFilterForWritableTapes() {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);

        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final StorageDomain storageDomain = mockDaoDriver.attainOneAndOnly(StorageDomain.class);
        final TapePartition tapePartition = mockDaoDriver.attainOneAndOnly(TapePartition.class);
        final TapePartition otherPartition = mockDaoDriver.createTapePartition(null, "other-partition");

        final Tape availableTape1 = mockDaoDriver.createTape(tapePartition.getId(), TapeState.NORMAL);
        final Tape availableTape2 = mockDaoDriver.createTape(tapePartition.getId(), TapeState.NORMAL);
        final Tape unlinkedTape = mockDaoDriver.createTape(otherPartition.getId(), TapeState.NORMAL);

        WhereClause filter = PersistenceTargetUtil.filterForWritableTapes(
            null, // no bucket isolation
            storageDomain.getId(),
            1000L, // bytes to write
            null, // no unavailable tapes
            null, // no specific partition
            true  // unallocated only
        );

        final Map<UUID, Tape> filteredTapes = dbSupport.getServiceManager()
            .getRetriever(Tape.class)
            .retrieveAll(filter)
            .toMap();

        assertEquals(2, filteredTapes.size(), "Should return exactly 2 available tapes");
        assertTrue(filteredTapes.containsKey(availableTape1.getId()), "Should include first available tape");
        assertTrue(filteredTapes.containsKey(availableTape2.getId()), "Should include second available tape");
        assertFalse(filteredTapes.containsKey(unlinkedTape.getId()), "Should not include tape from unlinked partition");

        mockDaoDriver.updateBean(
            tapePartition.setQuiesced(Quiesced.YES),
            TapePartition.QUIESCED
        );

        filter = PersistenceTargetUtil.filterForWritableTapes(
            null,
            storageDomain.getId(),
            1000L,
            null,
            null,
            true
        );

        Map<UUID, Tape> filtered = dbSupport.getServiceManager()
            .getRetriever(Tape.class)
            .retrieveAll(filter)
            .toMap();

        assertEquals(0, filtered.size(), "Should return 0 tapes when partition is quiesced");
        assertFalse(filtered.containsKey(availableTape1.getId()), "Should not include tape from quiesced partition");
        assertFalse(filtered.containsKey(availableTape2.getId()), "Should not include tape from quiesced partition");

        mockDaoDriver.updateBean(
            tapePartition.setQuiesced(Quiesced.NO),
            TapePartition.QUIESCED
        );

        filter = PersistenceTargetUtil.filterForWritableTapes(
            null,
            storageDomain.getId(),
            1000L,
            null,
            null,
            true
        );

        filtered = dbSupport.getServiceManager()
            .getRetriever(Tape.class)
            .retrieveAll(filter)
            .toMap();

        assertEquals(2, filtered.size(), "Should return 2 tapes after partition is unquiesced");
        assertTrue(filtered.containsKey(availableTape1.getId()), "Should include first available tape");
        assertTrue(filtered.containsKey(availableTape2.getId()), "Should include second available tape");

        final Tape lostTape = mockDaoDriver.createTape(tapePartition.getId(), TapeState.LOST);
        filter = PersistenceTargetUtil.filterForWritableTapes(
            null,
            storageDomain.getId(),
            0L, // test with 0 bytes
            Set.of(availableTape1.getId()), // mark availableTape1 as unavailable
            null,
            true
        );

        filtered = dbSupport.getServiceManager()
            .getRetriever(Tape.class)
            .retrieveAll(filter)
            .toMap();

        assertEquals(1, filtered.size(), "Should return only 1 tape");
        assertFalse(filtered.containsKey(availableTape1.getId()), "Should not include tape marked as unavailable");
        assertTrue(filtered.containsKey(availableTape2.getId()), "Should include second available tape");
        assertFalse(filtered.containsKey(lostTape.getId()), "Should not include LOST tape");

        final Tape smallCapacityTape = mockDaoDriver.createTape(tapePartition.getId(), TapeState.NORMAL);
        mockDaoDriver.updateBean(
            smallCapacityTape.setAvailableRawCapacity(500L),
            Tape.AVAILABLE_RAW_CAPACITY
        );
        final Tape largeCapacityTape = mockDaoDriver.createTape(tapePartition.getId(), TapeState.NORMAL);
        mockDaoDriver.updateBean(
            largeCapacityTape.setAvailableRawCapacity(20000L),
            Tape.AVAILABLE_RAW_CAPACITY
        );

        filter = PersistenceTargetUtil.filterForWritableTapes(
            null,
            storageDomain.getId(),
            1000L,
            null,
            null,
            true
        );

        filtered = dbSupport.getServiceManager()
            .getRetriever(Tape.class)
            .retrieveAll(filter)
            .toMap();

        assertFalse(filtered.containsKey(smallCapacityTape.getId()),
            "Should not include tape with insufficient capacity (500 bytes < 1000 bytes required)");
        assertTrue(filtered.containsKey(availableTape2.getId()),
            "Should include tape with sufficient capacity (2000 bytes >= 1000 bytes required)");

        filter = PersistenceTargetUtil.filterForWritableTapes(
            null,
            storageDomain.getId(),
            400L,
            null,
            null,
            true
        );

        filtered = dbSupport.getServiceManager()
            .getRetriever(Tape.class)
            .retrieveAll(filter)
            .toMap();

        assertTrue(filtered.containsKey(availableTape1.getId()),
            "Should include tape with sufficient capacity (500 bytes >= 400 bytes required)");
        assertTrue(filtered.containsKey(availableTape2.getId()),
            "Should include tape with sufficient capacity (2000 bytes >= 400 bytes required)");
    }

    @Test
    public void testFilterForWritableTapesBucketIsolation() {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);

        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final StorageDomain storageDomain = mockDaoDriver.attainOneAndOnly(StorageDomain.class);
        final TapePartition tapePartition = mockDaoDriver.attainOneAndOnly(TapePartition.class);

        final Tape tapeForBucket1 = mockDaoDriver.createTape(tapePartition.getId(), TapeState.NORMAL);
        final Tape tapeForBucket2 = mockDaoDriver.createTape(tapePartition.getId(), TapeState.NORMAL);
        final Tape unallocatedTape = mockDaoDriver.createTape(tapePartition.getId(), TapeState.NORMAL);
        final StorageDomainMember storageDomainMember = mockDaoDriver.getServiceManager()
                .getRetriever(StorageDomainMember.class).attain(
                        Require.beanPropertyEquals(
                                StorageDomainMember.TAPE_TYPE,
                                tapeForBucket1.getType()
                        )
                );

        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly(DataPolicy.class);
        final DataPersistenceRule rule = mockDaoDriver.attainOneAndOnly(DataPersistenceRule.class);
        mockDaoDriver.updateBean(
            rule.setIsolationLevel(DataIsolationLevel.BUCKET_ISOLATED),
            DataPersistenceRule.ISOLATION_LEVEL
        );

        final Bucket bucket1 = mockDaoDriver.createBucket(null, dataPolicy.getId(), "bucket1");
        final Bucket bucket2 = mockDaoDriver.createBucket(null, dataPolicy.getId(), "bucket2");

        mockDaoDriver.updateBean(
            tapeForBucket1
                .setBucketId(bucket1.getId())
                .setStorageDomainMemberId(storageDomainMember.getId())
                .setAssignedToStorageDomain(true),
            PersistenceTarget.BUCKET_ID,
            PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
            PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN
        );

        mockDaoDriver.updateBean(
            tapeForBucket2
                .setBucketId(bucket2.getId())
                .setStorageDomainMemberId(storageDomainMember.getId())
                .setAssignedToStorageDomain(true),
            PersistenceTarget.BUCKET_ID,
            PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
            PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN
        );

        WhereClause filter = PersistenceTargetUtil.filterForWritableTapes(
            null, // no bucket isolation
            storageDomain.getId(),
            100L,
            null,
            null,
            true // unallocated only
        );

        Map<UUID, Tape> filtered = dbSupport.getServiceManager()
            .getRetriever(Tape.class)
            .retrieveAll(filter)
            .toMap();

        assertEquals(1, filtered.size(), "Without bucket ID, should return only unallocated tape");
        assertTrue(filtered.containsKey(unallocatedTape.getId()), "Should include unallocated tape");
        assertFalse(filtered.containsKey(tapeForBucket1.getId()), "Should not include tape allocated to bucket1");
        assertFalse(filtered.containsKey(tapeForBucket2.getId()), "Should not include tape allocated to bucket2");

        final UUID isolatedBucketId1 = PersistenceTargetUtil.getIsolatedBucketId(
            bucket1.getId(),
            storageDomain.getId(),
            dbSupport.getServiceManager()
        );
        filter = PersistenceTargetUtil.filterForWritableTapes(
            isolatedBucketId1,
            storageDomain.getId(),
            100L,
            null,
            null,
            false // allocated tapes
        );

        filtered = dbSupport.getServiceManager()
            .getRetriever(Tape.class)
            .retrieveAll(filter)
            .toMap();

        assertEquals(1, filtered.size(), "With bucket1 ID and unallocated=false, should return only bucket1's tape");
        assertTrue(filtered.containsKey(tapeForBucket1.getId()), "Should include tape allocated to bucket1");
        assertFalse(filtered.containsKey(tapeForBucket2.getId()), "Should not include tape allocated to bucket2");
        assertFalse(filtered.containsKey(unallocatedTape.getId()), "Should not include unallocated tape");

        final UUID isolatedBucketId2 = PersistenceTargetUtil.getIsolatedBucketId(
            bucket2.getId(),
            storageDomain.getId(),
            dbSupport.getServiceManager()
        );
        filter = PersistenceTargetUtil.filterForWritableTapes(
            isolatedBucketId2,
            storageDomain.getId(),
            100L,
            null,
            null,
            false // allocated tapes
        );

        filtered = dbSupport.getServiceManager()
            .getRetriever(Tape.class)
            .retrieveAll(filter)
            .toMap();

        assertEquals(1, filtered.size(), "With bucket2 ID and unallocated=false, should return only bucket2's tape");
        assertTrue(filtered.containsKey(tapeForBucket2.getId()), "Should include tape allocated to bucket2");
        assertFalse(filtered.containsKey(tapeForBucket1.getId()), "Should not include tape allocated to bucket1");
        assertFalse(filtered.containsKey(unallocatedTape.getId()), "Should not include unallocated tape");

        filter = PersistenceTargetUtil.filterForWritableTapes(
            isolatedBucketId1,
            storageDomain.getId(),
            100L,
            null,
            null,
            true // unallocated only
        );

        filtered = dbSupport.getServiceManager()
            .getRetriever(Tape.class)
            .retrieveAll(filter)
            .toMap();

        assertEquals(1, filtered.size(), "With bucket1 ID and unallocated=true, should return only unallocated tape");
        assertTrue(filtered.containsKey(unallocatedTape.getId()), "Should include unallocated tape");
        assertFalse(filtered.containsKey(tapeForBucket1.getId()), "Should not include tape allocated to bucket1");
        assertFalse(filtered.containsKey(tapeForBucket2.getId()), "Should not include tape allocated to bucket2");
    }

    @Test
    public void testFilterForWritableTapesExclusions() {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);

        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final StorageDomain storageDomain = mockDaoDriver.attainOneAndOnly(StorageDomain.class);
        final TapePartition tapePartition = mockDaoDriver.attainOneAndOnly(TapePartition.class);

        final Tape availableTape = mockDaoDriver.createTape(tapePartition.getId(), TapeState.NORMAL);

        final Tape writeProtectedTape = mockDaoDriver.createTape(tapePartition.getId(), TapeState.NORMAL);
        mockDaoDriver.updateBean(
            writeProtectedTape.setWriteProtected(true),
            Tape.WRITE_PROTECTED
        );

        WhereClause filter = PersistenceTargetUtil.filterForWritableTapes(
            null,
            storageDomain.getId(),
            100L,
            null,
            null,
            true
        );

        Map<UUID, Tape> filtered = dbSupport.getServiceManager()
            .getRetriever(Tape.class)
            .retrieveAll(filter)
            .toMap();

        assertTrue(filtered.containsKey(availableTape.getId()), "Should include normal tape");
        assertFalse(filtered.containsKey(writeProtectedTape.getId()), "Should not include WRITE_PROTECTED tape");

        final Tape fullTape = mockDaoDriver.createTape(tapePartition.getId(), TapeState.NORMAL);
        mockDaoDriver.updateBean(
            fullTape.setFullOfData(true),
            Tape.FULL_OF_DATA
        );

        filter = PersistenceTargetUtil.filterForWritableTapes(
            null,
            storageDomain.getId(),
            100L,
            null,
            null,
            true
        );

        filtered = dbSupport.getServiceManager()
            .getRetriever(Tape.class)
            .retrieveAll(filter)
            .toMap();

        assertTrue(filtered.containsKey(availableTape.getId()), "Should include normal tape");
        assertFalse(filtered.containsKey(fullTape.getId()), "Should not include FULL_OF_DATA tape");

        final Tape ejectPendingTape = mockDaoDriver.createTape(tapePartition.getId(), TapeState.NORMAL);
        mockDaoDriver.updateBean(
            ejectPendingTape.setEjectPending(new Date()),
            Tape.EJECT_PENDING
        );

        filter = PersistenceTargetUtil.filterForWritableTapes(
            null,
            storageDomain.getId(),
            100L,
            null,
            null,
            true
        );

        filtered = dbSupport.getServiceManager()
            .getRetriever(Tape.class)
            .retrieveAll(filter)
            .toMap();

        assertTrue(filtered.containsKey(availableTape.getId()), "Should include normal tape");
        assertFalse(filtered.containsKey(ejectPendingTape.getId()), "Should not include tape with EJECT_PENDING");

        final Tape testRoleTape = mockDaoDriver.createTape(tapePartition.getId(), TapeState.NORMAL);
        mockDaoDriver.updateBean(
            testRoleTape.setRole(TapeRole.TEST),
            Tape.ROLE
        );

        filter = PersistenceTargetUtil.filterForWritableTapes(
            null,
            storageDomain.getId(),
            100L,
            null,
            null,
            true
        );

        filtered = dbSupport.getServiceManager()
            .getRetriever(Tape.class)
            .retrieveAll(filter)
            .toMap();

        assertTrue(filtered.containsKey(availableTape.getId()), "Should include tape with NORMAL role");
        assertFalse(filtered.containsKey(testRoleTape.getId()), "Should not include tape with TEST role");
    }

}

/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target;

import java.util.List;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.AzureTargetFailure;
import com.spectralogic.s3.common.dao.domain.target.BlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.ImportAzureTargetDirective;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.rpc.tape.domain.BlobOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectMetadataKeyValue;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.target.BucketOnPublicCloud;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.common.testfrmwrk.target.MockAzureConnection;
import com.spectralogic.s3.dataplanner.backend.target.task.ImportAzureTargetTask;
import com.spectralogic.s3.dataplanner.testfrmwrk.TargetTaskBuilder;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class ImportAzureTargetTask_Test
{

    @Test
    public void testImportDoesSoWhenSingleSegment()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final PoolPartition poolPartition =
                mockDaoDriver.createPoolPartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain.getId(), poolPartition.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        
        final AzureTarget target = mockDaoDriver.createAzureTarget( null );
        mockDaoDriver.createAzureDataReplicationRule( 
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        
        mockDaoDriver.createImportAzureTargetDirective( 
                target.getId(), user.getId(), dataPolicy.getId(), "blah" );
        
        final List< BucketOnPublicCloud > segments = 
                MockAzureConnection.createDiscoverableSegments( dbSupport, target, 1 );
        whackAllObjects( dbSupport );
        MockAzureConnection connection =
                new MockAzureConnection( dbSupport.getServiceManager(), segments );
        connection.expectTakeOwnershipCalls();
        ImportAzureTargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withAzureConnectionFactory(connection.toConnectionFactory())
                .buildImportAzureTargetTask(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId());
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );
        int totalBlobCount = verifyAllImportedCorrectly( dbSupport, segments );
        final Object actual5 = dbSupport.getServiceManager().getRetriever( AzureTargetFailure.class ).getCount();
        assertEquals( 0, actual5, "Should notta been any failures.");
        final Object actual4 = dbSupport.getServiceManager().getRetriever( DegradedBlob.class ).getCount();
        assertEquals(totalBlobCount, actual4, "Shoulda created a degraded blob for every blob.");
        final Object actual3 = dbSupport.getServiceManager().getRetriever( BlobAzureTarget.class ).getCount();
        assertEquals( totalBlobCount, actual3, "Shoulda created blob on target records.");
        connection.assertTakeOwnershipCallCountEquals( 1 );
        
        /*
         * Re-trying a successful import shouldn't hurt anything
         */
        connection = new MockAzureConnection( dbSupport.getServiceManager(), segments );
        connection.expectTakeOwnershipCalls();
        mockDaoDriver.createImportAzureTargetDirective( 
                target.getId(), user.getId(), dataPolicy.getId(), "blah" );
        task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withAzureConnectionFactory(connection.toConnectionFactory())
                .buildImportAzureTargetTask(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId());
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );
        totalBlobCount = verifyAllImportedCorrectly( dbSupport, segments );
        final Object actual2 = dbSupport.getServiceManager().getRetriever( AzureTargetFailure.class ).getCount();
        assertEquals((Object) 0, actual2, "Should notta been any failures.");
        final Object actual1 = dbSupport.getServiceManager().getRetriever( DegradedBlob.class ).getCount();
        assertEquals((Object) totalBlobCount, actual1, "Shoulda created a degraded blob for every blob.");
        final Object actual = dbSupport.getServiceManager().getRetriever( BlobAzureTarget.class ).getCount();
        assertEquals((Object) totalBlobCount, actual, "Shoulda created blob on target records.");
        connection.assertTakeOwnershipCallCountEquals( 1 );
    }
    
    
    @Test
    public void testImportDoesSoWhenMultipleSegments()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain1 =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomain storageDomain2 =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME + "2" );
        final PoolPartition poolPartition1 =
                mockDaoDriver.createPoolPartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        final PoolPartition poolPartition2 =
                mockDaoDriver.createPoolPartition( null, MockDaoDriver.DEFAULT_PARTITION_SN + "2" );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain1.getId(), poolPartition1.getId() );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain2.getId(), poolPartition2.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain1.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain2.getId() );
        
        final AzureTarget target = mockDaoDriver.createAzureTarget( null );
        mockDaoDriver.createAzureDataReplicationRule( 
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        
        mockDaoDriver.createImportAzureTargetDirective( 
                target.getId(), user.getId(), dataPolicy.getId(), "blah" );
        
        final List< BucketOnPublicCloud > segments = 
                MockAzureConnection.createDiscoverableSegments( dbSupport, target, 5 );
        whackAllObjects( dbSupport );
        MockAzureConnection connection =
                new MockAzureConnection( dbSupport.getServiceManager(), segments );
        connection.expectTakeOwnershipCalls();
        ImportAzureTargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withAzureConnectionFactory(connection.toConnectionFactory())
                .buildImportAzureTargetTask(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId());
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );
        int totalBlobCount = verifyAllImportedCorrectly( dbSupport, segments );
        final Object actual5 = dbSupport.getServiceManager().getRetriever( AzureTargetFailure.class ).getCount();
        assertEquals((Object) 0, actual5, "Should notta been any failures.");
        final Object actual4 = dbSupport.getServiceManager().getRetriever( DegradedBlob.class ).getCount();
        assertEquals((Object) (totalBlobCount * 2), actual4, "Shoulda created a degraded blob for every blob for every persistence rule.");
        final Object actual3 = dbSupport.getServiceManager().getRetriever( BlobAzureTarget.class ).getCount();
        assertEquals((Object) totalBlobCount, actual3, "Shoulda created blob on target records.");
        connection.assertTakeOwnershipCallCountEquals( 1 );
        
        /*
         * Re-trying a successful import shouldn't hurt anything
         */
        connection = new MockAzureConnection( dbSupport.getServiceManager(), segments );
        connection.expectTakeOwnershipCalls();
        mockDaoDriver.createImportAzureTargetDirective( 
                target.getId(), user.getId(), dataPolicy.getId(), "blah" );
        task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withAzureConnectionFactory(connection.toConnectionFactory())
                .buildImportAzureTargetTask(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId());
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );
        totalBlobCount = verifyAllImportedCorrectly( dbSupport, segments );
        final Object actual2 = dbSupport.getServiceManager().getRetriever( AzureTargetFailure.class ).getCount();
        assertEquals((Object) 0, actual2, "Should notta been any failures.");
        final Object actual1 = dbSupport.getServiceManager().getRetriever( DegradedBlob.class ).getCount();
        assertEquals((Object) (totalBlobCount * 2), actual1, "Shoulda created 2 degraded blobs for every blob.");
        final Object actual = dbSupport.getServiceManager().getRetriever( BlobAzureTarget.class ).getCount();
        assertEquals((Object) totalBlobCount, actual, "Shoulda created blob on target records.");
        connection.assertTakeOwnershipCallCountEquals( 1 );
    }
    
    
    @Test
    public void testImportWhenTakeOwnershipFailsGeneratesFailure()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final PoolPartition poolPartition =
                mockDaoDriver.createPoolPartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain.getId(), poolPartition.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        
        final AzureTarget target = mockDaoDriver.createAzureTarget( null );
        mockDaoDriver.createAzureDataReplicationRule( 
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        
        mockDaoDriver.createImportAzureTargetDirective( 
                target.getId(), user.getId(), dataPolicy.getId(), "blah" );
        
        final List< BucketOnPublicCloud > segments = 
                MockAzureConnection.createDiscoverableSegments( dbSupport, target, 1 );
        whackAllObjects( dbSupport );
        final MockAzureConnection connection =
                new MockAzureConnection( dbSupport.getServiceManager(), segments );
        final ImportAzureTargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withAzureConnectionFactory(connection.toConnectionFactory())
                .buildImportAzureTargetTask(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId());
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );
        final int totalBlobCount = verifyAllImportedCorrectly( dbSupport, segments );
        final Object actual2 = dbSupport.getServiceManager().getRetriever( AzureTargetFailure.class ).getCount();
        assertEquals((Object) 1, actual2, "Shoulda been a failures.");
        final Object actual1 = dbSupport.getServiceManager().getRetriever( ImportAzureTargetDirective.class ).getCount();
        assertEquals((Object) 0, actual1, "Shoulda created no degraded blobs.");
        final Object actual = dbSupport.getServiceManager().getRetriever( BlobAzureTarget.class ).getCount();
        assertEquals((Object) totalBlobCount, actual, "Shoulda created blob on target records.");
    }
    
    
    @Test
    public void testImportWhenNoDirectiveToDoSoGeneratesFailure()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final PoolPartition poolPartition =
                mockDaoDriver.createPoolPartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain.getId(), poolPartition.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        
        final AzureTarget target = mockDaoDriver.createAzureTarget( null );
        mockDaoDriver.createAzureDataReplicationRule( 
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        
        final List< BucketOnPublicCloud > segments = 
                MockAzureConnection.createDiscoverableSegments( dbSupport, target, 1 );
        whackAllObjects( dbSupport );
        final MockAzureConnection connection =
                new MockAzureConnection( dbSupport.getServiceManager(), segments );
        connection.expectTakeOwnershipCalls();
        final ImportAzureTargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withAzureConnectionFactory(connection.toConnectionFactory())
                .buildImportAzureTargetTask(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId());
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        final Object actual3 = dbSupport.getServiceManager().getRetriever( AzureTargetFailure.class ).getCount();
        assertEquals((Object) 1, actual3, "Shoulda been a failures.");
        final Object actual2 = dbSupport.getServiceManager().getRetriever( ImportAzureTargetDirective.class ).getCount();
        assertEquals((Object) 0, actual2, "Shoulda created no degraded blobs.");
        final Object actual1 = dbSupport.getServiceManager().getRetriever( Bucket.class ).getCount();
        assertEquals((Object) 0, actual1, "Should notta created any buckets, objects, or blobs.");
        final Object actual = dbSupport.getServiceManager().getRetriever( BlobAzureTarget.class ).getCount();
        assertEquals((Object) 0, actual, "Should notta created blob on target records.");
    }
    
    
    private void whackAllObjects( final DatabaseSupport dbSupport )
    {
        dbSupport.getDataManager().deleteBeans( Blob.class, Require.nothing() );
        dbSupport.getDataManager().deleteBeans( S3Object.class, Require.nothing() );
        dbSupport.getDataManager().deleteBeans( Bucket.class, Require.nothing() );
    }
    
    
    private int verifyAllImportedCorrectly( 
            final DatabaseSupport dbSupport,
            final List< BucketOnPublicCloud > segments )
    {
        final Object actual = dbSupport.getServiceManager().getRetriever( ImportAzureTargetDirective.class ).getCount();
        assertEquals((Object) 0, actual, "Shoulda whacked import directive.");

        int totalBlobCount = 0;
        for ( final BucketOnPublicCloud bucket : segments )
        {
            final Bucket daoBucket = dbSupport.getServiceManager().getRetriever( Bucket.class ).attain(
                    Bucket.NAME, bucket.getBucketName() );
            for ( final S3ObjectOnMedia oom : bucket.getObjects() )
            {
                final S3Object daoObject =
                        dbSupport.getServiceManager().getRetriever( S3Object.class ).attain( oom.getId() );
                final Object expected6 = daoBucket.getId();
                assertEquals(expected6,  daoObject.getBucketId(), "Shoulda populated object correctly.");
                assertEquals((Object) extractCreationDate(oom).longValue(),  daoObject.getCreationDate().getTime(), "Shoulda populated object correctly.");
                final Object expected5 = extractExpectedObjectMetadataCount( oom );
                assertEquals(expected5,  dbSupport.getServiceManager().getRetriever(S3ObjectProperty.class).getCount(
                        S3ObjectProperty.OBJECT_ID, oom.getId()), "Shoulda populated object correctly.");
                for ( final BlobOnMedia bom : oom.getBlobs() )
                {
                    ++totalBlobCount;
                    final Blob daoBlob =
                            dbSupport.getServiceManager().getRetriever( Blob.class ).attain( bom.getId() );
                    final Object expected4 = daoObject.getId();
                    assertEquals(expected4,  daoBlob.getObjectId(), "Shoulda populated blob correctly.");
                    final Object expected3 = bom.getChecksum();
                    assertEquals(expected3,  daoBlob.getChecksum(), "Shoulda populated blob correctly.");
                    final Object expected2 = bom.getChecksumType();
                    assertEquals(expected2,  daoBlob.getChecksumType(), "Shoulda populated blob correctly.");
                    final Object expected1 = bom.getLength();
                    assertEquals(expected1,  daoBlob.getLength(), "Shoulda populated blob correctly.");
                    final Object expected = bom.getOffset();
                    assertEquals(expected,  daoBlob.getByteOffset(), "Shoulda populated blob correctly.");
                }
            }
        }
        
        if ( 0 == totalBlobCount )
        {
            throw new RuntimeException( "Something is wrong.  Total blob count was zero." );
        }
        
        return totalBlobCount;
    }
    
    
    private Long extractCreationDate( final S3ObjectOnMedia oom )
    {
        for ( final S3ObjectMetadataKeyValue metadata : oom.getMetadata() )
        {
            if ( KeyValueObservable.CREATION_DATE.equals( metadata.getKey() ) )
            {
                return Long.valueOf( metadata.getValue() );
            }
        }
        
        return null;
    }
    
    
    private int extractExpectedObjectMetadataCount( final S3ObjectOnMedia oom )
    {
        int retval = 0;
        for ( final S3ObjectMetadataKeyValue metadata : oom.getMetadata() )
        {
            if ( KeyValueObservable.CREATION_DATE.equals( metadata.getKey() )
                    || KeyValueObservable.TOTAL_BLOB_COUNT.equals( metadata.getKey() ) )
            {
                continue;
            }
            ++retval;
        }
        
        return retval;
    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }
    
}

/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.SuspectBlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.common.rpc.tape.TapeResourceFailureCode;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailure;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailureType;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailures;
import com.spectralogic.s3.common.rpc.tape.domain.BlobOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.BucketOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsToVerify;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.BasicTestsInvocationHandler.MethodInvokeData;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.domain.Failure;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;


public final class VerifyTapeTask_Test
{
    @Test
    public void testRunWhenNothingToVerifyDueToLackOfStorageDomainAssignmentMarksTaskAsCompleted()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape.setVerifyPending( BlobStoreTaskPriority.values()[ 0 ] ),
                Tape.VERIFY_PENDING );
        mockDaoDriver.updateBean(
                tape.setTotalRawCapacity( 500 * 1024L * 1024L * 1024L ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );
        
        final VerifyTapeTask task = createVerifyTapeTask(tape.getId(), dbSupport.getServiceManager());
        
        task.prepareForExecutionIfPossible( new MockTapeDriveResource(), new MockTapeAvailability() );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda marked task as completed.");
        assertEquals(null, tape.getLastVerified(), "Should notta updated last verified.");
        assertNull(tape.getVerifyPending(), "Shoulda reset verify pending flag.");
        assertEquals(null, tape.getPartiallyVerifiedEndOfTape(), "Should notta updated partial verification date.");
    }
    
    
    @Test
    public void testRunWhenNothingToVerifyDueToNonNormalTapeStateMarksTaskAsCompleted()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        Tape tape = mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.updateBean(
                tape.setVerifyPending( BlobStoreTaskPriority.values()[ 0 ] ),
                Tape.VERIFY_PENDING );
        mockDaoDriver.updateBean(
                tape.setTotalRawCapacity( 500 * 1024L * 1024L * 1024L ),
                Tape.TOTAL_RAW_CAPACITY );
        final StorageDomain storageDomain = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean(
                tape.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );

        final VerifyTapeTask task = createVerifyTapeTask(tape.getId(), dbSupport.getServiceManager());
        
        task.prepareForExecutionIfPossible( new MockTapeDriveResource(), new MockTapeAvailability() );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda marked task as completed.");
        assertEquals(null, tape.getLastVerified(), "Should notta updated last verified yet.");
        assertNotNull( tape.getVerifyPending(), "Should notta reset flag to verify.");
        assertEquals(null, tape.getPartiallyVerifiedEndOfTape(), "Should notta updated partial verification date.");
    }
    
    
    @Test
    public void testRunWhenEntireContentsOfTapeFitInSingleSegmentWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape.setVerifyPending( BlobStoreTaskPriority.values()[ 0 ] ),
                Tape.VERIFY_PENDING );
        mockDaoDriver.updateBean(
                tape.setTotalRawCapacity( 500 * 1024L * 1024L * 1024L ),
                Tape.TOTAL_RAW_CAPACITY );
        final StorageDomain storageDomain = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean(
                tape.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );

        final VerifyTapeTask task = createVerifyTapeTask(tape.getId(), dbSupport.getServiceManager());
        
        final MockTapeDriveResource tdr = new MockTapeDriveResource();
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        tdr.setInvocationListener( InterfaceProxyFactory.getProxy( TapeDriveResource.class, btih ) );
        task.prepareForExecutionIfPossible( tdr, new MockTapeAvailability() );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        final Method methodVerifyData = ReflectUtil.getMethod( TapeDriveResource.class, "verifyData" );
        assertEquals(1,  btih.getMethodCallCount(methodVerifyData), "Shoulda sent down the creation date as an optional param.");
        assertEquals(3,  ((S3ObjectsToVerify) btih.getMethodInvokeData(methodVerifyData)
                .get(0).getArgs().get(0)).getOptionalS3ObjectMetadataKeys().length, "Shoulda sent down the creation date as an optional param.");

        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as ready.");
        final String message1 = String.valueOf(tape.getLastVerified());
        assertNull(tape.getLastVerified(), "Should notta updated last verified yet.");

        task.prepareForExecutionIfPossible( tdr, new MockTapeAvailability() );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda marked task as completed.");
        assertNotNull( tape.getLastVerified(), "Shoulda updated last verified.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated verification failure.");
        final String message = String.valueOf(tape.getVerifyPending());
        assertNull(tape.getVerifyPending(), "Shoulda cleared verify pending flag.");
        assertEquals(null, tape.getPartiallyVerifiedEndOfTape(), "Should notta updated partial verification date.");
    }
    
    
    @Test
    public void testRunWhenEntireContentsOfTapeDoNotFitInSingleSegmentWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape.setVerifyPending( BlobStoreTaskPriority.values()[ 0 ] ),
                Tape.VERIFY_PENDING );
        mockDaoDriver.updateBean(
                tape.setTotalRawCapacity( 500 * 1024L * 1024L * 1024L ),
                Tape.TOTAL_RAW_CAPACITY );
        final List< UUID > orderedBlobIds = new ArrayList<>();
        for ( int i = 0; i < 12; ++i )
        {
            final S3Object o = mockDaoDriver.createObject( null, "o" + i, -1 );
            final List< Blob > blobs = mockDaoDriver.createBlobs( o.getId(), 10, 1024L * 1024 * 1024 );
            for ( final Blob blob : blobs )
            {
                mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );
                orderedBlobIds.add( blob.getId() );
            }
        }
        final StorageDomain storageDomain = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean(
                tape.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );

        final VerifyTapeTask task = createVerifyTapeTask(tape.getId(), dbSupport.getServiceManager());
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeDriveResource listener = InterfaceProxyFactory.getProxy( TapeDriveResource.class, btih );
        final MockTapeDriveResource tdr = new MockTapeDriveResource();
        tdr.setInvocationListener( listener );
        task.prepareForExecutionIfPossible( tdr, new MockTapeAvailability() );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as ready.");

        assertNull(tape.getLastVerified(), "Should notta updated last verified yet.");

        task.prepareForExecutionIfPossible( tdr, new MockTapeAvailability() );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as ready.");
        assertNull(tape.getLastVerified(), "Should notta updated last verified yet.");

        task.prepareForExecutionIfPossible( tdr, new MockTapeAvailability() );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as ready.");
        assertNull(tape.getLastVerified(), "Should notta updated last verified yet.");
        assertNotNull( tape.getVerifyPending(), "Should notta reset flag to verify yet.");

        task.prepareForExecutionIfPossible( tdr, new MockTapeAvailability() );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda marked task as completed.");
        assertNotNull( tape.getLastVerified(), "Shoulda updated last verified.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated verification failure.");
        assertNull(tape.getVerifyPending(), "Shoulda cleared flag to verify tape.");

        final List< UUID > sentBlobIds = new ArrayList<>();
        final Method methodVerifyData = ReflectUtil.getMethod( TapeDriveResource.class, "verifyData" );
        for ( final MethodInvokeData mid : btih.getMethodInvokeData( methodVerifyData ) )
        {
            final S3ObjectsToVerify payload = (S3ObjectsToVerify)mid.getArgs().get( 0 );
            for ( final BucketOnMedia bom : payload.getBuckets() )
            {
                for ( final S3ObjectOnMedia oom : bom.getObjects() )
                {
                    for ( final BlobOnMedia blob : oom.getBlobs() )
                    {
                        sentBlobIds.add( blob.getId() );
                    }
                }
            }
        }
        assertEquals(orderedBlobIds, sentBlobIds, "Shoulda verified blobs in the order in which they were physically placed on tape.");
        assertEquals(null, tape.getPartiallyVerifiedEndOfTape(), "Should notta updated partial verification date.");
    }
    
    
    @Test
    public void testRunWhenEntireContentsOfTapeDoNotFitInSingleSegmentWorks2()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape.setTotalRawCapacity( 500 * 1024L * 1024L * 1024L ),
                Tape.TOTAL_RAW_CAPACITY );
        for ( int i = 0; i < 12; ++i )
        {
            final S3Object o = mockDaoDriver.createObject( null, "o" + i, -1 );
            final List< Blob > blobs = mockDaoDriver.createBlobs( o.getId(), 20, 1024L * 1024 * 1024 );
            for ( final Blob blob : blobs )
            {
                mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );
            }
        }
        final StorageDomain storageDomain = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean(
                tape.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );

        final VerifyTapeTask task = createVerifyTapeTask(tape.getId(), dbSupport.getServiceManager());
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final MockTapeAvailability mta = new MockTapeAvailability();
        final MockTapeDriveResource tdr = new MockTapeDriveResource();
        tdr.setInvocationListener( InterfaceProxyFactory.getProxy( TapeDriveResource.class, btih ) );
        task.prepareForExecutionIfPossible( tdr, mta );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as ready.");
        assertNull(tape.getLastVerified(), "Should notta updated last verified yet.");
        final S3ObjectsToVerify verifyPayload =
                (S3ObjectsToVerify)btih.getMethodInvokeData(
                        ReflectUtil.getMethod( TapeDriveResource.class, "verifyData" ) )
                        .get( 0 ).getArgs().get( 0 );
        assertEquals(1,  verifyPayload.getBuckets().length, "Shoulda sent down many objects.");
        int blobCount = 0;
        for ( final S3ObjectOnMedia oom : verifyPayload.getBuckets()[ 0 ].getObjects() )
        {
            blobCount += oom.getBlobs().length;
        }
        assertEquals(50,  blobCount, "Shoulda sent down many objects.");

        task.prepareForExecutionIfPossible( tdr, mta );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as ready.");
        assertNull(tape.getVerifyPending(), "Should notta updated last verified yet.");

        task.prepareForExecutionIfPossible( tdr, mta );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as ready.");
        assertNull(tape.getLastVerified(), "Should notta updated last verified yet.");

        task.prepareForExecutionIfPossible( tdr, mta );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as ready.");
        assertNull(tape.getLastVerified(), "Should notta updated last verified yet.");

        task.prepareForExecutionIfPossible( tdr, mta );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as ready.");
        final String message = String.valueOf(tape.getLastVerified());
        assertNull(tape.getLastVerified(), "Should notta updated last verified yet.");

        task.prepareForExecutionIfPossible( tdr, mta );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda marked task as completed.");
        assertNotNull( tape.getLastVerified(), "Shoulda updated last verified.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated verification failure.");
        assertEquals(null, tape.getPartiallyVerifiedEndOfTape(), "Should notta updated partial verification date.");
    }
    
    
    @Test
    public void testRunWhenFailuresReportedResultsInEventualBlobLoss()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final UUID partitionId = mockDaoDriver.createTapePartition( null, "tp1" ).getId();
        Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape.setVerifyPending( BlobStoreTaskPriority.values()[ 0 ] ),
                Tape.VERIFY_PENDING );
        mockDaoDriver.updateBean(
                tape.setTotalRawCapacity( 500 * 1024L * 1024L * 1024L ),
                Tape.TOTAL_RAW_CAPACITY );
        final StorageDomain storageDomain = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean(
                tape.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final UUID td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn1", tape.getId() ).getId();
        final UUID td2 = mockDaoDriver.createTapeDrive( partitionId, "tdsn2" ).getId();
        final UUID td3 = mockDaoDriver.createTapeDrive( partitionId, "tdsn3" ).getId();
        final UUID td4 = mockDaoDriver.createTapeDrive( partitionId, "tdsn4" ).getId();
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );

        final VerifyTapeTask task = createVerifyTapeTask(tape.getId(), dbSupport.getServiceManager());
        
        final MockTapeDriveResource tdr = new MockTapeDriveResource();
        tdr.setVerifyDataResult( 
                BeanFactory.newBean( BlobIoFailures.class ).setFailures( new BlobIoFailure[] 
                        { BeanFactory.newBean( BlobIoFailure.class )
                        .setFailure( BlobIoFailureType.values()[ 0 ] )
                        .setBlobId( blob.getId() ) } ) );
        task.prepareForExecutionIfPossible(
                tdr, 
                new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as ready.");
        final String message3 = String.valueOf(tape.getLastVerified());
        assertNull(tape.getLastVerified(), "Should notta updated last verified yet.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated verification failure.");

        task.prepareForExecutionIfPossible( 
                tdr, 
                new MockTapeAvailability().setDriveId( td2 ).setTapePartitionId( partitionId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as ready.");
        assertNull(tape.getLastVerified(), "Should notta updated last verified yet.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated another verification failure.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(BlobTape.class).getCount(), "Should notta recorded blob loss yet.");

        task.prepareForExecutionIfPossible(
                tdr, 
                new MockTapeAvailability().setDriveId( td3 ).setTapePartitionId( partitionId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );

        assertEquals(BlobStoreTaskState.READY, task.getState(), "Should notta marked task as completed yet.");
        assertNull(tape.getLastVerified(), "Should notta updated last verified.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated another verification failure.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(), "Shoulda recorded blob suspect.");
        assertNotNull( tape.getVerifyPending(), "Should notta cleared flag to verify tape yet.");

        tdr.setVerifyDataResult( BeanFactory.newBean( BlobIoFailures.class ) );
        
        task.prepareForExecutionIfPossible(
                tdr, 
                new MockTapeAvailability().setDriveId( td4 ).setTapePartitionId( partitionId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda marked task as completed.");
        assertNotNull( tape.getLastVerified(), "Shoulda updated last verified.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated another verification failure.");
        assertNull(tape.getVerifyPending(), "Shoulda cleared flag to verify tape.");
        assertEquals(null, tape.getPartiallyVerifiedEndOfTape(), "Should notta updated partial verification date.");
    }
    
    
    @Test
    public void testRunWhenFailuresReportedResultsInEventualBlobLossWhenComplexSetOfBlobsToVerify()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4" );
        final Blob b4 = mockDaoDriver.getBlobFor( o4.getId() );
        final S3Object o5 = mockDaoDriver.createObject( null, "o5" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        final UUID partitionId = mockDaoDriver.createTapePartition( null, "tp1" ).getId();
        Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape.setVerifyPending( BlobStoreTaskPriority.values()[ 0 ] ),
                Tape.VERIFY_PENDING );
        mockDaoDriver.updateBean(
                tape.setTotalRawCapacity( 500 * 1024L * 1024L * 1024L ),
                Tape.TOTAL_RAW_CAPACITY );
        final StorageDomain storageDomain = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean(
                tape.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final UUID td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn1", tape.getId() ).getId();
        final UUID td2 = mockDaoDriver.createTapeDrive( partitionId, "tdsn2" ).getId();
        final UUID td3 = mockDaoDriver.createTapeDrive( partitionId, "tdsn3" ).getId();
        final UUID td4 = mockDaoDriver.createTapeDrive( partitionId, "tdsn4" ).getId();
        final UUID td5 = mockDaoDriver.createTapeDrive( partitionId, "tdsn5" ).getId();
        final UUID td6 = mockDaoDriver.createTapeDrive( partitionId, "tdsn6" ).getId();
        mockDaoDriver.putBlobOnTape( tape.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), b3.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), b4.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), b5.getId() );
        
        mockDaoDriver.putBlobOnTape( tape2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( tape2.getId(), b3.getId() );

        final VerifyTapeTask task = new VerifyTapeTask( BlobStoreTaskPriority.values()[ 0 ],
                tape.getId(),
                2,
                new MockDiskManager(dbSupport.getServiceManager()),
                new TapeFailureManagement(dbSupport.getServiceManager()),
                dbSupport.getServiceManager());
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final MockTapeDriveResource tdr = new MockTapeDriveResource();
        tdr.setInvocationListener( InterfaceProxyFactory.getProxy( TapeDriveResource.class, btih ) );
        tdr.setVerifyDataResult( 
                BeanFactory.newBean( BlobIoFailures.class ).setFailures( new BlobIoFailure[] 
                        { BeanFactory.newBean( BlobIoFailure.class )
                        .setFailure( BlobIoFailureType.values()[ 0 ] )
                        .setBlobId( b1.getId() ) } ) );
        task.prepareForExecutionIfPossible(
                tdr, 
                new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        final S3ObjectsToVerify firstVerify = 
                (S3ObjectsToVerify)btih.getMethodInvokeData().get( 0 ).getArgs().get( 0 );
        assertEquals(2,  firstVerify.getBuckets()[0].getObjects().length, "Shoulda been 2 objects in first segment.");
        final Set< UUID > blobIds = CollectionFactory.toSet(
                firstVerify.getBuckets()[ 0 ].getObjects()[ 0 ].getBlobs()[ 0 ].getId(),
                firstVerify.getBuckets()[ 0 ].getObjects()[ 1 ].getBlobs()[ 0 ].getId() );
        final Object expected1 = CollectionFactory.toSet( b1.getId(), b2.getId() );
        assertEquals(expected1, blobIds, "Shoulda verified blobs according to their recorded order indexes.");
        final Object expected = b1.getId();
        assertEquals(expected, firstVerify.getBuckets()[ 0 ].getObjects()[ 0 ].getBlobs()[ 0 ].getId(), "The blobs verified within each segment should be sorted.");

        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as ready.");
        final String message6 = String.valueOf(tape.getLastVerified());
        assertNull(tape.getLastVerified(), "Should notta updated last verified yet.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated verification failure.");

        task.prepareForExecutionIfPossible( 
                tdr, 
                new MockTapeAvailability().setDriveId( td2 ).setTapePartitionId( partitionId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as ready.");
        assertNull(tape.getLastVerified(), "Should notta updated last verified yet.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated another verification failure.");
        assertEquals(0,dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(), "Should notta recorded blob suspect yet.");
        assertEquals(7,  dbSupport.getServiceManager().getRetriever(BlobTape.class).getCount(), "Should notta recorded blob loss yet.");

        task.prepareForExecutionIfPossible(
                tdr, 
                new MockTapeAvailability().setDriveId( td3 ).setTapePartitionId( partitionId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );

        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Should notta marked task as completed yet.");
        final String message4 = String.valueOf(tape.getLastVerified());
        assertNull(tape.getLastVerified(), "Should notta updated last verified.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated another verification failure.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(), "Shoulda recorded blob suspect.");
        assertEquals(7,  dbSupport.getServiceManager().getRetriever(BlobTape.class).getCount(), "Should notta whacked blob record for being suspect.");

        tdr.setVerifyDataResult( 
                BeanFactory.newBean( BlobIoFailures.class ).setFailures( new BlobIoFailure[] 
                        { BeanFactory.newBean( BlobIoFailure.class )
                        .setFailure( BlobIoFailureType.values()[ 0 ] )
                        .setBlobId( b4.getId() ) } ) );
        task.prepareForExecutionIfPossible(
                tdr, 
                new MockTapeAvailability().setDriveId( td4 ).setTapePartitionId( partitionId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as ready.");
        final String message3 = String.valueOf(tape.getLastVerified());
        assertNull(tape.getLastVerified(), "Should notta updated last verified yet.");
        assertEquals(4,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated verification failure.");

        task.prepareForExecutionIfPossible( 
                tdr, 
                new MockTapeAvailability().setDriveId( td5 ).setTapePartitionId( partitionId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as ready.");
        assertNull(tape.getLastVerified(), "Should notta updated last verified yet.");
        assertEquals(5,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated another verification failure.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(), "Should notta recorded blob suspect yet.");
        assertNotNull( tape.getVerifyPending(), "Should notta cleared verify pending flag yet.");

        task.prepareForExecutionIfPossible(
                tdr, 
                new MockTapeAvailability().setDriveId( td6 ).setTapePartitionId( partitionId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Should notta marked task as completed yet.");
        assertNull(tape.getLastVerified(), "Should notta updated last verified.");
        assertEquals(6,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated another verification failure.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(), "Shoulda recorded blob suspect.");

        tdr.setVerifyDataResult( 
                BeanFactory.newBean( BlobIoFailures.class ).setFailures(
                        (BlobIoFailure[])Array.newInstance( BlobIoFailure.class, 0 ) ) );
        
        int i = 10;
        while ( --i > 0 && BlobStoreTaskState.READY == task.getState() )
        {
            task.prepareForExecutionIfPossible( tdr, new MockTapeAvailability().setDriveId(
                    mockDaoDriver.createTapeDrive( partitionId, "tdsn-" + i )
                                 .getId() )
                                                                               .setTapePartitionId( partitionId ) );
    
            TestUtil.invokeAndWaitUnchecked( task );
        }
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Should marked task as completed.");
        assertNotNull( tape.getLastVerified(), "Shoulda updated last verified.");
        assertEquals(6,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated another verification failure.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(), "Shoulda recorded no further suspect blobs.");
        assertEquals(7,  dbSupport.getServiceManager().getRetriever(BlobTape.class).getCount(), "Should notta whacked blob record for being suspect.");
        assertNull(tape.getVerifyPending(), "Shoulda cleared verify pending flag.");
        assertEquals(null, tape.getPartiallyVerifiedEndOfTape(), "Should notta updated partial verification date.");
    }
    
    
    @Test
    public void testRunWhenFailuresReportedResultsInRetryThatIfItSucceedsVerificationPasses()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final UUID partitionId = mockDaoDriver.createTapePartition( null, "tp1" ).getId();
        Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape.setVerifyPending( BlobStoreTaskPriority.values()[ 0 ] ),
                Tape.VERIFY_PENDING );
        mockDaoDriver.updateBean(
                tape.setTotalRawCapacity( 500 * 1024L * 1024L * 1024L ),
                Tape.TOTAL_RAW_CAPACITY );
        final StorageDomain storageDomain = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean(
                tape.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final UUID td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn1", tape.getId() ).getId();
        final UUID td2 = mockDaoDriver.createTapeDrive( partitionId, "tdsn2" ).getId();
        final UUID td3 = mockDaoDriver.createTapeDrive( partitionId, "tdsn3" ).getId();
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );

        final VerifyTapeTask task = createVerifyTapeTask(tape.getId(), dbSupport.getServiceManager());
        
        final MockTapeDriveResource tdr = new MockTapeDriveResource();
        tdr.setVerifyDataResult( 
                BeanFactory.newBean( BlobIoFailures.class ).setFailures( new BlobIoFailure[] 
                        { BeanFactory.newBean( BlobIoFailure.class )
                        .setFailure( BlobIoFailureType.values()[ 0 ] )
                        .setBlobId( blob.getId() ) } ) );
        task.prepareForExecutionIfPossible(
                tdr, 
                new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as ready.");
        assertNull(tape.getLastVerified(), "Should notta updated last verified yet." );
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated verification failure.");

        tdr.setVerifyDataResult( 
                BeanFactory.newBean( BlobIoFailures.class ).setFailures(
                        (BlobIoFailure[])Array.newInstance( BlobIoFailure.class, 0 ) ) );
        task.prepareForExecutionIfPossible( 
                tdr, 
                new MockTapeAvailability().setDriveId( td2 ).setTapePartitionId( partitionId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as ready.");
        assertNull(tape.getLastVerified(), "Should notta updated last verified yet.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated another verification failure.");
        assertNotNull( tape.getVerifyPending(), "Should notta cleared flag to verify tape yet.");

        task.prepareForExecutionIfPossible(
                tdr, 
                new MockTapeAvailability().setDriveId( td3 ).setTapePartitionId( partitionId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda marked task as completed.");
        assertNotNull( tape.getLastVerified(), "Shoulda updated last verified.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated another verification failure.");
        assertNull(tape.getVerifyPending(), "Shoulda cleared flag to verify tape.");
        assertEquals(null, tape.getPartiallyVerifiedEndOfTape(), "Should notta updated partial verification date.");
    }
    
    
    @Test
    public void testRunWhenFailuresReportedRepeatedlyEventuallyResultsInRequiringDifferentTapeDrive()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final UUID partitionId = mockDaoDriver.createTapePartition( null, "tp1" ).getId();
        Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape.setTotalRawCapacity( 500 * 1024L * 1024L * 1024L ),
                Tape.TOTAL_RAW_CAPACITY );
        final StorageDomain storageDomain = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean(
                tape.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final UUID td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn", tape.getId() ).getId();
        final UUID td2 = mockDaoDriver.createTapeDrive( partitionId, "tdsn2", null ).getId();
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );

        final VerifyTapeTask task = createVerifyTapeTask(tape.getId(), dbSupport.getServiceManager());
        
        final MockTapeDriveResource tdr = new MockTapeDriveResource();
        tdr.setVerifyDataResult( 
                BeanFactory.newBean( BlobIoFailures.class ).setFailures( new BlobIoFailure[] 
                        { BeanFactory.newBean( BlobIoFailure.class )
                        .setFailure( BlobIoFailureType.values()[ 0 ] )
                        .setBlobId( blob.getId() ) } ) );
        task.prepareForExecutionIfPossible(
                tdr, 
                new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as ready.");
        assertNull(tape.getLastVerified(), "Should notta updated last verified yet.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated verification failure.");

        task.prepareForExecutionIfPossible( 
                tdr,
                new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as ready.");
        assertNull(tape.getLastVerified(), "Should notta updated last verified yet.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated verification failure.");
        assertFalse(task.canUseTapeAlreadyInDrive(new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) ), "Should notta been willing to retry against same tape drive.");
        assertTrue(
                task.canUseTapeAlreadyInDrive(new MockTapeAvailability().setDriveId( td2 ).setTapePartitionId( partitionId ).setPreferredTape( task.getTapeId() ) ),
                "Shoulda been willing to retry against different tape drive."
                 );

        task.prepareForExecutionIfPossible(
                tdr, 
                new MockTapeAvailability().setDriveId( td2 ) );
        assertEquals(BlobStoreTaskState.PENDING_EXECUTION, task.getState(), "Shoulda been willing to retry against different tape drive.");
        assertEquals(null, tape.getPartiallyVerifiedEndOfTape(), "Should notta updated partial verification date.");
    }
    
    @Test
    public void testRunPartialVerifyWhenNothingToVerify()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        configurePartialVerify( mockDaoDriver );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape.setVerifyPending( BlobStoreTaskPriority.values()[ 0 ] ),
                Tape.VERIFY_PENDING );
        mockDaoDriver.updateBean(
                tape.setTotalRawCapacity( 500 * 1024L * 1024L * 1024L ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );

        final VerifyTapeTask task = createVerifyTapeTask(tape.getId(), dbSupport.getServiceManager());
        
        task.prepareForExecutionIfPossible( new MockTapeDriveResource(), new MockTapeAvailability() );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda marked task as completed.");
        assertEquals(null, tape.getPartiallyVerifiedEndOfTape(), "Should notta updated last verified yet.");

        assertNull(tape.getVerifyPending(),"Shoulda cleared flag to verify tape.");
        assertEquals(null, tape.getLastVerified(), "Should notta updated last verification date for full verification since this wasn't full.");
    }
    
    
    @Test
    public void testRunPartialVerifyWhenNothingToVerifyDueToNonNormalTapeStateMarksTaskAsCompleted()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        configurePartialVerify( mockDaoDriver );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        Tape tape = mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.updateBean(
                tape.setVerifyPending( BlobStoreTaskPriority.values()[ 0 ] ),
                Tape.VERIFY_PENDING );
        mockDaoDriver.updateBean(
                tape.setTotalRawCapacity( 500 * 1024L * 1024L * 1024L ),
                Tape.TOTAL_RAW_CAPACITY );
        final StorageDomain storageDomain = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean(
                tape.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );

        final VerifyTapeTask task = createVerifyTapeTask(tape.getId(), dbSupport.getServiceManager());
        
        task.prepareForExecutionIfPossible( new MockTapeDriveResource(), new MockTapeAvailability() );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda marked task as completed.");
        assertEquals(null, tape.getPartiallyVerifiedEndOfTape(), "Should notta updated last verified yet.");
        assertNotNull( tape.getVerifyPending(), "Should notta reset flag to verify.");
        assertEquals(null, tape.getLastVerified(), "Should notta updated last verification date for full verification since this wasn't full.");
    }
    
    
    @Test
    public void testRunPartialVerifyWhenNothingToVerifyDueToNoBlobsMarksTaskAsCompleted()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        configurePartialVerify( mockDaoDriver );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape.setVerifyPending( BlobStoreTaskPriority.values()[ 0 ] ),
                Tape.VERIFY_PENDING );
        mockDaoDriver.updateBean(
                tape.setTotalRawCapacity( 500 * 1024L * 1024L * 1024L ),
                Tape.TOTAL_RAW_CAPACITY );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean(
                tape.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );

        final VerifyTapeTask task = createVerifyTapeTask(tape.getId(), dbSupport.getServiceManager());
        
        task.prepareForExecutionIfPossible( new MockTapeDriveResource(), new MockTapeAvailability() );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda marked task as completed.");
        assertEquals(null, tape.getPartiallyVerifiedEndOfTape(), "Should notta updated last verified.");
        assertNull(tape.getVerifyPending(), "Shoulda reset verify flag.");
        assertEquals( null,
                tape.getLastVerified(),
                "Should notta updated last verification date since it wasn't verified.");
    }
    
    
    @Test
    public void testRunPartialVerifyWhenTapeRawCapacityIsTheBasisWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        configurePartialVerify( mockDaoDriver );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 30 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 10 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 9 );
        final Blob blob3 = mockDaoDriver.getBlobFor( o3.getId() );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape.setVerifyPending( BlobStoreTaskPriority.values()[ 0 ] ),
                Tape.VERIFY_PENDING );
        mockDaoDriver.updateBean(
                tape.setTotalRawCapacity( 100L ),
                Tape.TOTAL_RAW_CAPACITY );
        final StorageDomain storageDomain = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean(
                tape.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob3.getId() );

        final VerifyTapeTask task = createVerifyTapeTask(tape.getId(), dbSupport.getServiceManager());
        
        final MockTapeDriveResource tdr = new MockTapeDriveResource();
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        tdr.setInvocationListener( InterfaceProxyFactory.getProxy( TapeDriveResource.class, btih ) );
        task.prepareForExecutionIfPossible( tdr, new MockTapeAvailability() );
        
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated verification failure.");

        final Method methodVerifyData = ReflectUtil.getMethod( TapeDriveResource.class, "verifyData" );
        assertEquals(1,  btih.getMethodCallCount(methodVerifyData), "Shoulda sent down the creation date as an optional param.");
        assertEquals(3,  ((S3ObjectsToVerify) btih.getMethodInvokeData(methodVerifyData)
                .get(0).getArgs().get(0)).getOptionalS3ObjectMetadataKeys().length, "Shoulda sent down the creation date as an optional param.");
        final Object expected = CollectionFactory.toSet( blob2.getId(), blob3.getId() );
        assertEquals(expected, parseBlobIds( btih.getMethodInvokeData( methodVerifyData ).get( 0 ) ), "Shoulda sent down blobs 2 and 3.");

        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda marked task as complete.");
        assertNotNull( tape.getPartiallyVerifiedEndOfTape(), "Shoulda updated last verified.");
        assertNull(tape.getVerifyPending(),"Shoulda cleared verify pending flag.");
        assertEquals(null, tape.getLastVerified(), "Should notta updated last verification date for full verification since this wasn't full.");
    }
    
    
    @Test
    public void testRunPartialVerifyWorksWhenBlobLogicalSizeIsTheBasis()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        configurePartialVerify( mockDaoDriver );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 30 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 10 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 9 );
        final Blob blob3 = mockDaoDriver.getBlobFor( o3.getId() );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape.setVerifyPending( BlobStoreTaskPriority.values()[ 0 ] ),
                Tape.VERIFY_PENDING );
        mockDaoDriver.updateBean(
                tape.setTotalRawCapacity( null ),
                Tape.TOTAL_RAW_CAPACITY );
        final StorageDomain storageDomain = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean(
                tape.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob3.getId() );

        final VerifyTapeTask task = createVerifyTapeTask(tape.getId(), dbSupport.getServiceManager());
        
        final MockTapeDriveResource tdr = new MockTapeDriveResource();
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        tdr.setInvocationListener( InterfaceProxyFactory.getProxy( TapeDriveResource.class, btih ) );
        task.prepareForExecutionIfPossible( tdr, new MockTapeAvailability() );
        
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated verification failure.");

        final Method methodVerifyData = ReflectUtil.getMethod( TapeDriveResource.class, "verifyData" );
        assertEquals(1,  btih.getMethodCallCount(methodVerifyData), "Shoulda sent down the creation date as an optional param.");
        assertEquals(3,  ((S3ObjectsToVerify) btih.getMethodInvokeData(methodVerifyData)
                .get(0).getArgs().get(0)).getOptionalS3ObjectMetadataKeys().length, "Shoulda sent down the creation date as an optional param.");
        final Object expected = CollectionFactory.toSet( blob3.getId() );
        assertEquals(expected, parseBlobIds( btih.getMethodInvokeData( methodVerifyData ).get( 0 ) ), "Shoulda sent down blob 3.");

        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda marked task as complete.");
        assertNotNull( tape.getPartiallyVerifiedEndOfTape(), "Shoulda updated last verified.");
        assertNull(tape.getVerifyPending(), "Shoulda cleared verify pending flag."  );
        assertEquals(null, tape.getLastVerified(), "Should notta updated last verification date for full verification since this wasn't full.");
    }
    
    
    @Test
    public void testRunPartialVerifySendsDownBlobsInCorrectOrder()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        configurePartialVerify( mockDaoDriver );
        
        final int numBlobs = 10;
        final List< Blob > blobs = new ArrayList<>();
        for ( int i = 0; i < numBlobs; ++i )
        {
            final S3Object o = mockDaoDriver.createObject( null, "o" + i );
            final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
            blobs.add( blob );
        }
        
        Collections.shuffle( blobs );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape.setTotalRawCapacity( 100000L ),
                Tape.TOTAL_RAW_CAPACITY );
        final List< UUID > orderedBlobIds = new ArrayList<>();
        for ( final Blob blob : blobs )
        {
            orderedBlobIds.add( blob.getId() );
            mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );
        }
        
        final StorageDomain storageDomain = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean(
                tape.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );

        final VerifyTapeTask task = createVerifyTapeTask(tape.getId(), dbSupport.getServiceManager());
        
        final MockTapeDriveResource tdr = new MockTapeDriveResource();
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        tdr.setInvocationListener( InterfaceProxyFactory.getProxy( TapeDriveResource.class, btih ) );
        task.prepareForExecutionIfPossible( tdr, new MockTapeAvailability() );
        
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated verification failure.");

        final Method methodVerifyData = ReflectUtil.getMethod( TapeDriveResource.class, "verifyData" );
        assertEquals(1,  btih.getMethodCallCount(methodVerifyData), "Shoulda sent down the creation date as an optional param.");
        assertEquals(3,  ((S3ObjectsToVerify) btih.getMethodInvokeData(methodVerifyData)
                .get(0).getArgs().get(0)).getOptionalS3ObjectMetadataKeys().length, "Shoulda sent down the creation date as an optional param.");
        assertEquals(orderedBlobIds, parseOrderedBlobIds( btih.getMethodInvokeData( methodVerifyData ).get( 0 ) ), "Shoulda sent down blobs in correct order.");

        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda marked task as complete.");
        assertNotNull( tape.getPartiallyVerifiedEndOfTape(), "Shoulda updated last verified.");
        assertEquals(null, tape.getLastVerified(), "Should notta updated last verification date for full verification since this wasn't full.");
    }
    
    
    @Test
    public void testRunPartialVerifyWhenFailuresReportedResultsInEventualBlobLoss()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        configurePartialVerify( mockDaoDriver );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 30 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 10 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 9 );
        final Blob blob3 = mockDaoDriver.getBlobFor( o3.getId() );
        final UUID partitionId = mockDaoDriver.createTapePartition( null, "tp1" ).getId();
        Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape.setVerifyPending( BlobStoreTaskPriority.values()[ 0 ] ),
                Tape.VERIFY_PENDING );
        mockDaoDriver.updateBean(
                tape.setTotalRawCapacity( null ),
                Tape.TOTAL_RAW_CAPACITY );
        final StorageDomain storageDomain = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean(
                tape.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final UUID td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn1", tape.getId() ).getId();
        final UUID td2 = mockDaoDriver.createTapeDrive( partitionId, "tdsn2" ).getId();
        final UUID td3 = mockDaoDriver.createTapeDrive( partitionId, "tdsn3" ).getId();
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob3.getId() );

        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final VerifyTapeTask task = createVerifyTapeTask(tape.getId(), dbSupport.getServiceManager(), tapeFailureManagement);

        final MockTapeDriveResource tdr = new MockTapeDriveResource();
        tdr.setVerifyDataResult( 
                BeanFactory.newBean( BlobIoFailures.class ).setFailures( new BlobIoFailure[] 
                        { BeanFactory.newBean( BlobIoFailure.class )
                        .setFailure( BlobIoFailureType.values()[ 0 ] )
                        .setBlobId( blob3.getId() ) } ) );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        tdr.setInvocationListener( InterfaceProxyFactory.getProxy( TapeDriveResource.class, btih ) );
        
        final Method methodVerifyData = ReflectUtil.getMethod( TapeDriveResource.class, "verifyData" );

        // Attempt #1
        btih.reset();
        task.prepareForExecutionIfPossible(
                tdr, 
                new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) );
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated verification failure.");
        assertEquals(1,  btih.getMethodCallCount(methodVerifyData), "Shoulda sent down the creation date as an optional param.");
        assertEquals(3,  ((S3ObjectsToVerify) btih.getMethodInvokeData(methodVerifyData)
                .get(0).getArgs().get(0)).getOptionalS3ObjectMetadataKeys().length, "Shoulda sent down the creation date as an optional param.");
        final Object expected2 = CollectionFactory.toSet( blob3.getId() );
        assertEquals(expected2, parseBlobIds( btih.getMethodInvokeData( methodVerifyData ).get( 0 ) ), "Shoulda sent down blob 3.");
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as complete.");
        assertNull(tape.getPartiallyVerifiedEndOfTape(),"Should notta updated last verified yet.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(), "Should notta reported suspect blob yet.");

        // Attempt #2
        btih.reset();
        task.prepareForExecutionIfPossible(
                tdr, 
                new MockTapeAvailability().setDriveId( td2 ).setTapePartitionId( partitionId ) );
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated verification failure.");
        assertEquals(1,  btih.getMethodCallCount(methodVerifyData), "Shoulda sent down the creation date as an optional param.");
        assertEquals(3,  ((S3ObjectsToVerify) btih.getMethodInvokeData(methodVerifyData)
                .get(0).getArgs().get(0)).getOptionalS3ObjectMetadataKeys().length, "Shoulda sent down the creation date as an optional param.");
        final Object expected1 = CollectionFactory.toSet( blob3.getId() );
        assertEquals(expected1, parseBlobIds( btih.getMethodInvokeData( methodVerifyData ).get( 0 ) ), "Shoulda sent down blob 3.");
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as complete.");
        assertNull(tape.getPartiallyVerifiedEndOfTape(), "Should notta updated last verified yet.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(), "Should notta reported suspect blob yet.");
        assertNotNull( tape.getVerifyPending(), "Should notta cleared verify pending flag yet.");

        // Attempt #3
        btih.reset();
        task.prepareForExecutionIfPossible(
                tdr, 
                new MockTapeAvailability().setDriveId( td3 ).setTapePartitionId( partitionId ) );
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated verification failure.");
        assertEquals(1,  btih.getMethodCallCount(methodVerifyData), "Shoulda sent down the creation date as an optional param.");
        assertEquals(3,  ((S3ObjectsToVerify) btih.getMethodInvokeData(methodVerifyData)
                .get(0).getArgs().get(0)).getOptionalS3ObjectMetadataKeys().length, "Shoulda sent down the creation date as an optional param.");
        final Object expected = CollectionFactory.toSet( blob3.getId() );
        assertEquals(expected, parseBlobIds( btih.getMethodInvokeData( methodVerifyData ).get( 0 ) ), "Shoulda sent down blob 3.");
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda marked task as complete.");
        assertNotNull( tape.getPartiallyVerifiedEndOfTape(), "Shoulda updated last verified.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(), "Shoulda reported suspect blob.");

        assertNull(tape.getVerifyPending(), "Shoulda cleared verify pending flag.");
        assertEquals(null, tape.getLastVerified(), "Should notta updated last verification date for full verification since this wasn't full.");

        assertFalse(tapeFailureManagement.getTapesAttemptedTooManyTimesOnDrive( td1 ).contains(tape.getId()), "Should have cleared strikes against drive1.");
        assertFalse(tapeFailureManagement.getTapesAttemptedTooManyTimesOnDrive( td2 ).contains(tape.getId()), "Should be no strikes against drive2.");
    }
    
    
    @Test
    public void testRunPartialVerifyWhenFailuresReportedButEventualSuccessPasses()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        configurePartialVerify( mockDaoDriver );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 30 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 10 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 9 );
        final Blob blob3 = mockDaoDriver.getBlobFor( o3.getId() );
        final UUID partitionId = mockDaoDriver.createTapePartition( null, "tp1" ).getId();
        Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape.setTotalRawCapacity( null ),
                Tape.TOTAL_RAW_CAPACITY );
        final StorageDomain storageDomain = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean(
                tape.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final UUID td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn1", tape.getId() ).getId();
        final UUID td2 = mockDaoDriver.createTapeDrive( partitionId, "tdsn2" ).getId();
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob3.getId() );

        final VerifyTapeTask task = createVerifyTapeTask(tape.getId(), dbSupport.getServiceManager());

        final MockTapeDriveResource tdr = new MockTapeDriveResource();
        tdr.setVerifyDataResult( 
                BeanFactory.newBean( BlobIoFailures.class ).setFailures( new BlobIoFailure[] 
                        { BeanFactory.newBean( BlobIoFailure.class )
                        .setFailure( BlobIoFailureType.values()[ 0 ] )
                        .setBlobId( blob3.getId() ) } ) );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        tdr.setInvocationListener( InterfaceProxyFactory.getProxy( TapeDriveResource.class, btih ) );
        
        final Method methodVerifyData = ReflectUtil.getMethod( TapeDriveResource.class, "verifyData" );

        // Attempt #1
        btih.reset();
        task.prepareForExecutionIfPossible(
                tdr, 
                new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) );
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated verification failure.");
        assertEquals(1,  btih.getMethodCallCount(methodVerifyData), "Shoulda sent down the creation date as an optional param.");
        assertEquals(3,  ((S3ObjectsToVerify) btih.getMethodInvokeData(methodVerifyData)
                .get(0).getArgs().get(0)).getOptionalS3ObjectMetadataKeys().length, "Shoulda sent down the creation date as an optional param.");
        final Object expected1 = CollectionFactory.toSet( blob3.getId() );
        assertEquals(expected1, parseBlobIds( btih.getMethodInvokeData( methodVerifyData ).get( 0 ) ), "Shoulda sent down blob 3.");
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as complete.");
        assertNull( tape.getPartiallyVerifiedEndOfTape(),"Should notta updated last verified yet.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(), "Should notta reported suspect blob yet.");

        // Attempt #2
        tdr.setVerifyDataResult( 
                BeanFactory.newBean( BlobIoFailures.class ).setFailures(
                        (BlobIoFailure[])Array.newInstance( BlobIoFailure.class, 0 ) ) );
        btih.reset();
        task.prepareForExecutionIfPossible(
                tdr, 
                new MockTapeAvailability().setDriveId( td2 ).setTapePartitionId( partitionId ) );
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated verification failure.");
        assertEquals(1,  btih.getMethodCallCount(methodVerifyData), "Shoulda sent down the creation date as an optional param.");
        assertEquals(3,  ((S3ObjectsToVerify) btih.getMethodInvokeData(methodVerifyData)
                .get(0).getArgs().get(0)).getOptionalS3ObjectMetadataKeys().length, "Shoulda sent down the creation date as an optional param.");
        final Object expected = CollectionFactory.toSet( blob3.getId() );
        assertEquals(expected, parseBlobIds( btih.getMethodInvokeData( methodVerifyData ).get( 0 ) ), "Shoulda sent down blob 3.");
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda marked task as complete.");
        assertNotNull( tape.getPartiallyVerifiedEndOfTape(), "Shoulda updated last verified.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(), "Should notta reported suspect blob.");

        assertEquals(null, tape.getLastVerified(), "Should notta updated last verification date for full verification since this wasn't full.");
    }


    @Test
    public void testRunPartialVerifyWhenVerifyDataThrowsLtfsExceptionResultsInEventualSuspectBlobs() throws InterruptedException {
        

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        configurePartialVerify( mockDaoDriver );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 30 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 10 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 9 );
        final Blob blob3 = mockDaoDriver.getBlobFor( o3.getId() );
        final UUID partitionId = mockDaoDriver.createTapePartition( null, "tp1" ).getId();
        Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape.setVerifyPending( BlobStoreTaskPriority.values()[ 0 ] ),
                Tape.VERIFY_PENDING );
        mockDaoDriver.updateBean(
                tape.setTotalRawCapacity( null ),
                Tape.TOTAL_RAW_CAPACITY );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean(
                tape.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final UUID td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn1", tape.getId() ).getId();
        final UUID td2 = mockDaoDriver.createTapeDrive( partitionId, "tdsn2" ).getId();
        final UUID td3 = mockDaoDriver.createTapeDrive( partitionId, "tdsn3" ).getId();
        final UUID td4 = mockDaoDriver.createTapeDrive( partitionId, "tdsn4" ).getId();
        final List<UUID> tapeDrives = Arrays.asList(td1, td2, td3, td4);

        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob3.getId() );

        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final VerifyTapeTask task = createVerifyTapeTask(tape.getId(), dbSupport.getServiceManager(), tapeFailureManagement, 10);

        final MockTapeDriveResource tdr = new MockTapeDriveResource();
        final Failure ltfsFailure = BeanFactory.newBean( Failure.class )
                .setMessage( "Test failure" )
                .setCode( TapeResourceFailureCode.LTFS_ERROR.toString() );
        tdr.setVerifyDataException(new RpcProxyException("Verify tape test failure", ltfsFailure ) );

        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        tdr.setInvocationListener( InterfaceProxyFactory.getProxy( TapeDriveResource.class, btih ) );

        final Method methodVerifyData = ReflectUtil.getMethod( TapeDriveResource.class, "verifyData" );

        // Attempt #1-4
        for (int i = 0; i < tapeDrives.size(); i++) {
            btih.reset();
            task.prepareForExecutionIfPossible(
                    tdr,
                    new MockTapeAvailability().setDriveId( tapeDrives.get(i) ).setTapePartitionId( partitionId ) );
            TestUtil.assertThrows(null, RpcProxyException.class, () -> TestUtil.invokeAndWaitChecked(task));

            final Object actual = dbSupport.getServiceManager().getRetriever( TapeFailure.class ).getCount();
            assertEquals(i+1, actual, "Shoulda generated verification failure.");
            assertEquals(1,  btih.getMethodCallCount(methodVerifyData), "Shoulda sent down the creation date as an optional param.");
            assertEquals(3,  ((S3ObjectsToVerify) btih.getMethodInvokeData(methodVerifyData)
                    .get(0).getArgs().get(0)).getOptionalS3ObjectMetadataKeys().length, "Shoulda sent down the creation date as an optional param.");
            final Object expected = CollectionFactory.toSet( blob3.getId() );
            assertEquals(expected, parseBlobIds( btih.getMethodInvokeData( methodVerifyData ).get( 0 ) ), "Shoulda sent down blob 3.");
            tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );
            assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked task as ready.");
            assertNull(tape.getPartiallyVerifiedEndOfTape(), "Should notta updated last verified yet.");
            assertEquals(0,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(), "Should notta reported suspect blob yet.");
        }

        // Final Attempt
        btih.reset();
        task.prepareForExecutionIfPossible(
                tdr,
                new MockTapeAvailability().setDriveId( td3 ).setTapePartitionId( partitionId ) );
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(4,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated verification failure.");
        assertEquals(1,  btih.getMethodCallCount(methodVerifyData), "Shoulda sent down the creation date as an optional param.");
        assertEquals(3,  ((S3ObjectsToVerify) btih.getMethodInvokeData(methodVerifyData)
                .get(0).getArgs().get(0)).getOptionalS3ObjectMetadataKeys().length, "Shoulda sent down the creation date as an optional param.");
        final Object expected = CollectionFactory.toSet( blob3.getId() );
        assertEquals(expected, parseBlobIds( btih.getMethodInvokeData( methodVerifyData ).get( 0 ) ), "Shoulda sent down blob 3.");
        tape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda marked task as complete.");
        assertNotNull( tape.getPartiallyVerifiedEndOfTape(), "Shoulda updated last verified.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(), "Shoulda reported suspect blob.");

        assertNull(tape.getVerifyPending(), "Shoulda cleared verify pending flag.");
        assertNull(tape.getLastVerified(), "Should notta updated last verification date for full verification since this wasn't full.");

        for (final UUID driveId : tapeDrives) {
            assertFalse(tapeFailureManagement.getTapesAttemptedTooManyTimesOnDrive( driveId ).contains(tape.getId()), "Should have cleared strikes against " + driveId);
        }
    }
    
    
    private Set< UUID > parseBlobIds( final MethodInvokeData mid )
    {
        return new HashSet<>( parseOrderedBlobIds( mid ) );
    }
    
    
    private List< UUID > parseOrderedBlobIds( final MethodInvokeData mid )
    {
        final S3ObjectsToVerify request = (S3ObjectsToVerify)mid.getArgs().get( 0 );
        
        final List< UUID > retval = new ArrayList<>();
        for ( final BucketOnMedia bom : request.getBuckets() )
        {
            for ( final S3ObjectOnMedia oom : bom.getObjects() )
            {
                for ( final BlobOnMedia blob : oom.getBlobs() )
                {
                    retval.add( blob.getId() );
                }
            }
        }
        
        return retval;
    }
    
    
    private void configurePartialVerify( final MockDaoDriver mockDaoDriver )
    {
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( DataPathBackend.class ).setPartiallyVerifyLastPercentOfTapes( 10 ),
                DataPathBackend.PARTIALLY_VERIFY_LAST_PERCENT_OF_TAPES );
    }


    private VerifyTapeTask createVerifyTapeTask(final UUID tapeId, final BeansServiceManager serviceManager ) {
        return createVerifyTapeTask( tapeId, serviceManager, new TapeFailureManagement( serviceManager ) );
    }

    private VerifyTapeTask createVerifyTapeTask(final UUID tapeId, final BeansServiceManager serviceManager, final TapeFailureManagement tapeFailureManagement ) {
        final VerifyTapeTask task = new VerifyTapeTask( BlobStoreTaskPriority.values()[ 0 ], tapeId, new MockDiskManager(serviceManager), tapeFailureManagement, serviceManager);
        return task;
    }

    private VerifyTapeTask createVerifyTapeTask(final UUID tapeId, final BeansServiceManager serviceManager, final TapeFailureManagement tapeFailureManagement, final int maxRetriesBeforeSuspensionRequired ) {
        final VerifyTapeTask task = new VerifyTapeTask( BlobStoreTaskPriority.values()[ 0 ], tapeId, new MockDiskManager(serviceManager), tapeFailureManagement, serviceManager, maxRetriesBeforeSuspensionRequired);
        return task;
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

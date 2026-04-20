package com.spectralogic.s3.dataplanner.backend.tape.processor.main;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.tape.TapeDriveService;
import com.spectralogic.s3.common.dao.service.tape.TapePartitionService;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.common.rpc.tape.TapeEnvironmentResource;
import com.spectralogic.s3.common.rpc.tape.TapePartitionResource;
import com.spectralogic.s3.common.rpc.tape.domain.LoadedTapeInformation;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeLockSupport;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.ReconcilingTapeEnvironmentManager;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironment;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.move.strategy.UnloadTapeFromDriveTapeMoveStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.cache.CacheResultProvider;
import com.spectralogic.util.cache.StaticCache;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;

import org.junit.jupiter.api.*;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

public class TapeEnvironmentImpl_Test {

    @Test
    public void testDeleteOfflinePartitionNullPartitionIdNotAllowed()
    {
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTapeDrive( null, null );
        mockDaoDriver.createTapeDrive( null, null );
        mockDaoDriver.createTapeDrive( null, null );

        final TapePartition p2 = mockDaoDriver.createTapePartition( null, null );
        mockDaoDriver.updateBean( p2.setState( TapePartitionState.OFFLINE ), TapePartition.STATE );
        mockDaoDriver.createTape( p2.getId(), TapeState.NORMAL );
        mockDaoDriver.createTape( p2.getId(), TapeState.NORMAL );
        mockDaoDriver.createTape( p2.getId(), TapeState.NORMAL );
        mockDaoDriver.createTape( p2.getId(), TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( p2.getId(), null );
        mockDaoDriver.createTapeDrive( p2.getId(), null );
        mockDaoDriver.createTapeDrive( p2.getId(), null );

        final TapeEnvironment environment = new TapeEnvironmentImpl(
                InterfaceProxyFactory.getProxy(ReconcilingTapeEnvironmentManager.class, null ),
                InterfaceProxyFactory.getProxy( TapeEnvironmentResource.class, null ),
                dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeLockSupport.class, null ),
                new StaticCache<>( InterfaceProxyFactory.getProxy( CacheResultProvider.class, null ) ),
                tapeFailureManagement);
        final TapePartitionService partitionService =
                dbSupport.getServiceManager().getService( TapePartitionService.class );
        partitionService.attain( TapePartition.STATE, TapePartitionState.ONLINE ).getId();
        partitionService.attain( TapePartition.STATE, TapePartitionState.OFFLINE ).getId();
        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> environment.deleteOfflineTapePartition( null ) );
    }

    @Test
    public void testDeleteOfflinePartitionPartitionNotOfflineNotAllowed()
    {
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTapeDrive( null, null );
        mockDaoDriver.createTapeDrive( null, null );
        mockDaoDriver.createTapeDrive( null, null );

        final TapePartition p2 = mockDaoDriver.createTapePartition( null, null );
        mockDaoDriver.updateBean( p2.setState( TapePartitionState.OFFLINE ), TapePartition.STATE );
        mockDaoDriver.createTape( p2.getId(), TapeState.NORMAL );
        mockDaoDriver.createTape( p2.getId(), TapeState.NORMAL );
        mockDaoDriver.createTape( p2.getId(), TapeState.NORMAL );
        mockDaoDriver.createTape( p2.getId(), TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( p2.getId(), null );
        mockDaoDriver.createTapeDrive( p2.getId(), null );
        mockDaoDriver.createTapeDrive( p2.getId(), null );

        final TapeEnvironment environment = new TapeEnvironmentImpl(
                InterfaceProxyFactory.getProxy(ReconcilingTapeEnvironmentManager.class, null ),
                InterfaceProxyFactory.getProxy( TapeEnvironmentResource.class, null ),
                dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeLockSupport.class, null ),
                new StaticCache<>( InterfaceProxyFactory.getProxy( CacheResultProvider.class, null ) ),
                tapeFailureManagement);
final TapePartitionService partitionService =
                dbSupport.getServiceManager().getService( TapePartitionService.class );
        final UUID onlinePartitionId =
                partitionService.attain( TapePartition.STATE, TapePartitionState.ONLINE ).getId();
        partitionService.attain( TapePartition.STATE, TapePartitionState.OFFLINE ).getId();
        TestUtil.assertThrows( null, GenericFailure.CONFLICT,
                () -> environment.deleteOfflineTapePartition( onlinePartitionId ) );
    }

    @Test
    public void testDeleteOfflinePartitionDoesSo()
    {
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTapeDrive( null, null );
        mockDaoDriver.createTapeDrive( null, null );
        mockDaoDriver.createTapeDrive( null, null );

        final TapePartition p2 = mockDaoDriver.createTapePartition( null, null );
        mockDaoDriver.updateBean( p2.setState( TapePartitionState.OFFLINE ), TapePartition.STATE );
        mockDaoDriver.createTape( p2.getId(), TapeState.NORMAL );
        mockDaoDriver.createTape( p2.getId(), TapeState.NORMAL );
        mockDaoDriver.createTape( p2.getId(), TapeState.NORMAL );
        mockDaoDriver.createTape( p2.getId(), TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( p2.getId(), null );
        mockDaoDriver.createTapeDrive( p2.getId(), null );
        mockDaoDriver.createTapeDrive( p2.getId(), null );

        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( TapeLibrary.class ).getCount(),
                "Shoulda had single library containing 1 partition initially."
                 );

        final TapeEnvironment environment = new TapeEnvironmentImpl(
                InterfaceProxyFactory.getProxy(ReconcilingTapeEnvironmentManager.class, null ),
                InterfaceProxyFactory.getProxy( TapeEnvironmentResource.class, null ),
                dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeLockSupport.class, null ),
                new StaticCache<>( InterfaceProxyFactory.getProxy( CacheResultProvider.class, null ) ),
                tapeFailureManagement);
final TapePartitionService partitionService =
                dbSupport.getServiceManager().getService( TapePartitionService.class );
        final UUID onlinePartitionId =
                partitionService.attain( TapePartition.STATE, TapePartitionState.ONLINE ).getId();
        final UUID offlinePartitionId =
                partitionService.attain( TapePartition.STATE, TapePartitionState.OFFLINE ).getId();

        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );

        final BeansRetriever<Tape> tapeRetriever =
                dbSupport.getServiceManager().getRetriever( Tape.class );
        final Tape t1 = tapeRetriever.retrieveAll( Tape.PARTITION_ID, onlinePartitionId ).getFirst();
        final Tape t2 = tapeRetriever.retrieveAll( Tape.PARTITION_ID, offlinePartitionId ).getFirst();
        mockDaoDriver.putBlobOnTape( t1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( t2.getId(), b2.getId() );

        environment.deleteOfflineTapePartition( offlinePartitionId );
        assertNull(
                partitionService.retrieve( offlinePartitionId ),
                "Shoulda deleted partition."
                );
        assertEquals(
                3,
                dbSupport.getServiceManager().getRetriever( TapeDrive.class ).getCount(
                        Require.beanPropertyEquals( TapeDrive.PARTITION_ID, onlinePartitionId ) ),
                "Should notta impacted any other partition."
                 );
        assertEquals(
                4,
                dbSupport.getServiceManager().getRetriever( Tape.class ).getCount(
                        Require.beanPropertyEquals( Tape.PARTITION_ID, onlinePartitionId ) ),
                "Should notta impacted any other partition."
                 );

        tapeRetriever.attain( t2.getId() );
        assertEquals(
                5,
                dbSupport.getServiceManager().getRetriever( Tape.class ).getCount(),
                "Shoulda kept tapes from online partition and 1 tape with data on it from the deleted ptn."
                );
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( TapeLibrary.class ).getCount(),
                "Shoulda had single library still."
                 );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setState( TapePartitionState.OFFLINE ),
                TapePartition.STATE );
        environment.deleteOfflineTapePartition( onlinePartitionId );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapeLibrary.class ).getCount(),
                "Shoulda whacked single library since no partitions left using it."
               );
    }

    @Test
    public void testDeleteOfflinePartitionWithOutTapeDoesSo()
    {
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapePartition tapePartition = mockDaoDriver.createTapePartition( null, null );
        mockDaoDriver.updateBean( tapePartition.setState( TapePartitionState.OFFLINE ), TapePartition.STATE );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "storageDomain" );
        mockDaoDriver.addTapePartitionToStorageDomain( storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "policy" );
        mockDaoDriver.createDataPersistenceRule( dataPolicy.getId(), DataPersistenceRuleType.PERMANENT,
                storageDomain.getId() );

        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever(
                                TapeLibrary
                                        .class )
                        .getCount(),
                "Shoulda had single library containing 1 partition initially."  );

        final TapeEnvironment environment = new TapeEnvironmentImpl(
                InterfaceProxyFactory.getProxy(ReconcilingTapeEnvironmentManager.class, null ),
                InterfaceProxyFactory.getProxy( TapeEnvironmentResource.class, null ),
                dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeLockSupport.class, null ),
                new StaticCache<>( InterfaceProxyFactory.getProxy( CacheResultProvider.class, null ) ),
                tapeFailureManagement);
        final TapePartitionService partitionService = dbSupport.getServiceManager()
                .getService( TapePartitionService.class );
        final UUID offlinePartitionId = partitionService.attain( TapePartition.STATE, TapePartitionState.OFFLINE )
                .getId();

        environment.deleteOfflineTapePartition( offlinePartitionId );
        assertNull(
                partitionService.retrieve( offlinePartitionId ),
                "Shoulda deleted partition."  );

        assertEquals(  0,
                dbSupport.getServiceManager()
                .getRetriever( TapeLibrary.class )
                .getCount(),
                "Shoulda deleted library." );
    }

    @Test
    public void testDeleteOfflinePartitionWithTapeDoesNot()
    {
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapePartition tapePartition = mockDaoDriver.createTapePartition( null, null );
        mockDaoDriver.updateBean( tapePartition.setState( TapePartitionState.OFFLINE ), TapePartition.STATE );
        mockDaoDriver.createTape( tapePartition.getId(), TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( tapePartition.getId(), null );

        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever(
                                TapeLibrary
                                        .class )
                        .getCount(),
                "Shoulda had single library containing 1 partition initially."  );

        final TapeEnvironment environment = new TapeEnvironmentImpl(
                InterfaceProxyFactory.getProxy(ReconcilingTapeEnvironmentManager.class, null ),
                InterfaceProxyFactory.getProxy( TapeEnvironmentResource.class, null ),
                dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeLockSupport.class, null ),
                new StaticCache<>( InterfaceProxyFactory.getProxy( CacheResultProvider.class, null ) ),
                tapeFailureManagement);
        final TapePartitionService partitionService = dbSupport.getServiceManager()
                .getService( TapePartitionService.class );
        final UUID offlinePartitionId = partitionService.attain( TapePartition.STATE, TapePartitionState.OFFLINE )
                .getId();

        final S3Object object = mockDaoDriver.createObject( null, "object" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );

        final BeansRetriever< Tape > tapeRetriever = dbSupport.getServiceManager()
                .getRetriever( Tape.class );
        final Tape tape = tapeRetriever.retrieveAll( Tape.PARTITION_ID, offlinePartitionId )
                .getFirst();
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );

        environment.deleteOfflineTapePartition( offlinePartitionId );
        assertNull( partitionService.retrieve( offlinePartitionId ),
                "Shoulda not deleted partition." );
    }

    @Test
    public void testDeleteOfflineTapeDriveNullDriveIdNotAllowed()
    {
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTapeDrive( null, null );
        mockDaoDriver.updateBean(
                mockDaoDriver.createTapeDrive( null, null ).setState( TapeDriveState.OFFLINE ),
                TapeDrive.STATE );

        final TapeEnvironment environment = new TapeEnvironmentImpl(
                InterfaceProxyFactory.getProxy(ReconcilingTapeEnvironmentManager.class, null ),
                InterfaceProxyFactory.getProxy( TapeEnvironmentResource.class, null ),
                dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeLockSupport.class, null ),
                new StaticCache<>( InterfaceProxyFactory.getProxy( CacheResultProvider.class, null ) ),
                tapeFailureManagement);
final TapeDriveService driveService =
                dbSupport.getServiceManager().getService( TapeDriveService.class );
        driveService.attain( TapeDrive.STATE, TapeDriveState.NORMAL ).getId();
        driveService.attain( TapeDrive.STATE, TapeDriveState.OFFLINE ).getId();
        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> environment.deleteOfflineTapeDrive( null ) );
    }

    @Test
    public void testDeleteOfflineTapeDriveNotOfflineNotAllowed()
    {
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTapeDrive( null, null );
        mockDaoDriver.updateBean(
                mockDaoDriver.createTapeDrive( null, null ).setState( TapeDriveState.OFFLINE ),
                TapeDrive.STATE );

        final TapeEnvironment environment = new TapeEnvironmentImpl(
                InterfaceProxyFactory.getProxy(ReconcilingTapeEnvironmentManager.class, null ),
                InterfaceProxyFactory.getProxy( TapeEnvironmentResource.class, null ),
                dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeLockSupport.class, null ),
                new StaticCache<>( InterfaceProxyFactory.getProxy( CacheResultProvider.class, null ) ),
                tapeFailureManagement);
final TapeDriveService driveService =
                dbSupport.getServiceManager().getService( TapeDriveService.class );
        final UUID onlineDriveId = driveService.attain( TapeDrive.STATE, TapeDriveState.NORMAL ).getId();
        driveService.attain( TapeDrive.STATE, TapeDriveState.OFFLINE ).getId();

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, () -> environment.deleteOfflineTapeDrive( onlineDriveId ) );
    }

    @Test
    public void testDeleteOfflineTapeDriveDoesSo()
    {
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTapeDrive( null, null );
        mockDaoDriver.updateBean(
                mockDaoDriver.createTapeDrive( null, null ).setState( TapeDriveState.OFFLINE ),
                TapeDrive.STATE );

        final TapeEnvironment environment = new TapeEnvironmentImpl(
                InterfaceProxyFactory.getProxy(ReconcilingTapeEnvironmentManager.class, null ),
                InterfaceProxyFactory.getProxy( TapeEnvironmentResource.class, null ),
                dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeLockSupport.class, null ),
                new StaticCache<>( InterfaceProxyFactory.getProxy( CacheResultProvider.class, null ) ),
                tapeFailureManagement);
final TapeDriveService driveService =
                dbSupport.getServiceManager().getService( TapeDriveService.class );
        driveService.attain( TapeDrive.STATE, TapeDriveState.NORMAL ).getId();
        final UUID offlineDriveId = driveService.attain( TapeDrive.STATE, TapeDriveState.OFFLINE ).getId();

        final TapePartitionService partitionService =
                dbSupport.getServiceManager().getService( TapePartitionService.class );

        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );

        final UUID partitionId = dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                Require.nothing() ).getId();

        final BeansRetriever< Tape > tapeRetriever =
                dbSupport.getServiceManager().getRetriever( Tape.class );
        final Tape t1 = tapeRetriever.retrieveAll( Tape.PARTITION_ID, partitionId ).getFirst();
        final Tape t2 = tapeRetriever.retrieveAll( Tape.PARTITION_ID, partitionId ).getFirst();
        mockDaoDriver.putBlobOnTape( t1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( t2.getId(), b2.getId() );

        driveService.update(
                driveService.attain( offlineDriveId ).setTapeId( t1.getId() ),
                TapeDrive.TAPE_ID );

        environment.deleteOfflineTapeDrive( offlineDriveId );
        assertNull(
                partitionService.retrieve( offlineDriveId ),
                "Shoulda deleted drive."
                );
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( TapeDrive.class ).getCount( Require.nothing() ),
                "Should notta impacted any other drive."
                 );
        assertEquals(
                4,
                dbSupport.getServiceManager().getRetriever( Tape.class ).getCount( Require.nothing() ),
                "Should notta impacted any other tape."
                 );

        tapeRetriever.attain( t1.getId() );
        tapeRetriever.attain( t2.getId() );
    }

    @Test
    public void testDeletePermanentlyOfflineTapeNotInLostOrEjectedStateNotAllowed()
    {
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeEnvironment environment = new TapeEnvironmentImpl(
                InterfaceProxyFactory.getProxy(ReconcilingTapeEnvironmentManager.class, null ),
                InterfaceProxyFactory.getProxy( TapeEnvironmentResource.class, null ),
                dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeLockSupport.class, null ),
                new StaticCache<>( InterfaceProxyFactory.getProxy( CacheResultProvider.class, null ) ),
                tapeFailureManagement);
final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o11 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b11 = mockDaoDriver.getBlobFor( o11.getId() );
        final S3Object o12 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b12 = mockDaoDriver.getBlobFor( o12.getId() );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        final S3Object o21 = mockDaoDriver.createObject( bucket2.getId(), "o1" );
        final Blob b21 = mockDaoDriver.getBlobFor( o21.getId() );
        final S3Object o22 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b22 = mockDaoDriver.getBlobFor( o22.getId() );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.FOREIGN );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b11.getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b21.getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b22.getId() );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.BAD );
        mockDaoDriver.putBlobOnTape( tape2.getId(), b12.getId() );
        mockDaoDriver.putBlobOnTape( tape2.getId(), b21.getId() );

        final BeansRetriever< BlobTape > retriever =
                dbSupport.getServiceManager().getRetriever( BlobTape.class );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT,
                () -> environment.deletePermanentlyLostTape( tape1.getId() ) );
        assertEquals(
                5,
                retriever.getCount(),
                "Should notta whacked blobs persisted on tape1."
                );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT,
                () -> environment.deletePermanentlyLostTape( tape2.getId() ) );
        assertEquals(
                5,
                retriever.getCount(),
                "Should notta whacked blobs persisted on tape2."
                 );
    }

    @Test
    public void testDeletePermanentlyOfflineTapeDoesSo()
    {
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeEnvironment environment = new TapeEnvironmentImpl(
                InterfaceProxyFactory.getProxy(ReconcilingTapeEnvironmentManager.class, null ),
                InterfaceProxyFactory.getProxy( TapeEnvironmentResource.class, null ),
                dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeLockSupport.class, null ),
                new StaticCache<>( InterfaceProxyFactory.getProxy( CacheResultProvider.class, null ) ),
                tapeFailureManagement);
final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o11 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b11 = mockDaoDriver.getBlobFor( o11.getId() );
        final S3Object o12 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b12 = mockDaoDriver.getBlobFor( o12.getId() );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        final S3Object o21 = mockDaoDriver.createObject( bucket2.getId(), "o1" );
        final Blob b21 = mockDaoDriver.getBlobFor( o21.getId() );
        final S3Object o22 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b22 = mockDaoDriver.getBlobFor( o22.getId() );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b11.getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b21.getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b22.getId() );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.LOST );
        mockDaoDriver.putBlobOnTape( tape2.getId(), b12.getId() );
        mockDaoDriver.putBlobOnTape( tape2.getId(), b21.getId() );

        final BeansRetriever< BlobTape > retriever =
                dbSupport.getServiceManager().getRetriever( BlobTape.class );
        environment.deletePermanentlyLostTape( tape1.getId() );
        assertEquals(
                2,
                retriever.getCount(),
                "Shoulda whacked blobs persisted on tape1."
                );
        environment.deletePermanentlyLostTape( tape2.getId() );
        assertEquals(
                0,
                retriever.getCount(),
                "Shoulda whacked blobs persisted on tape2."
                 );
    }

    @Test
    public void testPerformMoveVerifiesQuiescedOnlyWhenTapeHasManagedData()
    {
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapePartitionResource tpr = mock( TapePartitionResource.class );
        final TapeDriveResource tdr1 = mock ( TapeDriveResource.class );
        final TapeDriveResource tdr2 = mock ( TapeDriveResource.class );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        final LoadedTapeInformation tapeInfo1 = BeanFactory.newBean( LoadedTapeInformation.class );
        tapeInfo1.setTapeId( tape1.getId() );
        final LoadedTapeInformation tapeInfo2 = BeanFactory.newBean( LoadedTapeInformation.class );
        tapeInfo2.setTapeId( tape2.getId() );
        when(tpr.move(anyInt(), anyInt()) ).thenReturn( new RpcResponse<>(null) );
        when(tdr1.prepareForRemoval() ).thenReturn( new RpcResponse<>(null) );
        when(tdr1.getLoadedTapeInformation()).thenReturn( new RpcResponse<>(tapeInfo1) );
        when(tdr1.verifyQuiescedToCheckpoint( any(), anyBoolean() )).thenReturn( new RpcResponse<>("checkpoint1") );
        when(tdr2.prepareForRemoval() ).thenReturn( new RpcResponse<>(null) );
        when(tdr2.getLoadedTapeInformation()).thenReturn( new RpcResponse<>(tapeInfo2) );

        final StaticCache<String, TapePartitionResource> tpResourceProvider = new StaticCache<>( (String) -> tpr );
        final TapeEnvironment environment = new TapeEnvironmentImpl(
                InterfaceProxyFactory.getProxy(ReconcilingTapeEnvironmentManager.class, null ),
                InterfaceProxyFactory.getProxy( TapeEnvironmentResource.class, null ),
                dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeLockSupport.class, null ),
                tpResourceProvider,
                tapeFailureManagement);
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o11 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b11 = mockDaoDriver.getBlobFor( o11.getId() );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        final S3Object o21 = mockDaoDriver.createObject( bucket2.getId(), "o1" );
        final Blob b21 = mockDaoDriver.getBlobFor( o21.getId() );
        final S3Object o22 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b22 = mockDaoDriver.getBlobFor( o22.getId() );

        mockDaoDriver.putBlobOnTape( tape1.getId(), b11.getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b21.getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b22.getId() );

        final UUID partitionId = dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                Require.nothing() ).getId();
        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( partitionId, "sn1" );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( partitionId, "sn2" );
        mockDaoDriver.updateBean(drive1.setTapeId( tape1.getId() ), TapeDrive.TAPE_ID );
        mockDaoDriver.updateBean(drive2.setTapeId( tape2.getId() ), TapeDrive.TAPE_ID );

        environment.performMove( tdr1,
                "partitionSerial",
                tape1,
                new UnloadTapeFromDriveTapeMoveStrategy(drive1, ElementAddressType.STORAGE),
                true );
        verify(tdr1, times(0)
                .description("Shoulda not have called verify quiesced to checkpoint."))
                .verifyQuiescedToCheckpoint( any(), anyBoolean() );

        verify(tpr, times(1)
                .description("Shoulda made one call to move."))
                .move( anyInt(), anyInt() );

        environment.performMove( tdr2,
                "partitionSerial",
                tape2,
                new UnloadTapeFromDriveTapeMoveStrategy(drive2, ElementAddressType.STORAGE),
                true );
        verify(tdr2, times(0)
                .description("Shoulda made no calls to verify quiesced to checkpoint since tape has no managed data."))
                .verifyQuiescedToCheckpoint(any(), anyBoolean());
        verify(tpr, times(2)
                .description("Shoulda made two calls to move."))
                .move( anyInt(), anyInt() );

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

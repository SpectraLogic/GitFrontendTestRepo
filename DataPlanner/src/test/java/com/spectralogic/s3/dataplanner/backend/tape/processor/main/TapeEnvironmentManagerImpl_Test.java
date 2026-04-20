/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.tape.processor.main;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.ElementAddressType;
import com.spectralogic.s3.common.dao.domain.tape.ImportExportConfiguration;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveState;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionState;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.tape.TapeDriveService;
import com.spectralogic.s3.common.dao.service.tape.TapeFailureService;
import com.spectralogic.s3.common.dao.service.tape.TapePartitionFailureService;
import com.spectralogic.s3.common.dao.service.tape.TapePartitionService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.rpc.tape.domain.BasicTapeInformation;
import com.spectralogic.s3.common.rpc.tape.domain.ElementAddressBlockInformation;
import com.spectralogic.s3.common.rpc.tape.domain.TapeDriveInformation;
import com.spectralogic.s3.common.rpc.tape.domain.TapeEnvironmentInformation;
import com.spectralogic.s3.common.rpc.tape.domain.TapeLibraryInformation;
import com.spectralogic.s3.common.rpc.tape.domain.TapePartitionInformation;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;


import org.junit.jupiter.api.*;


@SuppressWarnings( "ThrowableNotThrown" )
public final class TapeEnvironmentManagerImpl_Test
{

    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> new TapeEnvironmentManagerImpl( null, -1 ) );
    }

    @Test
    public void testReconcileWithNullEnvironmentNotAllowed()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> manager.reconcileWith( null ) );
    }


    @Test
    public void testReconcileDoesNotThrowExceptions()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 2, 3, 5 ) );
        manager.reconcileWith( constructResponse( 2, 3, 5 ) );
        manager.reconcileWith( constructResponse( 2, 3, 6 ) );
        manager.reconcileWith( constructResponse( 2, 4, 6 ) );
        manager.reconcileWith( constructResponse( 3, 4, 6 ) );
        manager.reconcileWith( constructResponse( 4, 5, 7 ) );
        manager.reconcileWith( constructResponse( 4, 5, 7 ) );
        manager.reconcileWith( constructResponse( 4, 5, 6 ) );
        manager.reconcileWith( constructResponse( 4, 4, 6 ) );
        manager.reconcileWith( constructResponse( 3, 4, 6 ) );
        manager.reconcileWith( constructResponse( 2, 3, 5 ) );
        manager.reconcileWith( constructResponse( 0, 0, 0 ) );
        manager.reconcileWith( constructResponse( 2, 3, 5 ) );
    }

    @Test
    public void testReconcileDoesSo()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeEnvironmentInformation response = constructResponse( 2, 3, 5 );
        response.getLibraries()[ 0 ].getPartitions()[ 1 ].getTapes()[ 1 ].setBarCode( null );
        response.getLibraries()[ 0 ].getPartitions()[ 1 ].getTapes()[ 1 ].setType( null );
        manager.reconcileWith( response );

        final List< TapePartition > partitions = dbSupport.getServiceManager()
                                                          .getRetriever( TapePartition.class )
                                                          .retrieveAll()
                                                          .toList();
        partitions.sort( new BeanComparator<>( TapePartition.class, SerialNumberObservable.SERIAL_NUMBER ) );
        final List< TapeDrive > drives = dbSupport.getServiceManager()
                                                  .getRetriever( TapeDrive.class )
                                                  .retrieveAll()
                                                  .toList();
        drives.sort( new BeanComparator<>( TapeDrive.class, SerialNumberObservable.SERIAL_NUMBER ) );
        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();
        tapes.sort( new BeanComparator<>( Tape.class, Tape.BAR_CODE ) );

        assertEquals(
                2,
                partitions.size(),
                "Shoulda created dao objects to reflect tape environment response." );
        assertEquals(
                6,
                drives.size(),
                "Shoulda created dao objects to reflect tape environment response."  );
        assertEquals( 10,
                tapes.size(),
                "Shoulda created dao objects to reflect tape environment response."  );
        assertEquals(
                1,
                dbSupport.getDataManager()
                        .getCount( Tape.class,
                                Require
                                        .beanPropertyEqualsOneOf(
                                                Tape.STATE,
                                                TapeState
                                                        .BAR_CODE_MISSING )
                        ),
                "Shoulda been a tape with an unknown barcode."  );

        manager.reconcileWith( response );

        for ( TapePartition partition : partitions )
        {
            dbSupport.getServiceManager()
                     .getRetriever( TapePartition.class )
                     .attain( partition.getId() );
        }
        for ( TapeDrive drive : drives )
        {
            dbSupport.getServiceManager()
                     .getRetriever( TapeDrive.class )
                     .attain( drive.getId() );
        }
        for ( Tape tape : tapes )
        {
            dbSupport.getServiceManager()
                     .getRetriever( Tape.class )
                     .attain( tape.getId() );
        }

        final User user = mockDaoDriver.createUser( "user1" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket" );

        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "a", 1 );
        final UUID blobId = dbSupport.getServiceManager()
                                     .getRetriever( Blob.class )
                                     .attain( Blob.OBJECT_ID, object.getId() )
                                     .getId();

        final BlobTape ot = BeanFactory.newBean( BlobTape.class )
                                       .setBlobId( blobId )
                                       .setOrderIndex( 1 )
                                       .setTapeId( tapes.get( 0 )
                                                        .getId() );
        dbSupport.getDataManager().createBean( ot );

        manager.reconcileWith( constructResponse( 0, 0, 0 ) );
        assertEquals( 2, dbSupport.getServiceManager()
                .getRetriever(
                        TapePartition
                                .class )
                .getCount(),
                "Should notta deleted partitions already reconciled with." );

        final TapePartition partitionInUse = dbSupport.getServiceManager()
                                                      .getRetriever( TapePartition.class )
                                                      .attain( tapes.get( 0 )
                                                                    .getPartitionId() );
        dbSupport.getDataManager()
                 .updateBean( CollectionFactory.toSet( TapePartition.STATE ),
                         partitionInUse.setState( TapePartitionState.ONLINE ) );
        manager.reconcileWith( constructResponse( 0, 0, 0 ) );

        assertNotNull( dbSupport.getServiceManager()
                .getRetriever(
                        TapePartition.class )
                .retrieve(
                        partitionInUse.getId()
                ),
                "Shoulda retained the partition that we were using." );

        manager.reconcileWith( response );
    }

    @Test
    public void testReconcileIdentifiesTapesInOfflineDrives()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeEnvironmentInformation response = constructResponse( 2, 4, 10 );
        final BasicTapeInformation tape = response.getLibraries()[0].getPartitions()[0].getTapes()[0];
        final BasicTapeInformation tape2 = response.getLibraries()[0].getPartitions()[0].getTapes()[0];
        final TapeDriveInformation[] driveInfos = response.getLibraries()[0].getPartitions()[0].getDrives();
        final TapeDriveInformation drive = response.getLibraries()[0].getPartitions()[0].getDrives()[0];
        final TapeDriveInformation drive2 = response.getLibraries()[0].getPartitions()[0].getDrives()[0];
        tape.setElementAddress( drive.getElementAddress() );
        tape2.setElementAddress( drive2.getElementAddress() );
        manager.reconcileWith( response );

        List< TapePartition > partitions = dbSupport.getServiceManager()
                                                          .getRetriever( TapePartition.class )
                                                          .retrieveAll()
                                                          .toList();
        partitions.sort( new BeanComparator<>( TapePartition.class, SerialNumberObservable.SERIAL_NUMBER ) );
        List< TapeDrive > onlineDrives = dbSupport.getServiceManager()
                                                  .getRetriever( TapeDrive.class )
                                                  .retrieveAll( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.NORMAL ) )
                                                  .toList();
        onlineDrives.sort( new BeanComparator<>( TapeDrive.class, SerialNumberObservable.SERIAL_NUMBER ) );
        List< Tape > onlineTapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll( Require.not( Require.beanPropertyEqualsOneOf( Tape.STATE, TapeState.getStatesThatAreNotPhysicallyPresent() ) ) )
                                            .toList();
        onlineTapes.sort( new BeanComparator<>( Tape.class, Tape.BAR_CODE ) );

        assertEquals(
                2,
                partitions.size(),
                "Shoulda created dao objects to reflect tape environment response." );
        assertEquals(
                8,
                onlineDrives.size(),
                "Shoulda created dao objects to reflect tape environment response." );
        assertEquals(
                20,
                onlineTapes.size(),
                "Shoulda created dao objects to reflect tape environment response." );
        assertEquals(
                0,
                manager.getTapesInOfflineDrives().size(),
                "Should have noted that no tapes were in an offline drive." );

        final TapeDriveInformation[] trimmedDriveInfos = Arrays.copyOfRange(driveInfos, 1, driveInfos.length);
        response.getLibraries()[0].getPartitions()[0].setDrives(trimmedDriveInfos);
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( response );

        partitions = dbSupport.getServiceManager()
        		.getRetriever( TapePartition.class )
                .retrieveAll()
                .toList();
        partitions.sort( new BeanComparator<>( TapePartition.class, SerialNumberObservable.SERIAL_NUMBER ) );
        onlineDrives = dbSupport.getServiceManager()
                .getRetriever( TapeDrive.class )
                .retrieveAll( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.NORMAL ) )
                .toList();
        onlineDrives.sort( new BeanComparator<>( TapeDrive.class, SerialNumberObservable.SERIAL_NUMBER ) );
        onlineTapes = dbSupport.getServiceManager()
                .getRetriever( Tape.class )
                .retrieveAll( Require.not( Require.beanPropertyEqualsOneOf( Tape.STATE, TapeState.getStatesThatAreNotPhysicallyPresent() ) ) )
                .toList();
        onlineTapes.sort( new BeanComparator<>( Tape.class, Tape.BAR_CODE ) );

        assertEquals(
                2,
                partitions.size(),
                "Shoulda created dao objects to reflect tape environment response." );
        assertEquals(
                7,
                onlineDrives.size(),
                "Shoulda created dao objects to reflect tape environment response."  );
        assertEquals(
                20,
                onlineTapes.size(),
                "Shoulda created dao objects to reflect tape environment response." );
        assertEquals(
                1,
                manager.getTapesInOfflineDrives().size(),
                "Should have noted that one tape was in an offline drive."  );
    }

    @Test
    public void testReconcileWhenLibrariesGoingOfflineAndOnlineHandledCorrectly()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 2, 1, 1 ) );

        // This test assumes that auto-quiesce is not enabled on the tape partition when it goes off and online.
        mockDaoDriver.updateAllBeans( BeanFactory.newBean( TapePartition.class ).setAutoQuiesceEnabled( false ), TapePartition.AUTO_QUIESCE_ENABLED );

        final TapeEnvironmentInformation tei = BeanFactory.newBean( TapeEnvironmentInformation.class );
        tei.setLibraries( ( TapeLibraryInformation[] ) Array.newInstance( TapeLibraryInformation.class, 0 ) );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( tei );
        assertEquals(
                2,
                dbSupport.getServiceManager()
                        .getRetriever( TapePartition.class )
                        .getCount( TapePartition.STATE,
                                TapePartitionState
                                        .OFFLINE ),
                "All partitions shoulda been marked as offline."  );
        assertEquals(
                2,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                Tape.STATE,
                                                TapeState.LOST
                                        ) ) ,
                "All tapes should be LOST from offline partition." );

        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 2, 1, 1 ) );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( TapePartition.class )
                        .getCount( TapePartition.STATE,
                                TapePartitionState.OFFLINE
                        ),
                "All partitions shoulda been marked as online." );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Require.beanPropertyEquals( Tape.STATE,
                                TapeState.LOST ) ),
                "No tapes should be LOST anymore."  );
        assertEquals(
                2,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                Tape.STATE,
                                                TapeState.PENDING_INSPECTION
                                        ) ) ,
                "Tapes that were lost should now require inspection." );
    }

    @Test
    public void testReconcileWhenLibrariesGoingOfflineAndOnlineHandledCorrectlyWhenQuiesced()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 2, 1, 1 ) );
        mockDaoDriver.updateAllBeans(BeanFactory.newBean(Tape.class).setState(TapeState.NORMAL), Tape.STATE);

        final TapeEnvironmentInformation noLibraryResponse = BeanFactory.newBean( TapeEnvironmentInformation.class );
        noLibraryResponse.setLibraries( ( TapeLibraryInformation[] ) Array.newInstance( TapeLibraryInformation.class, 0 ) );
        manager.reconcileWith( noLibraryResponse );
        assertEquals(
                2,
                dbSupport.getServiceManager()
                        .getRetriever( TapePartition.class )
                        .getCount( TapePartition.STATE,
                                TapePartitionState
                                        .ONLINE ) ,
                "Partition is still quiesced, partitions should still be seen as online." );
        assertEquals(
                2,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                Tape.STATE,
                                                TapeState.NORMAL
                                        ) ),
                "Partition is still quiesced, tapes should still be in normal state."  );

        manager.reconcileWith( constructResponse( 2, 1, 1 ) );
        assertEquals(
                2,
                dbSupport.getServiceManager()
                        .getRetriever( TapePartition.class )
                        .getCount( TapePartition.STATE,
                                TapePartitionState.ONLINE
                        ),
                "All partitions shoulda been marked as online."  );
        assertEquals(
                2,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                Tape.STATE,
                                                TapeState.NORMAL
                                        ) ),
                "Tapes should have stayed in normal state."  );
        final Set<TapePartition> allPartitions =
                dbSupport.getServiceManager().getRetriever(TapePartition.class).retrieveAll().toSet();
        assertEquals(
                2,
                allPartitions.size(),
                "Should have been two partitions.");
        for (TapePartition partition : allPartitions) {
            final Set<UUID> tapesInPartition = manager.getTapesInPartition(partition.getId());
            assertFalse(
                    tapesInPartition.isEmpty(),
                    "Manager should have retained inventory from quiesced partition."
                   );
            for (UUID tape : tapesInPartition) {
                assertNotNull(
                        manager.getTapeElementAddress(tape),
                        "Manager should have retained inventory from quiesced partition."
                        );
            }
        }

        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 2, 1, 1 ) );
        assertEquals(
                2,
                dbSupport.getServiceManager()
                        .getRetriever( TapePartition.class )
                        .getCount( TapePartition.STATE,
                                TapePartitionState.ONLINE
                        ),
                "All partitions shoulda been marked as online." );
        assertEquals(
                2,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                Tape.STATE,
                                                TapeState.NORMAL
                                        ) ),
                "Tapes should have stayed in normal state." );
    }

    @Test
    public void testReconcileWhenLibraryDisappearHandledCorrectlyWhenAutoQuiesceEnabled()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        mockDaoDriver.updateAllBeans(BeanFactory.newBean(Tape.class).setState(TapeState.NORMAL), Tape.STATE);

        final TapeEnvironmentInformation noLibraryResponse = BeanFactory.newBean( TapeEnvironmentInformation.class );
        noLibraryResponse.setLibraries( ( TapeLibraryInformation[] ) Array.newInstance( TapeLibraryInformation.class, 0 ) );

        // Enable auto-quiesce and un-quiesce the tape partition
        final TapePartitionService tapePartitionService = dbSupport.getServiceManager().getService( TapePartitionService.class );
        final TapePartition partition = tapePartitionService.retrieveAll().getFirst();
        assertNotNull( partition );
        dbSupport.getServiceManager().getUpdater( TapePartition.class ).update( partition.setAutoQuiesceEnabled( true ), TapePartition.AUTO_QUIESCE_ENABLED );
        mockDaoDriver.unquiesceAllTapePartitions();

        // Verify loss of communication puts partition into quiesced state and that tape and partition state remains unchanged
        manager.reconcileWith( noLibraryResponse );
        assertEquals( Quiesced.YES, tapePartitionService.retrieve( partition.getId() ).getQuiesced() );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( TapePartition.class )
                        .getCount( TapePartition.STATE,
                                TapePartitionState
                                        .ONLINE ),
                "Partition is still quiesced, partitions should still be seen as online." );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                Tape.STATE,
                                                TapeState.NORMAL
                                        )) ,
                                "Partition is still quiesced, tapes should still be in normal state." );

        // Un-quiesce the tape partition and verify tape and partition states remain unchanged
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( TapePartition.class )
                        .getCount( TapePartition.STATE,
                                TapePartitionState.ONLINE
                        ) ,
                "All partitions shoulda been marked as online.");
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                Tape.STATE,
                                                TapeState.NORMAL
                                        ) ),
                "Tapes should have stayed in normal state." );
    }

    @Test
    public void testReconcileWhenPartitionsDisappearHandledCorrectlyWhenAutoQuiesceEnabled()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        mockDaoDriver.updateAllBeans(BeanFactory.newBean(Tape.class).setState(TapeState.NORMAL), Tape.STATE);

        final TapeEnvironmentInformation noPartitionsResponse = constructResponse( 1, 0, 0 );
        noPartitionsResponse.getLibraries()[0].setPartitions(new TapePartitionInformation[0]);

        assertEquals(
                1,
                noPartitionsResponse.getLibraries().length,
                "Constructed response should have one library, but no partitions" );
        assertEquals(
                0,
                noPartitionsResponse.getLibraries()[0].getPartitions().length,
                "Constructed response should have one library, but no partitions" );

        // Enable auto-quiesce and un-quiesce the tape partition
        final TapePartitionService tapePartitionService = dbSupport.getServiceManager().getService( TapePartitionService.class );
        final TapePartition partition = tapePartitionService.retrieveAll().getFirst();
        assertNotNull( partition );
        dbSupport.getServiceManager().getUpdater( TapePartition.class ).update( partition.setAutoQuiesceEnabled( true ), TapePartition.AUTO_QUIESCE_ENABLED );
        mockDaoDriver.unquiesceAllTapePartitions();

        // Verify loss of communication puts partition into quiesced state and that tape and partition state remains unchanged
        manager.reconcileWith( noPartitionsResponse );
        assertEquals( Quiesced.YES, tapePartitionService.retrieve( partition.getId() ).getQuiesced() );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( TapePartition.class )
                        .getCount( TapePartition.STATE,
                                TapePartitionState
                                        .ONLINE ),
                "Partition is still quiesced, partitions should still be seen as online."  );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                Tape.STATE,
                                                TapeState.NORMAL
                                        ) ),
                "Partition is still quiesced, tapes should still be in normal state." );

        // Un-quiesce the tape partition and verify tape and partition states remain unchanged
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        assertEquals(
                1, dbSupport.getServiceManager()
                        .getRetriever( TapePartition.class )
                        .getCount( TapePartition.STATE,
                                TapePartitionState.ONLINE
                        ),
                "All partitions shoulda been marked as online." );
        assertEquals(
                1, dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                Tape.STATE,
                                                TapeState.NORMAL
                                        ) ),
                "Tapes should have stayed in normal state." );
    }

    @Test
    public void testReconcileWhenTapesDisappearHandledCorrectlyWhenAutoQuiesceEnabled()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        mockDaoDriver.updateAllBeans(BeanFactory.newBean(Tape.class).setState(TapeState.NORMAL), Tape.STATE);
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        final Tape tape = getOnlyTape( tapeService );
        linkTapeToStorageDomain( tape, storageDomain, dbSupport.getServiceManager(), mockDaoDriver );

        final TapeEnvironmentInformation noTapesResponse = constructResponse( 1, 1, 0 );

        assertEquals(
                1,
                noTapesResponse.getLibraries().length,
                "Constructed response should have one library, but no tapes" );
        assertEquals(
                1,
                noTapesResponse.getLibraries()[0].getPartitions().length,
                "Constructed response should have one library, but no tapes");
        assertEquals(
                0,
                noTapesResponse.getLibraries()[0].getPartitions()[0].getTapes().length,
                "Constructed response should have one library, but no tapes" );

        // Enable auto-quiesce and un-quiesce the tape partition
        final TapePartitionService tapePartitionService = dbSupport.getServiceManager().getService( TapePartitionService.class );
        final TapePartition partition = tapePartitionService.retrieveAll().getFirst();
        assertNotNull( partition );
        dbSupport.getServiceManager().getUpdater( TapePartition.class ).update( partition.setAutoQuiesceEnabled( true ), TapePartition.AUTO_QUIESCE_ENABLED );
        mockDaoDriver.unquiesceAllTapePartitions();

        // Verify loss of communication puts partition into quiesced state and that tape and partition state remains unchanged
        manager.reconcileWith( noTapesResponse );
        assertEquals( Quiesced.YES, tapePartitionService.retrieve( partition.getId() ).getQuiesced() );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( TapePartition.class )
                        .getCount( TapePartition.STATE,
                                TapePartitionState
                                        .ONLINE ),
                "Partition is still quiesced, partitions should still be seen as online."  );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                Tape.STATE,
                                                TapeState.NORMAL
                                        ) ),
                "Partition is still quiesced, tapes should still be in normal state."  );

        // Un-quiesce the tape partition and verify tape and partition states remain unchanged
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( TapePartition.class )
                        .getCount( TapePartition.STATE,
                                TapePartitionState.ONLINE
                        ),
                "All partitions shoulda been marked as online."  );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class ).getCount(
                        Require
                                .beanPropertyEquals(
                                        Tape.STATE,
                                        TapeState.NORMAL
                                ) ),
                "Tapes should have stayed in normal state."  );
    }

    @Test
    public void testReconcileWhenEmptyTapesDisappearsHandledCorrectlyWhenAutoQuiesceEnabled()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        mockDaoDriver.updateAllBeans(BeanFactory.newBean(Tape.class).setState(TapeState.NORMAL), Tape.STATE);

        final TapeEnvironmentInformation noTapesResponse = constructResponse( 1, 1, 0 );

        assertEquals(
                1,
                noTapesResponse.getLibraries().length,
                "Constructed response should have one library, but no tapes" );
        assertEquals(
                1,
                noTapesResponse.getLibraries()[0].getPartitions().length,
                "Constructed response should have one library, but no tapes" );
        assertEquals(
                0,
                noTapesResponse.getLibraries()[0].getPartitions()[0].getTapes().length,
                "Constructed response should have one library, but no tapes" );

        // Enable auto-quiesce and un-quiesce the tape partition
        final TapePartitionService tapePartitionService = dbSupport.getServiceManager().getService( TapePartitionService.class );
        final TapePartition partition = tapePartitionService.retrieveAll().getFirst();
        assertNotNull( partition );
        dbSupport.getServiceManager().getUpdater( TapePartition.class ).update( partition.setAutoQuiesceEnabled( true ), TapePartition.AUTO_QUIESCE_ENABLED );
        mockDaoDriver.unquiesceAllTapePartitions();

        // Verify loss of communication puts partition into quiesced state and that tape and partition state remains unchanged
        manager.reconcileWith( noTapesResponse );
        assertEquals( Quiesced.NO, tapePartitionService.retrieve( partition.getId() ).getQuiesced() );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( TapePartition.class )
                        .getCount( TapePartition.STATE,
                                TapePartitionState
                                        .ONLINE ),
                "Partition is not quiesced, but should still be seen as online." );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(),
                "Empty tape should have been deleted when it disappeared" );

        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( TapePartition.class )
                        .getCount( TapePartition.STATE,
                                TapePartitionState.ONLINE
                        ),
                "All partitions shoulda been marked as online." );
    }

    @Test
    public void testReconcileWhenTapesWereAlreadyNotPresentDoesntAutoQuiesce()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        final DataPolicy dp = mockDaoDriver.createABMConfigDualCopyOnTape();
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "a", 1 );
        final UUID blobId = dbSupport.getServiceManager()
                                     .getRetriever( Blob.class )
                                     .attain( Blob.OBJECT_ID, object.getId() )
                                     .getId();
        final Tape tape = dbSupport.getServiceManager()
                                   .getRetriever( Tape.class ).retrieveAll().getFirst();
        mockDaoDriver.putBlobOnTape(tape.getId(), blobId);
        mockDaoDriver.updateBean(tape.setState(TapeState.EJECTED), Tape.STATE);

        final TapeEnvironmentInformation noTapesResponse = constructResponse( 1, 1, 0 );

        assertEquals(
                1,
                noTapesResponse.getLibraries().length,
                "Constructed response should have one library, but no tapes");
        assertEquals(
                1,
                noTapesResponse.getLibraries()[0].getPartitions().length,
                "Constructed response should have one library, but no tapes" );
        assertEquals(
                0,
                noTapesResponse.getLibraries()[0].getPartitions()[0].getTapes().length,
                "Constructed response should have one library, but no tapes" );

        // Enable auto-quiesce and un-quiesce the tape partition
        final TapePartitionService tapePartitionService = dbSupport.getServiceManager().getService( TapePartitionService.class );
        final TapePartition partition = tapePartitionService.retrieveAll().getFirst();
        assertNotNull( partition );
        dbSupport.getServiceManager().getUpdater( TapePartition.class ).update( partition.setAutoQuiesceEnabled( true ), TapePartition.AUTO_QUIESCE_ENABLED );
        mockDaoDriver.unquiesceAllTapePartitions();

        // Verify zero tapes reported did not put partition into quiesced state and that tape and partition state remains unchanged
        manager.reconcileWith( noTapesResponse );
        assertEquals( Quiesced.NO, tapePartitionService.retrieve( partition.getId() ).getQuiesced() );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( TapePartition.class )
                        .getCount( TapePartition.STATE,
                                TapePartitionState
                                        .ONLINE ),
                "Partition should still be seen as online." );
        assertEquals(
                TapeState.EJECTED,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .retrieveAll().getFirst().getState(),
                "Partition was not quiesced, no tape states should have changed."  );
    }

    @Test
    public void testReconcileWhenPartitionErredWithAutoQuiesceDisabledAndOnlineResultsInCorrectTapeDriveStateUpdates()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        mockDaoDriver.updateAllBeans(BeanFactory.newBean(Tape.class).setState(TapeState.NORMAL), Tape.STATE);

        // Disable auto-quiesce, then un-quiesce the tape partition
        final TapePartitionService tapePartitionService = dbSupport.getServiceManager().getService( TapePartitionService.class );
        final TapePartition partition = tapePartitionService.retrieveAll().getFirst();
        dbSupport.getServiceManager().getUpdater( TapePartition.class ).update(
                partition.setAutoQuiesceEnabled( false ),
                TapePartition.AUTO_QUIESCE_ENABLED);
        // Un-quiesce the tape partition
        mockDaoDriver.unquiesceAllTapePartitions();

        // Verify error response triggers auto-quiesce without changing tape or partition state
        final TapeEnvironmentInformation errTei = constructResponse( 1, 1, 1 );
        errTei.getLibraries()[ 0 ].getPartitions()[ 0 ].setErrorMessage( "oops" );
        manager.reconcileWith( errTei );
        assertEquals( Quiesced.NO, tapePartitionService.retrieve( partition.getId() ).getQuiesced() );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( TapePartition.class )
                        .getCount( TapePartition.STATE,
                                TapePartitionState
                                        .ERROR ),
                "Partition should be in error state." );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                Tape.STATE,
                                                TapeState.LOST
                                        ) ),
                "Partition in error state, tapes should still be in lost state." );

        // Verify successful library response marks partition as online and tape as pending inspection
        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( TapePartition.class )
                        .getCount( TapePartition.STATE,
                                TapePartitionState.ONLINE
                        ) ,
                "All partitions shoulda been marked as online.");
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                Tape.STATE,
                                                TapeState.PENDING_INSPECTION
                                        ) ),
                "Tapes should have stayed in normal state." );
    }

    @Test
    public void testReconcileWhenPartitionErredWithAutoQuiesceAndOnlineResultsInCorrectTapeDriveStateUpdates()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        mockDaoDriver.updateAllBeans(BeanFactory.newBean(Tape.class).setState(TapeState.NORMAL), Tape.STATE);

        final TapeEnvironmentInformation noLibraryResponse = BeanFactory.newBean( TapeEnvironmentInformation.class );
        noLibraryResponse.setLibraries( ( TapeLibraryInformation[] ) Array.newInstance( TapeLibraryInformation.class, 0 ) );

        // Un-quiesce the tape partition
        mockDaoDriver.unquiesceAllTapePartitions();

        // Verify error response triggers auto-quiesce without changing tape or partition state
        final TapePartitionService tapePartitionService = dbSupport.getServiceManager().getService( TapePartitionService.class );
        final TapePartition partition = tapePartitionService.retrieveAll().getFirst();
        final TapeEnvironmentInformation errTei = constructResponse( 1, 1, 1 );
        errTei.getLibraries()[ 0 ].getPartitions()[ 0 ].setErrorMessage( "oops" );
        manager.reconcileWith( errTei );
        assertEquals( Quiesced.YES, tapePartitionService.retrieve( partition.getId() ).getQuiesced() );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( TapePartition.class )
                        .getCount( TapePartition.STATE,
                                TapePartitionState
                                        .ONLINE ) ,
                "Partition should be in online state due to auto-quiesce.");
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                Tape.STATE,
                                                TapeState.NORMAL
                                        ) ),
                "Tapes should be in normal state due to auto-quiesce." );
    }

    @Test
    public void testReconcileWhenPartitionGoingErredAndOnlineWithAutoQuiesceDisabledResultsInCorrectTapeDriveStateUpdates()
    {
        //check what log4j conf file is being used by the test
        System.setProperty("log4j.configuration","/home/kyleh/Perforce/dev_kyleh_2024/product/frontend/DataPlanner/src/test/resources/log4j.xml");
        System.out.println("log4j.configuration: " + System.getProperty("log4j.configuration"));

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapePartitionService tapePartitionService = dbSupport.getServiceManager().getService( TapePartitionService.class );

        final TapeEnvironmentInformation okTei = constructResponse( 2, 3, 4 );
        final TapeEnvironmentInformation errTei = constructResponse( 2, 3, 4 );
        final TapePartitionInformation errorPartitionResponse = errTei.getLibraries()[ 0 ].getPartitions()[ 0 ];
        errorPartitionResponse.setErrorMessage( "oops" );
        manager.reconcileWith( okTei );

        assertEquals(
                8,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(),
                "All tapes shoulda reported as normal initially."  );
        assertEquals(
                6,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount() ,
                "All drives shoulda reported as normal initially." );
        assertEquals( 6,
                dbSupport.getServiceManager()
                .getRetriever( TapeDrive.class )
                .getCount(
                        Require
                                .beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .NORMAL ) ) ,
                "All drives shoulda reported as normal initially." );
        final TapePartition partition = tapePartitionService.retrieve( TapePartition.SERIAL_NUMBER, errorPartitionResponse.getSerialNumber() );
        dbSupport.getServiceManager().getUpdater( TapePartition.class ).update(
                partition.setAutoQuiesceEnabled( false ),
                TapePartition.AUTO_QUIESCE_ENABLED);
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( errTei );

        final TapePartitionService partitionService = dbSupport.getServiceManager()
                                                               .getService( TapePartitionService.class );

        partitionService.attain( TapePartition.STATE, TapePartitionState.ONLINE ).getId();
        partitionService.attain( TapePartition.STATE, TapePartitionState.ERROR ).getId();


        assertEquals(
                8,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(),
                "No tapes shoulda gone away from partition offlining."  );
        assertEquals(
                6,
                dbSupport.getServiceManager()
                        .getRetriever(
                                TapeDrive
                                        .class )
                        .getCount(),
                "3 drives shoulda gone offline with the offlined partition."  );
        assertEquals(
                3,
                dbSupport.getServiceManager()
                        .getRetriever(
                                TapeDrive
                                        .class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                TapeDrive.STATE,
                                                TapeDriveState.NORMAL ) ),
                "3 drives shoulda gone offline with the offlined partition."  );

        manager.reconcileWith( okTei );

        assertEquals(
                8,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(),
                "All tapes shoulda reported as normal after partition came back online." );
        assertEquals(
                6,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(),
                "All drives shoulda reported as normal after partition came back online."  );
        assertEquals(
                6,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.NORMAL ) ),
                "All drives shoulda reported as normal after partition came back online."  );
    }

    @Test
    public void testReconcileWhenPartitionGoingErredAndOnlineWithResultsInCorrectTapeDriveStateUpdates()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        final TapeEnvironmentInformation okTei = constructResponse( 2, 3, 4 );
        final TapeEnvironmentInformation errTei = constructResponse( 2, 3, 4 );
        errTei.getLibraries()[ 0 ].getPartitions()[ 0 ].setErrorMessage( "oops" );
        manager.reconcileWith( okTei );

        assertEquals(
                8,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(),
                "All tapes shoulda reported as normal initially."  );
        assertEquals(
                6,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(),
                "All drives shoulda reported as normal initially."  );
        assertEquals(
                6,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                TapeDrive.STATE,
                                                TapeDriveState
                                                        .NORMAL ) ),
                "All drives shoulda reported as normal initially."  );

        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( errTei );

        final TapePartitionService partitionService = dbSupport.getServiceManager()
                .getService( TapePartitionService.class );
        assertNull(partitionService.retrieve( TapePartition.STATE, TapePartitionState.ERROR ));

        assertEquals(
                8, dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(),
                "No tapes shoulda gone away from partition offlining."  );
        assertEquals(
                6, dbSupport.getServiceManager()
                        .getRetriever(
                                TapeDrive
                                        .class )
                        .getCount(),
                "No drives shoulda gone offline with the offlined partition." );
        assertEquals(
                6, dbSupport.getServiceManager()
                        .getRetriever(
                                TapeDrive
                                        .class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                TapeDrive.STATE,
                                                TapeDriveState.NORMAL ) ) ,
                "No drives shoulda gone offline with the offlined partition." );

        manager.reconcileWith( okTei );

        assertEquals(
                8,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(),
                "All tapes shoulda reported as normal after partition came back online."  );
        assertEquals(
                6,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(),
                "All drives shoulda reported as normal after partition came back online."  );
        assertEquals(
                6,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.NORMAL ) ),
                "All drives shoulda reported as normal after partition came back online."  );
    }

    @Test
    public void testReconcileWhenPartitionGoingOfflineAndOnlineResultsInCorrectTapeDriveStateUpdates()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        manager.reconcileWith( constructResponse( 2, 3, 4 ) );

        assertEquals(
                8,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(),
                "All tapes shoulda reported as normal initially."  );
        assertEquals(
                6,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(),
                "All drives shoulda reported as normal initially."  );
        assertEquals(
                6,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                TapeDrive.STATE,
                                                TapeDriveState
                                                        .NORMAL ) ),
                "All drives shoulda reported as normal initially."  );

        // This test assumes that auto-quiesce is not enabled on the tape partition when it goes off and online.
        mockDaoDriver.updateAllBeans( BeanFactory.newBean( TapePartition.class ).setAutoQuiesceEnabled( false ), TapePartition.AUTO_QUIESCE_ENABLED );

        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 3, 4 ) );

        final TapePartitionService partitionService = dbSupport.getServiceManager()
                                                               .getService( TapePartitionService.class );
        partitionService.attain( TapePartition.STATE, TapePartitionState.ONLINE ).getId();
        partitionService.attain( TapePartition.STATE, TapePartitionState.OFFLINE ).getId();

        assertEquals(
                8,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(),
                "No tapes shoulda gone away from partition offlining."  );

        assertEquals(
                4,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                Tape.STATE,
                                                TapeState.LOST
                                        ) ),
                "All tapes should be LOST from offline partition."  );
        assertEquals(
                6,
                dbSupport.getServiceManager()
                        .getRetriever(
                                TapeDrive
                                        .class )
                        .getCount(),
                "3 drives shoulda gone offline with the offlined partition."  );
        assertEquals(
                3,
                dbSupport.getServiceManager()
                        .getRetriever(
                                TapeDrive
                                        .class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                TapeDrive.STATE,
                                                TapeDriveState.NORMAL ) ) ,
                "3 drives shoulda gone offline with the offlined partition." );

        manager.reconcileWith( constructResponse( 2, 3, 4 ) );

        assertEquals(
                8,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(),
                "All tapes shoulda reported as normal after partition came back online."  );
        assertEquals(
                6,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(),
                "All drives shoulda reported as normal after partition came back online."  );
        assertEquals(
                6,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.NORMAL ) ),
                "All drives shoulda reported as normal after partition came back online."  );
    }

    @Test
    public void testReconcileWhenPartitionGoingOfflineAndOnlineResultsInCorrectTapeDriveStateUpdatesWhenQuiesced()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 2, 3, 4 ) );
        mockDaoDriver.updateAllBeans(BeanFactory.newBean(Tape.class).setState(TapeState.NORMAL), Tape.STATE);

        assertEquals(
                8,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Require.beanPropertyEquals( Tape.STATE, TapeState.NORMAL ) ),
                "All tapes shoulda reported as normal."  );
        assertEquals(
                6,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.NORMAL ) ) );
        manager.reconcileWith( constructResponse( 1, 3, 4 ) );



        assertEquals(
                8,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Require.beanPropertyEquals( Tape.STATE, TapeState.NORMAL ) ),
                "All tapes shoulda still reported as normal while quiesced." );
        assertEquals(
                6,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.NORMAL ) ) ,
                "All drives shoulda still reported as normal while quiesced." );
        final Set<TapePartition> allPartitions =
                dbSupport.getServiceManager().getRetriever(TapePartition.class).retrieveAll().toSet();
        assertEquals(
                2,
                allPartitions.size(),
                "Should have been two partitions.");
        for (TapePartition partition : allPartitions) {
            final Set<UUID> tapesInPartition = manager.getTapesInPartition(partition.getId());
            assertFalse(
                    tapesInPartition.isEmpty(),
                    "Manager should have retained inventory from quiesced partition."
                    );
            for (UUID tape : tapesInPartition) {
                assertNotNull(
                        manager.getTapeElementAddress(tape),
                        "Manager should have retained inventory from quiesced partition."
                       );
            }
        }
        manager.reconcileWith( constructResponse( 2, 3, 4 ) );

        assertEquals(
                8,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Require.beanPropertyEquals( Tape.STATE, TapeState.NORMAL ) ) ,
                "All tapes shoulda reported as normal after partition came back online.");
        assertEquals(
                6,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.NORMAL ) ) ,
                "All drives shoulda reported as normal after partition came back online." );
    }

    @Test
    public void testReconcileWhenTapeChangesPartitionsDoesSoWhenDataOnTapes()
    {
        final BeansRetriever< Tape > tapeRetriever = dbSupport.getServiceManager()
                                                              .getRetriever( Tape.class );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeEnvironmentInformation response = constructResponse( 2, 2, 2 );
        final BasicTapeInformation t1 = response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ];
        final BasicTapeInformation t2 = response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ];
        final BasicTapeInformation t3 = response.getLibraries()[ 0 ].getPartitions()[ 1 ].getTapes()[ 0 ];
        final BasicTapeInformation t4 = response.getLibraries()[ 0 ].getPartitions()[ 1 ].getTapes()[ 1 ];
        t1.setBarCode( "bc1" );
        t2.setBarCode( "bc2" );
        t3.setBarCode( "bc3" );
        t4.setBarCode( "bc4" );
        manager.reconcileWith( response );

        final Tape tape1 = tapeRetriever.attain( Tape.BAR_CODE, t1.getBarCode() );
        final Tape tape2 = tapeRetriever.attain( Tape.BAR_CODE, t2.getBarCode() );
        final Tape tape3 = tapeRetriever.attain( Tape.BAR_CODE, t3.getBarCode() );
        final Tape tape4 = tapeRetriever.attain( Tape.BAR_CODE, t4.getBarCode() );
        assertEquals(
                tape2.getPartitionId(),
                tape1.getPartitionId(),
                "Should notta updated tape partition associations yet."  );
        assertEquals(
                tape4.getPartitionId(),
                tape3.getPartitionId(),
                "Should notta updated tape partition associations yet."  );
        assertNotEquals(
                tape1.getPartitionId(),
                tape3.getPartitionId(),
                "Tapes shoulda been in different partitions."  );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "blah" );
        mockDaoDriver.putBlobOnTape( tape1.getId(), blob.getId() );
        mockDaoDriver.putBlobOnTape( tape3.getId(), blob.getId() );
        mockDaoDriver.putBlobOnTape( tape4.getId(), blob.getId() );
        linkTapeToStorageDomain( tape1, storageDomain, dbSupport.getServiceManager(), mockDaoDriver );
        linkTapeToStorageDomain( tape2, storageDomain, dbSupport.getServiceManager(), mockDaoDriver );
        linkTapeToStorageDomain( tape3, storageDomain, dbSupport.getServiceManager(), mockDaoDriver );
        linkTapeToStorageDomain( tape4, storageDomain, dbSupport.getServiceManager(), mockDaoDriver );

        response.getLibraries()[ 0 ].getPartitions()[ 0 ].setTapes( new BasicTapeInformation [] { t3, t2 } );
        response.getLibraries()[ 0 ].getPartitions()[ 1 ].setTapes( new BasicTapeInformation [] { t1, t4 } );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( response );

        final Tape tape21 = tapeRetriever.attain( Tape.BAR_CODE, t1.getBarCode() );
        final Tape tape22 = tapeRetriever.attain( Tape.BAR_CODE, t2.getBarCode() );
        final Tape tape23 = tapeRetriever.attain( Tape.BAR_CODE, t3.getBarCode() );
        final Tape tape24 = tapeRetriever.attain( Tape.BAR_CODE, t4.getBarCode() );
        assertEquals(
                tape24.getPartitionId(),
                tape21.getPartitionId(),
                "Shoulda updated tape partition associations as necessary." );
        assertEquals(
                tape22.getPartitionId(),
                tape23.getPartitionId(),
                "Shoulda updated tape partition associations as necessary." );
        assertEquals(
                4,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(),
                "Shoulda been 4 tapes total." );
    }

    @Test
    public void testReconcileWhenTapeChangesPartitionsFailsWhenDataOnTapesWithNoValidStorageDomain()
    {
        final BeansRetriever< Tape > tapeRetriever = dbSupport.getServiceManager()
                                                              .getRetriever( Tape.class );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeEnvironmentInformation response = constructResponse( 2, 2, 2 );
        final BasicTapeInformation t1 = response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ];
        final BasicTapeInformation t2 = response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ];
        final BasicTapeInformation t3 = response.getLibraries()[ 0 ].getPartitions()[ 1 ].getTapes()[ 0 ];
        final BasicTapeInformation t4 = response.getLibraries()[ 0 ].getPartitions()[ 1 ].getTapes()[ 1 ];
        t1.setBarCode( "bc1" );
        t2.setBarCode( "bc2" );
        t3.setBarCode( "bc3" );
        t4.setBarCode( "bc4" );
        response.getLibraries()[ 0 ].getPartitions()[ 0 ].setTapes( new BasicTapeInformation [] { t1, t2, t3, t4 } );
        response.getLibraries()[ 0 ].getPartitions()[ 1 ].setTapes( new BasicTapeInformation [] { } );
        manager.reconcileWith( response );

        final Tape tape1 = tapeRetriever.attain( Tape.BAR_CODE, t1.getBarCode() );
        final Tape tape2 = tapeRetriever.attain( Tape.BAR_CODE, t2.getBarCode() );
        final Tape tape3 = tapeRetriever.attain( Tape.BAR_CODE, t3.getBarCode() );
        final Tape tape4 = tapeRetriever.attain( Tape.BAR_CODE, t4.getBarCode() );
        assertEquals(
                tape2.getPartitionId(),
                tape1.getPartitionId(),
                "Should notta updated tape partition associations yet." );
        assertEquals(
                tape4.getPartitionId(),
                tape3.getPartitionId(),
                "Should notta updated tape partition associations yet.");
        assertEquals(
                tape1.getPartitionId(), tape3.getPartitionId(),
                "Tapes shoulda been in same partitions.");
        assertEquals(
                tape2.getPartitionId(), tape4.getPartitionId(),
                "Tapes shoulda been in same partitions.");

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "blah" );
        mockDaoDriver.putBlobOnTape( tape1.getId(), blob.getId() );
        mockDaoDriver.putBlobOnTape( tape3.getId(), blob.getId() );
        mockDaoDriver.putBlobOnTape( tape4.getId(), blob.getId() );
        linkTapeToStorageDomain( tape1, storageDomain, dbSupport.getServiceManager(), mockDaoDriver );
        linkTapeToStorageDomain( tape2, storageDomain, dbSupport.getServiceManager(), mockDaoDriver );
        linkTapeToStorageDomain( tape3, storageDomain, dbSupport.getServiceManager(), mockDaoDriver );
        linkTapeToStorageDomain( tape4, storageDomain, dbSupport.getServiceManager(), mockDaoDriver );

        response.getLibraries()[ 0 ].getPartitions()[ 0 ].setTapes( new BasicTapeInformation [] { } );
        response.getLibraries()[ 0 ].getPartitions()[ 1 ].setTapes( new BasicTapeInformation [] { t1, t2, t3, t4 } );
        manager.reconcileWith( response );

        final Tape tape21 = tapeRetriever.attain( Tape.BAR_CODE, t1.getBarCode() );
        final Tape tape22 = tapeRetriever.attain( Tape.BAR_CODE, t2.getBarCode() );
        final Tape tape23 = tapeRetriever.attain( Tape.BAR_CODE, t3.getBarCode() );
        final Tape tape24 = tapeRetriever.attain( Tape.BAR_CODE, t4.getBarCode() );
        assertEquals(
                tape1.getPartitionId(),
                tape21.getPartitionId(),
                "Should notta changed tape partition associations.");
        assertEquals(
                tape2.getPartitionId(),
                tape22.getPartitionId(),
                "Should notta changed tape partition associations.");
        assertEquals(
                tape3.getPartitionId(),
                tape23.getPartitionId(),
                "Should notta changed tape partition associations.");
        assertEquals(
                tape4.getPartitionId(),
                tape24.getPartitionId(),
                "Should notta changed tape partition associations."  );
        assertEquals(
                4,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(),
                "Shoulda been 4 tapes total." );
    }

    @Test
    public void testReconcileWhenTapeChangesPartitionsDoesSoWhenNoDataOnTapes()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final BeansRetriever< Tape > tapeRetriever = dbSupport.getServiceManager()
                                                              .getRetriever( Tape.class );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeEnvironmentInformation response = constructResponse( 2, 2, 2 );
        final BasicTapeInformation t1 = response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ];
        final BasicTapeInformation t2 = response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ];
        final BasicTapeInformation t3 = response.getLibraries()[ 0 ].getPartitions()[ 1 ].getTapes()[ 0 ];
        final BasicTapeInformation t4 = response.getLibraries()[ 0 ].getPartitions()[ 1 ].getTapes()[ 1 ];
        t1.setBarCode( "bc1" );
        t2.setBarCode( "bc2" );
        t3.setBarCode( "bc3" );
        t4.setBarCode( "bc4" );
        manager.reconcileWith( response );

        final Tape tape1 = tapeRetriever.attain( Tape.BAR_CODE, t1.getBarCode() );
        final Tape tape2 = tapeRetriever.attain( Tape.BAR_CODE, t2.getBarCode() );
        final Tape tape3 = tapeRetriever.attain( Tape.BAR_CODE, t3.getBarCode() );
        final Tape tape4 = tapeRetriever.attain( Tape.BAR_CODE, t4.getBarCode() );
        assertEquals(
                tape2.getPartitionId(),
                tape1.getPartitionId(),
                "Should notta updated tape partition associations yet." );
        assertEquals(
                tape4.getPartitionId(), tape3.getPartitionId(),
                "Should notta updated tape partition associations yet." );
        assertNotEquals(
                tape1.getPartitionId(),
                tape3.getPartitionId(),
                "Tapes shoulda been in different partitions."  );

        response.getLibraries()[ 0 ].getPartitions()[ 0 ].setTapes( new BasicTapeInformation [] { t3, t2 } );
        response.getLibraries()[ 0 ].getPartitions()[ 1 ].setTapes( new BasicTapeInformation [] { t1, t4 } );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( response );

        final Tape tape21 = tapeRetriever.attain( Tape.BAR_CODE, t1.getBarCode() );
        final Tape tape22 = tapeRetriever.attain( Tape.BAR_CODE, t2.getBarCode() );
        final Tape tape23 = tapeRetriever.attain( Tape.BAR_CODE, t3.getBarCode() );
        final Tape tape24 = tapeRetriever.attain( Tape.BAR_CODE, t4.getBarCode() );
        assertEquals(
                tape24.getPartitionId(),
                tape21.getPartitionId(),
                "Shoulda updated tape partition associations as necessary." );
        assertEquals(
                tape22.getPartitionId(),
                tape23.getPartitionId() ,
                "Shoulda updated tape partition associations as necessary.");
        assertEquals(
                4, dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(),
                "Shoulda been 4 tapes total." );
    }

    @Test
    public void testReconcileWhenTapeInDriveDoesSo()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeEnvironmentInformation response = constructResponse( 1, 3, 3 );
        response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setElementAddress(
                response.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 0 ].getElementAddress() );
        response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setElementAddress(
                response.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].getElementAddress() );
        manager.reconcileWith( response );

        final BeansRetriever< TapeDrive > tapeDriveRetriever = dbSupport.getServiceManager()
                                                                        .getRetriever( TapeDrive.class );
        assertEquals(
                2,
                tapeDriveRetriever.getCount( Require.not( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ) ),
                "Shoulda been 2 tapes in drives.");
        assertEquals(
                1,
                tapeDriveRetriever.getCount( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ),
                "Shoulda been 1 tape not in a drive."  );

        response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setElementAddress( 1010 );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( response );
        assertEquals(
                2,
                tapeDriveRetriever.getCount( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ),
                "Shoulda been 2 tapes not in drives."  );
        assertEquals(
                1,
                tapeDriveRetriever.getCount( Require.not( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ) ),
                "Shoulda been 1 tape in a drive."  );

        manager.reconcileWith( constructResponse( 1, 3, 0 ) );
        assertEquals(
                3,
                tapeDriveRetriever.getCount( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ),
                "Should notta been any tapes reported in drives." );
    }

    @Test
    public void testReconcileWhenTapeWithoutBarCodeInDriveDoesSo()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeEnvironmentInformation response = constructResponse( 1, 3, 3 );
        response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setType( TapeType.UNKNOWN );
        response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setType( TapeType.FORBIDDEN );
        response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setBarCode( null );
        response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setBarCode( null );
        response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setElementAddress(
                response.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 0 ].getElementAddress() );
        response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setElementAddress(
                response.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].getElementAddress() );
        manager.reconcileWith( response );

        final BeansRetriever< TapeDrive > tapeDriveRetriever = dbSupport.getServiceManager()
                                                                        .getRetriever( TapeDrive.class );
        assertEquals(
                2,
                tapeDriveRetriever.getCount( Require.not( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ) ),
                "Shoulda been 2 tapes in drives."  );
        assertEquals(
                1,
                tapeDriveRetriever.getCount( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ),
                "Shoulda been 1 tape not in a drive." );

        response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setElementAddress( 1010 );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( response );
        assertEquals(
                2,
                tapeDriveRetriever.getCount( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ),
                "Shoulda been 2 tapes not in drives."  );
        assertEquals(
                1,
                tapeDriveRetriever.getCount( Require.not( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ) ),
                "Shoulda been 1 tape in a drive."  );

        manager.reconcileWith( constructResponse( 1, 3, 0 ) );
        assertEquals(
                3,
                tapeDriveRetriever.getCount( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ),
                "Should notta been any tapes reported in drives."  );
    }

    @Test
    public void testReconcileWhenTapeWithDataInDriveDoesSo()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeEnvironmentInformation response = constructResponse( 1, 3, 3 );
        response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setElementAddress(
                response.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 0 ].getElementAddress() );
        response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setElementAddress(
                response.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].getElementAddress() );
        manager.reconcileWith( response );
        final TapePartition partition = mockDaoDriver.attainOneAndOnly( TapePartition.class );
        mockDaoDriver.updateBean(partition.setAutoQuiesceEnabled(false), TapePartition.AUTO_QUIESCE_ENABLED);

        final BeansRetriever< TapeDrive > tapeDriveRetriever = dbSupport.getServiceManager()
                                                                        .getRetriever( TapeDrive.class );
        assertEquals(
                2,
                tapeDriveRetriever.getCount( Require.not( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ) ),
                "Shoulda been 2 tapes in drives."  );
        assertEquals(
                1,
                tapeDriveRetriever.getCount( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ),
                "Shoulda been 1 tape not in a drive."  );
        for ( final Tape tape : dbSupport.getServiceManager()
                                         .getRetriever( Tape.class )
                                         .retrieveAll()
                                         .toSet() )
        {
            mockDaoDriver.putBlobOnTape( tape.getId(), b1.getId() );
            linkTapeToStorageDomain( tape, sd, dbSupport.getServiceManager(), mockDaoDriver );
        }

        response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setElementAddress( 1010 );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( response );
        assertEquals(
                2,
                tapeDriveRetriever.getCount( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ),
                "Shoulda been 2 tapes not in drives." );
        assertEquals(
                1,
                tapeDriveRetriever.getCount( Require.not( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ) ),
                "Shoulda been 1 tape in a drive."  );

        manager.reconcileWith( constructResponse( 1, 3, 0 ) );
        assertEquals(
                3,
                tapeDriveRetriever.getCount( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ),
                "Should notta been any tapes reported in drives." );
    }

    @Test
    public void testReconcileWhenTapeWithDataWithoutBarCodeInDriveDoesSo()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeEnvironmentInformation response = constructResponse( 1, 3, 3 );
        response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setType( TapeType.UNKNOWN );
        response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setType( TapeType.FORBIDDEN );
        response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setBarCode( null );
        response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setBarCode( null );
        response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setElementAddress(
                response.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 0 ].getElementAddress() );
        response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setElementAddress(
                response.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].getElementAddress() );
        manager.reconcileWith( response );

        final BeansRetriever< TapeDrive > tapeDriveRetriever = dbSupport.getServiceManager()
                                                                        .getRetriever( TapeDrive.class );
        assertEquals(
                2,
                tapeDriveRetriever.getCount( Require.not( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ) ),
                "Shoulda been 2 tapes in drives."  );
        assertEquals(
                1,
                tapeDriveRetriever.getCount( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ),
                "Shoulda been 1 tape not in a drive." );
        for ( final Tape tape : dbSupport.getServiceManager()
                                         .getRetriever( Tape.class )
                                         .retrieveAll()
                                         .toSet() )
        {
            mockDaoDriver.putBlobOnTape( tape.getId(), b1.getId() );
            linkTapeToStorageDomain( tape, sd, dbSupport.getServiceManager(), mockDaoDriver );
        }

        response.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setElementAddress( 1010 );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( response );
        assertEquals(
                2,
                tapeDriveRetriever.getCount( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ),
                "Shoulda been 2 tapes not in drives." );
        assertEquals(
                1,
                tapeDriveRetriever.getCount( Require.not( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ) ),
                "Shoulda been 1 tape in a drive."  );

        final TapePartition partition = mockDaoDriver.attainOneAndOnly( TapePartition.class );
        mockDaoDriver.updateBean(partition.setAutoQuiesceEnabled(false), TapePartition.AUTO_QUIESCE_ENABLED);
        manager.reconcileWith( constructResponse( 1, 3, 0 ) );
        assertEquals(
                3,
                tapeDriveRetriever.getCount( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ),
                "Should notta been any tapes reported in drives."  );
    }

    @Test
    public void testReconcilePartitionWithNoDrivesResultsInNoErrors()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        manager.reconcileWith( constructResponse( 1, 0, 3 ) );
        assertTapeFailures( dbSupport.getServiceManager() );
    }

    @Test
    public void testReconcilePartitionWithIncompatibleTapeMediaResultsInTapesInError()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        manager.reconcileWith( constructResponse( 1, 1, 3 ) );
        assertTapeFailures( dbSupport.getServiceManager() );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 0, 1 ) );
        TapeEnvironmentInformation tei = constructResponse( 1, 1, 3 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setType( TapeType.TS_JC );
        manager.reconcileWith( tei );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_MEDIA_TYPE_INCOMPATIBLE, 1,
                TapePartitionFailureType.TAPE_REMOVAL_UNEXPECTED, 2 );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Tape.STATE,
                                TapeState
                                        .INCOMPATIBLE ),
                "Shoulda reported single TS tape as being incompatible."  );

        manager.reconcileWith( constructResponse( 1, 0, 1 ) );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setType( TapeType.UNKNOWN );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setType( TapeType.FORBIDDEN );
        manager.reconcileWith( tei );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_REMOVAL_UNEXPECTED, 4 );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Tape.STATE,
                                TapeState.INCOMPATIBLE ),
                "Shoulda reported all tapes as being compatible."  );

        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setType( TapeType.LTO5 );
        manager.reconcileWith( tei );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_REMOVAL_UNEXPECTED, 4 );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Tape.STATE,
                                TapeState.INCOMPATIBLE ),
                "Shoulda reported all tapes as being compatible." );

        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setType( TapeType.LTO7 );
        manager.reconcileWith( tei );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_REMOVAL_UNEXPECTED, 4 );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Tape.STATE,
                                TapeState.INCOMPATIBLE ),
                "Shoulda reported all tapes as being compatible."  );

        tei = constructResponse( 2, 1, 4 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 3 ].setType( TapeType.LTO7 );
        manager.reconcileWith( tei );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_MEDIA_TYPE_INCOMPATIBLE, 1,
                TapePartitionFailureType.TAPE_REMOVAL_UNEXPECTED, 4 );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever(
                                Tape.class )
                        .getCount( Tape.STATE,
                                TapeState
                                        .INCOMPATIBLE ),
                "Shoulda reported single LTO7 tape as being incompatible."  );

        mockDaoDriver.updateAllBeans( BeanFactory.newBean( Tape.class )
                                                 .setType( TapeType.LTO6 ), Tape.TYPE );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 3 ].setType( TapeType.LTO6 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 0 ].setType( TapeDriveType.LTO7 );
        manager.reconcileWith( tei );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_REMOVAL_UNEXPECTED, 4 );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Tape.STATE,
                                TapeState.INCOMPATIBLE ),
                "Shoulda reported all tapes as being compatible."  );
        mockDaoDriver.updateAllBeans( BeanFactory.newBean( Tape.class )
                                                 .setType( TapeType.LTO6 ), Tape.TYPE );

        mockDaoDriver.updateAllBeans( BeanFactory.newBean( Tape.class )
                                                 .setType( TapeType.LTO7 ), Tape.TYPE );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 0 ].setType( TapeDriveType.LTO6 );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( tei );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_MEDIA_TYPE_INCOMPATIBLE, 2,
                TapePartitionFailureType.TAPE_REMOVAL_UNEXPECTED, 4 );
        assertEquals(
                8,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Tape.STATE,
                                TapeState.INCOMPATIBLE
                        ),
                "Shoulda reported all tapes as being incompatible." );

        mockDaoDriver.updateAllBeans( BeanFactory.newBean( Tape.class )
                                                 .setState( TapeState.EJECT_TO_EE_IN_PROGRESS ), Tape.STATE );
        manager.reconcileWith( tei );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_REMOVAL_UNEXPECTED, 4 );
        assertEquals(
                8,
                dbSupport.getServiceManager()
                        .getRetriever(
                                Tape.class )
                        .getCount( Tape.STATE,
                                TapeState
                                        .EJECT_TO_EE_IN_PROGRESS ),
                "Shoulda reported all tapes as being eject to ee in progress." );

    }

    @Test
    public void testReconcileDiscoveredLto7PartitionWithLtoM8TapesResultsInTapesSetToIncompatible() {

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        TapeEnvironmentInformation tei = constructResponse( 1, 2, 2 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 0 ].setType( TapeDriveType.LTO7 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].setType( TapeDriveType.LTO7 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setType( TapeType.LTOM8 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setType( TapeType.LTOM8 );

        manager.reconcileWith( tei );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_MEDIA_TYPE_INCOMPATIBLE, 1);
        assertEquals(
                2,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Tape.STATE,
                                TapeState.INCOMPATIBLE
                        ) ,
                "Shoulda reported all tapes as being incompatible." );
    }

    @Test
    public void testReconcilePartitionWithTapeDrivesInErrorResultsInTapeDrivesInError()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        manager.reconcileWith( constructResponse( 1, 3, 1 ) );
        assertEquals(
                3,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .NORMAL ) ) ,
                "Shoulda configured all tape drives as usable." );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .ERROR ) ),
                "Shoulda configured all tape drives as usable."  );
        assertTapeFailures( dbSupport.getServiceManager() );

        manager.reconcileWith( constructResponse( 1, 0, 1 ) );
        final TapeEnvironmentInformation tei = constructResponse( 1, 3, 1 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].setErrorMessage( "error" );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( tei );
        assertEquals(
                2,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.NORMAL ) ),
                "Shoulda configured all tape drives as usable, except the one in error."  );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.ERROR ) ),
                "Shoulda configured all tape drives as usable, except the one in error." );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_DRIVE_IN_ERROR, 1 );
    }

    @Test
    public void testReconcilePartitionWithTapeDrivesForcingTapeRemovalDoesSo()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        manager.reconcileWith( constructResponse( 1, 3, 1 ) );
        assertEquals(
                3,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .NORMAL ) ),
                "Shoulda configured all tape drives as usable."  );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .ERROR ) ),
                "Shoulda configured all tape drives as usable." );
        assertTapeFailures( dbSupport.getServiceManager() );

        manager.reconcileWith( constructResponse( 1, 0, 1 ) );
        final TapeEnvironmentInformation tei = constructResponse( 1, 3, 1 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].setErrorMessage( "error" );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].setForceTapeRemoval( Boolean.TRUE );
        manager.reconcileWith( tei );
        assertEquals(
                3,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.NORMAL ) ),
                "Shoulda configured all tape drives as usable, including the one in error."  );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.ERROR ) ),
                "Shoulda configured all tape drives as usable, including the one in error."  );
        assertTapeFailures( dbSupport.getServiceManager() );

        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].setForceTapeRemoval( Boolean.FALSE );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( tei );
        assertEquals(
                2,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.NORMAL ) ),
                "Shoulda configured all tape drives as usable, except the one in error." );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.ERROR ) ),
                "Shoulda configured all tape drives as usable, except the one in error."  );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_DRIVE_IN_ERROR, 1 );

        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].setForceTapeRemoval( Boolean.TRUE );
        manager.reconcileWith( tei );
        assertEquals(
                3,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.NORMAL ) ),
                "Shoulda configured all tape drives as usable, including the one in error." );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.ERROR ) ) ,
                "Shoulda configured all tape drives as usable, including the one in error." );
        assertTapeFailures( dbSupport.getServiceManager() );
    }

    @Test
    public void testReconcilePartitionWithTapeDrivesRequiringCleaningReportsThem()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        manager.reconcileWith( constructResponse( 1, 3, 1 ) );
        assertEquals(
                3,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .NORMAL ) ),
                "Shoulda configured all tape drives as usable." );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .ERROR ) ),
                "Shoulda configured all tape drives as usable."  );
        assertTapeFailures( dbSupport.getServiceManager() );
        assertEquals(
                0, manager.getDrivesRequiringCleaning()
                        .size(),
                "Shoulda reported no drives requiring cleaning."  );

        manager.reconcileWith( constructResponse( 1, 0, 1 ) );
        final TapeEnvironmentInformation tei = constructResponse( 1, 3, 1 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].setCleaningRequired( Boolean.TRUE );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( tei );
        assertEquals(
                3,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .NORMAL ) ),
                "Shoulda configured all tape drives as usable."  );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .ERROR ) ) ,
                "Shoulda configured all tape drives as usable." );
        assertTapeFailures( dbSupport.getServiceManager() );
        assertEquals(
                1, manager.getDrivesRequiringCleaning()
                        .size(),
                "Shoulda reported drive requiring cleaning."  );

        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].setCleaningRequired( Boolean.FALSE );
        manager.reconcileWith( tei );
        assertEquals(
                3,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .NORMAL ) ),
                "Shoulda configured all tape drives as usable." );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .ERROR ) ),
                "Shoulda configured all tape drives as usable."  );
        assertTapeFailures( dbSupport.getServiceManager() );
        assertEquals(
                0,
                manager.getDrivesRequiringCleaning()
                        .size(),
                "Shoulda reported no drives requiring cleaning." );
    }

    @Test
    public void testReconcilePartitionWithTapeDrivesRequiringCleaningThatHaveBeenCleanedReportsThem()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        final TapeDrive drive = dbSupport.getServiceManager()
                                         .getRetriever( TapeDrive.class )
                                         .attain( Require.nothing() );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 2, 1 ) );
        final TapeDrive drive2 = dbSupport.getServiceManager()
                                          .getRetriever( TapeDrive.class )
                                          .attain( Require.not(
                                                  Require.beanPropertyEquals( Identifiable.ID, drive.getId() ) ) );

        final TapePartition partition = mockDaoDriver.attainOneAndOnly( TapePartition.class );
        final Tape tape = mockDaoDriver.createTape( partition.getId(), TapeState.PENDING_INSPECTION );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        linkTapeToStorageDomain( tape, sd, dbSupport.getServiceManager(), mockDaoDriver );
        dbSupport.getServiceManager()
                 .getService( TapeFailureService.class )
                 .create( BeanFactory.newBean( TapeFailure.class )
                                     .setErrorMessage( "error" )
                                     .setTapeDriveId( drive.getId() )
                                     .setTapeId( tape.getId() )
                                     .setType( TapeFailureType.DRIVE_CLEAN_FAILED )
                                     .setDate( new Date() ), null );
        dbSupport.getServiceManager()
                 .getService( TapeFailureService.class )
                 .create( BeanFactory.newBean( TapeFailure.class )
                                     .setErrorMessage( "error" )
                                     .setTapeDriveId( drive.getId() )
                                     .setTapeId( tape.getId() )
                                     .setType( TapeFailureType.DRIVE_CLEANED )
                                     .setDate( new Date() ), null );
        dbSupport.getServiceManager()
                 .getService( TapeFailureService.class )
                 .create( BeanFactory.newBean( TapeFailure.class )
                                     .setErrorMessage( "error" )
                                     .setTapeDriveId( drive2.getId() )
                                     .setTapeId( tape.getId() )
                                     .setType( TapeFailureType.DRIVE_CLEANED )
                                     .setDate( new Date() ), null );
        dbSupport.getServiceManager()
                 .getService( TapeFailureService.class )
                 .create( BeanFactory.newBean( TapeFailure.class )
                                     .setErrorMessage( "error" )
                                     .setTapeDriveId( drive.getId() )
                                     .setTapeId( tape.getId() )
                                     .setType( TapeFailureType.DRIVE_CLEANED )
                                     .setDate( new Date( System.currentTimeMillis() - 3600 * 1000L * 30 ) ), null );
        dbSupport.getServiceManager()
                 .getService( TapeFailureService.class )
                 .create( BeanFactory.newBean( TapeFailure.class )
                                     .setErrorMessage( "error" )
                                     .setTapeDriveId( drive.getId() )
                                     .setTapeId( tape.getId() )
                                     .setType( TapeFailureType.BLOB_READ_FAILED )
                                     .setDate( new Date() ), null );

        manager.reconcileWith( constructResponse( 1, 0, 1 ) );
        final TapeEnvironmentInformation tei = constructResponse( 1, 3, 1 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].setCleaningRequired( Boolean.TRUE );
        manager.reconcileWith( tei );
        assertEquals(
                3,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .NORMAL ) ),
                "Shoulda configured all tape drives as usable."  );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .ERROR ) ),
                "Shoulda configured all tape drives as usable."  );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_REMOVAL_UNEXPECTED, 1 );
        assertEquals(
                1, manager.getDrivesRequiringCleaning()
                        .size(),
                "Shoulda reported drive requiring cleaning."  );

        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].setCleaningRequired( Boolean.FALSE );
        manager.reconcileWith( tei );
        assertEquals(
                3,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .NORMAL ) ),
                "Shoulda configured all tape drives as usable."  );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState.ERROR ) ),
                "Shoulda configured all tape drives as usable."  );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_REMOVAL_UNEXPECTED, 1 );
        assertEquals(
                0,
                manager.getDrivesRequiringCleaning()
                        .size(),
                "Shoulda reported no drives requiring cleaning."  );
    }

    @Test
    public void testReconcilePartitionWithTapeDrivesRequiringCleaningThatHaveBeenCleanedTooManyTimesReportsThemInError()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        manager.reconcileWith( constructResponse( 1, 1, 1 ) );

        final TapePartition partition = mockDaoDriver.attainOneAndOnly( TapePartition.class );
        final Tape tape = mockDaoDriver.createTape( partition.getId(), TapeState.PENDING_INSPECTION );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        linkTapeToStorageDomain( tape, sd, dbSupport.getServiceManager(), mockDaoDriver );
        final TapeDrive drive = dbSupport.getServiceManager()
                                         .getRetriever( TapeDrive.class )
                                         .attain( Require.nothing() );
        dbSupport.getServiceManager()
                 .getService( TapeFailureService.class )
                 .create( BeanFactory.newBean( TapeFailure.class )
                                     .setErrorMessage( "error" )
                                     .setTapeDriveId( drive.getId() )
                                     .setTapeId( tape.getId() )
                                     .setType( TapeFailureType.DRIVE_CLEAN_FAILED )
                                     .setDate( new Date() ), null );
        dbSupport.getServiceManager()
                 .getService( TapeFailureService.class )
                 .create( BeanFactory.newBean( TapeFailure.class )
                                     .setErrorMessage( "error" )
                                     .setTapeDriveId( drive.getId() )
                                     .setTapeId( tape.getId() )
                                     .setType( TapeFailureType.DRIVE_CLEANED )
                                     .setDate( new Date() ), null );
        dbSupport.getServiceManager()
                 .getService( TapeFailureService.class )
                 .create( BeanFactory.newBean( TapeFailure.class )
                                     .setErrorMessage( "error" )
                                     .setTapeDriveId( drive.getId() )
                                     .setTapeId( tape.getId() )
                                     .setType( TapeFailureType.DRIVE_CLEANED )
                                     .setDate( new Date( System.currentTimeMillis() - 3600 * 1000L * 20 ) ), null );
        dbSupport.getServiceManager()
                 .getService( TapeFailureService.class )
                 .create( BeanFactory.newBean( TapeFailure.class )
                                     .setErrorMessage( "error" )
                                     .setTapeDriveId( drive.getId() )
                                     .setTapeId( tape.getId() )
                                     .setType( TapeFailureType.BLOB_READ_FAILED )
                                     .setDate( new Date() ), null );

        final TapeEnvironmentInformation tei = constructResponse( 1, 3, 1 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 0 ].setCleaningRequired( Boolean.TRUE );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( tei );
        assertEquals(
                2,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.NORMAL ) ),
                "Shoulda configured drive that's requesting too many cleans as being in error."  );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.ERROR ) ) ,
                "Shoulda configured drive that's requesting too many cleans as being in error.");
        assertEquals(
                0, manager.getDrivesRequiringCleaning()
                        .size(),
                "Shoulda reported no drive requiring cleaning."  );

        manager.reconcileWith( constructResponse( 1, 3, 1 ) );
        assertEquals(
                3,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .NORMAL ) ),
                "Shoulda configured all tape drives as usable."  );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .ERROR ) ),
                "Shoulda configured all tape drives as usable."  );
        assertEquals(
                0,
                manager.getDrivesRequiringCleaning()
                        .size(),
                "Shoulda reported no drives requiring cleaning."  );
    }

    @Test
    public void testReconcilePartitionWithTapeDrivesWithBarcodeLessTapeLoadedInThemDoesSo()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        manager.reconcileWith( constructResponse( 1, 3, 1 ) );
        assertEquals(
                3,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .NORMAL ) ),
                "Shoulda configured all tape drives as usable."  );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .ERROR ) ),
                "Shoulda configured all tape drives as usable." );

        assertTapeFailures( dbSupport.getServiceManager() );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 0, 1 ) );
        final TapeEnvironmentInformation tei = constructResponse( 1, 3, 1 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setElementAddress(
                tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].getElementAddress() );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setBarCode( null );

        manager.reconcileWith( tei );
        assertEquals(
                3,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.NORMAL ) ),
                "Shoulda configured all tape drives as usable, including the one in error." );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.ERROR ) ) ,
                "Shoulda configured all tape drives as usable, including the one in error." );
        assertEquals(
                1, dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.FORCE_TAPE_REMOVAL,
                                        Boolean.TRUE )
                        ),
                "Shoulda forced removal of tape without bar code."  );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_REMOVAL_UNEXPECTED, 1 );

        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].setForceTapeRemoval( Boolean.FALSE );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setElementAddress(
                tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].getElementAddress() );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setBarCode( "something" );
        manager.reconcileWith( tei );
        assertEquals(
                3, dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .NORMAL ) ),
                "Shoulda configured all tape drives as usable." );
        assertEquals(
                0, dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                TapeDrive
                                                        .FORCE_TAPE_REMOVAL,

                                                Boolean.TRUE ) ),
                "Shoulda notta been any forced removal necessary."  );

        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].setForceTapeRemoval( Boolean.TRUE );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setElementAddress(
                tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].getElementAddress() );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setBarCode( null );
        manager.reconcileWith( tei );
        assertEquals(
                3,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.NORMAL ) ),
                "Shoulda configured all tape drives as usable, including the one in error." );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.ERROR ) ),
                "Shoulda configured all tape drives as usable, including the one in error."  );
        assertEquals(
                1, dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                TapeDrive
                                                        .FORCE_TAPE_REMOVAL,
                                                Boolean.TRUE )
                        ),
                "Shoulda forced removal of tape without bar code."  );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_REMOVAL_UNEXPECTED, 4 );
    }

    @Test
    public void testReconcilePartitionWithTapeDrivesInErrorWithBarcodeLessTapeLoadedInThemDoesSo()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        manager.reconcileWith( constructResponse( 1, 3, 1 ) );
        assertEquals(
                3, dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .NORMAL ) ),
                "Shoulda configured all tape drives as usable."  );
        assertEquals(
                0, dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .ERROR ) ),
                "Shoulda configured all tape drives as usable."  );
        assertEquals(
                0, dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals(
                                TapeDrive.FORCE_TAPE_REMOVAL,
                                Boolean.TRUE ) ),
                "Shoulda notta been any forced removals." );
        assertTapeFailures( dbSupport.getServiceManager() );

        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 0, 1 ) );
        final TapeEnvironmentInformation tei = constructResponse( 1, 3, 1 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].setErrorMessage( "error" );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setElementAddress(
                tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].getElementAddress() );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setBarCode( null );
        manager.reconcileWith( tei );
        assertEquals(
                2,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.NORMAL ) ) ,
                "Shoulda configured all tape drives as usable, except the one in error.");
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(

                                Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.ERROR ) ),
                "Shoulda configured all tape drives as usable, except the one in error."  );
        assertEquals(
                0, dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                TapeDrive
                                                        .FORCE_TAPE_REMOVAL,
                                                Boolean.TRUE ) ),
                "Shoulda notta been any forced removal necessary."  );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_DRIVE_IN_ERROR, 1,
                TapePartitionFailureType.TAPE_REMOVAL_UNEXPECTED, 1 );

        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].setErrorMessage( "error" );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].setForceTapeRemoval( Boolean.FALSE );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setElementAddress(
                tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].getElementAddress() );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setBarCode( "something" );
        manager.reconcileWith( tei );
        assertEquals(
                2,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.NORMAL ) ),
                "Shoulda configured all tape drives as usable, except the one in error." );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.ERROR ) ),
                "Shoulda configured all tape drives as usable, except the one in error."  );
        assertEquals(
                0, dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require
                                        .beanPropertyEquals(
                                                TapeDrive
                                                        .FORCE_TAPE_REMOVAL,
                                                Boolean.TRUE )
                        ),
                "Shoulda notta been any forced removal necessary." );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_DRIVE_IN_ERROR, 1,
                TapePartitionFailureType.TAPE_REMOVAL_UNEXPECTED, 2 );
    }

    @Test
    public void testReconcilePartitionWithTapeDriveCountMinimumOnlyGeneratesFailuresIfCountLessThanMinimum()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), 2 );

        manager.reconcileWith( constructResponse( 1, 3, 1 ) );
        assertTapeFailures( dbSupport.getServiceManager() );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 2, 1 ) );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_DRIVE_MISSING, 1 );

        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.MINIMUM_DRIVE_COUNT_NOT_MET, 1,
                TapePartitionFailureType.TAPE_DRIVE_MISSING, 2 );
        assertEquals(
                TapeDriveType.LTO6,
                dbSupport.getServiceManager()
                        .getRetriever( TapePartition.class )
                        .attain( Require.nothing() )
                        .getDriveType(),
                "Shoulda reported partition as using LTO 6 drives."  );

        manager.reconcileWith( constructResponse( 1, 0, 1 ) );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.MINIMUM_DRIVE_COUNT_NOT_MET, 1,
                TapePartitionFailureType.TAPE_DRIVE_MISSING, 3 );
        assertNull(
                dbSupport.getServiceManager()
                        .getRetriever(
                                TapePartition.class )
                        .attain(
                                Require
                                        .nothing() )
                        .getDriveType(),
                "Shoulda reported partition as not having a valid tape drive type." );

        manager.reconcileWith( constructResponse( 1, 2, 1 ) );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_DRIVE_MISSING, 1 );
        assertEquals(
                TapeDriveType.LTO6,
                dbSupport.getServiceManager()
                        .getRetriever( TapePartition.class )
                        .attain( Require.nothing() )
                        .getDriveType(),
                "Shoulda reported partition as using LTO 6 drives."  );

        manager.reconcileWith( constructResponse( 1, 3, 1 ) );
        assertTapeFailures( dbSupport.getServiceManager() );
    }

    @Test
    public void testReconcilePartitionWithTapeDrivesAllOfSameTypeResultsInAllDrivesUsable()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        manager.reconcileWith( constructResponse( 1, 3, 1 ) );
        assertEquals(
                3, dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .NORMAL ) ),
                "Shoulda configured all tape drives as usable."  );
        assertEquals(
                0, dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount(
                                Require.beanPropertyEquals(
                                        TapeDrive.STATE,
                                        TapeDriveState
                                                .NOT_COMPATIBLE_IN_PARTITION_DUE_TO_NEWER_TAPE_DRIVES ) ),
                "Shoulda configured all tape drives as usable."  );
        assertTapeFailures( dbSupport.getServiceManager() );

        final TapeEnvironmentInformation tei = constructResponse( 1, 3, 1 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives()[ 1 ].setType( TapeDriveType.LTO5 );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( tei );
        assertEquals(
                2,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE, TapeDriveState.NORMAL ) ),
                "Shoulda configured all tape drives as usable, except the one LTO 5 one."  );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( Require.beanPropertyEquals( TapeDrive.STATE,
                                TapeDriveState.NOT_COMPATIBLE_IN_PARTITION_DUE_TO_NEWER_TAPE_DRIVES ) ),
                "Shoulda configured all tape drives as usable, except the one LTO 5 one." );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_DRIVE_TYPE_MISMATCH, 1 );
        assertEquals(
                TapeDriveType.LTO6,
                dbSupport.getServiceManager()
                        .getRetriever( TapePartition.class )
                        .attain( Require.nothing() )
                        .getDriveType(),
                "Shoulda reported partition as using LTO 6 drives."  );
    }

    @Test
    public void testReconcilePartitionWithCleaningTapeNotPendingInspection()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        final TapeEnvironmentInformation tei = constructResponse( 1, 1, 1 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setType( TapeType.LTO_CLEANING_TAPE );
        manager.reconcileWith( tei );

        assertEquals(
                0, dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Require.beanPropertyEquals(
                                Tape.STATE,
                                TapeState.PENDING_INSPECTION ) ),
                "Should not have tapes as pending inspection." );
        assertEquals(
                1, dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Require.beanPropertyEquals(
                                Tape.STATE,
                                TapeState.NORMAL ) ),
                "Should have tapes as normal."  );
    }

    @Test
    public void testReconcilePartitionEnsuresThatStorageDomainMembersAreMarkedReadOnlyAsNecessary()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        manager.reconcileWith( constructResponse( 1, 3, 1 ) );
        final TapePartition partition = dbSupport.getServiceManager()
                                                 .getRetriever( TapePartition.class )
                                                 .attain( Require.nothing() );

        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), partition.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), partition.getId(), TapeType.LTO6 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), partition.getId(), TapeType.LTO7 );

        manager.reconcileWith( constructResponse( 1, 3, 1 ) );

        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( StorageDomainMember.class )
                        .getCount( StorageDomainMember.WRITE_PREFERENCE, WritePreferenceLevel.NEVER_SELECT ),
                "Shoulda marked storage domain members that aren't writable as read only." );
    }

    @Test
    public void testReconcilePartitionChangedSerialNumberWorks()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        final TapeEnvironmentInformation tapeEnvironmentInformation = constructResponse( 1, 5, 20 );
        final TapeLibraryInformation tapeLibraryInformation = tapeEnvironmentInformation.getLibraries()[ 0 ];

        manager.reconcileWith( tapeEnvironmentInformation );
        final TapePartitionInformation tapePartitionInformation = tapeLibraryInformation.getPartitions()[ 0 ];
        tapePartitionInformation.setSerialNumber( "POST_" + tapePartitionInformation.getSerialNumber() );

        manager.reconcileWith( tapeEnvironmentInformation );
        dbSupport.getServiceManager()
                 .getRetriever( TapePartition.class )
                 .attain( Require.nothing() );
    }

    @Test
    public void testReconcilePartitionChangedSerialNumberReplaceDriveWorks()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        final TapeEnvironmentInformation tapeEnvironmentInformation = constructResponse( 1, 5, 20 );
        final TapeLibraryInformation tapeLibraryInformation = tapeEnvironmentInformation.getLibraries()[ 0 ];

        manager.reconcileWith( tapeEnvironmentInformation );
        final TapePartitionInformation tapePartitionInformation = tapeLibraryInformation.getPartitions()[ 0 ];
        tapePartitionInformation.setSerialNumber( "POST_" + tapePartitionInformation.getSerialNumber() );
        final TapeDriveInformation driveInformation = tapePartitionInformation.getDrives()[ 0 ];
        driveInformation.setSerialNumber( "POST_" + driveInformation.getSerialNumber() );

        UUID partitionId = dbSupport.getServiceManager()
                                    .getRetriever( TapePartition.class )
                                    .attain( Require.nothing() )
                                    .getId();
        assertEquals(
                5, dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( TapeDrive.PARTITION_ID,
                                partitionId ),
                "Should of had 5 drives in partition" );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( tapeEnvironmentInformation );
        partitionId = dbSupport.getServiceManager()
                               .getRetriever( TapePartition.class )
                               .attain( Require.nothing() )
                               .getId();
        assertEquals(
                6, dbSupport.getServiceManager()
                        .getRetriever( TapeDrive.class )
                        .getCount( TapeDrive.PARTITION_ID,
                                partitionId ),
                "Should of had 6 drives in partition" );
        assertEquals(
                20, dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Tape.PARTITION_ID, partitionId ),
                "Should of had 20 tapes in partition"  );
    }

    @Test
    public void testReconcilePartitionChangedSerialNumberRemoveOneTapeChangesPartition()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        final TapeEnvironmentInformation tapeEnvironmentInformation = constructResponse( 1, 5, 20 );
        final TapeLibraryInformation tapeLibraryInformation = tapeEnvironmentInformation.getLibraries()[ 0 ];

        manager.reconcileWith( tapeEnvironmentInformation );
        final TapePartitionInformation tapePartitionInformation = tapeLibraryInformation.getPartitions()[ 0 ];
        tapePartitionInformation.setSerialNumber( "POST_" + tapePartitionInformation.getSerialNumber() );
        final BasicTapeInformation[] tapes = tapePartitionInformation.getTapes();
        tapePartitionInformation.setTapes( Arrays.copyOf( tapes, tapes.length - 1 ) );

        dbSupport.getServiceManager()
                 .getRetriever( TapePartition.class )
                 .attain( Require.nothing() )
                 .getId();
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( tapeEnvironmentInformation );
        assertNotNull(
                dbSupport.getServiceManager()
                        .getRetriever(
                                TapePartition.class )
                        .attain(
                                TapePartition
                                        .SERIAL_NUMBER,
                                tapePartitionInformation
                                        .getSerialNumber() ) ,
                "Should of found a partition with new serialNumber" );
        final UUID partitionId = dbSupport.getServiceManager()
                                          .getRetriever( TapePartition.class )
                                          .attain( Require.nothing() )
                                          .getId();
        assertEquals(
                19, dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Tape.PARTITION_ID, partitionId ),
                "Should of had 19 tapes in partition" );
    }

    @Test
    public void testReconcileDetectsSamePartitionWhenSerialNumberChangesAndTwoTapesEjected()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        final TapeEnvironmentInformation tapeEnvironmentInformation = constructResponse( 1, 5, 20 );
        final TapeLibraryInformation tapeLibraryInformation = tapeEnvironmentInformation.getLibraries()[ 0 ];

        manager.reconcileWith( tapeEnvironmentInformation );

        // Eject 2 tapes and update partition serial number.
        // There must be more than 1 difference in tapes to produce a new partition.
        final TapePartitionInformation tapePartitionInformation = tapeLibraryInformation.getPartitions()[ 0 ];
        tapePartitionInformation.setSerialNumber( "POST_" + tapePartitionInformation.getSerialNumber() );
        final BasicTapeInformation[] tapes = tapePartitionInformation.getTapes();

        List<TapePartition> partitions = dbSupport.getServiceManager().getRetriever(TapePartition.class).retrieveAll().toList();
        assertEquals(1, partitions.size());
        final TapePartition originalPartition = partitions.get(0);

        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );

        final Tape ejectedTape1 = tapeService.attain( Tape.BAR_CODE, tapes[tapes.length - 1].getBarCode() );
        tapeService.transistState( ejectedTape1, TapeState.EJECTED );

        final Tape ejectedTape2 = tapeService.attain( Tape.BAR_CODE, tapes[tapes.length - 2].getBarCode() );
        tapeService.transistState( ejectedTape2, TapeState.EJECTED );

        tapePartitionInformation.setTapes( Arrays.copyOf( tapes, tapes.length - 2 ) );

        dbSupport.getServiceManager()
                .getRetriever( TapePartition.class )
                .attain( Require.nothing() )
                .getId();
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( tapeEnvironmentInformation );

        partitions = dbSupport.getServiceManager().getRetriever(TapePartition.class).retrieveAll().toList();
        assertEquals(1, partitions.size());
        final TapePartition currentPartition = partitions.get(0);
        assertEquals(
                currentPartition.getId(), originalPartition.getId(),
                "Partition ID should not have changed." );
        assertNotEquals("Partition serial number should have updated.", currentPartition.getSerialNumber(), originalPartition.getSerialNumber());
        final UUID partitionId = dbSupport.getServiceManager()
                .getRetriever( TapePartition.class )
                .attain( Require.nothing() )
                .getId();
        assertEquals(
                18, dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Tape.PARTITION_ID, partitionId ),
                "Should of had 18 tapes in partition" );
    }

    @Test
    public void testReconcilePartitionChangedSerialNumberAddOneTapeChangesPartition()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        final TapeEnvironmentInformation tapeEnvironmentInformation = constructResponse( 1, 5, 20 );
        final TapeLibraryInformation tapeLibraryInformation = tapeEnvironmentInformation.getLibraries()[ 0 ];

        manager.reconcileWith( tapeEnvironmentInformation );
        final TapePartitionInformation tapePartitionInformation = tapeLibraryInformation.getPartitions()[ 0 ];
        tapePartitionInformation.setSerialNumber( "POST_" + tapePartitionInformation.getSerialNumber() );
        final BasicTapeInformation[] tapes = tapePartitionInformation.getTapes();
        final BasicTapeInformation[] tapesPlusOne = new BasicTapeInformation[ tapes.length + 1 ];
        System.arraycopy( tapes, 0, tapesPlusOne, 0, tapes.length );
        final BasicTapeInformation lastTapeInformation = tapes[ tapes.length - 1 ];
        final BasicTapeInformation tapeInformation = BeanFactory.newBean( BasicTapeInformation.class );
        tapeInformation.setElementAddress( lastTapeInformation.getElementAddress() + 1 );
        tapeInformation.setBarCode( lastTapeInformation.getBarCode() + "_after" );
        tapeInformation.setType( lastTapeInformation.getType() );
        tapesPlusOne[ tapes.length ] = tapeInformation;
        tapePartitionInformation.setTapes( tapesPlusOne );

        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( tapeEnvironmentInformation );
        dbSupport.getServiceManager()
                 .getRetriever( TapePartition.class )
                 .attain( Require.nothing() );
        assertNotNull(
                dbSupport.getServiceManager()
                        .getRetriever(
                                TapePartition.class )
                        .attain(
                                TapePartition
                                        .SERIAL_NUMBER,
                                tapePartitionInformation
                                        .getSerialNumber() ),
                "Should of found a partition with new serialNumber"  );
        final UUID partitionId = dbSupport.getServiceManager()
                                          .getRetriever( TapePartition.class )
                                          .attain( Require.nothing() )
                                          .getId();
        assertEquals(
                21, dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Tape.PARTITION_ID, partitionId ),
                "Should of had 21 tapes in partition"  );
    }

    @Test
    public void testReconcilePartitionChangedSerialNumberTwoTapeDifferenceCreatesNewPartition()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        final TapeEnvironmentInformation tapeEnvironmentInformation = constructResponse( 1, 5, 20 );
        final TapeLibraryInformation tapeLibraryInformation = tapeEnvironmentInformation.getLibraries()[ 0 ];

        manager.reconcileWith( tapeEnvironmentInformation );
        final TapePartitionInformation tapePartitionInformation = tapeLibraryInformation.getPartitions()[ 0 ];
        final String originalSerialNumber = tapePartitionInformation.getSerialNumber();
        tapePartitionInformation.setSerialNumber( "POST_" + originalSerialNumber );
        final BasicTapeInformation tapeInformation0 = tapePartitionInformation.getTapes()[ 0 ];
        tapeInformation0.setBarCode( "POST_" + tapeInformation0.getBarCode() );
        final BasicTapeInformation tapeInformation1 = tapePartitionInformation.getTapes()[ 1 ];
        tapeInformation1.setBarCode( "POST_" + tapeInformation1.getBarCode() );

        manager.reconcileWith( tapeEnvironmentInformation );
        assertNotNull(
                dbSupport.getServiceManager()
                        .getRetriever(
                                TapePartition
                                        .class )
                        .attain(
                                TapePartition
                                        .SERIAL_NUMBER,
                                tapePartitionInformation.getSerialNumber() ),
                "Should of found a single partition with new serialNumber"  );
        assertNotNull(
                dbSupport.getServiceManager()
                        .getRetriever(
                                TapePartition
                                        .class )
                        .attain(
                                TapePartition
                                        .SERIAL_NUMBER,
                                originalSerialNumber ),
                "Should of found a single partition with old serialNumber"  );
        final UUID partitionId = dbSupport.getServiceManager()
                                          .getRetriever( TapePartition.class )
                                          .attain( TapePartition.SERIAL_NUMBER,
                                                  tapePartitionInformation.getSerialNumber() )
                                          .getId();
        assertEquals(
                20, dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Tape.PARTITION_ID, partitionId ),
                "Should of had 20 tapes in partition"  );
    }

    @Test
    public void testReconcilePartitionChangedSerialNumberNoTapesCreatesNewPartition()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        final TapeEnvironmentInformation tapeEnvironmentInformation = constructResponse( 1, 5, 0 );
        final TapeLibraryInformation tapeLibraryInformation = tapeEnvironmentInformation.getLibraries()[ 0 ];

        manager.reconcileWith( tapeEnvironmentInformation );
        final TapePartitionInformation tapePartitionInformation = tapeLibraryInformation.getPartitions()[ 0 ];
        final String originalSerialNumber = tapePartitionInformation.getSerialNumber();
        tapePartitionInformation.setSerialNumber( "POST_" + originalSerialNumber );

        manager.reconcileWith( tapeEnvironmentInformation );
        assertNotNull(
                dbSupport.getServiceManager()
                        .getRetriever(
                                TapePartition
                                        .class )
                        .attain(
                                TapePartition
                                        .SERIAL_NUMBER,
                                tapePartitionInformation.getSerialNumber() ),
                "Should of found a single partition with new serialNumber"  );
        assertNotNull(
                dbSupport.getServiceManager()
                        .getRetriever(
                                TapePartition
                                        .class )
                        .attain(
                                TapePartition
                                        .SERIAL_NUMBER,
                                originalSerialNumber ),
                "Should of found a single partition with old serialNumber" );
    }

    @Test
    public void testReconcilePartitionDuplicateSerialNumberFails()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        final TapeEnvironmentInformation tapeEnvironmentInformation = constructResponse( 2, 1, 1 );
        final TapeLibraryInformation tapeLibraryInformation = tapeEnvironmentInformation.getLibraries()[ 0 ];
        final TapePartitionInformation tapePartitionInformation0 = tapeLibraryInformation.getPartitions()[ 0 ];
        final TapePartitionInformation tapePartitionInformation1 = tapeLibraryInformation.getPartitions()[ 1 ];
        tapePartitionInformation1.setSerialNumber( tapePartitionInformation0.getSerialNumber() );

        TestUtil.assertThrows( null, IllegalStateException.class,
                () -> manager.reconcileWith( tapeEnvironmentInformation ) );
    }

    @Test
    public void testReconcilePartitionUpdatesImportExportConfigurationAsItChanges()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        assertEquals(
                ImportExportConfiguration.SUPPORTED,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain( Require.nothing() )
                        .getImportExportConfiguration(),
                                "Shoulda reported correct import/export configuration.");
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 0, 0 ) );
        assertEquals(
                ImportExportConfiguration.SUPPORTED,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain( Require.nothing() )
                        .getImportExportConfiguration() ,
                "Shoulda reported correct import/export configuration." );

        final TapeEnvironmentInformation tei = constructResponse( 1, 1, 1 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].setImportExportConfiguration(
                ImportExportConfiguration.NOT_SUPPORTED );
        manager.reconcileWith( tei );
        assertEquals(
                ImportExportConfiguration.NOT_SUPPORTED,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain( Require.nothing() )
                        .getImportExportConfiguration(),
                "Shoulda reported correct import/export configuration." );
    }

    @Test
    public void testReconcileEjectFromEePendingTapesGraduallyPhysicallyRemovedWorks()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 1, 2 ) );
        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();
        dbSupport.getServiceManager()
                 .getService( TapeService.class )
                 .transistState( tapes.get( 0 ), TapeState.EJECT_FROM_EE_PENDING );
        dbSupport.getServiceManager()
                 .getService( TapeService.class )
                 .transistState( tapes.get( 1 ), TapeState.EJECT_FROM_EE_PENDING );
        dbSupport.getServiceManager()
                 .getService( TapePartitionFailureService.class )
                 .create( tapes.get( 0 )
                               .getPartitionId(), TapePartitionFailureType.TAPE_EJECTION_BY_OPERATOR_REQUIRED, "blah",
                         null );

        final TapeEnvironmentInformation tei = constructResponse( 1, 1, 1 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setElementAddress( 10000 );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( tei );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_EJECTION_BY_OPERATOR_REQUIRED,
                1 );
        assertEquals(
                1, dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Tape.STATE,
                                TapeState
                                        .EJECT_FROM_EE_PENDING ),
                "One tape shoulda been retained as being eject pending."  );
        assertEquals(
                1, dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(),
                "One tape shoulda been retained as being eject pending."  );

        manager.reconcileWith( constructResponse( 1, 1, 0 ) );
        assertTapeFailures( dbSupport.getServiceManager() );
        assertEquals(
                0, dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(),
                "Both tapes shoulda ejected."  );
    }

    @Test
    public void testReconcileEjectFromEePendingTapesGraduallyMovedToStorageSlotsWorks()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 1, 2 ) );
        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();
        dbSupport.getServiceManager()
                 .getService( TapeService.class )
                 .transistState( tapes.get( 0 ), TapeState.EJECT_FROM_EE_PENDING );
        dbSupport.getServiceManager()
                 .getService( TapeService.class )
                 .transistState( tapes.get( 1 ), TapeState.EJECT_FROM_EE_PENDING );
        dbSupport.getServiceManager()
                 .getService( TapePartitionFailureService.class )
                 .create( tapes.get( 0 )
                               .getPartitionId(), TapePartitionFailureType.TAPE_EJECTION_BY_OPERATOR_REQUIRED, "blah",
                         null );

        final TapeEnvironmentInformation tei = constructResponse( 1, 1, 2 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setElementAddress( 10000 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setElementAddress( 10001 );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( tei );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_EJECTION_BY_OPERATOR_REQUIRED,
                1 );
        assertEquals(
                2, dbSupport.getServiceManager()
                        .getRetriever(
                                Tape.class )
                        .getCount( Tape.STATE,
                                TapeState
                                        .EJECT_FROM_EE_PENDING ),
                "Both tapes shoulda been retained as being eject pending."  );
        assertEquals(
                0, dbSupport.getServiceManager()
                        .getRetriever(
                                Tape.class )
                        .getCount( Tape.STATE,
                                TapeState
                                        .PENDING_INSPECTION ),
                "Both tapes shoulda been retained as being eject pending."  );


        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setElementAddress( 1000 );
        manager.reconcileWith( tei );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_EJECTION_BY_OPERATOR_REQUIRED,
                1 );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Tape.STATE, TapeState.EJECT_FROM_EE_PENDING ),
                "One tape shoulda been retained as being eject pending, the other shoulda been pend insp."  );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Tape.STATE, TapeState.PENDING_INSPECTION ),
                "One tape shoulda been retained as being eject pending, the other shoulda been pend insp."  );

        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setElementAddress( 1001 );
        manager.reconcileWith( tei );
        assertTapeFailures( dbSupport.getServiceManager() );
        assertEquals(
                0, dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Tape.STATE,
                                TapeState.EJECT_FROM_EE_PENDING ),
                "Both tapes shoulda been pend insp."  );
        assertEquals(
                2, dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount( Tape.STATE,
                                TapeState.PENDING_INSPECTION ),
                "Both tapes shoulda been pend insp."  );
    }

    @Test
    public void testReconcileWithTapeMissingBarCodeInTapeDriveDoesSo()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeEnvironmentInformation response = constructResponse( 2, 3, 5 );

        final List< TapePartition > partitions = dbSupport.getServiceManager()
                                                          .getRetriever( TapePartition.class )
                                                          .retrieveAll()
                                                          .toList();
        partitions.sort( new BeanComparator<>( TapePartition.class, SerialNumberObservable.SERIAL_NUMBER ) );
        final List< TapeDrive > drives = dbSupport.getServiceManager()
                                                  .getRetriever( TapeDrive.class )
                                                  .retrieveAll()
                                                  .toList();
        drives.sort( new BeanComparator<>( TapeDrive.class, SerialNumberObservable.SERIAL_NUMBER ) );
        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();
        tapes.sort( new BeanComparator<>( Tape.class, Tape.BAR_CODE ) );

        removeBarCode( null, true, response );
        manager.reconcileWith( response );
        assertEquals(
                1, dbSupport.getDataManager()
                        .getCount( Tape.class,
                                Require.beanPropertyEquals(
                                        Tape.STATE,
                                        TapeState
                                                .BAR_CODE_MISSING )
                        ),
                "Shoulda been a tape with an unknown barcode."  );

        final Tape tape = dbSupport.getServiceManager()
                                   .getRetriever( Tape.class )
                                   .attain( Tape.STATE, TapeState.BAR_CODE_MISSING );
        final TapeDrive drive = dbSupport.getServiceManager()
                                         .getRetriever( TapeDrive.class )
                                         .retrieveAll()
                                         .getFirst();
        dbSupport.getServiceManager()
                 .getService( TapeDriveService.class )
                 .update( drive.setTapeId( tape.getId() ), TapeDrive.TAPE_ID );
        dbSupport.getServiceManager()
                 .getService( TapeService.class )
                 .transistState( tape, TapeState.BAR_CODE_MISSING );

        final int originalDriveElementAddress = manager.getDriveElementAddress( drive.getId() );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( response );
        assertEquals(
                originalDriveElementAddress,
                manager.getDriveElementAddress( drive.getId() ),
                "Shoulda reported same drive element address."  );
    }

    @Test
    public void testReconcileTapeDoesNotDeleteMissingTapeNotAssignedToStorageDomainWithBlobTapes()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );

        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        tapeService.transistState( getOnlyTape( tapeService ), TapeState.NORMAL );
        final Tape tape = mockDaoDriver.attainOneAndOnly( Tape.class );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );

        manager.reconcileWith( constructResponse( 1, 1, 0 ) );
        assertEquals(
                1, tapeService.getCount(),
                "Should notta removed the tape." );
    }

    @Test
    public void testReconcileTapeDeletesMissingTapeNotAssignedToStorageDomainWithNoBlobTapes()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        tapeService.transistState( getOnlyTape( tapeService ), TapeState.NORMAL );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 1, 0 ) );
        assertEquals(
                0, tapeService.getCount(),
                "Shoulda removed the tape."  );
    }

    @Test
    public void testReconcileTapeDoesNotSetBarCodeMissingTapeToLost()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        Tape tape;

        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        tape = getOnlyTape( tapeService );
        linkTapeToStorageDomain( tape, storageDomain, dbSupport.getServiceManager(), mockDaoDriver );
        tapeService.transistState( tape, TapeState.BAR_CODE_MISSING );

        manager.reconcileWith( constructResponse( 1, 1, 0 ) );
        tape = getOnlyTape( tapeService );
        assertEquals(
                TapeState.BAR_CODE_MISSING,
                tape.getState(),
                "The tape shoulda had a state of bar code missing." );
        assertNull(
                tape.getPreviousState(),
                "The tape should notta had a previous state." );
    }

    @Test
    public void testReconcileTapeThatIsEjectedThenShowsUpInStorageSlotResultsInPendingInspStateTransition()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        Tape tape;

        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        tape = getOnlyTape( tapeService );
        linkTapeToStorageDomain( tape, storageDomain, dbSupport.getServiceManager(), mockDaoDriver );
        tapeService.transistState( tape, TapeState.EJECTED );

        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 1, 0 ) );

        final TapeEnvironmentInformation tei = constructResponse( 1, 1, 1 );
        manager.reconcileWith( tei );
        tape = getOnlyTape( tapeService );
        assertEquals(
                TapeState.PENDING_INSPECTION, tape.getState(),
                "The tape shoulda had its state updated." );
        assertNull(
                tape.getPreviousState(),
                "The tape shoulda had its state updated."  );
    }

    @Test
    public void testReconcileTapeThatIsLostThenShowsUpInStorageSlotResultsInPendingInspectionStateTransition()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        Tape tape;

        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        tape = getOnlyTape( tapeService );
        linkTapeToStorageDomain( tape, storageDomain, dbSupport.getServiceManager(), mockDaoDriver );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 1, 0 ) );

        final TapeEnvironmentInformation tei = constructResponse( 1, 1, 1 );
        manager.reconcileWith( tei );
        tape = getOnlyTape( tapeService );
        assertEquals(TapeState.PENDING_INSPECTION, tape.getState(),  "The tape shoulda had its state updated.");
        assertNull( tape.getPreviousState(), "The tape shoulda had its state updated." );
    }

    @Test
    public void testReconcileTapeThatIsOnlineThenShowsUpInImportExportSlotResultsInOfflineStateTransition()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        Tape tape;

        final TapeEnvironmentInformation tei = constructResponse( 1, 1, 1 );
        manager.reconcileWith( tei );
        tape = getOnlyTape( tapeService );
        assertEquals(  TapeState.PENDING_INSPECTION, tape.getState() , "The tape shoulda initially been online.");

        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setElementAddress( 10000 );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( tei );
        tape = getOnlyTape( tapeService );
        assertEquals( TapeState.OFFLINE, tape.getState(), "The tape shoulda had its state updated." );
    }

    @Test
    public void testReconcileTapeThatIsOfflineThenShowsUpInStorageSlotResultsInOnlineStateTransition()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        Tape tape;

        final TapeEnvironmentInformation tei = constructResponse( 1, 1, 1 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setElementAddress( 10000 );
        manager.reconcileWith( tei );
        tape = getOnlyTape( tapeService );
        assertEquals(
                TapeState.OFFLINE, tape.getState(),
                "The tape shoulda initially been offline." );

        tapeService.transistState( tape, TapeState.ONLINE_PENDING );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( tei );
        tape = getOnlyTape( tapeService );
        assertEquals(
                TapeState.ONLINE_PENDING, tape.getState(),
                "The tape should notta had its state updated."  );

        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        tape = getOnlyTape( tapeService );
        assertEquals(
                TapeState.PENDING_INSPECTION, tape.getState(),
                "The tape shoulda had its state updated."  );
    }

    @Test
    public void testReconcileTapeThatIsEjectedThenShowsUpInImportExportSlotResultsInOfflineStateTransition()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        Tape tape;

        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        tape = getOnlyTape( tapeService );
        linkTapeToStorageDomain( tape, storageDomain, dbSupport.getServiceManager(), mockDaoDriver );
        tapeService.transistState( tape, TapeState.EJECTED );

        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 1, 0 ) );

        final TapeEnvironmentInformation tei = constructResponse( 1, 1, 1 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setElementAddress( 10000 );
        manager.reconcileWith( tei );
        tape = getOnlyTape( tapeService );
        assertEquals(
                TapeState.OFFLINE, tape.getState(),
                "The tape shoulda had its state updated."  );
        assertEquals(
                TapeState.PENDING_INSPECTION,
                tape.getPreviousState(),
                "The tape shoulda had its state updated."  );
    }

    @Test
    public void testReconcileTapeThatIsLostThenShowsUpInImportExportSlotResultsInOfflineStateTransition()
    {

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );

        Tape tape;

        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        final TapePartition partition = mockDaoDriver.attainOneAndOnly( TapePartition.class );
        mockDaoDriver.updateBean(partition.setAutoQuiesceEnabled(false), TapePartition.AUTO_QUIESCE_ENABLED);
        tape = getOnlyTape( tapeService );
        linkTapeToStorageDomain( tape, storageDomain, dbSupport.getServiceManager(), mockDaoDriver );
        tapeService.transistState( tape, TapeState.NORMAL );
        tapeService.update( tape.setEjectPending( new Date() ), Tape.EJECT_PENDING );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 1, 0 ) );
        tape = getOnlyTape( tapeService );
        assertNull(
                tape.getEjectPending(),
                "Eject pending flag shoulda been cleared when tape went away."  );
        tapeService.update( tape.setEjectPending( new Date() ), Tape.EJECT_PENDING );

        final TapeEnvironmentInformation tei = constructResponse( 1, 1, 1 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setElementAddress( 10000 );
        manager.reconcileWith( tei );
        tape = getOnlyTape( tapeService );
        assertEquals(
                TapeState.OFFLINE, tape.getState(),
                "The tape shoulda had its state updated." );
        assertEquals( TapeState.NORMAL, tape.getPreviousState(),
                "The tape shoulda had its state updated."  );
        assertNull(
                tape.getEjectPending(),
                "Eject pending flag shoulda been cleared when tape showed back up."  );
    }

    @Test
    public void testReconcileTapeThatShowsUpForTheFirstTimeInImportExportSlotResultsInOfflineState()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );

        final TapeEnvironmentInformation tei = constructResponse( 1, 1, 1 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setElementAddress( 10000 );
        manager.reconcileWith( tei );
        final Tape tape = getOnlyTape( tapeService );
        assertEquals(
                TapeState.OFFLINE, tape.getState(),
                "The tape shoulda had its state initialized to offline." );
        assertEquals(
                TapeState.PENDING_INSPECTION, tape.getPreviousState(),
                "The tape shoulda had its state initialized to offline." );
    }

    @Test
    public void testReconcileNonDataTapeThatShowsUpForTheFirstTimeInImportExportSlotResultsInOfflineState()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );

        final TapeEnvironmentInformation tei = constructResponse( 1, 1, 1 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setElementAddress( 10000 );
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].setType( TapeType.LTO_CLEANING_TAPE );
        manager.reconcileWith( tei );
        final Tape tape = getOnlyTape( tapeService );
        assertEquals(
                TapeState.OFFLINE, tape.getState(),
                "The tape shoulda had its state initialized to offline." );
        assertEquals(
                TapeState.NORMAL, tape.getPreviousState(),
                "The tape shoulda had its state initialized to offline."  );
    }

    @Test
    public void testTapeLossEventResultsInNoTapePartitionFailureIfNotIllegalSinceNoNeedToRetainTapeAtAll()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        final Tape tape = dbSupport.getServiceManager()
                                   .getRetriever( Tape.class )
                                   .attain( Require.nothing() );
        mockDaoDriver.updateBean( tape.setAssignedToStorageDomain( true ), PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );

        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 1, 0 ) );
        assertEquals(
                0, dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .getCount(),
                "Shoulda deleted tape." );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( StorageDomainFailure.class )
                        .getCount(),
                "Should notta generated a failure since ejection wasn't illegal." );
    }

    @Test
    public void testTapeLossDisassociatedWithPartition()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        final TapePartition partition = mockDaoDriver.attainOneAndOnly( TapePartition.class );
        mockDaoDriver.updateBean(partition.setAutoQuiesceEnabled(false), TapePartition.AUTO_QUIESCE_ENABLED);
        final Tape tape = dbSupport.getServiceManager()
                .getRetriever( Tape.class )
                .attain( Require.nothing() );
        linkTapeToStorageDomain( tape, sd, dbSupport.getServiceManager(), mockDaoDriver );

        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 1, 0 ) );
        mockDaoDriver.attainAndUpdate( tape );
        assertEquals(
                TapeState.LOST, tape.getState(),
                "Shoulda recorded tape as lost." );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( StorageDomainFailure.class )
                        .getCount(),
                "Should notta generated a failure since ejection wasn't illegal."  );
        assertNull(
                tape.getPartitionId(),
                "Should have disassociated with partition." );
    }

    @Test
    public void testTapeLossEventResultsInNoTapePartitionFailureIfNotIllegalSinceTapeNotAllocatedToStorDom()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        final Tape tape = dbSupport.getServiceManager()
                                   .getRetriever( Tape.class )
                                   .attain( Require.nothing() );
        linkTapeToStorageDomain( tape, sd, dbSupport.getServiceManager(), mockDaoDriver );

        mockDaoDriver.unquiesceAllTapePartitions();

        mockDaoDriver.updateBean( tape.setStorageDomainMemberId( null )
                                      .setAssignedToStorageDomain( false ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 1, 0 ) );
        assertNull(
                mockDaoDriver.retrieve( tape ),
                "Shoulda whacked tape since it no longer contains data on it."  );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( StorageDomainFailure.class )
                        .getCount(),
                "Should notta generated a failure since ejection wasn't illegal."  );
    }

    @Test
    public void testTapeEjectionEventDisassociatesFromPartition()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        final Tape tape = dbSupport.getServiceManager()
                .getRetriever( Tape.class )
                .attain( Require.nothing() );
        mockDaoDriver.updateBean( tape.setState( TapeState.EJECT_FROM_EE_PENDING ), Tape.STATE );
        linkTapeToStorageDomain( tape, sd, dbSupport.getServiceManager(), mockDaoDriver );
        mockDaoDriver.unquiesceAllTapePartitions();

        manager.reconcileWith( constructResponse( 1, 1, 0 ) );
        mockDaoDriver.attainAndUpdate( tape );
        assertEquals(
                TapeState.EJECTED, tape.getState(),
                "Shoulda recorded tape as ejected."  );
        assertNull(
                tape.getPartitionId(),
                "Should have disassociated from tape partition" );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( StorageDomainFailure.class )
                        .getCount(),
                "Should notta generated a failure since ejection wasn't illegal." );
    }

    @Test
    public void testTapeEjectEventResultsInNoTapePartitionFailureIfNotIllegalSinceTapeNotAllocatedToStorDom()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        final Tape tape = dbSupport.getServiceManager()
                                   .getRetriever( Tape.class )
                                   .attain( Require.nothing() );
        mockDaoDriver.updateBean( tape.setState( TapeState.EJECT_FROM_EE_PENDING ), Tape.STATE );
        linkTapeToStorageDomain( tape, sd, dbSupport.getServiceManager(), mockDaoDriver );
        mockDaoDriver.unquiesceAllTapePartitions();

        mockDaoDriver.updateBean( tape.setStorageDomainMemberId( null )
                                      .setAssignedToStorageDomain( false ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        manager.reconcileWith( constructResponse( 1, 1, 0 ) );
        assertNull(
                mockDaoDriver.retrieve( tape ),
                "Shoulda whacked tape since it no longer contains data on it." );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( StorageDomainFailure.class )
                        .getCount(),
                "Should notta generated a failure since ejection wasn't illegal."  );
    }

    @Test
    public void testTapeLossEventResultsInNoTapePartitionFailureIfNotIllegalSinceTapeAllocatedToStorDomPermittingEjctn()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.updateBean( storageDomain.setMediaEjectionAllowed( true ), StorageDomain.MEDIA_EJECTION_ALLOWED );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        final TapePartition partition = mockDaoDriver.attainOneAndOnly( TapePartition.class );
        mockDaoDriver.updateBean(partition.setAutoQuiesceEnabled(false), TapePartition.AUTO_QUIESCE_ENABLED);
        final Tape tape = dbSupport.getServiceManager()
                                   .getRetriever( Tape.class )
                                   .attain( Require.nothing() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );
        linkTapeToStorageDomain( tape, storageDomain, dbSupport.getServiceManager(), mockDaoDriver );

        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 1, 0 ) );
        mockDaoDriver.attainAndUpdate( tape );
        assertEquals(
                TapeState.LOST, tape.getState(),
                "Shoulda recorded tape as lost." );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever( StorageDomainFailure.class )
                        .getCount() ,
                "Should notta generated a failure since ejection wasn't illegal.");
    }

    @Test
    public void testTapeLossEventResultsInTapePartitionFailureIfIllegalSinceTapeAllocatedToStorDomDisallowingEjection()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.updateBean( storageDomain.setMediaEjectionAllowed( false ),
                StorageDomain.MEDIA_EJECTION_ALLOWED );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );

        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        final TapePartition partition = mockDaoDriver.attainOneAndOnly( TapePartition.class );
        mockDaoDriver.updateBean(partition.setAutoQuiesceEnabled(false), TapePartition.AUTO_QUIESCE_ENABLED);
        final Tape tape = dbSupport.getServiceManager()
                                   .getRetriever( Tape.class )
                                   .attain( Require.nothing() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );
        linkTapeToStorageDomain( tape, storageDomain, dbSupport.getServiceManager(), mockDaoDriver );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 1, 0 ) );
        mockDaoDriver.attainAndUpdate( tape );
        assertEquals(
                TapeState.LOST, tape.getState(),
                "Shoulda recorded tape as lost."  );
        assertEquals(
                1, dbSupport.getServiceManager()
                        .getRetriever(
                                StorageDomainFailure.class )
                        .getCount(),
                "Shoulda generated a failure since ejection was illegal."  );
    }

    @Test
    public void testReconcileTapesWhenDuplicateBarCodeHandledAsGracefullyAsPossible()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );

        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        final Tape tape = getOnlyTape( tapeService );
        linkTapeToStorageDomain( tape, storageDomain, dbSupport.getServiceManager(), mockDaoDriver );

        mockDaoDriver.unquiesceAllTapePartitions();

        final TapeEnvironmentInformation tei = constructResponse( 1, 1, 3 );
        final String duplicateBarCode = tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 0 ].getBarCode();
        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setBarCode( duplicateBarCode );
        manager.reconcileWith( tei );

        assertEquals(
                TapeState.LOST,
                tapeService.attain( Tape.BAR_CODE, duplicateBarCode ).getState(),
                        "Shoulda reported bad state for tapes with duplicate bar codes."  );
        assertEquals(
                TapeState.PENDING_INSPECTION,
                tapeService.attain( Require.not( Require.beanPropertyEquals( Tape.BAR_CODE, duplicateBarCode ) ) )
                        .getState(),
                "Shoulda reported good state for tapes with non-duplicate bar codes."
                 );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.DUPLICATE_TAPE_BAR_CODES_DETECTED,
                1, TapePartitionFailureType.TAPE_REMOVAL_UNEXPECTED, 1 );

        manager.reconcileWith( tei );

        assertEquals(
                TapeState.LOST,
                tapeService.attain( Tape.BAR_CODE, duplicateBarCode ).getState(),
                "Shoulda reported bad state for tapes with duplicate bar codes." );
        assertEquals(
                TapeState.PENDING_INSPECTION,
                tapeService.attain( Require.not( Require.beanPropertyEquals( Tape.BAR_CODE, duplicateBarCode ) ) )
                        .getState(),
                "Shoulda reported good state for tapes with non-duplicate bar codes."
                 );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.DUPLICATE_TAPE_BAR_CODES_DETECTED,
                1, TapePartitionFailureType.TAPE_REMOVAL_UNEXPECTED, 2 );

        tei.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes()[ 1 ].setBarCode( "nonduplicatebarcode" );

        manager.reconcileWith( tei );

        assertEquals(
                TapeState.PENDING_INSPECTION,
                tapeService.attain( Tape.BAR_CODE, duplicateBarCode ).getState(),
                "Shoulda reported ok state for tape without duplicate bar code."  );
        assertTapeFailures( dbSupport.getServiceManager(), TapePartitionFailureType.TAPE_REMOVAL_UNEXPECTED, 2 );
    }

    @Test
    public void testReconcileTapeLosesTapeInCancellableStateAndBringsItBackCancelled()
    {
        final TapeState[] tapeStates = { TapeState.EJECT_TO_EE_IN_PROGRESS, TapeState.FORMAT_PENDING };
        for ( final TapeState tapeState : tapeStates )
        {
            checkTapeLostWorkflow( TapeState.NORMAL, tapeState, TapeState.NORMAL );
        }
    }

    @Test
    public void testReconcileTapeLosesTapeAndBringsItBack()
    {
        final TapeState[] tapeStates =
                { TapeState.EJECTED, TapeState.EJECT_TO_EE_IN_PROGRESS, TapeState.EJECT_FROM_EE_PENDING,
                        TapeState.FORMAT_IN_PROGRESS, TapeState.FOREIGN, TapeState.IMPORT_IN_PROGRESS,
                        TapeState.LTFS_WITH_FOREIGN_DATA, TapeState.NORMAL, TapeState.PENDING_INSPECTION,
                        TapeState.SERIAL_NUMBER_MISMATCH, TapeState.UNKNOWN };
        for ( final TapeState tapeState : tapeStates )
        {
            checkTapeLostWorkflow( null, tapeState, tapeState );
        }
    }


    private void checkTapeLostWorkflow( final TapeState startingPreviousState, final TapeState startingState,
            final TapeState endingState )

    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        Tape tape;

        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        final TapePartition partition = mockDaoDriver.attainOneAndOnly( TapePartition.class );
        mockDaoDriver.updateBean(partition.setAutoQuiesceEnabled(false), TapePartition.AUTO_QUIESCE_ENABLED);
        tape = getOnlyTape( tapeService );
        tape.setPreviousState( startingPreviousState );
        tape.setState( startingState );
        tape.setWriteProtected( true );
        tape.setEjectDate( new Date( 100 ) );
        tape.setEjectLabel( "label" );
        tape.setEjectLocation( "location" );
        tapeService.update( tape, Tape.WRITE_PROTECTED, Tape.EJECT_DATE, Tape.EJECT_LABEL, Tape.EJECT_LOCATION );
        linkTapeToStorageDomain( tape, storageDomain, dbSupport.getServiceManager(), mockDaoDriver );
        dbSupport.getDataManager()
                 .updateBean( CollectionFactory.toSet( Tape.STATE, Tape.PREVIOUS_STATE ), tape );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( constructResponse( 1, 1, 0 ) );
        tape = getOnlyTape( tapeService );
        assertEquals(
                ( TapeState.EJECT_FROM_EE_PENDING == startingState || TapeState.EJECTED == startingState ) ?
                        TapeState.EJECTED : TapeState.LOST, tape.getState() ,
                "The tape shoulda had a state of lost."
               );
        assertEquals(
                ( endingState.canBePreviousState() ) ? endingState : null, tape.getPreviousState(),
                "The tape shoulda had the expected previous state."
               );
        if ( TapeState.EJECTED == startingState || TapeState.LOST == startingState )
        {
            assertEquals(
                    100, tape.getEjectDate()
                            .getTime(),
                    "Should notta set eject date when tape was still away."  );
        }
        else
        {
            assertTrue(
                    tape.getEjectDate()
                            .getTime() + 1000 >
                            System.currentTimeMillis(),
                    "Shoulda set eject date when tape went away."  );
        }
        assertEquals(
                "label", tape.getEjectLabel(),
                "Should notta modified eject label or location."  );
        assertEquals(
                "location", tape.getEjectLocation(),
                "Should notta modified eject label or location."  );
        manager.reconcileWith( constructResponse( 1, 1, 1 ) );
        tape = getOnlyTape( tapeService );
        assertEquals(
                ( endingState.canBePreviousState() && TapeState.PENDING_INSPECTION != endingState ) ? endingState :
                        null, tape.getPreviousState(),
                "The tape shoulda had the expected state kept as its previous state."
                );
        assertEquals(
                TapeState.PENDING_INSPECTION, tape.getState(),
                "The tape shoulda been put into a pending inspection state since it was lost."
               );
        assertFalse(
                tape.isWriteProtected(),
                "Since the tape was lost, we don't know if it's write protected anymore, so shoulda updated."
                );
        assertNull( tape.getEjectDate(), "Shoulda set eject date when tape came back." );
        assertEquals( "label" , tape.getEjectLabel(), "Should notta modified eject label or location."  );
        assertEquals( "location", tape.getEjectLocation(), "Should notta modified eject label or location."   );
    }


    private static Tape getOnlyTape( final TapeService tapeService )
    {
        final List< Tape > tapes = tapeService.retrieveAll().toList();
        assertEquals( 1, tapes.size(), "Shoulda returned exactly one tape.");
        return tapes.get( 0 );
    }

    @Test
    public void testReconcileWithTapeThatWasOnceKnownAndIsNowMissingBarCodeInTapeDriveDoesSo()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        final TapeEnvironmentInformation response = constructResponse( 2, 3, 5 );
        response.getLibraries()[ 0 ].getPartitions()[ 1 ].getTapes()[ 1 ].setBarCode( null );
        response.getLibraries()[ 0 ].getPartitions()[ 1 ].getTapes()[ 1 ].setType( null );
        manager.reconcileWith( response );

        final List< TapePartition > partitions = dbSupport.getServiceManager()
                                                          .getRetriever( TapePartition.class )
                                                          .retrieveAll()
                                                          .toList();
        partitions.sort( new BeanComparator<>( TapePartition.class, SerialNumberObservable.SERIAL_NUMBER ) );
        final List< TapeDrive > drives = dbSupport.getServiceManager()
                                                  .getRetriever( TapeDrive.class )
                                                  .retrieveAll()
                                                  .toList();
        drives.sort( new BeanComparator<>( TapeDrive.class, SerialNumberObservable.SERIAL_NUMBER ) );
        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();
        tapes.sort( new BeanComparator<>( Tape.class, Tape.BAR_CODE ) );

        assertEquals(
                2, partitions.size(),
                "Shoulda created dao objects to reflect tape environment response."  );
        assertEquals(
                6, drives.size(),
                "Shoulda created dao objects to reflect tape environment response."  );
        assertEquals(
                10, tapes.size(),
                "Shoulda created dao objects to reflect tape environment response."  );
        assertEquals(
                1, dbSupport.getDataManager()
                        .getCount( Tape.class,
                                Require.beanPropertyEqualsOneOf(
                                        Tape.STATE,
                                        TapeState
                                                .BAR_CODE_MISSING )
                        ),
                "Shoulda been a tape with an unknown barcode."  );
        mockDaoDriver.unquiesceAllTapePartitions();
        manager.reconcileWith( response );

        final Tape tape = dbSupport.getServiceManager()
                                   .getRetriever( Tape.class )
                                   .retrieveAll()
                                   .getFirst();
        final TapeDrive drive = dbSupport.getServiceManager()
                                         .getRetriever( TapeDrive.class )
                                         .retrieveAll()
                                         .getFirst();
        dbSupport.getServiceManager()
                 .getService( TapeDriveService.class )
                 .update( drive.setTapeId( tape.getId() ), TapeDrive.TAPE_ID );
        dbSupport.getServiceManager()
                 .getService( TapeService.class )
                 .transistState( tape, TapeState.BAR_CODE_MISSING );
        removeBarCode( tape.getBarCode(), true, response );

        final int originalDriveElementAddress = manager.getDriveElementAddress( drive.getId() );
        manager.reconcileWith( response );
        assertEquals(
                originalDriveElementAddress,
                manager.getDriveElementAddress( drive.getId() ),
                "Shoulda reported same drive element address."  );
    }

    @Test
    public void testMoveTapeToDriveNullTapeIdNotAllowed()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        final BeansRetriever< TapeDrive > driveRetriever = dbSupport.getServiceManager()
                                                                    .getRetriever( TapeDrive.class );
        final List< TapeDrive > drives = driveRetriever.retrieveAll()
                                                       .toList();
        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();
        final List< Integer > availableSlots = CollectionFactory.toList( manager.getTapeElementAddress( tapes.get( 0 )
                                                                                                             .getId() ),
                manager.getTapeElementAddress( tapes.get( 1 )
                                                    .getId() ), manager.getTapeElementAddress( tapes.get( 2 )
                                                                                                    .getId() ) );
        Collections.sort( availableSlots );
        assertEquals(
                3, drives.size(),
                "Shoulda created db objects as expected for the reconciled tape environment." );
        assertEquals(
                5, tapes.size(),
                "Shoulda created db objects as expected for the reconciled tape environment."  );

        TestUtil.assertThrows( null, IllegalArgumentException.class,
                () -> manager.moveTapeToDrive( null, drives.get( 0 ) ) );
    }

    @Test
    public void testMoveTapeToDriveNullTapeDriveNotAllowed()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        final BeansRetriever< TapeDrive > driveRetriever = dbSupport.getServiceManager()
                                                                    .getRetriever( TapeDrive.class );
        final List< TapeDrive > drives = driveRetriever.retrieveAll()
                                                       .toList();
        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();
        final List< Integer > availableSlots = CollectionFactory.toList( manager.getTapeElementAddress( tapes.get( 0 )
                                                                                                             .getId() ),
                manager.getTapeElementAddress( tapes.get( 1 )
                                                    .getId() ), manager.getTapeElementAddress( tapes.get( 2 )
                                                                                                    .getId() ) );
        Collections.sort( availableSlots );
        assertEquals(
                3, drives.size(),
                "Shoulda created db objects as expected for the reconciled tape environment."  );
        assertEquals(
                5, tapes.size(),
                "Shoulda created db objects as expected for the reconciled tape environment." );

        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> manager.moveTapeToDrive( tapes.get( 0 )
                                                                                                         .getId(),
                null ) );
    }

    @Test
    public void testMoveTapeToDriveNonNullTapeInTapeDriveNotAllowed()
    {


        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        final BeansRetriever< TapeDrive > driveRetriever = dbSupport.getServiceManager()
                                                                    .getRetriever( TapeDrive.class );
        final List< TapeDrive > drives = driveRetriever.retrieveAll()
                                                       .toList();
        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();
        final List< Integer > availableSlots = CollectionFactory.toList( manager.getTapeElementAddress( tapes.get( 0 )
                                                                                                             .getId() ),
                manager.getTapeElementAddress( tapes.get( 1 )
                                                    .getId() ), manager.getTapeElementAddress( tapes.get( 2 )
                                                                                                    .getId() ) );
        Collections.sort( availableSlots );
        assertEquals(
                3, drives.size(),
                "Shoulda created db objects as expected for the reconciled tape environment." );
        assertEquals(
                5, tapes.size(),
                "Shoulda created db objects as expected for the reconciled tape environment."  );

        drives.get( 0 ).setTapeId( tapes.get( 1 ).getId() );
        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> manager.moveTapeToDrive( tapes.get( 0 )
                                                                                                         .getId(),
                drives.get( 0 ) ) );
    }

    @Test
    public void testMoveTapeFromDriveNullTapeDriveNotAllowed()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        final BeansRetriever< TapeDrive > driveRetriever = dbSupport.getServiceManager()
                                                                    .getRetriever( TapeDrive.class );
        final List< TapeDrive > drives = driveRetriever.retrieveAll()
                                                       .toList();
        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();
        final List< Integer > availableSlots = CollectionFactory.toList( manager.getTapeElementAddress( tapes.get( 0 )
                                                                                                             .getId() ),
                manager.getTapeElementAddress( tapes.get( 1 )
                                                    .getId() ), manager.getTapeElementAddress( tapes.get( 2 )
                                                                                                    .getId() ) );
        Collections.sort( availableSlots );
        assertEquals(
                3, drives.size(),
                "Shoulda created db objects as expected for the reconciled tape environment."  );
        assertEquals(
                5, tapes.size(),
                "Shoulda created db objects as expected for the reconciled tape environment."  );

        TestUtil.assertThrows( null, IllegalArgumentException.class,
                () -> manager.moveTapeFromDrive( null, ElementAddressType.STORAGE ) );
    }

    @Test
    public void testMoveTapeFromDriveNullTapeIdNotAllowed()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        final BeansRetriever< TapeDrive > driveRetriever = dbSupport.getServiceManager()
                                                                    .getRetriever( TapeDrive.class );
        final List< TapeDrive > drives = driveRetriever.retrieveAll()
                                                       .toList();
        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();
        final List< Integer > availableSlots = CollectionFactory.toList( manager.getTapeElementAddress( tapes.get( 0 )
                                                                                                             .getId() ),
                manager.getTapeElementAddress( tapes.get( 1 )
                                                    .getId() ), manager.getTapeElementAddress( tapes.get( 2 )
                                                                                                    .getId() ) );
        Collections.sort( availableSlots );
        assertEquals(
                3, drives.size(),
                "Shoulda created db objects as expected for the reconciled tape environment." );
        assertEquals( 5, tapes.size() ,
                "Shoulda created db objects as expected for the reconciled tape environment.");

        TestUtil.assertThrows( null, IllegalArgumentException.class,
                () -> manager.moveTapeFromDrive( drives.get( 0 ), ElementAddressType.STORAGE ) );
    }

    @Test
    public void testMoveTapeToAndFromDrivesUpdatesStateCorrectly()
    {


        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        final BeansRetriever< TapeDrive > driveRetriever = dbSupport.getServiceManager()
                                                                    .getRetriever( TapeDrive.class );
        final List< TapeDrive > drives = driveRetriever.retrieveAll()
                                                       .toList();
        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();
        final List< Integer > availableSlots = CollectionFactory.toList( manager.getTapeElementAddress( tapes.get( 0 )
                                                                                                             .getId() ),
                manager.getTapeElementAddress( tapes.get( 1 )
                                                    .getId() ), manager.getTapeElementAddress( tapes.get( 2 )
                                                                                                    .getId() ) );
        Collections.sort( availableSlots );
        assertEquals(
                3,
                drives.size(),
                "Shoulda created db objects as expected for the reconciled tape environment."
                 );
        assertEquals(
                5,
                tapes.size(),
                "Shoulda created db objects as expected for the reconciled tape environment." );

        assertNull(
                driveRetriever.attain( drives.get( 0 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );
        assertNull(
                driveRetriever.attain( drives.get( 1 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );
        assertNull(
                driveRetriever.attain( drives.get( 2 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );

        manager.moveTapeToDrive( tapes.get( 0 ).getId(), drives.get( 0 ) );
        assertEquals(
                tapes.get( 0 )
                        .getId(), driveRetriever.attain( drives.get( 0 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );
        assertNull(
                driveRetriever.attain( drives.get( 1 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertNull(
                driveRetriever.attain( drives.get( 2 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );

        manager.moveTapeToDrive( tapes.get( 1 ).getId(), drives.get( 1 ) );
        assertEquals(
                tapes.get( 0 )
                        .getId(), driveRetriever.attain( drives.get( 0 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertEquals(
                tapes.get( 1 )
                        .getId(), driveRetriever.attain( drives.get( 1 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );
        assertNull(
                driveRetriever.attain( drives.get( 2 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );

        manager.moveTapeToDrive( tapes.get( 2 ).getId(), drives.get( 2 ) );
        assertEquals(
                tapes.get( 0 )
                        .getId(), driveRetriever.attain( drives.get( 0 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive.");
        assertEquals(
                tapes.get( 1 )
                        .getId(), driveRetriever.attain( drives.get( 1 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertEquals(
                tapes.get( 2 )
                        .getId(), driveRetriever.attain( drives.get( 2 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );

        manager.moveTapeFromDrive( driveRetriever.attain( drives.get( 0 )
                                                                .getId() ), ElementAddressType.STORAGE );
        assertEquals(
                availableSlots.get( 0 )
                        .intValue(),
                manager.getTapeElementAddress( tapes.get( 0 ).getId() ),
                "Shoulda moved tape from drive into first available slot." );
        assertNull(
                driveRetriever.attain( drives.get( 0 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );
        assertEquals(
                tapes.get( 1 )
                        .getId(), driveRetriever.attain( drives.get( 1 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertEquals(
                tapes.get( 2 )
                        .getId(), driveRetriever.attain( drives.get( 2 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );

        manager.moveTapeFromDrive( driveRetriever.attain( drives.get( 1 )
                                                                .getId() ), ElementAddressType.STORAGE );
        assertEquals(
                availableSlots.get( 1 )
                        .intValue(),
                manager.getTapeElementAddress( tapes.get( 1 ).getId() ),
                "Shoulda moved tape from drive into first available slot."  );
        assertNull(
                driveRetriever.attain( drives.get( 0 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertNull(
                driveRetriever.attain( drives.get( 1 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertEquals(
                tapes.get( 2 )
                        .getId(), driveRetriever.attain( drives.get( 2 )
                                .getId() )
                        .getTapeId() ,
                "Shoulda reported correct tape in drive." );

        manager.moveTapeFromDrive( driveRetriever.attain( drives.get( 2 )
                                                                .getId() ), ElementAddressType.STORAGE );
        assertEquals(
                availableSlots.get( 2 )
                        .intValue(),
                manager.getTapeElementAddress( tapes.get( 2 ).getId() ) ,
                "Shoulda moved tape from drive into first available slot." );
        assertNull( driveRetriever.attain( drives.get( 0 )
                        .getId() )
                .getTapeId(),
                "Shoulda reported correct tape in drive."  );
        assertNull( driveRetriever.attain( drives.get( 1 )
                        .getId() )
                .getTapeId(),
                "Shoulda reported correct tape in drive."  );
        assertNull(
                driveRetriever.attain( drives.get( 2 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );
    }

    @Test
    public void testMoveTapeDriveToDriveNullSrcDriveNotAllowed()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        final BeansRetriever< TapeDrive > driveRetriever = dbSupport.getServiceManager()
                                                                    .getRetriever( TapeDrive.class );
        final List< TapeDrive > drives = driveRetriever.retrieveAll()
                                                       .toList();
        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();
        final List< Integer > availableSlots = CollectionFactory.toList( manager.getTapeElementAddress( tapes.get( 0 )
                                                                                                             .getId() ),
                manager.getTapeElementAddress( tapes.get( 1 )
                                                    .getId() ), manager.getTapeElementAddress( tapes.get( 2 )
                                                                                                    .getId() ) );
        Collections.sort( availableSlots );
        assertEquals(
                3,
                drives.size(),
                "Shoulda created db objects as expected for the reconciled tape environment."  );
        assertEquals(
                5,
                tapes.size(),
                "Shoulda created db objects as expected for the reconciled tape environment.");

        manager.moveTapeToDrive( tapes.get( 0 ).getId(), drives.get( 0 ) );

        TestUtil.assertThrows( null, IllegalArgumentException.class,
                () -> manager.moveTapeDriveToDrive( null, drives.get( 1 ) ) );
    }

    @Test
    public void testMoveTapeDriveToDriveNullDestDriveNotAllowed()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        final BeansRetriever< TapeDrive > driveRetriever = dbSupport.getServiceManager()
                                                                    .getRetriever( TapeDrive.class );
        final List< TapeDrive > drives = driveRetriever.retrieveAll()
                                                       .toList();
        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();
        final List< Integer > availableSlots = CollectionFactory.toList( manager.getTapeElementAddress( tapes.get( 0 )
                                                                                                             .getId() ),
                manager.getTapeElementAddress( tapes.get( 1 )
                                                    .getId() ), manager.getTapeElementAddress( tapes.get( 2 )
                                                                                                    .getId() ) );
        Collections.sort( availableSlots );
        assertEquals(
                3,
                drives.size(),
                "Shoulda created db objects as expected for the reconciled tape environment."  );
        assertEquals(
                5,
                tapes.size(),
                "Shoulda created db objects as expected for the reconciled tape environment." );

        manager.moveTapeToDrive( tapes.get( 0 ).getId(), drives.get( 0 ) );

        TestUtil.assertThrows( null, IllegalArgumentException.class,
                () -> manager.moveTapeDriveToDrive( drives.get( 0 ), null ) );
    }

    @Test
    public void testMoveTapeDriveToDriveSrcDriveEmptyNotAllowed()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        final BeansRetriever< TapeDrive > driveRetriever = dbSupport.getServiceManager()
                                                                    .getRetriever( TapeDrive.class );
        final List< TapeDrive > drives = driveRetriever.retrieveAll()
                                                       .toList();
        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();
        final List< Integer > availableSlots = CollectionFactory.toList( manager.getTapeElementAddress( tapes.get( 0 )
                                                                                                             .getId() ),
                manager.getTapeElementAddress( tapes.get( 1 )
                                                    .getId() ), manager.getTapeElementAddress( tapes.get( 2 )
                                                                                                    .getId() ) );
        Collections.sort( availableSlots );
        assertEquals(
                3,
                drives.size(),
                "Shoulda created db objects as expected for the reconciled tape environment.");
        assertEquals(
                5,
                tapes.size(),
                "Shoulda created db objects as expected for the reconciled tape environment.");

        TestUtil.assertThrows( null, IllegalStateException.class,
                () -> manager.moveTapeDriveToDrive( drives.get( 0 ), drives.get( 1 ) ) );
    }

    @Test
    public void testMoveTapeDriveToDriveDestDriveNonEmptyNotAllowed()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        final BeansRetriever< TapeDrive > driveRetriever = dbSupport.getServiceManager()
                                                                    .getRetriever( TapeDrive.class );
        final List< TapeDrive > drives = driveRetriever.retrieveAll()
                                                       .toList();
        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();
        final List< Integer > availableSlots = CollectionFactory.toList( manager.getTapeElementAddress( tapes.get( 0 )
                                                                                                             .getId() ),
                manager.getTapeElementAddress( tapes.get( 1 )
                                                    .getId() ), manager.getTapeElementAddress( tapes.get( 2 )
                                                                                                    .getId() ) );
        Collections.sort( availableSlots );
        assertEquals(
                3,
                drives.size(),
                "Shoulda created db objects as expected for the reconciled tape environment." );
        assertEquals(
                5,
                tapes.size(),
                "Shoulda created db objects as expected for the reconciled tape environment.");

        manager.moveTapeToDrive( tapes.get( 0 ).getId(), drives.get( 0 ) );
        manager.moveTapeToDrive( tapes.get( 1 ).getId(), drives.get( 1 ) );

        TestUtil.assertThrows( null, IllegalStateException.class,
                () -> manager.moveTapeDriveToDrive( drives.get( 0 ), drives.get( 1 ) ) );
    }

    @Test
    public void testMoveTapeDriveToDriveDoesSo()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        final BeansRetriever< TapeDrive > driveRetriever = dbSupport.getServiceManager()
                                                                    .getRetriever( TapeDrive.class );
        final List< TapeDrive > drives = driveRetriever.retrieveAll()
                                                       .toList();
        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();
        final List< Integer > availableSlots = CollectionFactory.toList( manager.getTapeElementAddress( tapes.get( 0 )
                                                                                                             .getId() ),
                manager.getTapeElementAddress( tapes.get( 1 )
                                                    .getId() ), manager.getTapeElementAddress( tapes.get( 2 )
                                                                                                    .getId() ) );
        Collections.sort( availableSlots );
        assertEquals(
                3,
                drives.size(),
                "Shoulda created db objects as expected for the reconciled tape environment." );
        assertEquals(
                5,
                tapes.size(),
                "Shoulda created db objects as expected for the reconciled tape environment." );

        manager.moveTapeToDrive( tapes.get( 0 ).getId(), drives.get( 0 ) );
        manager.moveTapeDriveToDrive( drives.get( 0 ), drives.get( 1 ) );

        assertNull(
                mockDaoDriver.attain( drives.get( 0 ) )
                        .getTapeId(),
                "Shoulda moved tape from src drv to dest drv." );
        assertEquals(
                tapes.get( 0 )
                        .getId(),
                mockDaoDriver.attain( drives.get( 1 ) )
                        .getTapeId(),
                "Shoulda moved tape from src drv to dest drv."  );
    }

    @Test
    public void testComplexMovesUpdateStateCorrectly()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        final BeansRetriever< TapeDrive > driveRetriever = dbSupport.getServiceManager()
                                                                    .getRetriever( TapeDrive.class );
        final List< TapeDrive > drives = driveRetriever.retrieveAll()
                                                       .toList();
        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();
        final UUID partitionId = drives.get( 0 ).getPartitionId();
        final List< Integer > availableStorageSlots = CollectionFactory.toList( manager.getTapeElementAddress(
                tapes.get( 0 )
                     .getId() ), manager.getTapeElementAddress( tapes.get( 1 )
                                                                     .getId() ), manager.getTapeElementAddress(
                tapes.get( 2 )
                     .getId() ) );
        Collections.sort( availableStorageSlots );
        assertEquals(
                3,
                drives.size(),
                "Shoulda created db objects as expected for the reconciled tape environment." );
        assertEquals(
                5,
                tapes.size(),
                "Shoulda created db objects as expected for the reconciled tape environment." );

        assertNull(
                driveRetriever.attain( drives.get( 0 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertNull(
                driveRetriever.attain( drives.get( 1 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertNull(
                driveRetriever.attain( drives.get( 2 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertTrue(
                manager.isSlotAvailable( partitionId, ElementAddressType.STORAGE ),
                "Shoulda reported slot available of type."
                );
        assertTrue(
                manager.isSlotAvailable( partitionId, ElementAddressType.IMPORT_EXPORT ),
                "Shoulda reported slot available of type."
                 );

        manager.moveTapeToDrive( tapes.get( 0 ).getId(), drives.get( 0 ) );
        assertEquals(
                tapes.get( 0 )
                        .getId(), driveRetriever.attain( drives.get( 0 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );
        assertNull(
                driveRetriever.attain( drives.get( 1 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );
        assertNull(
                driveRetriever.attain( drives.get( 2 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );
        assertTrue(
                manager.isSlotAvailable( partitionId, ElementAddressType.STORAGE ),
                "Shoulda reported slot available of type."
                );
        assertTrue(
                manager.isSlotAvailable( partitionId, ElementAddressType.IMPORT_EXPORT ),
                "Shoulda reported slot available of type."
                 );

        manager.moveTapeToDrive( tapes.get( 1 ).getId(), drives.get( 1 ) );
        assertEquals(
                tapes.get( 0 )
                        .getId(), driveRetriever.attain( drives.get( 0 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );
        assertEquals(
                tapes.get( 1 )
                        .getId(), driveRetriever.attain( drives.get( 1 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertNull(
                driveRetriever.attain( drives.get( 2 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertTrue(
                manager.isSlotAvailable( partitionId, ElementAddressType.STORAGE ),
                "Shoulda reported slot available of type."
                 );
        assertTrue(
                manager.isSlotAvailable( partitionId, ElementAddressType.IMPORT_EXPORT ),
                "Shoulda reported slot available of type."
                 );

        manager.moveTapeToDrive( tapes.get( 2 ).getId(), drives.get( 2 ) );
        assertEquals(
                tapes.get( 0 )
                        .getId(), driveRetriever.attain( drives.get( 0 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );
        assertEquals(
                tapes.get( 1 )
                        .getId(), driveRetriever.attain( drives.get( 1 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive.");
        assertEquals(
                tapes.get( 2 )
                        .getId(), driveRetriever.attain( drives.get( 2 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );
        assertTrue(
                manager.isSlotAvailable( partitionId, ElementAddressType.STORAGE ),
                "Shoulda reported slot available of type."
                 );
        assertTrue(
                manager.isSlotAvailable( partitionId, ElementAddressType.IMPORT_EXPORT ),
                "Shoulda reported slot available of type."
                 );
        TestUtil.assertThrows( null, IllegalStateException.class, () -> manager.moveTapeSlotToSlot( tapes.get( 2 )
                                                                                                         .getId(),
                ElementAddressType.IMPORT_EXPORT ) );

        manager.moveTapeFromDrive( driveRetriever.attain( drives.get( 0 )
                                                                .getId() ), ElementAddressType.IMPORT_EXPORT );
        assertEquals(
                10000,
                manager.getTapeElementAddress( tapes.get( 0 ).getId() ) ,
                "Shoulda moved tape from drive into first available slot." );
        assertNull( driveRetriever.attain( drives.get( 0 )
                        .getId() )
                .getTapeId() ,
                "Shoulda reported correct tape in drive."  );
        assertEquals(
                tapes.get( 1 )
                        .getId(), driveRetriever.attain( drives.get( 1 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertEquals(
                tapes.get( 2 )
                        .getId(), driveRetriever.attain( drives.get( 2 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertTrue(
                manager.isSlotAvailable( partitionId, ElementAddressType.STORAGE ),
                "Shoulda reported slot available of type."
                 );
        assertTrue(
                manager.isSlotAvailable( partitionId, ElementAddressType.IMPORT_EXPORT ),
                "Shoulda reported slot available of type."
                 );

        manager.moveTapeFromDrive( driveRetriever.attain( drives.get( 1 )
                                                                .getId() ), ElementAddressType.IMPORT_EXPORT );
        assertEquals(
                10001,
                manager.getTapeElementAddress( tapes.get( 1 ).getId() ),
                "Shoulda moved tape from drive into first available slot."  );
        assertNull(
                driveRetriever.attain( drives.get( 0 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertNull(
                driveRetriever.attain( drives.get( 1 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );
        assertEquals(
                tapes.get( 2 )
                        .getId(), driveRetriever.attain( drives.get( 2 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );
        assertTrue(
                manager.isSlotAvailable( partitionId, ElementAddressType.STORAGE ),
                "Shoulda reported slot available of type."
                 );
        assertFalse(
                manager.isSlotAvailable( partitionId, ElementAddressType.IMPORT_EXPORT ),
                "Shoulda reported no slot available of type."
                 );

        TestUtil.assertThrows( null, IllegalStateException.class, () -> manager.moveTapeFromDrive(
                driveRetriever.attain( drives.get( 2 )
                                             .getId() ), ElementAddressType.IMPORT_EXPORT ) );
        manager.moveTapeFromDrive( driveRetriever.attain( drives.get( 2 )
                                                                .getId() ), ElementAddressType.STORAGE );
        assertEquals(
                availableStorageSlots.get( 0 )
                        .intValue(),
                manager.getTapeElementAddress( tapes.get( 2 ).getId() ),
                "Shoulda moved tape from drive into first available slot." );
        assertNull(
                driveRetriever.attain( drives.get( 0 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertNull(
                driveRetriever.attain( drives.get( 1 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );
        assertNull(
                driveRetriever.attain( drives.get( 2 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );
        assertTrue(
                manager.isSlotAvailable( partitionId, ElementAddressType.STORAGE ),
                "Shoulda reported slot available of type."
                 );
        assertFalse(
                manager.isSlotAvailable( partitionId, ElementAddressType.IMPORT_EXPORT ),
                "Shoulda reported no slot available of type."
                 );

        TestUtil.assertThrows( null, IllegalStateException.class, () -> manager.moveTapeSlotToSlot( tapes.get( 2 )
                                                                                                         .getId(),
                ElementAddressType.IMPORT_EXPORT ) );
        manager.moveTapeSlotToSlot( tapes.get( 1 ).getId(), ElementAddressType.STORAGE );
        assertEquals(
                availableStorageSlots.get( 1 )
                        .intValue(),
                manager.getTapeElementAddress( tapes.get( 1 ).getId() ),
                "Shoulda moved tape into first available slot."  );
        assertNull(
                driveRetriever.attain( drives.get( 0 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );
        assertNull(
                driveRetriever.attain( drives.get( 1 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );
        assertNull(
                driveRetriever.attain( drives.get( 2 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive."  );
        assertTrue(
                manager.isSlotAvailable( partitionId, ElementAddressType.STORAGE ),
                "Shoulda reported slot available of type."
                 );
        assertTrue(
                manager.isSlotAvailable( partitionId, ElementAddressType.IMPORT_EXPORT ),
                "Shoulda reported slot available of type."
                 );

        manager.moveTapeSlotToSlot( tapes.get( 2 ).getId(), ElementAddressType.IMPORT_EXPORT );
        assertEquals(
                10001, manager.getTapeElementAddress(
                        tapes.get( 2 )
                                .getId() ),
                "Shoulda moved tape into first available slot." );
        assertNull(
                driveRetriever.attain( drives.get( 0 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertNull(
                driveRetriever.attain( drives.get( 1 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertNull(
                driveRetriever.attain( drives.get( 2 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertTrue(
                manager.isSlotAvailable( partitionId, ElementAddressType.STORAGE ) ,
                "Shoulda reported slot available of type."
                );
        assertFalse(
                manager.isSlotAvailable( partitionId, ElementAddressType.IMPORT_EXPORT ),
                "Shoulda reported no slot available of type."
                 );

        manager.moveTapeSlotToSlot( tapes.get( 2 )
                                         .getId(), availableStorageSlots.get( 0 ) );
        assertEquals(
                availableStorageSlots.get( 1 )
                        .intValue(),
                manager.getTapeElementAddress( tapes.get( 1 ).getId() ),
                "Shoulda moved tape into first available slot." );
        assertNull(
                driveRetriever.attain( drives.get( 0 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertNull(
                driveRetriever.attain( drives.get( 1 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertNull(
                driveRetriever.attain( drives.get( 2 )
                                .getId() )
                        .getTapeId(),
                "Shoulda reported correct tape in drive." );
        assertTrue(
                manager.isSlotAvailable( partitionId, ElementAddressType.STORAGE ),
                "Shoulda reported slot available of type."
                 );
        assertTrue(
                manager.isSlotAvailable( partitionId, ElementAddressType.IMPORT_EXPORT ),
                "Shoulda reported slot available of type."
                 );

        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> manager.moveTapeSlotToSlot( tapes.get( 1 )
                                                                                                            .getId(),
                availableStorageSlots.get( 0 ) ) );

        manager.moveTapeSlotToSlot( tapes.get( 2 ).getId(), ElementAddressType.IMPORT_EXPORT );
        manager.moveTapeSlotToSlot( tapes.get( 1 )
                                         .getId(), availableStorageSlots.get( 0 ) );
    }

    @Test
    public void testMoveTapeSlotToSlotNullTapeIdNotAllowed()
    {

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        TestUtil.assertThrows( null, IllegalArgumentException.class,
                () -> manager.moveTapeSlotToSlot( null, ElementAddressType.IMPORT_EXPORT ) );
    }

    @Test
    public void testMoveTapeSlotToSlotNullDestinationElementAddressTypeNotAllowed()
    {

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();

        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> manager.moveTapeSlotToSlot( tapes.get( 0 )
                                                                                                            .getId(),
                null ) );
    }

    @Test
    public void testMoveTapeSlotToSlotDestinationElementAddressTypeSameAsCurrentNotAllowed()
    {

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();

        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> manager.moveTapeSlotToSlot( tapes.get( 0 )
                                                                                                            .getId(),
                ElementAddressType.STORAGE ) );
    }

    @Test
    public void testMoveTapeSlotToSlotWhenTapeInDriveNotAllowed()
    {

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        final BeansRetriever< TapeDrive > driveRetriever = dbSupport.getServiceManager()
                                                                    .getRetriever( TapeDrive.class );
        final List< TapeDrive > drives = driveRetriever.retrieveAll()
                                                       .toList();

        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();
        manager.moveTapeToDrive( tapes.get( 0 ).getId(), drives.get( 0 ) );

        TestUtil.assertThrows( null, IllegalStateException.class, () -> manager.moveTapeSlotToSlot( tapes.get( 0 )
                                                                                                         .getId(),
                ElementAddressType.IMPORT_EXPORT ) );
    }

    @Test
    public void testMoveTapeSlotToSlotWhenDestinationSlotTypeDisallowedNotAllowed()
    {

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();

        TestUtil.assertThrows( null, UnsupportedOperationException.class, () -> manager.moveTapeSlotToSlot(
                tapes.get( 0 )
                     .getId(), ElementAddressType.ROBOT ) );
    }

    @Test
    public void testMoveTapeSlotToSlotWhenNoSlotsAvailableOfTypeNotAllowed1()
    {

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();

        manager.moveTapeSlotToSlot( tapes.get( 1 ).getId(), ElementAddressType.IMPORT_EXPORT );
        manager.moveTapeSlotToSlot( tapes.get( 2 ).getId(), ElementAddressType.IMPORT_EXPORT );
        TestUtil.assertThrows( null, IllegalStateException.class, () -> manager.moveTapeSlotToSlot( tapes.get( 0 )
                                                                                                         .getId(),
                ElementAddressType.IMPORT_EXPORT ) );
    }

    @Test
    public void testMoveTapeSlotToSlotWhenNoSlotsAvailableOfTypeNotAllowed2()
    {

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        dbSupport.getServiceManager()
                 .getService( TapePartitionFailureService.class )
                 .create( dbSupport.getServiceManager()
                                   .getRetriever( TapePartition.class )
                                   .attain( Require.nothing() )
                                   .getId(), TapePartitionFailureType.TAPE_EJECTION_BY_OPERATOR_REQUIRED,
                         "Eject them tapes.", null );

        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();

        manager.moveTapeSlotToSlot( tapes.get( 1 ).getId(), ElementAddressType.IMPORT_EXPORT );
        manager.moveTapeSlotToSlot( tapes.get( 2 ).getId(), ElementAddressType.IMPORT_EXPORT );
        TestUtil.assertThrows( null, GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT, () -> manager.moveTapeSlotToSlot(
                tapes.get( 0 )
                     .getId(), ElementAddressType.IMPORT_EXPORT ) );
    }

    @Test
    public void testMoveTapeSlotToSlotWhenStorageSlotOvercommitWouldOccurNotAllowed()
    {

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        final List< Tape > tapes = dbSupport.getServiceManager()
                                            .getRetriever( Tape.class )
                                            .retrieveAll()
                                            .toList();

        manager.moveTapeSlotToSlot( tapes.get( 0 ).getId(), ElementAddressType.IMPORT_EXPORT );
        manager.moveTapeSlotToSlot( tapes.get( 1 ).getId(), ElementAddressType.IMPORT_EXPORT );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        for ( int i = 0; i < 17; ++i )
        {
            final Tape tape = mockDaoDriver.createTape( tapes.get( 0 ).getPartitionId(), TapeState.NORMAL );
            mockDaoDriver.createTapeDrive( tapes.get( 0 )
                                                .getPartitionId(), "MOCKED-UNLOADED-" + i );
            mockDaoDriver.createTapeDrive( tapes.get( 0 )
                                                .getPartitionId(), "MOCKED-LOADED-" + i, tape.getId() );
        }

        final int availableStorageSlot = manager.moveTapeSlotToSlot( tapes.get( 0 )
                                                                          .getId(), ElementAddressType.STORAGE );
        TestUtil.assertThrows( null, IllegalStateException.class, () -> manager.moveTapeSlotToSlot( tapes.get( 1 )
                                                                                                         .getId(),
                ElementAddressType.STORAGE ) );

        manager.moveTapeSlotToSlot( tapes.get( 0 ).getId(), ElementAddressType.IMPORT_EXPORT );
        manager.moveTapeSlotToSlot( tapes.get( 0 ).getId(), 1010 );
        TestUtil.assertThrows( null, IllegalStateException.class, () -> manager.moveTapeSlotToSlot( tapes.get( 1 )
                                                                                                         .getId(),
                availableStorageSlot ) );
    }

    @Test
    public void testMoveOperationsWhenMultiplePartitionsWorks()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 2, 2, 3 ) );
        final List< TapePartition > partitions = dbSupport.getServiceManager()
                                                          .getRetriever( TapePartition.class )
                                                          .retrieveAll()
                                                          .toList();
        final List< TapeDrive > drivesInPartition1 = dbSupport.getServiceManager()
                                                              .getRetriever( TapeDrive.class )
                                                              .retrieveAll( TapeDrive.PARTITION_ID, partitions.get( 0 )
                                                                                                              .getId() )
                                                              .toList();
        final List< TapeDrive > drivesInPartition2 = dbSupport.getServiceManager()
                                                              .getRetriever( TapeDrive.class )
                                                              .retrieveAll( TapeDrive.PARTITION_ID, partitions.get( 1 )
                                                                                                              .getId() )
                                                              .toList();
        final List< Tape > tapesInPartition1 = dbSupport.getServiceManager()
                                                        .getRetriever( Tape.class )
                                                        .retrieveAll( Tape.PARTITION_ID, partitions.get( 0 )
                                                                                                   .getId() )
                                                        .toList();
        final List< Tape > tapesInPartition2 = dbSupport.getServiceManager()
                                                        .getRetriever( Tape.class )
                                                        .retrieveAll( Tape.PARTITION_ID, partitions.get( 1 )
                                                                                                   .getId() )
                                                        .toList();

        for ( int j = 0; j < 2; ++j )
        {
            for ( int i = 0; i < 3; ++i )
            {
                manager.moveTapeToDrive( tapesInPartition1.get( 0 ).getId(), drivesInPartition1.get( 0 ) );
                manager.moveTapeToDrive( tapesInPartition2.get( 0 ).getId(), drivesInPartition2.get( 0 ) );
                TestUtil.assertThrows( "Should notta allowed move into drive in wrong partition.",
                        IllegalStateException.class, () -> manager.moveTapeToDrive( tapesInPartition2.get( 1 )
                                                                                                     .getId(),
                                drivesInPartition1.get( 1 ) ) );
                manager.moveTapeToDrive( tapesInPartition1.get( 1 )
                                                          .getId(), drivesInPartition1.get( 1 ) );
                manager.moveTapeToDrive( tapesInPartition2.get( 1 )
                                                          .getId(), drivesInPartition2.get( 1 ) );

                manager.moveTapeFromDrive( drivesInPartition1.get( 0 ), ElementAddressType.STORAGE );
                manager.moveTapeFromDrive( drivesInPartition2.get( 0 ), ElementAddressType.STORAGE );
                manager.moveTapeFromDrive( drivesInPartition1.get( 1 ), ElementAddressType.IMPORT_EXPORT );
                manager.moveTapeFromDrive( drivesInPartition2.get( 1 ), ElementAddressType.IMPORT_EXPORT );
            }
            mockDaoDriver.unquiesceAllTapePartitions();
            manager.reconcileWith( constructResponse( 2, 2, 3 ) );
        }
    }

    @Test
    public void testGetTapeElementAddressForUnknownTapeNotAllowed()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        TestUtil.assertThrows( null, IllegalStateException.class,
                () -> manager.getTapeElementAddress( UUID.randomUUID() ) );
    }

    @Test
    public void testGetTapeDriveElementAddressForUnknownTapeDriveNotAllowed()
    {

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 1, 3, 5 ) );

        TestUtil.assertThrows( null, IllegalStateException.class,
                () -> manager.getDriveElementAddress( UUID.randomUUID() ) );
    }

    @Test
    public void testGetTapesNotInPartitionWherePartitionSpecifiedIsUnknownReturnsAllTapes()
    {

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 3, 2, 5 ) );

        final Set< UUID > tapesNotInPartition = manager.getTapesNotInPartition( UUID.randomUUID() );
        assertEquals(
                15, tapesNotInPartition.size(),
                "Shoulda reported every tape not in partition."  );
    }

    @Test
    public void testGetTapesNotInPartitionDoesSo()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 3, 2, 5 ) );

        final Tape tape = dbSupport.getServiceManager().getRetriever( Tape.class ).retrieveAll().getFirst();
        final Set< UUID > tapesNotInPartition = manager.getTapesNotInPartition( tape.getPartitionId() );
        assertEquals(
                10, tapesNotInPartition.size(),
                "Shoulda reported every tape not in partition."  );
        assertFalse(
                tapesNotInPartition.contains( tape.getId() ),
                "Shoulda reported every tape not in partition."  );
        assertTrue(
                tapesNotInPartition.contains(
                        dbSupport.getServiceManager()
                                .getRetriever( Tape.class )
                                .retrieveAll(
                                        Require.not( Require.beanPropertyEquals( Tape.PARTITION_ID, tape.getPartitionId() ) ) )
                                .getFirst()
                                .getId() ),
                "Shoulda reported every tape not in partition." );
    }

    @Test
    public void testGetTapesInPartitionWherePartitionIsUnknownReturnsEmptySet()
    {

        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 3, 2, 5 ) );

        final Set< UUID > tapesInPartition = manager.getTapesInPartition( UUID.randomUUID() );
        assertEquals(
                0, tapesInPartition.size(),
                "Shoulda reported every tape in partition."  );
    }

    @Test
    public void testGetTapesInPartitionDoesSo()
    {
        final TapeEnvironmentManagerImpl manager = new TapeEnvironmentManagerImpl( dbSupport.getServiceManager(), -1 );
        manager.reconcileWith( constructResponse( 3, 2, 5 ) );

        final Tape tape = dbSupport.getServiceManager().getRetriever( Tape.class ).retrieveAll().getFirst();
        final Set< UUID > tapesInPartition = manager.getTapesInPartition( tape.getPartitionId() );
        assertEquals(
                5, tapesInPartition.size(),
                "Shoulda reported every tape in partition." );
        assertTrue(
                tapesInPartition.contains( tape.getId() ),
                "Shoulda reported every tape in partition."  );
        assertFalse(  tapesInPartition.contains(
                dbSupport.getServiceManager()
                        .getRetriever( Tape.class )
                        .retrieveAll(
                                Require.not( Require.beanPropertyEquals( Tape.PARTITION_ID, tape.getPartitionId() ) ) )
                        .getFirst()
                        .getId() ),
                "Shoulda reported every tape in partition." );
    }


    private String removeBarCode( final String barCode, final boolean putIntoTapeDrive,
            final TapeEnvironmentInformation environment )
    {
        for ( final TapeLibraryInformation library : environment.getLibraries() )
        {
            for ( final TapePartitionInformation partition : library.getPartitions() )
            {
                for ( final BasicTapeInformation tape : partition.getTapes() )
                {
                    if ( null == barCode || tape.getBarCode().equals( barCode ) )
                    {
                        final String retval = tape.getBarCode();
                        tape.setBarCode( null );
                        if ( putIntoTapeDrive )
                        {
                            tape.setElementAddress( 100 );
                        }
                        return retval;
                    }
                }
            }
        }

        throw new RuntimeException( "No tape found with bar code: " + barCode );
    }


    private TapeEnvironmentInformation constructResponse( final int numberOfPartitions, final int numberOfTapeDrives,
            final int numberOfTapes )
    {
        final TapeEnvironmentInformation retval = BeanFactory.newBean( TapeEnvironmentInformation.class );

        final TapePartitionInformation[] partitions = new TapePartitionInformation[ numberOfPartitions ];
        for ( int i = 0; i < numberOfPartitions; ++i )
        {
            partitions[ i ] = constructPartition( i, numberOfTapeDrives, numberOfTapes );
            partitions[ i ].setSerialNumber( "SN" + i );
            partitions[ i ].setName( "S/N " + i );
        }

        final TapeLibraryInformation library = BeanFactory.newBean( TapeLibraryInformation.class )
                                                          .setSerialNumber( "LSN" )
                                                          .setName( "LN" )
                                                          .setManagementUrl( "LMURL" );
        library.setPartitions( partitions );
        retval.setLibraries( new TapeLibraryInformation [] { library } );

        return retval;
    }


    private TapePartitionInformation constructPartition( final int partitionNumber, final int numberOfTapeDrives,
            final int numberOfTapes )
    {
        final TapePartitionInformation retval = BeanFactory.newBean( TapePartitionInformation.class );

        final TapeDriveInformation [] drives = new TapeDriveInformation[ numberOfTapeDrives ];
        for ( int i = 0; i < numberOfTapeDrives; ++i )
        {
            drives[ i ] = BeanFactory.newBean( TapeDriveInformation.class );
            drives[ i ].setElementAddress( 100 + i );
            drives[ i ].setSerialNumber( "SN" + partitionNumber + "N" + ( i ) );
            drives[ i ].setMfgSerialNumber( "MFG SN" + partitionNumber + "N" + ( i ) );
            drives[ i ].setType( TapeDriveType.LTO6 );
        }
        retval.setDrives( drives );

        final BasicTapeInformation [] tapes = new BasicTapeInformation[ numberOfTapes ];
        for ( int i = 0; i < numberOfTapes; ++i )
        {
            tapes[ i ] = BeanFactory.newBean( BasicTapeInformation.class );
            tapes[ i ].setElementAddress( 1000 + i );
            tapes[ i ].setBarCode( "BC" + partitionNumber + ( i ) );
            tapes[ i ].setType( ( 1 == i ) ? TapeType.LTO5 : TapeType.LTO6 );
        }
        retval.setTapes( tapes );

        retval.setElementAddressBlocks( constructElementAddressBlocks() );
        retval.setImportExportConfiguration( ImportExportConfiguration.SUPPORTED );

        return retval;
    }


    private ElementAddressBlockInformation [] constructElementAddressBlocks()
    {
        final List< ElementAddressBlockInformation > retval = new ArrayList<>();

        retval.add( constructElementAddressBlock( ElementAddressType.ROBOT, 0, 1 ) );
        retval.add( constructElementAddressBlock( ElementAddressType.TAPE_DRIVE, 100, 110 ) );
        retval.add( constructElementAddressBlock( ElementAddressType.STORAGE, 1000, 1020 ) );
        retval.add( constructElementAddressBlock( ElementAddressType.IMPORT_EXPORT, 10000, 10001 ) );

        return CollectionFactory.toArray( ElementAddressBlockInformation.class, retval );
    }


    private ElementAddressBlockInformation constructElementAddressBlock( final ElementAddressType type, final int start,
            final int end )
    {
        final ElementAddressBlockInformation retval = BeanFactory.newBean( ElementAddressBlockInformation.class );
        retval.setType( type );
        retval.setStartAddress( start );
        retval.setEndAddress( end );
        return retval;
    }


    /**
     * Asserts that there are no tape failures
     */
    private void assertTapeFailures( final BeansRetrieverManager brm )
    {
        assertTapeFailures( brm, new HashMap<>() );
    }


    private void assertTapeFailures( final BeansRetrieverManager brm, final TapePartitionFailureType type1,
            final int expectedCount1 )
    {
        final Map< TapePartitionFailureType, Integer > expected = new HashMap<>();
        expected.put( type1, expectedCount1 );
        assertTapeFailures( brm, expected );
    }


    private void assertTapeFailures( final BeansRetrieverManager brm, final TapePartitionFailureType type1,
            final int expectedCount1, final TapePartitionFailureType type2, final int expectedCount2 )
    {
        final Map< TapePartitionFailureType, Integer > expected = new HashMap<>();
        expected.put( type1, expectedCount1 );
        expected.put( type2, expectedCount2 );
        assertTapeFailures( brm, expected );
    }


    private void assertTapeFailures( final BeansRetrieverManager brm,
            final Map< TapePartitionFailureType, Integer > expected )
    {
        final Map< TapePartitionFailureType, Integer > actual = new HashMap<>();
        for ( final TapePartitionFailure failure : brm.getRetriever( TapePartitionFailure.class )
                                                      .retrieveAll()
                                                      .toSet() )
        {
            if ( !actual.containsKey( failure.getType() ) )
            {
                actual.put( failure.getType(), 0 );
            }
            actual.put( failure.getType(), actual.get( failure.getType() ) + 1 );
        }

        if ( !actual.equals( expected ) )
        {
            fail( "Tape partition failures expected were " + expected + ", but actual was " + actual + "." );
        }
    }


    private void linkTapeToStorageDomain( final Tape tape, final StorageDomain storageDomain,
            final BeansServiceManager serviceManager, final MockDaoDriver mockDaoDriver )
    {
        if ( TapeType.UNKNOWN == tape.getType() )
        {
            throw new IllegalStateException( "Must not assign tapes with type unknown" );
        }
        StorageDomainMember sdm = serviceManager.getRetriever( StorageDomainMember.class ).retrieve(
                Require.all( Require.beanPropertyEquals( StorageDomainMember.TAPE_TYPE, tape.getType() ),
                        Require.beanPropertyEquals( StorageDomainMember.TAPE_PARTITION_ID, tape.getPartitionId() ),
                        Require.beanPropertyEquals( StorageDomainMember.STORAGE_DOMAIN_ID, storageDomain.getId() ) ) );
        if ( null == sdm )
        {
            sdm = mockDaoDriver.addTapePartitionToStorageDomain( storageDomain.getId(), tape.getPartitionId(),
                    tape.getType() );
        }
        serviceManager.getService( TapeService.class )
                      .update( tape.setStorageDomainMemberId( sdm.getId() )
                                   .setAssignedToStorageDomain( true ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                              PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
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
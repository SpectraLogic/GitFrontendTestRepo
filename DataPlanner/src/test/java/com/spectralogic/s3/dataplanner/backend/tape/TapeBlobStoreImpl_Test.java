/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.tape;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.tape.TapePartitionService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.rpc.tape.TapeEnvironmentResource;
import com.spectralogic.s3.common.rpc.tape.domain.TapeEnvironmentInformation;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeBlobStore;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeLockSupport;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeBlobStoreProcessorImpl;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeEnvironmentImpl;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeBlobStoreProcessor;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironment;
import com.spectralogic.s3.dataplanner.backend.tape.task.FormatTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.task.MockTapeAvailability;
import com.spectralogic.s3.dataplanner.backend.tape.task.MockTapeDriveResource;
import com.spectralogic.s3.dataplanner.backend.tape.task.VerifyTapeTask;
import com.spectralogic.s3.dataplanner.frmwk.DataPlannerException;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.ForceFlagRequiredException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.net.rpc.client.RpcClient;
import com.spectralogic.util.net.rpc.frmwrk.ConcurrentRequestExecutionPolicy;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;

import org.junit.jupiter.api.*;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.TimeUnit;


import static org.junit.jupiter.api.Assertions.*;
@RunWith(PowerMockRunner.class)
@PrepareForTest({TapeBlobStoreImpl.class, TapeEnvironmentImpl.class, TapeLockSupportImpl.class
})

public final class TapeBlobStoreImpl_Test
{
    private static final String DATA_POLICY_TAPE_DUAL_COPY_NAME = "Dual Copy on Tape";

    private RpcClient getRpcClient()
    {
        return InterfaceProxyFactory.getProxy( RpcClient.class, ( proxy, method, args ) -> {
            if ( "getRpcResource".equals( method.getName() ) )
            {
                return InterfaceProxyFactory.getProxy( ( Class< ? > ) args[ 0 ], null );
            }
            return null;
                } );
    }

    private RpcClient getNonNullRpcClient() {
        RpcClient mockRpcClient = Mockito.mock(RpcClient.class);

        // Stub the getRpcResource method to return a proxy for the requested resource type
        TapeEnvironmentResource mockResource = Mockito.mock(TapeEnvironmentResource.class);
        RpcFuture<Long> mockFuture = Mockito.mock(RpcFuture.class);

        RpcFuture<?> mockQuiesceFuture = Mockito.mock(RpcFuture.class);
        Mockito.doReturn(mockQuiesceFuture)
                .when(mockResource)
                .quiesceState();



// Make the future behave as if it completed successfully
        Mockito.when(mockQuiesceFuture.isDone()).thenReturn(true);
        Mockito.when(mockQuiesceFuture.isSuccess()).thenReturn(true);


        Mockito.when(mockResource.isServiceable()).thenReturn(true);
        Mockito.when(mockResource.getTapeEnvironmentGenerationNumber())
                .thenReturn(mockFuture);



// Mock the RpcFuture methods
        Mockito.when(mockFuture.isDone()).thenReturn(true);
        Mockito.when(mockFuture.isSuccess()).thenReturn(true);
        Mockito.when(mockFuture.getWithoutBlocking()).thenReturn(123L);
        Mockito.when(mockFuture.get(Mockito.any(RpcFuture.Timeout.class))).thenReturn(123L);
        Mockito.when(mockFuture.get(Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenReturn(123L);


        RpcFuture<TapeEnvironmentInformation> mockTapeEnvironmentFuture = Mockito.mock(RpcFuture.class);

// Create a mock for the TapeEnvironmentInformation to be returned
        TapeEnvironmentInformation mockTapeEnvironmentInfo = Mockito.mock(TapeEnvironmentInformation.class);

// Configure the future to return the mock TapeEnvironmentInformation
        Mockito.when(mockResource.getTapeEnvironment()).thenReturn(mockTapeEnvironmentFuture);

// Configure the future's behavior
        Mockito.when(mockTapeEnvironmentFuture.isDone()).thenReturn(true);
        Mockito.when(mockTapeEnvironmentFuture.isSuccess()).thenReturn(true);
        Mockito.when(mockTapeEnvironmentFuture.getWithoutBlocking()).thenReturn(mockTapeEnvironmentInfo);

        Mockito.when(mockTapeEnvironmentFuture.get(RpcFuture.Timeout.DEFAULT)).thenReturn(mockTapeEnvironmentInfo);
        Mockito.when(mockRpcClient.getRpcResource(
                Mockito.eq(TapeEnvironmentResource.class),
                Mockito.isNull(),
                Mockito.any(ConcurrentRequestExecutionPolicy.class)
        )).thenReturn(mockResource);
        return mockRpcClient;
    }

    @Test
    public void testCancelEjectForTapeThatIsEjectFromEePendingDoesSo()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.EJECT_FROM_EE_PENDING );
        
        store.cancelEjectTape( tape.getId() );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.ONLINE_PENDING,
                tape.getState(),
                "Shoulda canceled queued ejection."
                 );

        final Tape tp = tape;
        TestUtil.assertThrows( null, RuntimeException.class, () -> store.cancelEjectTape( tp.getId() ) );
    }
    
    @Test
    public void testCancelEjectForTapeThatIsEjectPendingDoesSo()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        store.ejectTape( null, tape.getId(), null, null );
        
        tape = service.attain( tape.getId() );
        assertNotNull(
                tape.getEjectPending(),
                "Shoulda queued tape for ejection."
                 );
        
        store.cancelEjectTape( tape.getId() );
        
        tape = service.attain( tape.getId() );
        assertNull(
                tape.getEjectPending(),
                "Shoulda canceled queued ejection."
                );

        final Tape tp = tape;
        TestUtil.assertThrows( null, RuntimeException.class, () -> store.cancelEjectTape( tp.getId() ) );
    }

    @Test
    public void testCancelEjectForTapeThatIsQueuedForEjectFails()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        final UUID tapeId = tape.getId();
        store.ejectTape( null, tapeId, null, null );

        final TapePartitionService partitionService = dbSupport.getServiceManager().getService( TapePartitionService.class );
        TapePartition partition = partitionService.attain( tape.getPartitionId() );
        partition.setImportExportConfiguration( ImportExportConfiguration.NOT_SUPPORTED );
        partitionService.update( partition, TapePartition.IMPORT_EXPORT_CONFIGURATION );

        tape = service.attain( tapeId );
        assertNotNull(
                tape.getEjectPending(),
                "Should have queued tape for ejection."
                );


        TestUtil.assertThrows( null, RuntimeException.class, () -> store.cancelEjectTape( tapeId ) );

        tape = service.attain( tapeId );
        assertNotNull(
                tape.getEjectPending(),
                "Should not have canceled queued ejection."
                 );
    }
    
    @Test
    public void testCancelEjectForTapeThatIsNotEjectPendingDoesNothing()
    {
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        TestUtil.assertThrows( null, RuntimeException.class, () -> store.cancelEjectTape( tape.getId() ) );
    }
    
    @Test
    public void testCancelEjectNullTapeIdNotAllowed()
    {
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape( TapeState.NORMAL );
    
        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> store.cancelEjectTape( null ) );
    }
    
    @Test
    public void testCancelFormatForTapeThatWasFormatInProgressDoesNotCancelFormat()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.UNKNOWN );
        
        store.formatTape( BlobStoreTaskPriority.values()[ 1 ], tape.getId(), false, false);
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.FORMAT_PENDING,
                tape.getState(),
                "Shoulda been format pending."
                );
        synchronized ( extractProcessor( store ).getTaskStateLock() )
        {
            final TapeTask task = extractProcessor( store ).getTapeTasks().get( tape.getId() ).get(0);
            assertNotNull(task);
            task.prepareForExecutionIfPossible(
                    new MockTapeDriveResource(),
                    new MockTapeAvailability() );
        }

        final Tape tp = tape;
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, () -> store.cancelFormatTape( tp.getId() ) );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.FORMAT_IN_PROGRESS,
                tape.getState(),
                "Should notta succeeded in cancelling the tape format since its task has started."
                 );
    }
    
    @Test
    public void testCancelFormatForTapeThatWasFormatPendingDoesSo()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.UNKNOWN );
        
        store.formatTape( BlobStoreTaskPriority.values()[ 1 ], tape.getId(), false, false);
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.FORMAT_PENDING,
                tape.getState(),
                "Shoulda been format pending."
                 );
        
        store.cancelFormatTape( tape.getId() );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.UNKNOWN,
                tape.getState(),
                "Shoulda succeeded in cancelling the tape format."
                 );
    }
    
    @Test
    public void testCancelFormatForTapeThatWasNeverFormatPendingDoesNotCancelFormat()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.UNKNOWN );

        final Tape tp = tape;
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, () -> store.cancelFormatTape( tp.getId() ) );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.UNKNOWN,
                tape.getState(),
                "Should notta succeeded in cancelling the tape format since format never queued."
                 );
    }
    
    @Test
    public void testCancelImportForTapeThatWasImportInProgressDoesNotCancelImport()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        Tape tape = mockDaoDriver.createTape( TapeState.FOREIGN );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                mockDaoDriver.attainOneAndOnly( TapePartition.class ).getId(),
                tape.getType() );
        mockDaoDriver.createDataPersistenceRule(dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );

        final ImportTapeDirective directive = mockDaoDriver.createImportTapeDirective(
                tape.getId(),
                user.getId(),
                dp.getId() );
        store.importTape(
                BlobStoreTaskPriority.values()[ 1 ],
                directive );
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.IMPORT_PENDING,
                tape.getState(),
                "Shoulda been import pending."
                 );
        synchronized ( extractProcessor( store ).getTaskStateLock() )
        {
            final TapeTask task = extractProcessor( store ).getTapeTasks().get( tape.getId() ).get(0);
            assertNotNull(task);
            task.prepareForExecutionIfPossible(
                    new MockTapeDriveResource(),
                    new MockTapeAvailability() );
        }

        final Tape tp = tape;
        TestUtil.assertThrows( null, RuntimeException.class, () -> store.cancelImportTape( tp.getId() ) );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.IMPORT_IN_PROGRESS,
                tape.getState(),
                "Should notta succeeded in cancelling the tape import since its task has started."
                );
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( ImportTapeDirective.class ).getCount(),
                "Should notta succeeded in cancelling the tape import."
                 );
    }
    
    @Test
    public void testCancelImportForTapeThatWasImportPendingDoesSo()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        Tape tape = mockDaoDriver.createTape( TapeState.FOREIGN );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                mockDaoDriver.attainOneAndOnly( TapePartition.class ).getId(),
                tape.getType() );
        mockDaoDriver.createDataPersistenceRule(dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );

        final ImportTapeDirective directive = mockDaoDriver.createImportTapeDirective(
                tape.getId(),
                user.getId(),
                dp.getId() );
        store.importTape(
                BlobStoreTaskPriority.values()[ 1 ],
                directive );
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.IMPORT_PENDING,
                tape.getState(),
                "Shoulda been import pending."
                 );
        
        store.cancelImportTape( tape.getId() );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.FOREIGN,
                tape.getState(),
                "Shoulda succeeded in cancelling the tape import."
                 );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( ImportTapeDirective.class ).getCount(),
                "Shoulda succeeded in cancelling the tape import."
                 );
    }
    
    @Test
    public void testCancelImportForTapeThatWasNeverImportPendingDoesNotCancelImport()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.UNKNOWN );

        final Tape tp = tape;
        TestUtil.assertThrows( null, RuntimeException.class, () -> store.cancelImportTape( tp.getId() ) );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.UNKNOWN,
                tape.getState(),
                "Should notta succeeded in cancelling the tape import since import never queued."
                 );
    }
    
    @Test
    public void testCancelOnlineForTapeThatIsNotOnlinePendingDoesNothing()
    {
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.OFFLINE );
        TestUtil.assertThrows( null, RuntimeException.class, () -> store.cancelOnlineTape( tape.getId() ) );
    }
    
    @Test
    public void testCancelOnlineForTapeThatIsOnlinePendingDoesSo()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.OFFLINE );
        store.onlineTape( tape.getId() );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.ONLINE_PENDING,
                tape.getState(),
                "Shoulda queued tape for onlining."
                 );
        
        store.cancelOnlineTape( tape.getId() );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.OFFLINE,
                tape.getState(),
                "Shoulda canceled queued onlining."
                 );

        final Tape tp = tape;
        TestUtil.assertThrows( null, RuntimeException.class, () -> store.cancelOnlineTape( tp.getId() ) );
    }
    
    @Test
    public void testCancelOnlineNullTapeIdNotAllowed()
    {
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape( TapeState.OFFLINE );
    
        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> store.cancelOnlineTape( null ) );
    }
    
    @Test
    public void testCancelRawImportForTapeThatWasImportInProgressDoesNotCancelImport()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.LTFS_WITH_FOREIGN_DATA );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                mockDaoDriver.attainOneAndOnly( TapePartition.class ).getId(),
                tape.getType() );
        mockDaoDriver.createDataPersistenceRule(dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        service.update( tape.setWriteProtected( true ), Tape.WRITE_PROTECTED );
        

        final RawImportTapeDirective directive = mockDaoDriver.createRawImportTapeDirective(
                tape.getId(),
                null );
        store.importTape(
                BlobStoreTaskPriority.values()[ 1 ],
                directive );
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.RAW_IMPORT_PENDING,
                tape.getState(),
                "Shoulda been import pending."
                 );
        synchronized ( extractProcessor( store ).getTaskStateLock() )
        {
            final TapeTask task = extractProcessor( store ).getTapeTasks().get( tape.getId() ).get(0);
            assertNotNull(task);
            task.prepareForExecutionIfPossible(
                    new MockTapeDriveResource(),
                    new MockTapeAvailability() );
        }

        final Tape tp = tape;
        TestUtil.assertThrows( null, RuntimeException.class, () -> store.cancelImportTape( tp.getId() ) );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.RAW_IMPORT_IN_PROGRESS,
                tape.getState(),
                "Should notta succeeded in cancelling the tape import since its task has started."
                );
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( RawImportTapeDirective.class ).getCount(),
                "Should notta succeeded in cancelling the tape import."
                 );
    }
    
    @Test
    public void testCancelRawImportForTapeThatWasImportPendingDoesSo()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.LTFS_WITH_FOREIGN_DATA );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                mockDaoDriver.attainOneAndOnly( TapePartition.class ).getId(),
                tape.getType() );
        mockDaoDriver.createDataPersistenceRule(dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        service.update( tape.setWriteProtected( true ), Tape.WRITE_PROTECTED );

        final RawImportTapeDirective directive = mockDaoDriver.createRawImportTapeDirective(
                tape.getId(),
                null );
        store.importTape(
                BlobStoreTaskPriority.values()[ 1 ],
                directive );
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.RAW_IMPORT_PENDING,
                tape.getState(),
                "Shoulda been import pending."
                 );
        
        store.cancelImportTape( tape.getId() );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.LTFS_WITH_FOREIGN_DATA,
                tape.getState(),
                "Shoulda succeeded in cancelling the tape import."
                 );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( RawImportTapeDirective.class ).getCount(),
                "Shoulda succeeded in cancelling the tape import."
                 );
    }
    
    @Test
    public void testCancelRawImportForTapeThatWasNeverImportPendingDoesNotCancelImport()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.UNKNOWN );

        final Tape tp = tape;
        TestUtil.assertThrows( null, RuntimeException.class, () -> store.cancelImportTape( tp.getId() ) );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.UNKNOWN,
                tape.getState(),
                "Should notta succeeded in cancelling the tape import since import never queued."
                 );
    }
    
    @Test
    public void testCancelVerifyTapeWhereNoTaskYetWorks()
    {
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape.setVerifyPending( BlobStoreTaskPriority.NORMAL ), Tape.VERIFY_PENDING );
        store.cancelVerifyTape( tape.getId() );
        assertNull(
                mockDaoDriver.attain( tape ).getVerifyPending(),
                "Shoulda canceled verify."
                 );
    }
    
    @Test
    public void testCancelVerifyTapeWhereStartedTaskNotAllowed()
    {
        final DiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );
        store = new TapeBlobStoreImpl(
                getRpcClient(),  mockDiskManager,
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        final TapeBlobStoreProcessor processor = extractProcessor( store );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape.setVerifyPending( BlobStoreTaskPriority.NORMAL ), Tape.VERIFY_PENDING );
        synchronized ( processor.getTaskStateLock() )
        {
            final VerifyTapeTask task =
                    new VerifyTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), mockDiskManager, new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager() );
            processor.getTapeTasks().addStaticTask(task );
            task.prepareForExecutionIfPossible( new MockTapeDriveResource(), new MockTapeAvailability() );
        }
        assertEquals(
                1,
                store.getTasks().size(),
                "Shoulda reported single task."
                );
        TestUtil.assertThrows( null, RuntimeException.class, () -> store.cancelVerifyTape( tape.getId() ) );
        assertNotNull(
                mockDaoDriver.attain( tape ).getVerifyPending(),
                "Should notta canceled verify."
                 );
        assertEquals(
                1,
                store.getTasks().size(),
                "Should notta canceled verify."
                 );
    }
    
    @Test
    public void testCancelVerifyTapeWhereTaskNotYetStartedWorks()
    {
        final DiskManager diskManager = new MockDiskManager( dbSupport.getServiceManager() );
        store = new TapeBlobStoreImpl(
                getRpcClient(), diskManager,
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        final TapeBlobStoreProcessor processor = extractProcessor( store );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape.setVerifyPending( BlobStoreTaskPriority.NORMAL ), Tape.VERIFY_PENDING );
        synchronized ( processor.getTaskStateLock() )
        {
            processor.getTapeTasks().addStaticTask(
                    new VerifyTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), diskManager, new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager() ) );
        }
        assertEquals(
                1,
                store.getTasks().size(),
                "Shoulda reported single task."
                 );
        store.cancelVerifyTape( tape.getId() );
        assertNull(
                mockDaoDriver.attain( tape ).getVerifyPending(),
                "Shoulda canceled verify."
                 );
        assertEquals(
                0,
                store.getTasks().size(),
                "Shoulda canceled verify."
                 );
    }
    
    @Test
    public void testCleanDriveFailsDueToNoCleaningTapeThrowsError()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape( TapeState.NORMAL );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "aa" );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, DataPlannerException.class, () -> store.cleanDrive( drive.getId() ) );
        
        assertEquals(
                TapePartitionFailureType.CLEANING_TAPE_REQUIRED,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).attain(
                        Require.nothing() ).getType(),
                "Shoulda been a tape partition failure."
                 );
    }
    
    @Test
    public void testCleanDriveFailsDueToNoNormalStateCleaningTapeThrowsError()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                mockDaoDriver.createTape( TapeState.LOST ).setType( TapeType.LTO_CLEANING_TAPE ),
                Tape.TYPE );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "aa" );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, DataPlannerException.class, () -> store.cleanDrive( drive.getId() ) );
        
        assertEquals(
                TapePartitionFailureType.CLEANING_TAPE_REQUIRED,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).attain(
                        Require.nothing() ).getType(),
                "Shoulda been a tape partition failure."
                 );
    }
    
    @Test
    public void testCleanDriveIgnoredDueToAlreadyScheduledCleaningOnDriveReturns()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                mockDaoDriver.createTape( TapeState.NORMAL ).setType( TapeType.LTO_CLEANING_TAPE ),
                Tape.TYPE );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                mockDaoDriver.createTape( TapeState.NORMAL ).setType( TapeType.LTO_CLEANING_TAPE ),
                Tape.TYPE );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "aa" );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( null, "ab" );
        final TapeDrive drive3 = mockDaoDriver.createTapeDrive( null, "ac" );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        store.cleanDrive( drive.getId() );
        store.cleanDrive( drive.getId() );
        store.cleanDrive( drive2.getId() );
        store.cleanDrive( drive2.getId() );
        store.cleanDrive( drive2.getId() );
        TestUtil.assertThrows( null, DataPlannerException.class, () -> store.cleanDrive( drive3.getId() ) );
        store.cleanDrive( drive.getId() );
        store.cleanDrive( drive2.getId() );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Should notta been any tape partition failures."
                );
    }
    
    @Test
    public void testCleanDriveWithSuccessReturnsWithoutError()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                mockDaoDriver.createTape( TapeState.NORMAL ).setType( TapeType.LTO_CLEANING_TAPE ),
                Tape.TYPE );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "aa" );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        store.cleanDrive( drive.getId() );
        
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Should notta been any tape partition failures."
                );
    }

    @Test
    public void testTestDriveFailsDueToNoTestTapeThrowsError()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape( TapeState.NORMAL );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "aa" );

        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, DataPlannerException.class, () -> store.testDrive( drive.getId(), null, true) );
    }

    @Test
    public void testTestDriveFailsDueToNoNormalStateTestTapeThrowsError()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                mockDaoDriver.createTape( TapeState.FOREIGN ).setRole( TapeRole.TEST ),
                Tape.ROLE );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "aa" );

        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, DataPlannerException.class, () -> store.testDrive( drive.getId(), null, true) );
    }

    @Test
    public void testTestDriveIgnoredDueToAlreadyScheduledTestOnDriveReturns()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape1.setRole( TapeRole.TEST ),
                Tape.ROLE );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape2.setRole( TapeRole.TEST ),
                Tape.ROLE );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "aa" );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( null, "ab" );
        final TapeDrive drive3 = mockDaoDriver.createTapeDrive( null, "ac" );

        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        store.testDrive( drive.getId(), tape1.getId(), false);
        store.testDrive( drive.getId(), null, false);
        store.testDrive( drive2.getId(), tape2.getId(), false);
        store.testDrive( drive2.getId(), null, false);
        store.testDrive( drive2.getId(), null, false);
        TestUtil.assertThrows( null, DataPlannerException.class, () -> store.testDrive( drive3.getId(), null, false) );
        store.testDrive( drive.getId(), null, false);
        store.testDrive( drive2.getId(), null, false);
        final Tape tape3 = mockDaoDriver.createTape( TapeState.NORMAL );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape3.setRole( TapeRole.TEST ),
                Tape.ROLE );
        TestUtil.assertThrows( null, DataPlannerException.class, () -> store.testDrive( drive2.getId(), tape1.getId(), false) );
        store.testDrive( drive2.getId(), tape3.getId(), false);
        store.testDrive( drive2.getId(), tape2.getId(), false);
        TestUtil.assertThrows( null, DataPlannerException.class, () -> store.testDrive( drive2.getId(), tape1.getId(), false) );
        store.testDrive( drive.getId(), tape3.getId(), false);
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Should notta been any tape partition failures."
                 );
    }

    @Test
    public void testTestDriveWithSuccessReturnsWithoutError()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                mockDaoDriver.createTape( TapeState.NORMAL ).setRole( TapeRole.TEST ),
                Tape.ROLE );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "aa" );

        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        store.testDrive( drive.getId(), null, false);

        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Should notta been any tape partition failures."
                 );
    }

    @Test
    public void testCancelTestDriveWithSuccessReturnsWithoutError()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                mockDaoDriver.createTape( TapeState.NORMAL ).setRole( TapeRole.TEST ),
                Tape.ROLE );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "aa" );

        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        store.cancelTestDrive( drive.getId() );

        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Should notta been any tape partition failures."
                 );
    }

    @Test
    public void testConstructorDoesNotDieWhenAttemptingRestartsTapesFormatInProgressWhenAttemptFails()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        Tape tape = mockDaoDriver.createTape( TapeState.FORMAT_IN_PROGRESS );
        service.update( tape.setType( TapeType.LTO_CLEANING_TAPE ), Tape.TYPE );
        
        new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.UNKNOWN,
                tape.getState(),
                "Shoulda handled failure to format."
               );
    }
    
    @Test
    public void testConstructorDoesNotDieWhenAttemptingRestartsTapesPendingFormatWhenAttemptFails()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        Tape tape = mockDaoDriver.createTape( TapeState.FORMAT_PENDING );
        service.update( tape.setType( TapeType.LTO_CLEANING_TAPE ), Tape.TYPE );
        
        new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.UNKNOWN,
                tape.getState(),
                "Shoulda handled format failure."
                 );
    }
    
    @Test
    public void testConstructorHandlesGracefullyCorruptTapesImportInProgress()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        Tape tape = mockDaoDriver.createTape( TapeState.IMPORT_IN_PROGRESS );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.FOREIGN,
                tape.getState(),
                "Should notta queued import since missing directive."
                 );
        assertEquals(
                0,
                store.getTasks().size(),
                "Should notta queued import since missing directive."
                 );
    }
    
    @Test
    public void testConstructorHandlesGracefullyCorruptTapesRawImportInProgress()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        Tape tape = mockDaoDriver.createTape( TapeState.RAW_IMPORT_IN_PROGRESS );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.LTFS_WITH_FOREIGN_DATA,
                tape.getState(),
                "Should notta queued import since missing directive."
                );
        assertEquals(
                0,
                store.getTasks().size(),
                "Should notta queued import since missing directive."
                );
    }
    
    @Test
    public void testConstructorNullCacheManagerNotAllowed()
    {
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> new TapeBlobStoreImpl( getRpcClient(), null,
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), dbSupport.getServiceManager() ) );
    }
    
    @Test
    public void testConstructorNullJobProgressManagerNotAllowed()
    {
        
        TestUtil.assertThrows( null, IllegalArgumentException.class,
                () -> new TapeBlobStoreImpl( getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ), null,
                        dbSupport.getServiceManager() ) );
    }
    
    @Test
    public void testConstructorNullRpcClientNotAllowed()
    {
        
        TestUtil.assertThrows( null, IllegalArgumentException.class,
                () -> new TapeBlobStoreImpl( null, new MockDiskManager( dbSupport.getServiceManager() ),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        dbSupport.getServiceManager() ) );
    }
    
    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class,
                () -> new TapeBlobStoreImpl( getRpcClient(), new MockDiskManager( null ),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), null ) );
    }
    
    @Test
    public void testConstructorRestartsTapesEjectToEeInProgress()
    {
        System.out.println("hereeee");
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        System.out.println("hereeee");
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        service.transistState( tape, TapeState.EJECT_TO_EE_IN_PROGRESS );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.NORMAL,
                tape.getState(),
                "Shoulda queued eject."
                );
        assertNotNull(
                tape.getEjectPending(),
                "Shoulda queued eject."
                 );
        assertEquals(
                0,
                store.getTasks().size(),
                "Shoulda queued eject."
                );
    }
    
    @Test
    public void testConstructorRestartsTapesFormatInProgress()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        Tape tape = mockDaoDriver.createTape( TapeState.FORMAT_IN_PROGRESS );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.FORMAT_PENDING,
                tape.getState(),
                "Shoulda been a single format task created."
               );
        assertEquals(
                1,
                store.getTasks().size(),
                "Shoulda been a single format task created."
                 );
        assertEquals(
                FormatTapeTask.class,
                store.getTasks().iterator().next().getClass(),
                "Shoulda been a single format task created."
                );
        assertEquals(
                tape.getId(),
                store.getTasks().iterator().next().getTapeId(),
                "Shoulda been a single format task created."
                 );
    }
    
    @Test
    public void testConstructorRestartsTapesImportInProgress()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        Tape tape = mockDaoDriver.createTape( TapeState.IMPORT_IN_PROGRESS );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                mockDaoDriver.attainOneAndOnly( TapePartition.class ).getId(),
                tape.getType() );
        mockDaoDriver.createDataPersistenceRule(dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createImportTapeDirective( tape.getId(), null, null );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.IMPORT_PENDING,
                tape.getState(),
                "Shoulda queued import."
                );
        assertEquals(
                1,
                store.getTasks().size(),
                "Shoulda queued import."
                 );
    }
    
    @Test
    public void testConstructorRestartsTapesImportPending()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        Tape tape = mockDaoDriver.createTape( TapeState.IMPORT_PENDING );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                mockDaoDriver.attainOneAndOnly( TapePartition.class ).getId(),
                tape.getType() );
        mockDaoDriver.createDataPersistenceRule(dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createImportTapeDirective( tape.getId(), null, null );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.IMPORT_PENDING,
                tape.getState(),
                "Shoulda queued import."
                );
        assertEquals(
                1,
                store.getTasks().size(),
                "Shoulda queued import."
                );
    }
    
    @Test
    public void testConstructorRestartsTapesOnlineInProgress()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        Tape tape = mockDaoDriver.createTape( TapeState.ONLINE_IN_PROGRESS );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.ONLINE_PENDING,
                tape.getState(),
                "Shoulda queued onlining."
                 );
        assertEquals(
                0,
                store.getTasks().size(),
                "Shoulda queued onlining."
                );
    }
    
    @Test
    public void testConstructorRestartsTapesPendingFormat()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        Tape tape = mockDaoDriver.createTape( TapeState.FORMAT_PENDING );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.FORMAT_PENDING,
                tape.getState(),
                "Shoulda been a single format task created."
                 );
        assertEquals(
                1,
                store.getTasks().size(),
                "Shoulda been a single format task created."
                 );
        assertEquals(
                FormatTapeTask.class,
                store.getTasks().iterator().next().getClass(),
                "Shoulda been a single format task created."
                 );
        assertEquals(
                tape.getId(),
                store.getTasks().iterator().next().getTapeId(),
                "Shoulda been a single format task created."
                 );
    }
    
    @Test
    public void testConstructorRestartsTapesRawImportInProgress()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        Tape tape = mockDaoDriver.createTape( TapeState.RAW_IMPORT_IN_PROGRESS );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                mockDaoDriver.attainOneAndOnly( TapePartition.class ).getId(),
                tape.getType() );
        mockDaoDriver.createDataPersistenceRule(dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        service.update( tape.setWriteProtected( true ), Tape.WRITE_PROTECTED );
        mockDaoDriver.createRawImportTapeDirective( tape.getId(), null );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.RAW_IMPORT_PENDING,
                tape.getState(),
                "Shoulda queued import."
                 );
        assertEquals(
                1,
                store.getTasks().size(),
                "Shoulda queued import."
                 );
    }
    
    @Test
    public void testConstructorRestartsTapesRawImportPending()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        Tape tape = mockDaoDriver.createTape( TapeState.RAW_IMPORT_PENDING );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                mockDaoDriver.attainOneAndOnly( TapePartition.class ).getId(),
                tape.getType() );
        mockDaoDriver.createDataPersistenceRule(dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        service.update( tape.setWriteProtected( true ), Tape.WRITE_PROTECTED );
        mockDaoDriver.createRawImportTapeDirective( tape.getId(), null );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.RAW_IMPORT_PENDING,
                tape.getState(),
                "Shoulda queued import."
                 );
        assertEquals(
                1,
                store.getTasks().size(),
                "Shoulda queued import."
                );
    }
    
    @Test
    public void testEjectTapeAlreadyEjectInProgressOnlyUpdatesLabelAndLocationAttributes()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.EJECT_TO_EE_IN_PROGRESS );
        final UUID tapeId = tape.getId();
        service.update( tape.setEjectLabel( "original" ).setEjectLocation( "original" ),
                        Tape.EJECT_LABEL, Tape.EJECT_LOCATION );

        store.ejectTape( null, tapeId, "label1", "location1" );
        store.ejectTape( null, tapeId, "label", "location" );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                "label",
                tape.getEjectLabel(),
                "Shoulda updated tape's label attribute."
                );
        assertEquals(
                "location",
                tape.getEjectLocation(),
                "Shoulda updated tape's location attribute."
                );
        assertNull(
                tape.getEjectPending(),
                "Should notta queued tape for ejection."
                 );
    }
    
    @Test
    public void testEjectTapeAlreadyEjectPendingOnlyUpdatesLabelAndLocationAttributes()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        final UUID tapeId = tape.getId();
        service.update( tape.setEjectLabel( "original" ).setEjectLocation( "original" ),
                        Tape.EJECT_LABEL, Tape.EJECT_LOCATION );

        store.ejectTape( null, tapeId, "label1", "location1" );
        store.ejectTape( null, tapeId, "label", "location" );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                "label",
                tape.getEjectLabel(),
                "Shoulda updated tape's label attribute."
                 );
        assertEquals(
                "location",
                tape.getEjectLocation(),
                "Shoulda updated tape's location attribute."
                 );
        assertNotNull(
                tape.getEjectPending(),
                "Shoulda queued tape for ejection."
                 );
    }
    
    @Test
    public void testEjectTapeAlreadyPhysicallyRemovedOnlyUpdatesLabelAndLocationAttributes()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.EJECTED );
        final UUID tapeId = tape.getId();
        service.update( tape.setEjectLabel( "original" ).setEjectLocation( "original" ),
                        Tape.EJECT_LABEL, Tape.EJECT_LOCATION );

        store.ejectTape( null, tapeId, "label1", "location1" );
        store.ejectTape( null, tapeId, "label", "location" );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                "label",
                tape.getEjectLabel(),
                "Shoulda updated tape's label attribute."
                 );
        assertEquals(
                "location",
                tape.getEjectLocation(),
                "Shoulda updated tape's location attribute."
                 );
        assertNull(
                tape.getEjectPending(),
                "Should notta queued tape for ejection."
                 );
    }
    
    @Test
    public void testEjectTapeAwaitingPhysicalRemovalOnlyUpdatesLabelAndLocationAttributes()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.EJECT_FROM_EE_PENDING );
        final UUID tapeId = tape.getId();
        service.update( tape.setEjectLabel( "original" ).setEjectLocation( "original" ),
                        Tape.EJECT_LABEL, Tape.EJECT_LOCATION );

        store.ejectTape( null, tapeId, "label1", "location1" );
        store.ejectTape( null, tapeId, "label", "location" );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                "label",
                tape.getEjectLabel(),
                "Shoulda updated tape's label attribute."
                );
        assertEquals(
                "location",
                tape.getEjectLocation(),
                "Shoulda updated tape's location attribute."
                );
        assertNull(
                tape.getEjectPending(),
                "Should notta queued tape for ejection."
                 );
    }
    
    @Test
    public void testEjectTapeSchedulesVerifyIffVerifyPriorToEjectRequested()
    {
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape1.getPartitionId(), tape1.getType() );
        mockDaoDriver.updateBean( tape2.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        store.ejectTape( null, tape1.getId(), "label", "location" );
        store.ejectTape( BlobStoreTaskPriority.values()[ 1 ], tape2.getId(), "label", "location" );
        
        assertNotNull(
                mockDaoDriver.attain( tape1 ).getEjectPending(),
                "Shoulda updated tape's state for pending eject."
                );
        assertNull(
                mockDaoDriver.attain( tape1 ).getVerifyPending(),
                "Shoulda updated tape's state for pending eject."
                 );
        assertNotNull(
                mockDaoDriver.attain( tape2 ).getEjectPending(),
                "Shoulda updated tape's state for pending eject."
                 );
        assertNotNull(
                mockDaoDriver.attain( tape2 ).getVerifyPending(),
                "Shoulda updated tape's state for pending eject."
                 );
    }
    
    @Test
    public void testEjectTapeThatHasBeenAllocatedToStorageDomainDisallowingMediaEjectionNotAllowed()
    {
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "storageDomain" );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean(
                storageDomain.setMediaEjectionAllowed( false ),
                StorageDomain.MEDIA_EJECTION_ALLOWED );
        mockDaoDriver.updateBean(
                tape.setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
    
        TestUtil.assertThrows( null, GenericFailure.CONFLICT,
                () -> store.ejectTape( null, tape.getId(), "label", "location" ) );
        
        mockDaoDriver.attainAndUpdate( tape );
        assertNull(
                tape.getEjectLabel(),
                "Should notta processed eject request."
                 );
        assertNull(
                tape.getEjectLocation(),
                "Should notta processed eject request."
                 );
        assertNull(
                tape.getEjectPending(),
                "Should notta processed eject request."
                 );
    }
    
    @Test
    public void testEjectTapeThatHasBeenAllocatedToStorageDomainPermittingMediaEjectionAllowed()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "storageDomain" );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean(
                storageDomain.setMediaEjectionAllowed( true ),
                StorageDomain.MEDIA_EJECTION_ALLOWED );
        mockDaoDriver.updateBean(
                tape.setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        store.ejectTape( null, tape.getId(), "label", "location" );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                "label",
                tape.getEjectLabel(),
                "Shoulda updated tape's state for pending eject."
                 );
        assertEquals(
                "location",
                tape.getEjectLocation(),
                "Shoulda updated tape's state for pending eject."
                 );
        assertNotNull(
                tape.getEjectPending(),
                "Shoulda updated tape's state for pending eject."
                );
    }
    
    @Test
    public void testEjectTapeWithLabelAndLocationUpdatesTapeStateForPendingEject()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        service.update( tape.setEjectLabel( "original" ).setEjectLocation( "original" ),
                        Tape.EJECT_LABEL, Tape.EJECT_LOCATION );
        
        store.ejectTape( null, tape.getId(), "label", "location" );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                "label",
                tape.getEjectLabel(),
                "Shoulda updated tape's state for pending eject."
                 );
        assertEquals(
                "location",
                tape.getEjectLocation(),
                "Shoulda updated tape's state for pending eject."
                 );
        assertNotNull(
                tape.getEjectPending(),
                "Shoulda updated tape's state for pending eject."
                 );
    }
    
    @Test
    public void testEjectTapeWithPendingTapeOperationsAllowed()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        service.update( tape.setEjectLabel( "original" ).setEjectLocation( "original" ),
                        Tape.EJECT_LABEL, Tape.EJECT_LOCATION );
        
        store.inspectTape( BlobStoreTaskPriority.values()[ 0 ], tape.getId() );
        store.formatTape( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), true, false);
        store.ejectTape( null, tape.getId(), "label", "location" );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                "label",
                tape.getEjectLabel(),
                "Shoulda updated tape's state for pending eject."
                 );
        assertEquals(
                "location",
                tape.getEjectLocation(),
                "Shoulda updated tape's state for pending eject."
                );
        assertNotNull(
                tape.getEjectPending(),
                "Shoulda updated tape's state for pending eject."
                 );
    }
    
    @Test
    public void testEjectTapeWithoutLabelOrLocationUpdatesTapeStateForPendingEject()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        service.update( tape.setEjectLabel( "original" ).setEjectLocation( "original" ),
                        Tape.EJECT_LABEL, Tape.EJECT_LOCATION );
        
        store.ejectTape( null, tape.getId(), null, null );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                "original",
                tape.getEjectLabel(),
                "Should notta updated eject label."
                );
        assertEquals(
                "original",
                tape.getEjectLocation(),
                "Should notta updated eject location."
                 );
        assertNotNull(
                tape.getEjectPending(),
                "Shoulda updated tape's state for pending eject."
                 );
    }
    
    @Test
    public void testFormatTapeDoesSo()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.UNKNOWN );

        store.formatTape( BlobStoreTaskPriority.values()[ 1 ], tape.getId(), false, false);
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.FORMAT_PENDING,
                tape.getState(),
                "Shoulda updated tape's state."
                 );
    }

    @Test
    public void testFormatTapeLTO5InLTO7DriveThrowsError()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final TapePartition partition = mockDaoDriver.createTapePartition( null, null, TapeDriveType.LTO7 );
        Tape tape = mockDaoDriver.createTape( partition.getId(), TapeState.UNKNOWN, TapeType.LTO5 );
        final UUID tapeId = tape.getId();

        TestUtil.assertThrows( null, IllegalStateException.class,
                () -> store.formatTape( BlobStoreTaskPriority.values()[ 1 ], tapeId, true, false) );

        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.UNKNOWN,
                tape.getState(),
                "State should remain unchanged"
                 );
    }
    
    @Test
    public void testFormatTapeThatCannotContainDataNotAllowedWithoutForceFlag()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.UNKNOWN );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape.setType( TapeType.LTO_CLEANING_TAPE ), Tape.TYPE );
        
        final UUID tapeId = tape.getId();
        TestUtil.assertThrows( null, IllegalStateException.class,
                () -> store.formatTape( BlobStoreTaskPriority.values()[ 1 ], tapeId, true, false) );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.UNKNOWN,
                tape.getState(),
                "Should notta updated tape's state."
                 );
    }
    
    @Test
    public void testFormatTapeThatHasDataOnItNotAllowed()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain(
                MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        dbSupport.getServiceManager()
                 .getService( TapeService.class )
                 .update(
                tape.setStorageDomainMemberId( sdm.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );

        final UUID tapeId = tape.getId();
        TestUtil.assertThrows(
                null, FailureTypeObservableException.class,
                () -> store.formatTape( BlobStoreTaskPriority.values()[ 1 ], tapeId, true, false) );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.NORMAL,
                tape.getState(),
                "Should notta updated tape's state."
                 );
    }
    
    @Test
    public void testFormatTapeThatIsAlreadyNormalNotAllowedWithoutForceFlag()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );

        final UUID tapeId = tape.getId();
        TestUtil.assertThrows( null, ForceFlagRequiredException.class,
                () -> store.formatTape( BlobStoreTaskPriority.values()[ 1 ], tapeId, false, false) );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.NORMAL,
                tape.getState(),
                "Should notta updated tape's state."
                 );
        
        store.formatTape( BlobStoreTaskPriority.values()[ 1 ], tape.getId(), true, false);
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.FORMAT_PENDING,
                tape.getState(),
                "Shoulda updated tape's state."
                 );
    }
    
    @Test
    public void testFormatTapeThatIsAssignedToBucketButHasNoDataOnItAllowed()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.UNKNOWN );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain(
                MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        dbSupport.getServiceManager()
                 .getService( TapeService.class )
                 .update(
                tape.setStorageDomainMemberId( sdm.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );

        store.formatTape( BlobStoreTaskPriority.values()[ 1 ], tape.getId(), false, false);
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.FORMAT_PENDING,
                tape.getState(),
                "Shoulda updated tape's state."
                 );
    }
    
    @Test
    public void testFormatTapeThatIsForeignNotAllowedWithoutForceFlag()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.FOREIGN );

        final UUID tapeId = tape.getId();
        TestUtil.assertThrows( null, ForceFlagRequiredException.class,
                () -> store.formatTape( BlobStoreTaskPriority.values()[ 1 ], tapeId, false, false) );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.FOREIGN,
                tape.getState(),
                "Should notta updated tape's state."
                 );
        
        store.formatTape( BlobStoreTaskPriority.values()[ 1 ], tape.getId(), true, false);
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.FORMAT_PENDING,
                tape.getState(),
                "Shoulda updated tape's state."
                 );
    }
    
    @Test
    public void testFormatTapeThatIsInStateDisallowingTapeOperationsNotAllowed()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.LOST );

        final UUID tapeId = tape.getId();
        TestUtil.assertThrows(
                null, IllegalStateException.class,
                () -> store.formatTape( BlobStoreTaskPriority.values()[ 1 ], tapeId, true, false) );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.LOST,
                tape.getState(),
                "Should notta updated tape's state."
                 );
    }
    
    @Test
    public void testFormatTapeThatIsInStatePendingInspectionWithoutForceFlagNotAllowed()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.PENDING_INSPECTION );
        
        final UUID tapeId = tape.getId();
        TestUtil.assertThrows( null, ForceFlagRequiredException.class,
                () -> store.formatTape( BlobStoreTaskPriority.values()[ 1 ], tapeId, false, false) );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.PENDING_INSPECTION,
                tape.getState(),
                "Should notta updated tape's state."
                );
        
        store.formatTape( BlobStoreTaskPriority.values()[ 1 ], tape.getId(), true, false);
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.FORMAT_PENDING,
                tape.getState(),
                "Shoulda updated tape's state."
                 );
    }
    
    @Test
    public void testFormatTapeThatIsLtfsForeignWithDataAllowedWithoutForceFlag()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.LTFS_WITH_FOREIGN_DATA );

        store.formatTape( BlobStoreTaskPriority.values()[ 1 ], tape.getId(), false, false);
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.FORMAT_PENDING,
                tape.getState(),
                "Shoulda updated tape's state."
                 );
    }
    
    @Test
    public void testFormatTapeTwiceWhereSecondRequestDowngradesPriorityResultsInNewPriorityRetained()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.UNKNOWN );

        final TapeBlobStoreProcessor processor = extractProcessor( store );
        synchronized ( processor.getTaskStateLock() )
        {
            assertEquals(
                    0,
                    processor.getTapeTasks().size(),
                    "Should notta been any tape tasks initially."
                     );
        }
        
        store.formatTape( BlobStoreTaskPriority.values()[ 2 ], tape.getId(), false, false);
        final TapeTask formatTask;
        synchronized ( processor.getTaskStateLock() )
        {
            assertEquals(
                    1,
                    processor.getTapeTasks().size(),
                    "Shoulda been single tape task to format."
                     );
            formatTask = processor.getTapeTasks().get( tape.getId() ).get(0);
            assertNotNull(formatTask);
            assertEquals(
                    BlobStoreTaskPriority.values()[ 2 ],
                    formatTask.getPriority(),
                    "Shoulda set priority to format tape priority specified."
                    );
        }
        
        store.formatTape( BlobStoreTaskPriority.values()[ 1 ], tape.getId(), false, false);
        synchronized ( processor.getTaskStateLock() )
        {
            assertEquals(
                    1,
                    processor.getTapeTasks().size(),
                    "Shoulda been single tape task to format."
                     );
            assertEquals(
                    BlobStoreTaskPriority.values()[ 1 ],
                    formatTask.getPriority(),
                    "Shoulda updated to new priority specified."
                     );
        }
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.FORMAT_PENDING,
                tape.getState(),
                "Shoulda updated tape's state."
                 );
    }
    
    @Test
    public void testFormatTapeTwiceWhereSecondRequestUpgradesPriorityResultsInPriorityUpgrade()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.UNKNOWN );

        final TapeBlobStoreProcessor processor = extractProcessor( store );
        synchronized ( extractProcessor( store ).getTaskStateLock() )
        {
            assertEquals(
                    0,
                    processor.getTapeTasks().size(),
                    "Should notta been any tape tasks initially."
                    );
        }
        
        store.formatTape( BlobStoreTaskPriority.values()[ 1 ], tape.getId(), false, false);
        final TapeTask formatTask;
        synchronized ( extractProcessor( store ).getTaskStateLock() )
        {
            assertEquals(
                    1,
                    processor.getTapeTasks().size(),
                    "Shoulda been single tape task to format."
                     );
            formatTask = processor.getTapeTasks().get( tape.getId() ).get(0);
            assertNotNull(formatTask);
            assertEquals(
                    BlobStoreTaskPriority.values()[ 1 ],
                    formatTask.getPriority(),
                    "Shoulda set priority to format tape priority specified."
                    );
        }
        
        store.formatTape( BlobStoreTaskPriority.values()[ 2 ], tape.getId(), false, false);
        synchronized ( extractProcessor( store ).getTaskStateLock() )
        {
            assertEquals(
                    1,
                    processor.getTapeTasks().size(),
                    "Shoulda been single tape task to format."
                    );
            assertEquals(
                    BlobStoreTaskPriority.values()[ 2 ],
                    formatTask.getPriority(),
                    "Shoulda set priority to format tape priority specified."
                     );
        }
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.FORMAT_PENDING,
                tape.getState(),
                "Shoulda updated tape's state."
                 );
    }
    
    @Test
    public void testHappyConstruction()
    {
        new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
    }
    
    @Test
    public void testImportTapeDoesSo()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        Tape tape = mockDaoDriver.createTape( TapeState.FOREIGN );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                mockDaoDriver.attainOneAndOnly( TapePartition.class ).getId(),
                tape.getType() );
        mockDaoDriver.createDataPersistenceRule(dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );

        final ImportTapeDirective directive = mockDaoDriver.createImportTapeDirective(
                tape.getId(),
                user.getId(),
                dp.getId() );
        store.importTape(
                BlobStoreTaskPriority.values()[ 1 ],
                directive );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.IMPORT_PENDING,
                tape.getState(),
                "Shoulda updated tape's state."
                 );
    }
    
    @Test
    public void testImportTapeThatHasDataOnItAllowed()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        Tape tape = mockDaoDriver.createTape( TapeState.FOREIGN );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                mockDaoDriver.attainOneAndOnly( TapePartition.class ).getId(),
                tape.getType() );
        mockDaoDriver.createDataPersistenceRule(dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain(
                MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        dbSupport.getServiceManager()
                 .getService( TapeService.class )
                 .update(
                tape.setStorageDomainMemberId( sdm.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );

        final ImportTapeDirective directive = mockDaoDriver.createImportTapeDirective(
                tape.getId(),
                user.getId(),
                dp.getId() );
        store.importTape(
                BlobStoreTaskPriority.values()[ 1 ],
                directive );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.IMPORT_PENDING,
                tape.getState(),
                "Shoulda updated tape's state."
                 );
    }
    
    @Test
    public void testImportTapeThatIsInStateDisallowingTapeOperationsNotAllowed()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        Tape tape = mockDaoDriver.createTape( TapeState.LOST );

        final ImportTapeDirective directive = mockDaoDriver.createImportTapeDirective(
                tape.getId(),
                user.getId(),
                mockDaoDriver.createDataPolicy( "dp" ).getId() );
        TestUtil.assertThrows(
                null, GenericFailure.CONFLICT,
                () -> store.importTape( BlobStoreTaskPriority.values()[ 1 ], directive ) );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.LOST,
                tape.getState(),
                "Should notta updated tape's state."
                 );
    }
    
    @Test
    public void testImportTapeThatIsNotForeignNotAllowed()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );

        final ImportTapeDirective directive = mockDaoDriver.createImportTapeDirective(
                tape.getId(),
                user.getId(),
                mockDaoDriver.createDataPolicy( "dp" ).getId() );
        TestUtil.assertThrows(
                null, GenericFailure.CONFLICT,
                () -> store.importTape( BlobStoreTaskPriority.values()[ 1 ], directive ) );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.NORMAL,
                tape.getState(),
                "Should notta updated tape's state."
                 );
    }
    
    @Test
    public void testImportTapeWithoutApplicableABMConfigFails()
    {
        dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, "tp1" );
        final TapePartition partition2 = mockDaoDriver.createTapePartition( null, "tp2" );
        final TapePartition partition3 = mockDaoDriver.createTapePartition( null, "tp3" );
        final Tape tape = mockDaoDriver.createTape( partition1.getId(), TapeState.FOREIGN, TapeType.LTO5 );
        final Tape tape2 = mockDaoDriver.createTape( partition2.getId(), TapeState.FOREIGN, TapeType.LTO5 );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                partition1.getId(),
                TapeType.LTO6 );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                partition3.getId(),
                TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd2.getId(),
                partition1.getId(),
                TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );

        final ImportTapeDirective directive = mockDaoDriver.createImportTapeDirective(
                tape.getId(),
                user.getId(),
                dp.getId() );
        TestUtil.assertThrows(
                "Shoulda thrown exception since we don't have the needed storage domain member.",
                GenericFailure.CONFLICT, () -> store.importTape( BlobStoreTaskPriority.values()[ 1 ], directive ) );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                partition1.getId(),
                TapeType.LTO5 );
        store.importTape(
                BlobStoreTaskPriority.values()[ 1 ],
                directive );
                
        final ImportTapeDirective directive2 = mockDaoDriver.createImportTapeDirective(
                tape2.getId(),
                user.getId(),
                dp.getId() );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd2.getId(),
                partition2.getId(),
                TapeType.LTO5 );
        TestUtil.assertThrows(
                "Shoulda thrown exception since we don't have the needed persistence rule", GenericFailure.CONFLICT,
                () -> store.importTape( BlobStoreTaskPriority.values()[ 1 ], directive2 ) );
        mockDaoDriver.createDataPersistenceRule(dp.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        TestUtil.assertThrows(
                "Shoulda thrown exception since policy has multiple copies and IOM is enabled", GenericFailure.CONFLICT,
                () -> store.importTape( BlobStoreTaskPriority.values()[ 1 ], directive2 ) );
        mockDaoDriver.updateBean(
                mockDaoDriver.attainOneAndOnly(DataPathBackend.class).setIomEnabled(false),
                DataPathBackend.IOM_ENABLED);
        store.importTape(
                BlobStoreTaskPriority.values()[ 1 ],
                directive2 );
    }
    
    @Test
    public void testInspectTapeDoesSo()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        assertEquals(
                0,
                store.getTasks().size(),
                "Should notta scheduled inspection task yet."
                 );
        store.inspectTape( BlobStoreTaskPriority.values()[ 2 ], tape.getId() );
        assertEquals(
                1,
                store.getTasks().size(),
                "Shoulda scheduled inspection task."
                );
    }
    
    @Test
    public void testInspectTapeNotPhysicallyInSystemNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.LOST );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, DataPlannerException.class,
                () -> store.inspectTape( BlobStoreTaskPriority.values()[ 2 ], tape.getId() ) );
    }
    
    @Test
    public void testInspectTapeIncompatibleNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.INCOMPATIBLE );
        
        store =
                new TapeBlobStoreImpl( getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, DataPlannerException.class,
                () -> store.inspectTape( BlobStoreTaskPriority.values()[ 2 ], tape.getId() ) );
    }
    
    @Test
    public void testInspectTapeNullPriorityNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> store.inspectTape( null, tape.getId() ) );
    }
    
    @Test
    public void testInspectTapeNullTapeIdNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape( TapeState.NORMAL );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, IllegalArgumentException.class,
                () -> store.inspectTape( BlobStoreTaskPriority.values()[ 2 ], null ) );
    }
    
    @Test
    public void testOnlineTapeAlreadyPendingOnlineDoesNothing()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.ONLINE_PENDING );
        final UUID tapeId = tape.getId();

        store.onlineTape( tapeId );
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.ONLINE_PENDING,
                tape.getState(),
                "Should notta queued tape for onlining."
                );
    }
    
    @Test
    public void testOnlineTapeInProcessOfBeingOnlinedDoesNothing()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.ONLINE_IN_PROGRESS );
        final UUID tapeId = tape.getId();

        store.onlineTape( tapeId );
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.ONLINE_IN_PROGRESS,
                tape.getState(),
                "Should notta queued tape for onlining."
                 );
    }
    
    @Test
    public void testOnlineTapeNotInOfflineStateNotAllowed()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        final UUID tapeId = tape.getId();
    
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, () -> store.onlineTape( tapeId ) );
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.NORMAL,
                tape.getState(),
                "Should notta queued tape for onlining."
                 );
    }
    
    @Test
    public void testOnlineTapeUpdatesTapeForOnlinePending()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.OFFLINE );
        final UUID tapeId = tape.getId();

        store.onlineTape( tapeId );
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.ONLINE_PENDING,
                tape.getState(),
                "Shoulda queued tape for onlining."
                 );
    }
    
    @Test
    public void testRawImportOfNonWriteProtectedTapeFails()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.LTFS_WITH_FOREIGN_DATA );
        service.update( tape.setWriteProtected( false ), Tape.WRITE_PROTECTED );

        final RawImportTapeDirective directive = mockDaoDriver.createRawImportTapeDirective(
                tape.getId(),
                null );
        TestUtil.assertThrows( null, DataPlannerException.class,
                () -> store.importTape( BlobStoreTaskPriority.values()[ 1 ], directive ) );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.LTFS_WITH_FOREIGN_DATA,
                tape.getState(),
                "Should notta updated tape's state."
                 );
    }
    
    @Test
    public void testRawImportTapeDoesSo()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.LTFS_WITH_FOREIGN_DATA );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                mockDaoDriver.attainOneAndOnly( TapePartition.class ).getId(),
                tape.getType() );
        mockDaoDriver.createDataPersistenceRule(dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        service.update( tape.setWriteProtected( true ), Tape.WRITE_PROTECTED );

        final RawImportTapeDirective directive = mockDaoDriver.createRawImportTapeDirective(
                tape.getId(),
                null );
        store.importTape(
                BlobStoreTaskPriority.values()[ 1 ],
                directive );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.RAW_IMPORT_PENDING,
                tape.getState(),
                "Shoulda updated tape's state."
                 );
    }
    
    @Test
    public void testRawImportTapeThatHasDataOnItAllowed()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.LTFS_WITH_FOREIGN_DATA );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                mockDaoDriver.attainOneAndOnly( TapePartition.class ).getId(),
                tape.getType() );
        mockDaoDriver.createDataPersistenceRule(dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        service.update( tape.setWriteProtected( true ), Tape.WRITE_PROTECTED );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain(
                MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        dbSupport.getServiceManager()
                 .getService( TapeService.class )
                 .update(
                tape.setStorageDomainMemberId( sdm.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );

        final RawImportTapeDirective directive = mockDaoDriver.createRawImportTapeDirective(
                tape.getId(),
                null );
        store.importTape(
                BlobStoreTaskPriority.values()[ 1 ],
                directive );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.RAW_IMPORT_PENDING,
                tape.getState(),
                "Shoulda updated tape's state."
                 );
    }
    
    @Test
    public void testRawImportTapeThatIsInStateDisallowingTapeOperationsNotAllowed()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.LOST );

        final RawImportTapeDirective directive = mockDaoDriver.createRawImportTapeDirective(
                tape.getId(),
                null );
        TestUtil.assertThrows(
                null, GenericFailure.CONFLICT,
                () -> store.importTape( BlobStoreTaskPriority.values()[ 1 ], directive ) );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.LOST,
                tape.getState(),
                "Should notta updated tape's state."
                 );
    }
    
    @Test
    public void testRawImportTapeThatIsNotForeignNotAllowed()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );

        final RawImportTapeDirective directive = mockDaoDriver.createRawImportTapeDirective(
                tape.getId(),
                null );
        TestUtil.assertThrows(
                null, GenericFailure.CONFLICT,
                () -> store.importTape( BlobStoreTaskPriority.values()[ 1 ], directive ) );
        
        tape = service.attain( tape.getId() );
        assertEquals(
                TapeState.NORMAL,
                tape.getState(),
                "Should notta updated tape's state."
                );
    }
    
    @Test
    public void testVerifyCleaningTapeNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean( tape.setType( TapeType.LTO_CLEANING_TAPE ), Tape.TYPE );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, DataPlannerException.class,
                () -> store.verify( BlobStoreTaskPriority.values()[ 2 ], tape.getId() ) );
        assertNull(
                mockDaoDriver.attain( tape ).getVerifyPending(),
                "Should notta noted verify pending."
                 );
    }
    
    @Test
    public void testVerifyForeignTapeNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.FOREIGN );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean( tape.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, DataPlannerException.class,
                () -> store.verify( BlobStoreTaskPriority.values()[ 2 ], tape.getId() ) );
        assertNull(
                mockDaoDriver.attain( tape ).getVerifyPending(),
                "Should notta noted verify pending."
                 );
    }
    
    @Test
    public void testVerifyTapeDoesSo()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean( tape.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        
        assertEquals(
                0,
                store.getTasks().size(),
                "Should notta scheduled verify task yet."
                 );
        store.verify( BlobStoreTaskPriority.values()[ 2 ], tape.getId() );
        assertNotNull(
                mockDaoDriver.attain( tape ).getVerifyPending(),
                "Shoulda noted verify pending."
                 );
        assertEquals(
                0,
                store.getTasks().size(),
                "Should notta scheduled verify task."
                 );
    }
    
    @Test
    public void testVerifyTapeNotAssignedToStorageDomainNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape.setAssignedToStorageDomain( true ),
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
    
        TestUtil.assertThrows( null, DataPlannerException.class,
                () -> store.verify( BlobStoreTaskPriority.values()[ 2 ], tape.getId() ) );
        assertEquals(
                0,
                store.getTasks().size(),
                "Should notta scheduled verify task."
                );
        assertNull(
                mockDaoDriver.attain( tape ).getVerifyPending(),
                "Should notta noted verify pending."
                 );
    }
    
    @Test
    public void testVerifyTapeNotPhysicallyInSystemNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.LOST );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean( tape.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        store = new TapeBlobStoreImpl(
                getRpcClient(), new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, DataPlannerException.class,
                () -> store.verify( BlobStoreTaskPriority.values()[ 2 ], tape.getId() ) );
        assertNull(
                mockDaoDriver.attain( tape ).getVerifyPending(),
                "Should notta noted verify pending."
                 );
    }

    @Test
    public void testTaskCreation() throws Exception {
        RpcClient mockRpcClient = getNonNullRpcClient();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        TapePartition partition = mockDaoDriver.createTapePartition( null, null, TapeDriveType.LTO7 );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( DATA_POLICY_TAPE_DUAL_COPY_NAME );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                partition.getId(),
                tape.getType() );
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(),"bucket1" );

        DataPersistenceRule rule = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId());

        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );

        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain(
                MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        dbSupport.getServiceManager()
                .getService( TapeService.class )
                .update(
                        tape.setStorageDomainMemberId( sdm.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final Job job2 = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.updateBean(job.setPriority(BlobStoreTaskPriority.CRITICAL), Job.PRIORITY);
        mockDaoDriver.updateBean(job2.setPriority(BlobStoreTaskPriority.CRITICAL), Job.PRIORITY);
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job2.getId(), b2 );
        mockDaoDriver.markBlobInCache(b1.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());
        mockDaoDriver.markBlobInCache(b2.getId());
        mockDiskManager.blobLoadedToCache(b2.getId());
        List<JobEntry> entries = Arrays.asList(entry1, entry2);
        mockDaoDriver.createLocalBlobDestinations(entries, Arrays.asList(rule), bucket.getId() );
        TapeDrive drive1 = mockDaoDriver.createTapeDrive(partition.getId(), "tdsn1");
        TapeDrive drive2 =mockDaoDriver.createTapeDrive( partition.getId(), "tdsn2" );
        mockDaoDriver.updateBean(drive1.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        mockDaoDriver.updateBean(drive2.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        Set<UUID> uuidSet = new HashSet<>();
        uuidSet.add(drive1.getId());
        uuidSet.add(drive2.getId());
        TapeBlobStoreImpl tapeBlobStore = new TapeBlobStoreImpl(
                mockRpcClient,
                new MockDiskManager(dbSupport.getServiceManager()),
                InterfaceProxyFactory.getProxy(JobProgressManager.class, null),
                dbSupport.getServiceManager());
        TapeBlobStoreImpl tapeBlobStoreSpy = Mockito.spy(tapeBlobStore);
        TapeEnvironment mockTapeEnvironment = Mockito.mock(TapeEnvironment.class);


        Mockito.when(mockTapeEnvironment.ensurePhysicalTapeEnvironmentUpToDate(Mockito.any()))
                .thenReturn(true);
        Mockito.when(mockTapeEnvironment.getTapesInPartition(Mockito.any()))
                .thenReturn(Collections.singleton(tape.getId()));

        try {
            Field tapeEnvField = TapeBlobStoreImpl.class.getDeclaredField("m_tapeEnvironment");
            tapeEnvField.setAccessible(true);

            Field processorField = TapeBlobStoreImpl.class.getDeclaredField("m_processor");
            processorField.setAccessible(true);
            TapeBlobStoreProcessor processor = (TapeBlobStoreProcessor) processorField.get(tapeBlobStoreSpy);

            Field tapeLockSupportField = TapeBlobStoreProcessorImpl.class.getDeclaredField("m_tapeLockSupport");
            tapeLockSupportField.setAccessible(true);

            TapeLockSupport<Object> mockTapeLockSupport = Mockito.mock(TapeLockSupport.class);
            Mockito.when(mockTapeLockSupport.getAvailableTapeDrives())
                    .thenReturn(uuidSet);

            tapeLockSupportField.set(processor, mockTapeLockSupport);

            tapeEnvField.set(tapeBlobStoreSpy, mockTapeEnvironment);

            TapeLockSupport<Object> verifySupport = (TapeLockSupport<Object>) tapeLockSupportField.get(processor);

            if (processor != null) {
                Field processorTapeEnvField = processor.getClass().getDeclaredField("m_tapeEnvironment");
                processorTapeEnvField.setAccessible(true);
                processorTapeEnvField.set(processor, mockTapeEnvironment);

            }

        } catch (Exception e) {
            System.err.println("Field replacement failed: " + e.getMessage());
            e.printStackTrace();
        }

        PowerMockito.field(TapeBlobStoreImpl.class, "m_tapeEnvironment").set(tapeBlobStoreSpy, mockTapeEnvironment);

        tapeBlobStoreSpy
                .taskSchedulingRequired();
        long deadline = System.currentTimeMillis() + 5_000;
        while (tapeBlobStoreSpy.getTasks().size() < 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(1, tapeBlobStoreSpy.getTasks().size());
    }

    @Test
    public void testMultipleTaskCreation_JobType() throws Exception {
        RpcClient mockRpcClient = getNonNullRpcClient();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        TapePartition partition = mockDaoDriver.createTapePartition( null, null, TapeDriveType.LTO7 );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( DATA_POLICY_TAPE_DUAL_COPY_NAME );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                partition.getId(),
                tape.getType() );
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(),"bucket1" );

        DataPersistenceRule rule = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId());

        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );

        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain(
                MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        dbSupport.getServiceManager()
                .getService( TapeService.class )
                .update(
                        tape.setStorageDomainMemberId( sdm.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final Job job2 = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.GET );
        mockDaoDriver.updateBean(job.setPriority(BlobStoreTaskPriority.CRITICAL), Job.PRIORITY);
        mockDaoDriver.updateBean(job2.setPriority(BlobStoreTaskPriority.CRITICAL), Job.PRIORITY);
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job2.getId(), b2 );
        mockDaoDriver.markBlobInCache(b1.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());
        mockDaoDriver.markBlobInCache(b2.getId());
        mockDaoDriver.updateBean( entry2.setReadFromTapeId(tape.getId()), JobEntry.READ_FROM_TAPE_ID);
        List<JobEntry> entries = Arrays.asList(entry1, entry2);
        mockDaoDriver.createLocalBlobDestinations(entries, Arrays.asList(rule), bucket.getId() );
        TapeDrive drive1 = mockDaoDriver.createTapeDrive(partition.getId(), "tdsn1");
        TapeDrive drive2 =mockDaoDriver.createTapeDrive( partition.getId(), "tdsn2" );
        mockDaoDriver.updateBean(drive1.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        mockDaoDriver.updateBean(drive2.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        Set<UUID> uuidSet = new HashSet<>();
        uuidSet.add(drive1.getId());
        uuidSet.add(drive2.getId());
        TapeBlobStoreImpl tapeBlobStore = new TapeBlobStoreImpl(
                mockRpcClient,
                new MockDiskManager(dbSupport.getServiceManager()),
                InterfaceProxyFactory.getProxy(JobProgressManager.class, null),
                dbSupport.getServiceManager());
        TapeBlobStoreImpl tapeBlobStoreSpy = Mockito.spy(tapeBlobStore);
        TapeEnvironment mockTapeEnvironment = Mockito.mock(TapeEnvironment.class);


        Mockito.when(mockTapeEnvironment.ensurePhysicalTapeEnvironmentUpToDate(Mockito.any()))
                .thenReturn(true);
        Mockito.when(mockTapeEnvironment.getTapesInPartition(Mockito.any()))
                .thenReturn(Collections.singleton(tape.getId()));

        try {
            Field tapeEnvField = TapeBlobStoreImpl.class.getDeclaredField("m_tapeEnvironment");
            tapeEnvField.setAccessible(true);

            Field processorField = TapeBlobStoreImpl.class.getDeclaredField("m_processor");
            processorField.setAccessible(true);
            TapeBlobStoreProcessor processor = (TapeBlobStoreProcessor) processorField.get(tapeBlobStoreSpy);

            Field tapeLockSupportField = TapeBlobStoreProcessorImpl.class.getDeclaredField("m_tapeLockSupport");
            tapeLockSupportField.setAccessible(true);

            TapeLockSupport<Object> mockTapeLockSupport = Mockito.mock(TapeLockSupport.class);
            Mockito.when(mockTapeLockSupport.getAvailableTapeDrives())
                    .thenReturn(uuidSet);

            tapeLockSupportField.set(processor, mockTapeLockSupport);

            tapeEnvField.set(tapeBlobStoreSpy, mockTapeEnvironment);

            TapeLockSupport<Object> verifySupport = (TapeLockSupport<Object>) tapeLockSupportField.get(processor);

            if (processor != null) {
                Field processorTapeEnvField = processor.getClass().getDeclaredField("m_tapeEnvironment");
                processorTapeEnvField.setAccessible(true);
                processorTapeEnvField.set(processor, mockTapeEnvironment);

            }


        } catch (Exception e) {
            System.err.println("Field replacement failed: " + e.getMessage());
            e.printStackTrace();
        }

        PowerMockito.field(TapeBlobStoreImpl.class, "m_tapeEnvironment").set(tapeBlobStoreSpy, mockTapeEnvironment);

        tapeBlobStoreSpy
                .taskSchedulingRequired();
        TestUtil.waitUpTo(10, TimeUnit.SECONDS, () -> {
            assertEquals(2,  tapeBlobStoreSpy
                    .getTasks().size(), "Should create task.");
        });
    }

    @Test
    public void testMultipleTaskCreation_StorageDomain() throws Exception {
        RpcClient mockRpcClient = getNonNullRpcClient();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        TapePartition partition = mockDaoDriver.createTapePartition( null, null, TapeDriveType.LTO7 );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( DATA_POLICY_TAPE_DUAL_COPY_NAME );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                partition.getId(),
                tape.getType() );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd2.getId(),
                partition.getId(),
                tape2.getType() );
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(),"bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dp.getId(),"bucket2" );


        DataPersistenceRule rule = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId());

        DataPersistenceRule rule2 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId());

        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );


        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final Job job2 = mockDaoDriver.createJob( bucket2.getId(), null, JobRequestType.PUT );
        mockDaoDriver.updateBean(job.setPriority(BlobStoreTaskPriority.CRITICAL), Job.PRIORITY);
        mockDaoDriver.updateBean(job2.setPriority(BlobStoreTaskPriority.CRITICAL), Job.PRIORITY);

        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job2.getId(), b2 );
        mockDaoDriver.markBlobInCache(b1.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());
        mockDaoDriver.markBlobInCache(b2.getId());
        mockDiskManager.blobLoadedToCache(b2.getId());

        mockDaoDriver.createLocalBlobDestinations(Arrays.asList(entry1), Arrays.asList(rule), bucket.getId() );
        mockDaoDriver.createLocalBlobDestinations(Arrays.asList(entry2), Arrays.asList(rule2), bucket.getId() );

        TapeDrive drive1 = mockDaoDriver.createTapeDrive(partition.getId(), "tdsn1");
        mockDaoDriver.updateBean(drive1.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        mockDaoDriver.updateBean(drive1.setTapeId(tape2.getId()), TapeDrive.TAPE_ID);
        Set<UUID> uuidSet = new HashSet<>();
        uuidSet.add(drive1.getId());
        TapeBlobStoreImpl tapeBlobStore = new TapeBlobStoreImpl(
                mockRpcClient,
                new MockDiskManager(dbSupport.getServiceManager()),
                InterfaceProxyFactory.getProxy(JobProgressManager.class, null),
                dbSupport.getServiceManager());
        TapeBlobStoreImpl tapeBlobStoreSpy = Mockito.spy(tapeBlobStore);
        TapeEnvironment mockTapeEnvironment = Mockito.mock(TapeEnvironment.class);


        Mockito.when(mockTapeEnvironment.ensurePhysicalTapeEnvironmentUpToDate(Mockito.any()))
                .thenReturn(true);
        Mockito.when(mockTapeEnvironment.getTapesInPartition(Mockito.any()))
                .thenReturn(Collections.singleton(tape.getId()));

        try {
            Field tapeEnvField = TapeBlobStoreImpl.class.getDeclaredField("m_tapeEnvironment");
            tapeEnvField.setAccessible(true);

            Field processorField = TapeBlobStoreImpl.class.getDeclaredField("m_processor");
            processorField.setAccessible(true);
            TapeBlobStoreProcessor processor = (TapeBlobStoreProcessor) processorField.get(tapeBlobStoreSpy);

            Field tapeLockSupportField = TapeBlobStoreProcessorImpl.class.getDeclaredField("m_tapeLockSupport");
            tapeLockSupportField.setAccessible(true);

            TapeLockSupport<Object> mockTapeLockSupport = Mockito.mock(TapeLockSupport.class);
            Mockito.when(mockTapeLockSupport.getAvailableTapeDrives())
                    .thenReturn(uuidSet);

            tapeLockSupportField.set(processor, mockTapeLockSupport);

            tapeEnvField.set(tapeBlobStoreSpy, mockTapeEnvironment);

            TapeLockSupport<Object> verifySupport = (TapeLockSupport<Object>) tapeLockSupportField.get(processor);

            if (processor != null) {
                Field processorTapeEnvField = processor.getClass().getDeclaredField("m_tapeEnvironment");
                processorTapeEnvField.setAccessible(true);
                processorTapeEnvField.set(processor, mockTapeEnvironment);

            }


        } catch (Exception e) {
            System.err.println("Field replacement failed: " + e.getMessage());
            e.printStackTrace();
        }

        PowerMockito.field(TapeBlobStoreImpl.class, "m_tapeEnvironment").set(tapeBlobStoreSpy, mockTapeEnvironment);

        tapeBlobStoreSpy
                .taskSchedulingRequired();
        long deadline = System.currentTimeMillis() + 5_000;
        while (tapeBlobStoreSpy.getTasks().size() < 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(2, tapeBlobStoreSpy.getTasks().size());
    }

    @Test
    public void testMultipleTaskCreation_SingleJob() throws Exception {
        RpcClient mockRpcClient = getNonNullRpcClient();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        TapePartition partition = mockDaoDriver.createTapePartition( null, null, TapeDriveType.LTO7 );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( DATA_POLICY_TAPE_DUAL_COPY_NAME );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                partition.getId(),
                tape.getType() );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd2.getId(),
                partition.getId(),
                tape2.getType() );
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(),"bucket1" );


        DataPersistenceRule rule = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId());

        DataPersistenceRule rule2 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId());

        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );

        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );

        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        mockDaoDriver.markBlobInCache(b1.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());

        mockDaoDriver.createLocalBlobDestinations(Arrays.asList(entry1), Arrays.asList(rule), bucket.getId() );
        mockDaoDriver.createLocalBlobDestinations(Arrays.asList(entry1), Arrays.asList(rule2), bucket.getId() );

        TapeDrive drive1 = mockDaoDriver.createTapeDrive(partition.getId(), "tdsn1");
        mockDaoDriver.updateBean(drive1.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        mockDaoDriver.updateBean(drive1.setTapeId(tape2.getId()), TapeDrive.TAPE_ID);
        Set<UUID> uuidSet = new HashSet<>();
        uuidSet.add(drive1.getId());
        TapeBlobStoreImpl tapeBlobStore = new TapeBlobStoreImpl(
                mockRpcClient,
                new MockDiskManager(dbSupport.getServiceManager()),
                InterfaceProxyFactory.getProxy(JobProgressManager.class, null),
                dbSupport.getServiceManager());
        TapeBlobStoreImpl tapeBlobStoreSpy = Mockito.spy(tapeBlobStore);
        TapeEnvironment mockTapeEnvironment = Mockito.mock(TapeEnvironment.class);


        Mockito.when(mockTapeEnvironment.ensurePhysicalTapeEnvironmentUpToDate(Mockito.any()))
                .thenReturn(true);
        Mockito.when(mockTapeEnvironment.getTapesInPartition(Mockito.any()))
                .thenReturn(Collections.singleton(tape.getId()));

        try {
            Field tapeEnvField = TapeBlobStoreImpl.class.getDeclaredField("m_tapeEnvironment");
            tapeEnvField.setAccessible(true);

            Field processorField = TapeBlobStoreImpl.class.getDeclaredField("m_processor");
            processorField.setAccessible(true);
            TapeBlobStoreProcessor processor = (TapeBlobStoreProcessor) processorField.get(tapeBlobStoreSpy);

            Field tapeLockSupportField = TapeBlobStoreProcessorImpl.class.getDeclaredField("m_tapeLockSupport");
            tapeLockSupportField.setAccessible(true);

            TapeLockSupport<Object> mockTapeLockSupport = Mockito.mock(TapeLockSupport.class);
            Mockito.when(mockTapeLockSupport.getAvailableTapeDrives())
                    .thenReturn(uuidSet);

            tapeLockSupportField.set(processor, mockTapeLockSupport);

            tapeEnvField.set(tapeBlobStoreSpy, mockTapeEnvironment);


            if (processor != null) {
                Field processorTapeEnvField = processor.getClass().getDeclaredField("m_tapeEnvironment");
                processorTapeEnvField.setAccessible(true);
                processorTapeEnvField.set(processor, mockTapeEnvironment);

            }


        } catch (Exception e) {
            System.err.println("Field replacement failed: " + e.getMessage());
            e.printStackTrace();
        }

        PowerMockito.field(TapeBlobStoreImpl.class, "m_tapeEnvironment").set(tapeBlobStoreSpy, mockTapeEnvironment);

        tapeBlobStoreSpy
                .taskSchedulingRequired();
        long deadline = System.currentTimeMillis() + 5_000;
        while (tapeBlobStoreSpy.getTasks().size() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(2, tapeBlobStoreSpy.getTasks().size());

    }
    
    private TapeBlobStoreProcessor extractProcessor( final TapeBlobStore store )
    {
        try
        {
            final Field field = 
                    TapeBlobStoreImpl.class.getDeclaredField( TapeBlobStoreImpl.FIELD_PROCESSOR );
            field.setAccessible( true );
            return (TapeBlobStoreProcessor)field.get( store );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }
    private static  DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    TapeBlobStore store;


    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    }

    @AfterEach
    public  void reset_DB() {
        dbSupport.reset();
        if (store != null)
            store.shutdown();
    }

}

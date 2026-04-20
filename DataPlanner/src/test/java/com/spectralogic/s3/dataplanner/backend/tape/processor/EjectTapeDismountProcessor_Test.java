/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironment;


import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.tape.ElementAddressType;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.tape.TapeDriveService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeBlobStoreProcessor;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeMoveListener;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.MockBeansServiceManager;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class EjectTapeDismountProcessor_Test
{
    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> new EjectTapeDismountProcessor( null,
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, null ), InterfaceProxyFactory.getProxy( TapeEnvironment.class, null ), 1,
                new EjectTapeProcessor( new MockBeansServiceManager(),
                        InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, null ), InterfaceProxyFactory.getProxy( TapeEnvironment.class, null ), 1 ) ) );
    }
    
    @Test
    public void testConstructorNullEjectTapeProcessorNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> new EjectTapeDismountProcessor( null,
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, null ), InterfaceProxyFactory.getProxy( TapeEnvironment.class, null ), 1, null ) );
    }
    
    @Test
    public void testConstructorNullBlobStoreProcessorNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class,
                () -> new EjectTapeDismountProcessor( new MockBeansServiceManager(), null, null, 1,
                        new EjectTapeProcessor( new MockBeansServiceManager(),
                                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, null ), InterfaceProxyFactory.getProxy( TapeEnvironment.class, null ), 1 ) ) );
    }
    
    @Test
    public void testHappyConstruction()
    {
        final EjectTapeDismountProcessor ejector = new EjectTapeDismountProcessor( new MockBeansServiceManager(),
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, null ), InterfaceProxyFactory.getProxy( TapeEnvironment.class, null ), 1,
                new EjectTapeProcessor( new MockBeansServiceManager(),
                        InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, null ), InterfaceProxyFactory.getProxy( TapeEnvironment.class, null ), 1 ) );
        ejector.shutdown();
    }
    
    @Test
    public void testTapeEjectNormalSuccessPathSingleTape()
    {
        final TapeService tapeService = getTapeService( dbSupport );
        final TapeDriveService tapeDriveService = getTapeDriveService( dbSupport );
        final MockDaoDriver mockDaoDriver = getMockDaoDriver( dbSupport );
        
        final Map< UUID, TapeAndDriveWrapper > tapes = new HashMap<>();
        final TapeAndDriveWrapper tape = new TapeAndDriveWrapper( mockDaoDriver );
        tapes.put( tape.getTape()
                       .getId(), tape );
        
        final Map< UUID, TapeMoveListener > moveListeners = new ConcurrentHashMap<>();
        final BasicTestsInvocationHandler btih =
                getBasicTestsInvocationHandler( tapeDriveService, tapes, 1, moveListeners );
        
        final EjectTapeProcessor ejectTapeToBeScheduled = new EjectTapeProcessor( dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, btih ), InterfaceProxyFactory.getProxy( TapeEnvironment.class, btih ), 1 );
        final EjectTapeDismountProcessor ejector = new EjectTapeDismountProcessor( dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, btih ), InterfaceProxyFactory.getProxy( TapeEnvironment.class, btih ), 10, ejectTapeToBeScheduled );
        
        tapeService.update( tape.getTape()
                                .setEjectPending( new Date( 1000 ) ), Tape.EJECT_PENDING );
        assertEquals(
                0,
                dbSupport.getServiceManager()
                        .getRetriever(
                                TapePartitionFailure.class )
                        .getCount(),
                "Should notta been any notifications yet."  );
        ejector.schedule();
        TestUtil.sleep( 150 );
        
        checkTapeState( dbSupport, tape.getTape(), TapeState.NORMAL,
                "Should notta changed tape's state from normal prior to validation success." );
        validationCompleted( tape.getTape()
                                 .getId(), moveListeners, tape.getTape()
                                                              .getId(), null, 2000 );
        moveListeners.get( tape.getTape()
                               .getId() )
                     .moveSucceeded( tape.getTape()
                                         .getId() );
        
        checkTapeState( dbSupport, tape.getTape(), TapeState.EJECT_TO_EE_IN_PROGRESS,
                "Shoulda upgraded tape's state to pending eject from EE after move succeeded." );
        
        TestUtil.sleep( 150 );
        checkTapeState( dbSupport, tape.getTape(), TapeState.EJECT_TO_EE_IN_PROGRESS,
                "Should notta upgraded tape's state from eject in progress prior to validation success." );
        validationCompleted( tape.getTape()
                                 .getId(), moveListeners, tape.getTape()
                                                              .getId(), null, 2000 );
        moveListeners.get( tape.getTape()
                               .getId() )
                     .moveSucceeded( tape.getTape()
                                         .getId() );
        
        assertEquals(
                TapePartitionFailureType.TAPE_EJECTION_BY_OPERATOR_REQUIRED, dbSupport.getServiceManager()
                        .getRetriever(
                                TapePartitionFailure
                                        .class )
                        .attain( Require.nothing() )
                        .getType(),
                "Shoulda been a notification generated."
                 );
        
        checkTapeState( dbSupport, tape.getTape(), TapeState.EJECT_FROM_EE_PENDING,
                "Shoulda upgraded tape's state to pending " + "eject from EE." );
        
        ejector.shutdown();
        ejectTapeToBeScheduled.shutdown();
    }
    
    
    private BasicTestsInvocationHandler getBasicTestsInvocationHandler( final TapeDriveService tapeDriveService,
            final Map< UUID, TapeAndDriveWrapper > tapes, final int numberOfSlots,
            final Map< UUID, TapeMoveListener > moveListeners )
    {
        final AtomicInteger numAvailableImportExportSlots = new AtomicInteger( numberOfSlots );
        final Method methodIsSlotAvailable = ReflectUtil.getMethod( TapeEnvironment.class, "isSlotAvailable" );
        final Method methodGetTaskStateLock = ReflectUtil.getMethod( TapeBlobStoreProcessor.class, "getTaskStateLock" );
        final Method methodMoveTapeToSlot = ReflectUtil.getMethod( TapeEnvironment.class, "moveTapeToSlot" );
        
        return new BasicTestsInvocationHandler( MockInvocationHandler.forMethod( methodGetTaskStateLock,
                new ConstantResponseInvocationHandler( new Object() ),
                MockInvocationHandler.forMethod( methodMoveTapeToSlot, ( proxy, method, args ) -> {
                    final UUID tapeUUID = ( UUID ) args[ 0 ];
                    switch ( ( ElementAddressType ) args[ 1 ] )
                    {
                        case STORAGE:
                            if ( !tapes.get( tapeUUID )
                                       .getMovedToSlot()
                                       .get() )
                            {
                                tapeDriveService.update( tapes.get( tapeUUID )
                                                              .getDrive()
                                                              .setTapeId( null ), TapeDrive.TAPE_ID );
                                tapes.get( tapeUUID )
                                     .getMovedToSlot()
                                     .set( true );
                            }
                            else
                            {
                                throw new RuntimeException( "Should notta moved tape to a storage slot twice." );
                            }
                            break;
                        case IMPORT_EXPORT:
                            if ( !tapes.get( tapeUUID )
                                       .getMovedToSlot()
                                       .get() )
                            {
                                throw new RuntimeException( "Shoulda moved tape to an storage slot first." );
                            }
                            if ( 0 == numAvailableImportExportSlots.get() )
                            {
                                throw new RuntimeException( "No slot available." );
                            }
                            numAvailableImportExportSlots.decrementAndGet();
                            break;
                    }
                    moveListeners.put( tapeUUID, ( TapeMoveListener ) args[ 2 ] );
                    return Boolean.TRUE;
                }, MockInvocationHandler.forMethod( methodIsSlotAvailable,
                        new ConstantResponseInvocationHandler( Boolean.FALSE ), null ) ) ) );
    }
    
    
    private MockDaoDriver getMockDaoDriver( final DatabaseSupport dbSupport )
    {
        return new MockDaoDriver( dbSupport );
    }
    
    
    private TapeService getTapeService( final DatabaseSupport dbSupport )
    {
        return dbSupport.getServiceManager()
                        .getService( TapeService.class );
    }
    
    @Test
    public void testTapeEjectDriveDrive()
    {
        final TapeService tapeService = getTapeService( dbSupport );
        final TapeDriveService tapeDriveService = getTapeDriveService( dbSupport );
        final MockDaoDriver mockDaoDriver = getMockDaoDriver( dbSupport );
        
        final Map< UUID, TapeAndDriveWrapper > tapes = new HashMap<>();
        final TapeAndDriveWrapper tape1 = new TapeAndDriveWrapper( mockDaoDriver );
        tapes.put( tape1.getTape()
                        .getId(), tape1 );
        final TapeAndDriveWrapper tape2 = new TapeAndDriveWrapper( mockDaoDriver );
        tapes.put( tape2.getTape()
                        .getId(), tape2 );
        
        final Map< UUID, TapeMoveListener > moveListeners = new ConcurrentHashMap<>();
        final Method methodMoveTape = ReflectUtil.getMethod( TapeEnvironment.class, "moveTapeToSlot" );
        final BasicTestsInvocationHandler btih =
                getGetTaskStateLock( tapeDriveService, tapes, moveListeners, methodMoveTape );
        
        final EjectTapeProcessor ejectTapeToBeScheduled = new EjectTapeProcessor( dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, btih ), InterfaceProxyFactory.getProxy( TapeEnvironment.class, btih ), 10 );
        final EjectTapeDismountProcessor ejector = new EjectTapeDismountProcessor( dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, btih ), InterfaceProxyFactory.getProxy( TapeEnvironment.class, btih ), 20, ejectTapeToBeScheduled );
        
        tapeService.update( tape1.getTape()
                                 .setEjectPending( new Date( 1000 ) ), Tape.EJECT_PENDING );
        tapeService.update( tape2.getTape()
                                 .setEjectPending( new Date( 10000 ) ), Tape.EJECT_PENDING );
        
        ejector.schedule();
        TestUtil.sleep( 600 );
        
        assertEquals(
                4,
                btih.getMethodCallCount( methodMoveTape ),
                "Shoulda processed all 2 tapes." );
        assertEquals(
                tape1.getTape()
                        .getId(),
                btih.getMethodInvokeData( methodMoveTape )
                        .get( 0 )
                        .getArgs()
                        .get( 0 ),
                "Shoulda processed tape1 dismount in the order eject was requested in." );
        
        checkTapeState( dbSupport, tape1.getTape(), TapeState.EJECT_FROM_EE_PENDING,
                "Shoulda changed tape1's state to pending eject from EE after move succeeded." );
        checkTapeState( dbSupport, tape2.getTape(), TapeState.EJECT_FROM_EE_PENDING,
                "Shoulda changed tape2's state to pending eject from EE after move succeeded." );
        
        ejector.shutdown();
        ejectTapeToBeScheduled.shutdown();
    }
    
    @Test
    public void testTapeEjectDriveStorage()
    {
        final DatabaseSupport dbSupport = getDatabaseSupport();
        final TapeService tapeService = getTapeService( dbSupport );
        final TapeDriveService tapeDriveService = getTapeDriveService( dbSupport );
        final MockDaoDriver mockDaoDriver = getMockDaoDriver( dbSupport );
        
        final Map< UUID, TapeAndDriveWrapper > tapes = new HashMap<>();
        final TapeAndDriveWrapper tape1 = new TapeAndDriveWrapper( mockDaoDriver );
        tapes.put( tape1.getTape()
                        .getId(), tape1 );
        final Tape tape2 = mockDaoDriver.createTape();
        
        final Map< UUID, TapeMoveListener > moveListeners = new ConcurrentHashMap<>();
        final Method methodMoveTape = ReflectUtil.getMethod( TapeEnvironment.class, "moveTapeToSlot" );
        final BasicTestsInvocationHandler btih =
                getGetTaskStateLock( tapeDriveService, tapes, moveListeners, methodMoveTape );
        
        final EjectTapeProcessor ejectTapeToBeScheduled = new EjectTapeProcessor( dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, btih ), InterfaceProxyFactory.getProxy( TapeEnvironment.class, btih ), 100 );
        final EjectTapeDismountProcessor ejector = new EjectTapeDismountProcessor( dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, btih ), InterfaceProxyFactory.getProxy( TapeEnvironment.class, btih ), 10, ejectTapeToBeScheduled );
        
        tapeService.update( tape1.getTape()
                                 .setEjectPending( new Date( 1000 ) ), Tape.EJECT_PENDING );
        tapeService.update( tape2.setEjectPending( new Date( 10000 ) ), Tape.EJECT_PENDING );
        
        ejector.schedule();
        TestUtil.sleep( 500 );
        
        assertEquals( 3,
                btih.getMethodCallCount( methodMoveTape ) ,
                "Shoulda processed all 2 tapes.");
        
        checkTapeState( dbSupport, tape1.getTape(), TapeState.EJECT_FROM_EE_PENDING,
                "Should notta changed tape1's state to pending eject from EE after move succeeded." );
        checkTapeState( dbSupport, tape2, TapeState.EJECT_FROM_EE_PENDING,
                "Shoulda upgraded tape2's state to pending eject from EE after move succeeded." );
        
        ejector.shutdown();
        ejectTapeToBeScheduled.shutdown();
    }
    
    
    private BasicTestsInvocationHandler getGetTaskStateLock( final TapeDriveService tapeDriveService,
            final Map< UUID, TapeAndDriveWrapper > tapes, final Map< UUID, TapeMoveListener > moveListeners,
            final Method methodMoveTape )
    {
        return new BasicTestsInvocationHandler( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( TapeBlobStoreProcessor.class, "getTaskStateLock" ),
                new ConstantResponseInvocationHandler( new Object() ),
                MockInvocationHandler.forMethod( methodMoveTape, ( proxy, method, args ) -> {
                    final UUID tapeUUID = ( UUID ) args[ 0 ];
                    moveListeners.put( tapeUUID, ( TapeMoveListener ) args[ 2 ] );
                    ( ( TapeMoveListener ) args[ 2 ] ).validationCompleted( tapeUUID, null );
                    SystemWorkPool.getInstance()
                                  .submit( () -> ( ( TapeMoveListener ) args[ 2 ] ).moveSucceeded( tapeUUID ) );
                    if ( null != tapes.get( tapeUUID ) )
                    {
                        tapeDriveService.update( tapes.get( tapeUUID )
                                                      .getDrive()
                                                      .setTapeId( null ), TapeDrive.TAPE_ID );
                    }
                    return Boolean.TRUE;
                }, null ) ) );
    }
    
    @Test
    public void testTapeEjectNormalSuccessPathDoubleTape()
    {
        final TapeService tapeService = getTapeService( dbSupport );
        final TapeDriveService tapeDriveService = getTapeDriveService( dbSupport );
        final MockDaoDriver mockDaoDriver = getMockDaoDriver( dbSupport );
        
        final Map< UUID, TapeAndDriveWrapper > tapes = new HashMap<>();
        final TapeAndDriveWrapper tape1 = new TapeAndDriveWrapper( mockDaoDriver );
        tapes.put( tape1.getTape()
                        .getId(), tape1 );
        final TapeAndDriveWrapper tape2 = new TapeAndDriveWrapper( mockDaoDriver );
        tapes.put( tape2.getTape()
                        .getId(), tape2 );
        
        final Map< UUID, TapeMoveListener > moveListeners = new ConcurrentHashMap<>();
        final BasicTestsInvocationHandler btih =
                getBasicTestsInvocationHandler( tapeDriveService, tapes, 1, moveListeners );
        
        final EjectTapeProcessor ejectTapeToBeScheduled = new EjectTapeProcessor( dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, btih ), InterfaceProxyFactory.getProxy( TapeEnvironment.class, btih ), 1 );
        final EjectTapeDismountProcessor ejector = new EjectTapeDismountProcessor( dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, btih ), InterfaceProxyFactory.getProxy( TapeEnvironment.class, btih ), 10, ejectTapeToBeScheduled );
        
        tapeService.update( tape1.getTape()
                                 .setEjectPending( new Date( 1000 ) ), Tape.EJECT_PENDING );
        tapeService.update( tape2.getTape()
                                 .setEjectPending( new Date( 10000 ) ), Tape.EJECT_PENDING );
        assertEquals( 0,
                dbSupport.getServiceManager()
                .getRetriever(
                        TapePartitionFailure.class )
                .getCount(),
                "Should notta been any notifications yet." );
        ejector.schedule();
        TestUtil.sleep( 150 );
        
        checkTapeState( dbSupport, tape1.getTape(), TapeState.NORMAL,
                "Should notta changed tape1's state from normal prior to validation success." );
        checkTapeState( dbSupport, tape2.getTape(), TapeState.NORMAL,
                "Should notta changed tape2's state from normal prior to validation success." );
        
        validationCompleted( tape1.getTape()
                                  .getId(), moveListeners, tape1.getTape()
                                                                .getId(), null, 2000 );
        moveListeners.get( tape1.getTape()
                                .getId() )
                     .moveSucceeded( tape1.getTape()
                                          .getId() );
        
        checkTapeState( dbSupport, tape1.getTape(), TapeState.EJECT_TO_EE_IN_PROGRESS,
                "Should notta changed tape1's state from normal prior to validation success." );
        checkTapeState( dbSupport, tape2.getTape(), TapeState.NORMAL,
                "Shoulda upgraded tape2's state to pending eject from EE after move succeeded." );
        
        TestUtil.sleep( 150 );
        
        validationCompleted( tape2.getTape()
                                  .getId(), moveListeners, tape2.getTape()
                                                                .getId(), null, 2000 );
        moveListeners.get( tape2.getTape()
                                .getId() )
                     .moveSucceeded( tape1.getTape()
                                          .getId() );
        
        checkTapeState( dbSupport, tape1.getTape(), TapeState.EJECT_TO_EE_IN_PROGRESS,
                "Should notta changed tape1's state from normal prior to validation success." );
        checkTapeState( dbSupport, tape2.getTape(), TapeState.EJECT_TO_EE_IN_PROGRESS,
                "Shoulda upgraded tape2's state to pending eject from EE after move succeeded." );
        
        validationCompleted( tape1.getTape()
                                  .getId(), moveListeners, tape1.getTape()
                                                                .getId(), null, 2000 );
        moveListeners.get( tape1.getTape()
                                .getId() )
                     .moveSucceeded( tape1.getTape()
                                          .getId() );
        
        assertEquals(
                TapePartitionFailureType.TAPE_EJECTION_BY_OPERATOR_REQUIRED, dbSupport.getServiceManager()
                        .getRetriever(
                                TapePartitionFailure
                                        .class )
                        .attain( Require.nothing() )
                        .getType(),
                "Shoulda been a notification generated."
                 );
        
        checkTapeState( dbSupport, tape1.getTape(), TapeState.EJECT_FROM_EE_PENDING,
                "Should notta changed tape1's state from normal prior to validation success." );
        checkTapeState( dbSupport, tape2.getTape(), TapeState.EJECT_TO_EE_IN_PROGRESS,
                "Shoulda upgraded tape2's state to pending eject from EE after move succeeded." );
        
        ejector.shutdown();
        ejectTapeToBeScheduled.shutdown();
    }
    
    
    private void checkTapeState( final DatabaseSupport dbSupport, final Tape tape, final TapeState expectedTapeState,
            final String message )
    {
        assertEquals( expectedTapeState,
                dbSupport.getServiceManager()
                .getRetriever( Tape.class )
                .attain( tape.getId() )
                .getState(),
                message );
    }
    
    
    private TapeDriveService getTapeDriveService( final DatabaseSupport dbSupport )
    {
        return dbSupport.getServiceManager()
                        .getService( TapeDriveService.class );
    }
    
    
    private DatabaseSupport getDatabaseSupport()
    {
        return DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
    }
    
    
    private void validationCompleted( final UUID listenerKey, final Map< UUID, TapeMoveListener > moveListeners,
            final UUID tapeId, final RuntimeException failure, final int maxWaitInMillis )
    {
        final Duration duration = new Duration();
        do
        {
            if ( moveListeners.containsKey( listenerKey ) )
            {
                moveListeners.get( listenerKey )
                             .validationCompleted( tapeId, failure );
                return;
            }
            TestUtil.sleep( 10 );
        }
        while ( duration.getElapsedMillis() < maxWaitInMillis );
        
        Assertions.fail( "Timeout occurred waiting for move listener key " + listenerKey + "." );
    }
    
    
    class TapeAndDriveWrapper
    {
        TapeAndDriveWrapper( final MockDaoDriver daoDriver )
        {
            m_tape = daoDriver.createTape( TapeState.NORMAL );
            m_drive = daoDriver.createTapeDrive( null, null, m_tape.getId() );
        }
        
        
        public Tape getTape()
        {
            return m_tape;
        }
        
        
        public TapeDrive getDrive()
        {
            return m_drive;
        }
        
        
        AtomicBoolean getMovedToSlot()
        {
            return m_movedToSlot;
        }
        
        
        final Tape m_tape;
        final AtomicBoolean m_movedToSlot = new AtomicBoolean( false );
        TapeDrive m_drive;
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

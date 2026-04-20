/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironment;


import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.ElementAddressType;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionState;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeBlobStoreProcessor;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeMoveListener;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.MockBeansServiceManager;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.*;

public final class OnlineTapeProcessor_Test
{

    @Timeout(1000)
    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new OnlineTapeProcessor(
                        null,
                        InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, null ),
                        InterfaceProxyFactory.getProxy( TapeEnvironment.class, null ),
                        1 );
            }
        } );
    }

    @Timeout(1000)
    @Test
    public void testConstructorNullBlobStoreProcessorNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new OnlineTapeProcessor(
                        new MockBeansServiceManager(),
                        null,
                        null,
                        1 );
            }
        } );
    }

    @Timeout(1000)
    @Test
    public void testHappyConstruction()
    {
        final OnlineTapeProcessor onliner = new OnlineTapeProcessor(
                new MockBeansServiceManager(),
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, null ),
                InterfaceProxyFactory.getProxy( TapeEnvironment.class, null ),
                1 );
        onliner.shutdown();
    }

    @Timeout(value = 2, unit = MINUTES)
    @Test
    public void testTapeOnliningWhenNoStorageSlotsResultsInNotificationClearedOnceStorageSlotsAvailable()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape0 = mockDaoDriver.createTape( TapeState.OFFLINE );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.OFFLINE );
        mockDaoDriver.createTape( TapeState.EJECT_TO_EE_IN_PROGRESS );
        mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.createTape( TapeState.LOST );
        mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.PENDING_INSPECTION );

        final Map< UUID, TapeMoveListener > moveListeners = new ConcurrentHashMap<>();
        final Throwable [] moveException = new Throwable[ 1 ];
        final Map< UUID, Object > tapeLockHolders = new HashMap<>();
        final Method methodGetTaskStateLock =
                ReflectUtil.getMethod( TapeBlobStoreProcessor.class, "getTaskStateLock" );
        final Method methodGetTapeLockHolder =
                ReflectUtil.getMethod( TapeEnvironment.class, "getTapeLockHolder" );
        final Method methodMoveTape =
                ReflectUtil.getMethod( TapeEnvironment.class, "moveTapeToSlot" );
        final Map< Method, Integer > expectedCallCounts = new HashMap<>();
        expectedCallCounts.put( methodGetTaskStateLock, null );
        expectedCallCounts.put( methodGetTapeLockHolder, null );
        // Create mocks for dependencies
        TapeBlobStoreProcessor tapeBlobStoreProcessorMock = Mockito.mock(TapeBlobStoreProcessor.class);
        Mockito.when(tapeBlobStoreProcessorMock.getTaskStateLock())
                .thenReturn(new Object());

        TapeEnvironment tapeEnvironmentMock = Mockito.mock(TapeEnvironment.class);

        // Mock behavior for getTapeLockHolder
        Mockito.when(tapeEnvironmentMock.getTapeLockHolder(Mockito.any()))
                .thenAnswer(invocation -> tapeLockHolders.get(invocation.getArgument(0)));

        // Mock behavior for moveTapeToSlot
        Mockito.when(tapeEnvironmentMock.moveTapeToSlot(
                Mockito.any(UUID.class),
                Mockito.any(ElementAddressType.class),
                Mockito.any(TapeMoveListener.class)
        )).thenAnswer(invocation -> {
            UUID tapeId = invocation.getArgument(0);
            ElementAddressType type = invocation.getArgument(1);
            TapeMoveListener listener = invocation.getArgument(2);

            if (type != ElementAddressType.STORAGE) {
                throw new RuntimeException("Shoulda moved tape to a storage slot.");
            }

            // Add failure if exceptions exist
            if (moveException[0] != null) {
                throw moveException[0];
            }

            // Register listener for tape moves
            moveListeners.put(tapeId, listener);
            return Boolean.TRUE;
        });


        service.update( tape0.setBarCode( "0" ), Tape.BAR_CODE ); //set bar codes to control processing order
        service.update( tape1.setBarCode( "1" ), Tape.BAR_CODE );
        service.transistState( tape0, TapeState.ONLINE_PENDING );
        service.transistState( tape1, TapeState.ONLINE_PENDING );

        final OnlineTapeProcessor onliner = new OnlineTapeProcessor(
                dbSupport.getServiceManager(),
                tapeBlobStoreProcessorMock,
                tapeEnvironmentMock,
                10 );
        onliner.schedule();
        TestUtil.sleep( 50 );
        assertEquals(
                TapeState.ONLINE_PENDING,
                service.attain( tape0.getId() ).getState(),
                "Should notta updated tape state until move validation completed."
        );
        assertEquals(
                TapeState.ONLINE_PENDING,
                service.attain( tape1.getId() ).getState(),
                "Should notta touched tape1 yet."
        );
        assertNull(
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).retrieve(
                        Require.nothing() ),
                "Should notta generated notification."
        );
        long startTime = System.currentTimeMillis();

        while ( null == moveListeners.get( tape0.getId() ) )
        {
            if (System.currentTimeMillis() - startTime > timeout) {
                throw new RuntimeException("Timeout waiting for moveListeners to be updated.");
            }
            TestUtil.sleep( 1000 );
        }
        moveListeners.get( tape0.getId() ).validationCompleted(
                tape0.getId(), new RuntimeException( "No storage slot available" ) );

        assertEquals(
                TapeState.ONLINE_PENDING,
                service.attain( tape0.getId() ).getState(),
                "Should notta updated tape state since validation failed."
        );
        assertEquals(
                TapeState.ONLINE_PENDING,
                service.attain( tape1.getId() ).getState(),
                "Should notta touched tape1 yet."
        );
        assertEquals(
                TapePartitionFailureType.ONLINE_STALLED_DUE_TO_NO_STORAGE_SLOTS,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).attain(
                        Require.nothing() ).getType(),
                "Shoulda generated notification."
        );



        startTime = System.currentTimeMillis();
        while ( null == moveListeners.get( tape0.getId() ) )
        {
            if (System.currentTimeMillis() - startTime > timeout) {
                throw new RuntimeException("Timeout waiting for moveListeners to be updated.");
            }

            TestUtil.sleep( sleepInterval);
        }
        moveListeners.get( tape0.getId() ).validationCompleted( tape0.getId(), null );
        assertNull(
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).retrieve(
                        Require.nothing() ),
                "Shoulda whacked generated notification."
        );
        onliner.shutdown();
    }

    @Timeout(value = 2, unit = MINUTES)
    @Test
    public void testTapeOnliningWhenMoveSucceededResultsInTapeStateChangesAndWorkingOnNextTape()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape0 = mockDaoDriver.createTape( TapeState.OFFLINE );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.OFFLINE );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.OFFLINE );
        mockDaoDriver.updateBean( tape1.setType( TapeType.LTO_CLEANING_TAPE ), Tape.TYPE );
        mockDaoDriver.updateBean( tape2.setType( TapeType.LTO9 ), Tape.TYPE );
        mockDaoDriver.createTape( TapeState.EJECT_TO_EE_IN_PROGRESS );
        mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.createTape( TapeState.LOST );
        mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.PENDING_INSPECTION );

        final Map< UUID, TapeMoveListener > moveListeners = new ConcurrentHashMap<>();
        final Throwable [] moveException = new Throwable[ 1 ];
        final Map< UUID, Object > tapeLockHolders = new HashMap<>();
        final Method methodGetTaskStateLock =
                ReflectUtil.getMethod( TapeBlobStoreProcessor.class, "getTaskStateLock" );
        final Method methodGetTapeLockHolder =
                ReflectUtil.getMethod( TapeEnvironment.class, "getTapeLockHolder" );

        final Map< Method, Integer > expectedCallCounts = new HashMap<>();
        expectedCallCounts.put( methodGetTaskStateLock, null );
        expectedCallCounts.put( methodGetTapeLockHolder, null );

        // Create a mock for TapeBlobStoreProcessor
        TapeBlobStoreProcessor tapeBlobStoreProcessorMock = Mockito.mock(TapeBlobStoreProcessor.class);
        Mockito.when(tapeBlobStoreProcessorMock.getTaskStateLock())
                .thenReturn(new Object());

        // Create a mock for TapeEnvironment
        TapeEnvironment tapeEnvironmentMock = Mockito.mock(TapeEnvironment.class);

        // Stub the method getTapeLockHolder
        Mockito.when(tapeEnvironmentMock.getTapeLockHolder(Mockito.any()))
                .thenAnswer(invocation -> {
                    Object tapeId = invocation.getArgument(0);
                    return tapeLockHolders.get(tapeId);
                });

        // Stub the method moveTapeToSlot
        Mockito.when(tapeEnvironmentMock.moveTapeToSlot(
                Mockito.any(UUID.class),
                Mockito.any(ElementAddressType.class),
                Mockito.any(TapeMoveListener.class)
        )).thenAnswer(invocation -> {
            UUID tapeId = invocation.getArgument(0);
            ElementAddressType type = invocation.getArgument(1);
            TapeMoveListener listener = invocation.getArgument(2);

            // Validation logic
            if (type != ElementAddressType.STORAGE) {
                throw new RuntimeException("Shoulda moved tape to a storage slot.");
            }

            // Simulate exception for failure during move
            if (moveException[0] != null) {
                throw moveException[0];
            }

            // Register the listener
            moveListeners.put(tapeId, listener);
            return Boolean.TRUE;
        });


        service.update( tape0.setBarCode( "0" ), Tape.BAR_CODE ); //set bar codes to control processing order
        service.update( tape1.setBarCode( "1" ), Tape.BAR_CODE );
        service.update( tape2.setBarCode( "2" ), Tape.BAR_CODE );
        service.transistState( tape0, TapeState.ONLINE_PENDING );
        service.transistState( tape1, TapeState.ONLINE_PENDING );
        service.transistState( tape2, TapeState.ONLINE_PENDING );
        // Create and schedule OnlineTapeProcessor
        final OnlineTapeProcessor onliner = new OnlineTapeProcessor(
                dbSupport.getServiceManager(),
                tapeBlobStoreProcessorMock,
                tapeEnvironmentMock,
                10
        );

        onliner.schedule();
        TestUtil.sleep( 50 );
        assertEquals(
                TapeState.ONLINE_PENDING,
                service.attain( tape0.getId() ).getState(),
                "Should notta updated tape state until move validation completed."
        );
        assertEquals(
                TapeState.ONLINE_PENDING,
                service.attain( tape1.getId() ).getState(),
                "Should notta touched tape1 yet."
        );
        assertNull(
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).retrieve(
                        Require.nothing() ),
                "Should notta generated notification."
        );

        long startTime = System.currentTimeMillis();
        while ( null == moveListeners.get( tape0.getId() )  )
        {
            if (System.currentTimeMillis() - startTime > timeout) {
                throw new RuntimeException("Timeout waiting for moveListeners to be updated.");
            }

            TestUtil.sleep( sleepInterval );
        }
        moveListeners.get( tape0.getId() ).validationCompleted( tape0.getId(), null );
        assertEquals(
                TapeState.ONLINE_IN_PROGRESS,
                service.attain( tape0.getId() ).getState(),
                "Shoulda updated tape state now that validation completed."
        );
        assertEquals(
                TapeState.ONLINE_PENDING,
                service.attain( tape1.getId() ).getState(),
                "Should notta touched tape1 yet."
        );
        assertNull(
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).retrieve(
                        Require.nothing() ),
                "Should notta generated notification."
        );

        moveListeners.get( tape0.getId() ).moveSucceeded( tape0.getId() );
        assertEquals(
                TapeState.PENDING_INSPECTION,
                service.attain( tape0.getId() ).getState(),
                "Shoulda updated tape state now that move completed."
        );
        assertNull(moveListeners.get(tape1.getId()), "Should notta touched tape1 yet.");
        assertNull(moveListeners.get(tape2.getId()), "Should notta touched tape2 yet.");
        assertNull(
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).retrieve(
                        Require.nothing() ),
                "Should notta generated notification."
        );


        startTime = System.currentTimeMillis();
        while ( null == moveListeners.get( tape1.getId() )  )
        {
            if (System.currentTimeMillis() - startTime > timeout) {
                throw new RuntimeException("Timeout waiting for moveListeners to be updated.");
            }

            TestUtil.sleep( sleepInterval );
        }
        moveListeners.get( tape1.getId() ).validationCompleted( tape1.getId(), null );
        assertEquals(
                TapeState.ONLINE_IN_PROGRESS,
                service.attain( tape1.getId() ).getState(),
                "Shoulda started working on tape1."
        );
        assertNull(
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).retrieve(
                        Require.nothing() ),
                "Should notta generated notification."
        );
        moveListeners.get( tape0.getId() ).moveSucceeded( tape1.getId() );
        assertEquals(
                TapeState.NORMAL,
                service.attain( tape1.getId() ).getState(),
                "Shoulda updated tape state now that move completed."
        );
        startTime = System.currentTimeMillis();

        while ( null == moveListeners.get( tape2.getId() )  )
        {
            if (System.currentTimeMillis() - startTime > timeout) {
                throw new RuntimeException("Timeout waiting for moveListeners to be updated.");
            }

            TestUtil.sleep( sleepInterval );
        }
        moveListeners.get( tape2.getId() ).validationCompleted( tape2.getId(), null );
        assertEquals(
                TapeState.ONLINE_IN_PROGRESS,
                service.attain( tape2.getId() ).getState(),
                "Shoulda started working on tape2."
        );
        assertNull(
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).retrieve(
                        Require.nothing() ),
                "Should notta generated notification."
        );
        moveListeners.get( tape0.getId() ).moveSucceeded( tape2.getId() );
        assertEquals(
                TapeState.INCOMPATIBLE,
                service.attain( tape2.getId() ).getState(),
                "Shoulda updated tape state now that move completed."
        );
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Shoulda created tape partition failure for incompatible tape."
        );
        onliner.shutdown();
    }

    @Timeout(value = 2, unit = MINUTES)
    @Test
    public void testTapeOnliningWhenMoveRequestsNotImmediatelyAcceptedStillWorks()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape0 = mockDaoDriver.createTape( TapeState.OFFLINE );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.OFFLINE );
        mockDaoDriver.createTape( TapeState.EJECT_TO_EE_IN_PROGRESS );
        mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.createTape( TapeState.LOST );
        mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.PENDING_INSPECTION );

        final Map< UUID, TapeMoveListener > moveListeners = new ConcurrentHashMap<>();
        final Throwable [] moveException = new Throwable[ 1 ];
        final Map< UUID, Object > tapeLockHolders = new HashMap<>();
        final Method methodGetTaskStateLock =
                ReflectUtil.getMethod( TapeBlobStoreProcessor.class, "getTaskStateLock" );
        final Method methodGetTapeLockHolder =
                ReflectUtil.getMethod( TapeEnvironment.class, "getTapeLockHolder" );
        final Method methodMoveTape =
                ReflectUtil.getMethod( TapeEnvironment.class, "moveTapeToSlot" );
        final Map< Method, Integer > expectedCallCounts = new HashMap<>();
        expectedCallCounts.put( methodGetTaskStateLock, null );
        expectedCallCounts.put( methodGetTapeLockHolder, null );
        final Boolean [] methodMoveTapeResult = new Boolean [] { Boolean.FALSE };
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler(
                MockInvocationHandler.forMethod(
                        methodGetTaskStateLock,
                        new ConstantResponseInvocationHandler( new Object() ),
                        MockInvocationHandler.forMethod(
                                methodGetTapeLockHolder,
                                new InvocationHandler()
                                {
                                    public Object invoke( Object proxy, Method method, Object[] args )
                                            throws Throwable
                                    {
                                        return tapeLockHolders.get( args[ 0 ] );
                                    }
                                },
                                MockInvocationHandler.forMethod(
                                        methodMoveTape,
                                        new InvocationHandler()
                                        {
                                            public Object invoke( Object proxy, Method method, Object[] args )
                                                    throws Throwable
                                            {
                                                if ( ElementAddressType.STORAGE != args[ 1 ] )
                                                {
                                                    throw new RuntimeException(
                                                            "Shoulda moved tape to a storage slot." );
                                                }
                                                if ( null != moveException[ 0 ] )
                                                {
                                                    throw moveException[ 0 ];
                                                }

                                                if ( methodMoveTapeResult[ 0 ].booleanValue() )
                                                {
                                                    moveListeners.put(
                                                            (UUID)args[ 0 ],
                                                            (TapeMoveListener)args[ 2 ] );
                                                }
                                                return methodMoveTapeResult[ 0 ];
                                            }
                                        },
                                        null ) ) ) );

        service.update( tape0.setBarCode( "0" ), Tape.BAR_CODE ); //set bar codes to control processing order
        service.update( tape1.setBarCode( "1" ), Tape.BAR_CODE );
        service.transistState( tape0, TapeState.ONLINE_PENDING );
        service.transistState( tape1, TapeState.ONLINE_PENDING );

        final OnlineTapeProcessor onliner = new OnlineTapeProcessor(
                dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, btih ),
                InterfaceProxyFactory.getProxy( TapeEnvironment.class, btih ),
                10 );
        onliner.schedule();
        TestUtil.sleep( 50 );
        assertEquals(
                TapeState.ONLINE_PENDING,
                service.attain( tape0.getId() ).getState(),
                "Should notta updated tape state until move validation completed."
        );
        assertEquals(
                TapeState.ONLINE_PENDING,
                service.attain( tape1.getId() ).getState(),
                "Should notta touched tape1 yet."
        );
        assertNull(
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).retrieve(
                        Require.nothing() ),
                "Should notta generated notification."
        );
        methodMoveTapeResult[ 0 ] = Boolean.TRUE;
        TestUtil.sleep( 50 );
        assertEquals(
                TapeState.ONLINE_PENDING,
                service.attain( tape0.getId() ).getState(),
                "Should notta updated tape state until move validation completed."
        );
        assertEquals(
                TapeState.ONLINE_PENDING,
                service.attain( tape1.getId() ).getState(),
                "Should notta touched tape1 yet."
        );
        assertNull(
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).retrieve(
                        Require.nothing() ),
                "Should notta generated notification."
        );

        long startTime = System.currentTimeMillis();
        while( null == moveListeners.get( tape0.getId() ) )
        {
            if (System.currentTimeMillis() - startTime > timeout) {
                throw new RuntimeException("Timeout waiting for moveListeners to be updated.");
            }

            TestUtil.sleep( sleepInterval );
        }
        moveListeners.get( tape0.getId() ).validationCompleted( tape0.getId(), null );
        assertEquals(
                TapeState.ONLINE_IN_PROGRESS,
                service.attain( tape0.getId() ).getState(),
                "Shoulda updated tape state now that validation completed."
        );
        assertEquals(
                TapeState.ONLINE_PENDING,
                service.attain( tape1.getId() ).getState(),
                "Should notta touched tape1 yet."
        );
        assertNull(
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).retrieve(
                        Require.nothing() ),
                "Should notta generated notification."
        );

        moveListeners.get( tape0.getId() ).moveSucceeded( tape0.getId() );
        assertEquals(
                TapeState.PENDING_INSPECTION,
                service.attain( tape0.getId() ).getState(),
                "Shoulda updated tape state now that move completed."
        );
        assertNull(moveListeners.get(tape1.getId()), "Should notta touched tape1 yet.");
        assertNull(
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).retrieve(
                        Require.nothing() ),
                "Should notta generated notification."
        );

        startTime = System.currentTimeMillis();
        while ( null == moveListeners.get( tape1.getId() ) )
        {
            if (System.currentTimeMillis() - startTime > timeout) {
                throw new RuntimeException("Timeout waiting for moveListeners to be updated.");
            }

            TestUtil.sleep( 100 );
        }

        moveListeners.get( tape1.getId() ).validationCompleted( tape1.getId(), null );
        assertEquals(
                TapeState.ONLINE_IN_PROGRESS,
                service.attain( tape1.getId() ).getState(),
                "Shoulda started working on tape1."
        );
        assertNull(
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).retrieve(
                        Require.nothing() ),
                "Should notta generated notification."
        );
        onliner.shutdown();
    }

    @Timeout(value = 2, unit = MINUTES)
    @Test
    public void testTapeOnliningWhenMoveFailedResultsInTapeStateRollbackAndRetry()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape0 = mockDaoDriver.createTape( TapeState.OFFLINE );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.OFFLINE );
        mockDaoDriver.createTape( TapeState.EJECT_TO_EE_IN_PROGRESS );
        mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.createTape( TapeState.LOST );
        mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.PENDING_INSPECTION );

        final Map< UUID, TapeMoveListener > moveListeners = new ConcurrentHashMap<>();
        final Throwable [] moveException = new Throwable[ 1 ];
        final Map< UUID, Object > tapeLockHolders = new HashMap<>();
        final Method methodGetTaskStateLock =
                ReflectUtil.getMethod( TapeBlobStoreProcessor.class, "getTaskStateLock" );
        final Method methodGetTapeLockHolder =
                ReflectUtil.getMethod( TapeEnvironment.class, "getTapeLockHolder" );
        final Map< Method, Integer > expectedCallCounts = new HashMap<>();
        expectedCallCounts.put( methodGetTaskStateLock, null );
        expectedCallCounts.put( methodGetTapeLockHolder, null );
        // Mocks for dependencies
        TapeBlobStoreProcessor tapeBlobStoreProcessorMock = Mockito.mock(TapeBlobStoreProcessor.class);
        Mockito.when(tapeBlobStoreProcessorMock.getTaskStateLock())
                .thenReturn(new Object());

        TapeEnvironment tapeEnvironmentMock = Mockito.mock(TapeEnvironment.class);
        Mockito.when(tapeEnvironmentMock.getTapeLockHolder(Mockito.any()))
                .thenAnswer(invocation -> tapeLockHolders.get(invocation.getArgument(0)));

        Mockito.when(tapeEnvironmentMock.moveTapeToSlot(
                Mockito.any(UUID.class),
                Mockito.any(ElementAddressType.class),
                Mockito.any(TapeMoveListener.class)
        )).thenAnswer(invocation -> {
            UUID tapeId = invocation.getArgument(0);
            ElementAddressType type = invocation.getArgument(1);
            TapeMoveListener listener = invocation.getArgument(2);

            if (ElementAddressType.STORAGE != type) {
                throw new RuntimeException("Shoulda moved tape to a storage slot.");
            }

            // Fail tape move if an exception exists
            if (moveException[0] != null) {
                throw moveException[0];
            }

            // Register listener for tape moves
            moveListeners.put(tapeId, listener);
            return Boolean.TRUE;
        });


        service.update( tape0.setBarCode( "0" ), Tape.BAR_CODE ); //set bar codes to control processing order
        service.update( tape1.setBarCode( "1" ), Tape.BAR_CODE );
        service.transistState( tape0, TapeState.ONLINE_PENDING );
        service.transistState( tape1, TapeState.ONLINE_PENDING );

        final OnlineTapeProcessor onliner = new OnlineTapeProcessor(
                dbSupport.getServiceManager(),
                tapeBlobStoreProcessorMock,
                tapeEnvironmentMock,
                10 );
        onliner.schedule();
        TestUtil.sleep( 50 );
        assertEquals(
                TapeState.ONLINE_PENDING,
                service.attain( tape0.getId() ).getState(),
                "Should notta updated tape state until move validation completed."
        );
        assertEquals(
                TapeState.ONLINE_PENDING,
                service.attain( tape1.getId() ).getState(),
                "Should notta touched tape1 yet."
        );
        assertNull(
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).retrieve(
                        Require.nothing() ),
                "Should notta generated notification."
        );

        long startTime = System.currentTimeMillis();
        while ( null == moveListeners.get( tape0.getId() ) )
        {
            if (System.currentTimeMillis() - startTime > timeout) {
                throw new RuntimeException("Timeout waiting for moveListeners to be updated.");
            }

            TestUtil.sleep( sleepInterval );
        }
        moveListeners.get( tape0.getId() ).validationCompleted( tape0.getId(), null );

        assertEquals(
                TapeState.ONLINE_IN_PROGRESS,
                service.attain( tape0.getId() ).getState(),
                "Shoulda updated tape state now that validation completed."
        );
        assertEquals(
                TapeState.ONLINE_PENDING,
                service.attain( tape1.getId() ).getState(),
                "Should notta touched tape1 yet."
        );
        assertNull(
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).retrieve(
                        Require.nothing() ),
                "Should notta generated notification."
        );

        moveListeners.get( tape0.getId() ).moveFailed( tape0.getId() );
        assertEquals(
                TapeState.ONLINE_PENDING,
                service.attain( tape0.getId() ).getState(),
                "Shoulda updated tape state now that move failed."
        );
        assertNull(moveListeners.get(tape1.getId()), "Should notta touched tape1 yet.");
        assertNull(
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).retrieve(
                        Require.nothing() ),
                "Should notta generated notification."
        );
        onliner.shutdown();
    }

    @Timeout(value = 2, unit = MINUTES)
    @Test
    public void testWillNotOnlineTapeUntilTapePartitionInStateToAllowIt()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape0 = mockDaoDriver.createTape( TapeState.OFFLINE );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.OFFLINE );
        mockDaoDriver.updateBean( tape1.setType( TapeType.LTO_CLEANING_TAPE ), Tape.TYPE );
        mockDaoDriver.createTape( TapeState.EJECT_TO_EE_IN_PROGRESS );
        mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.createTape( TapeState.LOST );
        mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.PENDING_INSPECTION );

        final TapePartition partition = mockDaoDriver.attainOneAndOnly( TapePartition.class );
        mockDaoDriver.updateBean( partition.setQuiesced( Quiesced.PENDING ), TapePartition.QUIESCED );
        mockDaoDriver.updateBean( partition.setState( TapePartitionState.ERROR ), TapePartition.STATE );

        final Map< UUID, TapeMoveListener > moveListeners = new ConcurrentHashMap<>();
        final Throwable [] moveException = new Throwable[ 1 ];
        final Map< UUID, Object > tapeLockHolders = new HashMap<>();
        final Method methodGetTaskStateLock =
                ReflectUtil.getMethod( TapeBlobStoreProcessor.class, "getTaskStateLock" );
        final Method methodGetTapeLockHolder =
                ReflectUtil.getMethod( TapeEnvironment.class, "getTapeLockHolder" );

        final Map< Method, Integer > expectedCallCounts = new HashMap<>();
        expectedCallCounts.put( methodGetTaskStateLock, null );
        expectedCallCounts.put( methodGetTapeLockHolder, null );
        // Create mocks for dependencies
        TapeBlobStoreProcessor tapeBlobStoreProcessorMock = Mockito.mock(TapeBlobStoreProcessor.class);
        Mockito.when(tapeBlobStoreProcessorMock.getTaskStateLock())
                .thenReturn(new Object());

        TapeEnvironment tapeEnvironmentMock = Mockito.mock(TapeEnvironment.class);

        // Mock behavior for getTapeLockHolder
        Mockito.when(tapeEnvironmentMock.getTapeLockHolder(Mockito.any()))
                .thenAnswer(invocation -> tapeLockHolders.get(invocation.getArgument(0)));

        // Mock behavior for moveTapeToSlot
        Mockito.when(tapeEnvironmentMock.moveTapeToSlot(
                Mockito.any(UUID.class),
                Mockito.any(ElementAddressType.class),
                Mockito.any(TapeMoveListener.class)
        )).thenAnswer(invocation -> {
            UUID tapeId = invocation.getArgument(0);
            ElementAddressType type = invocation.getArgument(1);
            TapeMoveListener listener = invocation.getArgument(2);

            if (type != ElementAddressType.STORAGE) {
                throw new RuntimeException("Shoulda moved tape to a storage slot.");
            }

            // Add failures if exceptions exist
            if (moveException[0] != null) {
                throw moveException[0];
            }

            // Register listener for move
            moveListeners.put(tapeId, listener);
            return Boolean.TRUE;
        });


        service.update( tape0.setBarCode( "0" ), Tape.BAR_CODE ); //set bar codes to control processing order
        service.update( tape1.setBarCode( "1" ), Tape.BAR_CODE );
        service.transistState( tape0, TapeState.ONLINE_PENDING );
        service.transistState( tape1, TapeState.ONLINE_PENDING );

        final OnlineTapeProcessor onliner = new OnlineTapeProcessor(
                dbSupport.getServiceManager(),
                tapeBlobStoreProcessorMock,
                tapeEnvironmentMock,
                10 );
        onliner.schedule();
        TestUtil.sleep( 50 );
        assertTrue(
                moveListeners.isEmpty(),
                "Should notta been able to start onlining yet due to partition's state."
        );

        mockDaoDriver.updateBean( partition.setQuiesced( Quiesced.NO ), TapePartition.QUIESCED );
        TestUtil.sleep( 50 );
        assertTrue(
                moveListeners.isEmpty(),
                "Should notta been able to start onlining yet due to partition's state."
        );

        mockDaoDriver.updateBean( partition.setQuiesced( Quiesced.YES ), TapePartition.QUIESCED );
        mockDaoDriver.updateBean( partition.setState( TapePartitionState.ONLINE ), TapePartition.STATE );
        TestUtil.sleep( 50 );
        assertTrue(
                moveListeners.isEmpty(),
                "Should notta been able to start onlining yet due to partition's state."
        );

        mockDaoDriver.updateBean( partition.setQuiesced( Quiesced.NO ), TapePartition.QUIESCED );
        mockDaoDriver.updateBean( partition.setState( TapePartitionState.ONLINE ), TapePartition.STATE );
        long startTime = System.currentTimeMillis();

        while ( moveListeners.isEmpty() )
        {
            if (System.currentTimeMillis() - startTime > timeout) {
                throw new RuntimeException("Timeout waiting for moveListeners to be updated.");
            }

            TestUtil.sleep( sleepInterval );
        }
        assertFalse(
                moveListeners.isEmpty(),
                "Shoulda been able to start onlining due to good partition's state."
        );

        assertEquals(
                TapeState.ONLINE_PENDING,
                service.attain( tape0.getId() ).getState(),
                "Should notta updated tape state until move validation completed."
        );
        assertEquals(
                TapeState.ONLINE_PENDING,
                service.attain( tape1.getId() ).getState(),
                "Should notta touched tape1 yet."
        );
        assertNull(
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).retrieve(
                        Require.nothing() ),
                "Should notta generated notification."
        );
        moveListeners.get( tape0.getId() ).validationCompleted( tape0.getId(), null );
        assertEquals(
                TapeState.ONLINE_IN_PROGRESS,
                service.attain( tape0.getId() ).getState(),
                "Shoulda updated tape state now that validation completed."
        );
        assertEquals(
                TapeState.ONLINE_PENDING,
                service.attain( tape1.getId() ).getState(),
                "Should notta touched tape1 yet."
        );
        assertNull(
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).retrieve(
                        Require.nothing() ),
                "Should notta generated notification."
        );

        moveListeners.get( tape0.getId() ).moveSucceeded( tape0.getId() );
        assertEquals(
                TapeState.PENDING_INSPECTION,
                service.attain( tape0.getId() ).getState(),
                "Shoulda updated tape state now that move completed."
        );
        assertNull(moveListeners.get(tape1.getId()), "Should notta touched tape1 yet.");
        assertNull(
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).retrieve(
                        Require.nothing() ),
                "Should notta generated notification."
        );

        while ( null == moveListeners.get( tape1.getId() )  )
        {
            TestUtil.sleep( 100 );
        }
        moveListeners.get( tape1.getId() ).validationCompleted( tape1.getId(), null );
        assertEquals(
                TapeState.ONLINE_IN_PROGRESS,
                service.attain( tape1.getId() ).getState(),
                "Shoulda started working on tape1."
        );
        assertNull(
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).retrieve(
                        Require.nothing() ),
                "Should notta generated notification."
        );
        moveListeners.get( tape0.getId() ).moveSucceeded( tape1.getId() );
        assertEquals(
                TapeState.NORMAL,
                service.attain( tape1.getId() ).getState(),
                "Shoulda updated tape state now that move completed."
        );
        onliner.shutdown();
    }

    long timeout = 5000;
    int sleepInterval = 100;
    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public void tearDown() throws InterruptedException {
        if (!SystemWorkPool.getInstance().awaitEmpty(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("System work pool did not empty in a timely manner.");
        }
        dbSupport.reset();
    }
}

/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironment;


import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.ElementAddressType;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionState;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeBlobStoreProcessor;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeMoveListener;
import com.spectralogic.s3.dataplanner.backend.tape.task.NoOpTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.task.VerifyTapeTask;
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
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

public final class EjectTapeProcessor_Test
{
    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new EjectTapeProcessor(
                        null,
                        InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, null ),
                        InterfaceProxyFactory.getProxy( TapeEnvironment.class, null ),
                1 );
            }
        } );
    }
    
    @Test
    public void testConstructorNullBlobStoreProcessorNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new EjectTapeProcessor(
                        new MockBeansServiceManager(), 
                        null,
                        null,
                        1 );
            }
        } );
    }
    
    @Test
    public void testHappyConstruction()
    {
        final EjectTapeProcessor ejector = new EjectTapeProcessor(
                new MockBeansServiceManager(), 
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, null ),
                InterfaceProxyFactory.getProxy( TapeEnvironment.class, null ),
                1 );
        ejector.shutdown();
    }
    
    @Test
    public void testTapesAreEjectedAsTheyAreEligibleForEjection()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape0 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape3 = mockDaoDriver.createTape( TapeState.UNKNOWN );
        final Tape tape4 = mockDaoDriver.createTape( TapeState.LOST );
        final Tape tape5 = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.EJECT_TO_EE_IN_PROGRESS );
        mockDaoDriver.createTape( TapeState.EJECTED );
        service.update( tape0.setEjectPending( new Date() ), Tape.EJECT_PENDING );
        service.update( 
                tape5.setEjectPending( new Date() ).setVerifyPending( BlobStoreTaskPriority.values()[ 0 ] ),
                Tape.EJECT_PENDING, Tape.VERIFY_PENDING );

        final Map< UUID, TapeMoveListener > moveListeners = new ConcurrentHashMap<>();
        final Throwable [] moveException = new Throwable[ 1 ];
        final Map< UUID, Object > tapeLockHolders = 
                Collections.synchronizedMap( new HashMap< UUID, Object >() );
        final Method methodIsSlotAvailable =
                ReflectUtil.getMethod( TapeEnvironment.class, "isSlotAvailable" );
        final Method methodGetTaskStateLock =
                ReflectUtil.getMethod( TapeBlobStoreProcessor.class, "getTaskStateLock" );
        final Method methodGetTapeLockHolder =
                ReflectUtil.getMethod( TapeEnvironment.class, "getTapeLockHolder" );
        final Method methodMoveTape = 
                ReflectUtil.getMethod( TapeEnvironment.class, "moveTapeToSlot" );
        final Map< Method, Integer > expectedCallCounts = new HashMap<>();
        expectedCallCounts.put( methodGetTaskStateLock, null );
        expectedCallCounts.put( methodGetTapeLockHolder, null );
        expectedCallCounts.put( methodIsSlotAvailable, null );
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
                                                if ( null != moveException[ 0 ] )
                                                {
                                                    throw moveException[ 0 ];
                                                }
                                                
                                                if ( ElementAddressType.IMPORT_EXPORT != args[ 1 ] )
                                                {
                                                    throw new RuntimeException(
                                                            "Shoulda moved tape to an EE slot." );
                                                }
                                                moveListeners.put( 
                                                        (UUID)args[ 0 ],
                                                        (TapeMoveListener)args[ 2 ] );
                                                ( (TapeMoveListener)args[ 2 ] ).validationCompleted(
                                                        (UUID)args[ 0 ], null );
                                                return Boolean.TRUE;
                                            }
                                        },
                                        MockInvocationHandler.forMethod(
                                                methodIsSlotAvailable, 
                                                new ConstantResponseInvocationHandler( Boolean.TRUE ),
                                                null ) ) ) ) );
        final EjectTapeProcessor ejector = new EjectTapeProcessor( 
                dbSupport.getServiceManager(), 
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, btih ),
                InterfaceProxyFactory.getProxy( TapeEnvironment.class, btih ),
                10 );
        
        expectedCallCounts.put( 
                ReflectUtil.getMethod( TapeBlobStoreProcessor.class, "addTaskSchedulingListener" ), 
                Integer.valueOf( 1 ) );
        expectedCallCounts.put( methodMoveTape, Integer.valueOf( 1 ) );
        btih.eventuallyVerifyMethodInvocations( expectedCallCounts, 2000 );
        int i = 1000;
        while ( --i > 0 && null != service.attain( tape0.getId() ).getEjectPending() )
        {
            TestUtil.sleep( 10 );
        }
        assertEquals(
                null,
                service.attain( tape0.getId() ).getEjectPending(),
                "Shoulda updated tape0."
                 );
        assertEquals(
                TapeState.EJECT_TO_EE_IN_PROGRESS,
                service.attain( tape0.getId() ).getState(),
                "Shoulda updated tape0."
                 );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Should only create a tape partition failure if offline tapes are holding things up."
                );
        
        btih.eventuallyVerifyMethodInvocations( expectedCallCounts, 2000 );

        service.update( tape1.setEjectPending( new Date() ), Tape.EJECT_PENDING );
        service.transistState( tape2, TapeState.OFFLINE );
        
        ejector.taskSchedulingRequired( null );
        ejector.taskSchedulingRequired( InterfaceProxyFactory.getProxy( TapeTask.class, null ) );
        
        i = 100;
        while ( --i > 0 && 0 != dbSupport.getServiceManager().getRetriever( 
                TapePartitionFailure.class ).getCount() )
        {
            TestUtil.sleep( 10 );
        }
        
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Should only create a tape partition failure if offline tapes are holding things up."
                );
        ejector.taskSchedulingRequired(
                new VerifyTapeTask( BlobStoreTaskPriority.values()[ 0 ],
                        UUID.randomUUID(),
                        new MockDiskManager(dbSupport.getServiceManager()),
                        new TapeFailureManagement(dbSupport.getServiceManager()),
                        dbSupport.getServiceManager() ) );
        
        btih.eventuallyVerifyMethodInvocations( expectedCallCounts, 2000 );
        i = 1000;
        while ( --i > 0 
                && 0 == dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount() )
        {
            TestUtil.sleep( 10 );
        }
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Should only create a tape partition failure if offline tapes are holding things up."
                 );
        
        service.transistState( tape2, TapeState.ONLINE_IN_PROGRESS );
        btih.eventuallyVerifyMethodInvocations( expectedCallCounts, 2000 );
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Should only create a tape partition failure if offline tapes are holding things up."
               );
        
        service.transistState( tape2, TapeState.NORMAL );
        expectedCallCounts.put( methodMoveTape, Integer.valueOf( 2 ) );
        btih.eventuallyVerifyMethodInvocations( expectedCallCounts, 2000 );
        i = 1000;
        while ( --i > 0 && null != service.attain( tape1.getId() ).getEjectPending() )
        {
            TestUtil.sleep( 10 );
        }
        assertEquals(
                null,
                service.attain( tape1.getId() ).getEjectPending(),
                "Shoulda updated tape1."
                );
        assertEquals(
                TapeState.EJECT_TO_EE_IN_PROGRESS,
                service.attain( tape1.getId() ).getState(),
                "Shoulda updated tape1."
                 );

        service.update( tape2.setEjectPending( new Date( 100 ) ), Tape.EJECT_PENDING );
        service.update( tape3.setEjectPending( new Date( 1000 ) ), Tape.EJECT_PENDING );
        service.update( tape4.setEjectPending( new Date( 10000 ) ), Tape.EJECT_PENDING );
        tapeLockHolders.put( tape3.getId(), InterfaceProxyFactory.getProxy( TapeTask.class, null ) );
        ejector.schedule();

        expectedCallCounts.put( methodMoveTape, Integer.valueOf( 3 ) );
        btih.eventuallyVerifyMethodInvocations( expectedCallCounts, 2000 );
        
        i = 1000;
        while ( --i > 0 && null != service.attain( tape2.getId() ).getEjectPending() )
        {
            TestUtil.sleep( 10 );
        }
        
        assertNull(
                service.attain( tape2.getId() ).getEjectPending(),
                "Shoulda updated tape2."
                );
        assertEquals(
                TapeState.EJECT_TO_EE_IN_PROGRESS,
                service.attain( tape2.getId() ).getState(),
                "Shoulda updated tape2."
                 );
        assertNotNull(
                service.attain( tape3.getId() ).getEjectPending(),
                "Should notta updated tape3."
                 );
        assertEquals(
                TapeState.UNKNOWN,
                service.attain( tape3.getId() ).getState(),
                "Should notta updated tape3."
                 );
        assertNotNull(
                service.attain( tape4.getId() ).getEjectPending(),
                "Should notta updated tape4."
                 );
        assertEquals(
                TapeState.LOST,
                service.attain( tape4.getId() ).getState(),
                "Should notta updated tape4."
                 );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Shoulda whacked tape partition failure for stall since the stall no longer applies."
               );

        tapeLockHolders.put( tape3.getId(), null );
        TestUtil.sleep( 50 );
        btih.verifyMethodInvocations( expectedCallCounts );
        assertNull(
                service.attain( tape2.getId() ).getEjectPending(),
                "Shoulda updated tape2."
                );
        assertEquals(
                TapeState.EJECT_TO_EE_IN_PROGRESS,
                service.attain( tape2.getId() ).getState(),
                "Shoulda updated tape2."
                 );
        assertNotNull(
                service.attain( tape3.getId() ).getEjectPending(),
                "Should notta updated tape3 since tape2's move didn't succeed yet."
                 );
        assertEquals(
                TapeState.UNKNOWN,
                service.attain( tape3.getId() ).getState(),
                "Should notta updated tape3 since tape2's move didn't succeed yet."
                 );
        assertNotNull(
                service.attain( tape4.getId() ).getEjectPending(),
                "Should notta updated tape4."
                );
        assertEquals(
                TapeState.LOST,
                service.attain( tape4.getId() ).getState(),
                "Should notta updated tape4."
                 );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Shoulda whacked tape partition failure for stall since the stall no longer applies."
                 );
        
        moveListeners.get( tape2.getId() ).moveSucceeded( tape2.getId() );
        expectedCallCounts.put( methodMoveTape, Integer.valueOf( 4 ) );
        btih.eventuallyVerifyMethodInvocations( expectedCallCounts, 2000 );
        
        i = 500;
        while ( --i > 0 && 
                ( null != service.attain( tape2.getId() ).getEjectPending() ) ||
                ( ! TapeState.EJECT_FROM_EE_PENDING.equals( service.attain( tape2.getId() ).getState() ) ) ||
                ( null != service.attain( tape3.getId() ).getEjectPending() ) ||
                ( ! TapeState.EJECT_TO_EE_IN_PROGRESS.equals(
                               service.attain( tape3.getId() ).getState() ) ) ||
                ( null == service.attain( tape4.getId() ).getEjectPending() ) ||
                ( ! TapeState.LOST.equals( service.attain( tape4.getId() ).getState() ) ) ||
                ( 0 != dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount() )
              )
        {
            TestUtil.sleep( 20 );
        }
        assertNull(
                service.attain( tape2.getId() ).getEjectPending(),
                "Shoulda updated tape2."
                 );
        assertEquals(
                TapeState.EJECT_FROM_EE_PENDING,
                service.attain( tape2.getId() ).getState(),
                "Shoulda updated tape2."
                 );
        assertNull(
                service.attain( tape3.getId() ).getEjectPending(),
                "Shoulda updated tape3."
                 );
        assertEquals(
                TapeState.EJECT_TO_EE_IN_PROGRESS,
                service.attain( tape3.getId() ).getState(),
                "Shoulda updated tape3."
                 );
        assertNotNull(
                service.attain( tape4.getId() ).getEjectPending(),
                "Should notta updated tape4."
                 );
        assertEquals(
                TapeState.LOST,
                service.attain( tape4.getId() ).getState(),
                "Should notta updated tape4."
                 );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Shoulda whacked tape partition failure for stall since the stall no longer applies."
                 );

        moveException[ 0 ] = new UnsupportedOperationException( "I like to throw up." );
        service.transistState( tape4, TapeState.NORMAL );
        moveListeners.get( tape3.getId() ).moveSucceeded( tape3.getId() );
        
        i = 1000;
        while ( --i > 0 && 5 > btih.getMethodCallCount( methodMoveTape ) )
        {
            TestUtil.sleep( 10 );
        }
        assertTrue(
                4 < btih.getMethodCallCount( methodMoveTape ),
                "Shoulda kept on retrying the failed moved."
                 );
        assertNotNull(
                service.attain( tape4.getId() ).getEjectPending(),
                "Should notta updated tape4."
                 );
        assertEquals(
                TapeState.NORMAL,
                service.attain( tape4.getId() ).getState(),
                "Should notta updated tape4."
               );
        
        moveException[ 0 ] = null;
        i = 1000;
        while ( --i > 0 && null != service.attain( tape4.getId() ).getEjectPending() )
        {
            TestUtil.sleep( 10 );
        }
        assertNull(
                service.attain( tape4.getId() ).getEjectPending(),
                "Shoulda updated tape4."
                 );
        assertEquals(
                TapeState.EJECT_TO_EE_IN_PROGRESS,
                service.attain( tape4.getId() ).getState(),
                "Shoulda updated tape4."
                 );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Shoulda whacked tape partition failure for stall since the stall no longer applies."
                 );
        ejector.shutdown();
    }
    
    @Test
    public void testTapesAreEjectedInTheOrderInWhichEjectRequestsWereReceivedForThem()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape3 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape4 = mockDaoDriver.createTape( TapeState.NORMAL );

        final Map< UUID, TapeMoveListener > moveListeners = new ConcurrentHashMap<>();
        final Method methodIsSlotAvailable =
                ReflectUtil.getMethod( TapeEnvironment.class, "isSlotAvailable" );
        final Method methodGetTaskStateLock =
                ReflectUtil.getMethod( TapeBlobStoreProcessor.class, "getTaskStateLock" );
        final Method methodMoveTape = 
                ReflectUtil.getMethod( TapeEnvironment.class, "moveTapeToSlot" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( 
                MockInvocationHandler.forMethod(
                        methodGetTaskStateLock,
                        new ConstantResponseInvocationHandler( new Object() ), 
                        MockInvocationHandler.forMethod( 
                                methodMoveTape, 
                                new InvocationHandler()
                                {
                                    public Object invoke( Object proxy, Method method, final  Object[] args )
                                            throws Throwable
                                    {
                                        if ( ElementAddressType.IMPORT_EXPORT != args[ 1 ] )
                                        {
                                            throw new RuntimeException(
                                                    "Shoulda moved tape to an EE slot." );
                                        }
                                        moveListeners.put( 
                                                (UUID)args[ 0 ],
                                                (TapeMoveListener)args[ 2 ] );
                                        ( (TapeMoveListener)args[ 2 ] ).validationCompleted(
                                                (UUID)args[ 0 ], null );
                                        SystemWorkPool.getInstance().submit( new Runnable()
                                        {
                                            public void run()
                                            {
                                                ( (TapeMoveListener)args[ 2 ] ).moveSucceeded(
                                                        (UUID)args[ 0 ] );
                                            }
                                        } );
                                        return Boolean.TRUE;
                                    }
                                },
                                MockInvocationHandler.forMethod(
                                        methodIsSlotAvailable, 
                                        new ConstantResponseInvocationHandler( Boolean.FALSE ),
                                        null ) ) ) );

        final EjectTapeProcessor ejector = new EjectTapeProcessor( 
                dbSupport.getServiceManager(), 
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, btih ),
                InterfaceProxyFactory.getProxy( TapeEnvironment.class, btih ),
                10 );
        
        service.update( tape1.setEjectPending( new Date( 1000 ) ), Tape.EJECT_PENDING );
        service.update( tape4.setEjectPending( new Date( 10000 ) ), Tape.EJECT_PENDING );
        service.update( tape3.setEjectPending( new Date( 100000 ) ), Tape.EJECT_PENDING );
        service.update( tape2.setEjectPending( new Date( 1000000 ) ), Tape.EJECT_PENDING );
        
        ejector.schedule();
        TestUtil.sleep( 1000 );
        
        assertEquals(
                4,
                btih.getMethodCallCount( methodMoveTape ),
                "Shoulda processed all 4 tapes."
                 );
        assertEquals(
                tape1.getId(),
                btih.getMethodInvokeData( methodMoveTape ).get( 0 ).getArgs().get( 0 ),
                "Shoulda processed tapes in the order eject was requested in."
                );
        assertEquals(
                tape4.getId(),
                btih.getMethodInvokeData( methodMoveTape ).get( 1 ).getArgs().get( 0 ),
                "Shoulda processed tapes in the order eject was requested in."
                );
        assertEquals(
                tape3.getId(),
                btih.getMethodInvokeData( methodMoveTape ).get( 2 ).getArgs().get( 0 ),
                "Shoulda processed tapes in the order eject was requested in."
                );
        assertEquals(
                tape2.getId(),
                btih.getMethodInvokeData( methodMoveTape ).get( 3 ).getArgs().get( 0 ),
                "Shoulda processed tapes in the order eject was requested in."
                );
        ejector.shutdown();
    }
    
    @Test
    public void testLockedTapesToEjectAreSkippedOverAndRemainingTapesAreEjectedInTheOrderInWhichEjectRequestsWereReceived()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape3 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape4 = mockDaoDriver.createTape( TapeState.NORMAL );

        final Map< UUID, Object > tapeLockHolders = new ConcurrentHashMap<>();
        tapeLockHolders.put( 
                tape1.getId(), 
                new NoOpTapeTask( "lock it", BlobStoreTaskPriority.values()[ 0 ], tape1.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager() ) );
        final Map< UUID, TapeMoveListener > moveListeners = new ConcurrentHashMap<>();
        final Method methodGetTapeLockHolder =
                ReflectUtil.getMethod( TapeEnvironment.class, "getTapeLockHolder" );
        final Method methodIsSlotAvailable =
                ReflectUtil.getMethod( TapeEnvironment.class, "isSlotAvailable" );
        final Method methodGetTaskStateLock =
                ReflectUtil.getMethod( TapeBlobStoreProcessor.class, "getTaskStateLock" );
        final Method methodMoveTape = 
                ReflectUtil.getMethod( TapeEnvironment.class, "moveTapeToSlot" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( 
                MockInvocationHandler.forMethod(
                        methodGetTaskStateLock,
                        new ConstantResponseInvocationHandler( new Object() ), 
                        MockInvocationHandler.forMethod( 
                                methodMoveTape, 
                                new InvocationHandler()
                                {
                                    public Object invoke( Object proxy, Method method, final  Object[] args )
                                            throws Throwable
                                    {
                                        if ( ElementAddressType.IMPORT_EXPORT != args[ 1 ] )
                                        {
                                            throw new RuntimeException(
                                                    "Shoulda moved tape to an EE slot." );
                                        }
                                        moveListeners.put( 
                                                (UUID)args[ 0 ],
                                                (TapeMoveListener)args[ 2 ] );
                                        ( (TapeMoveListener)args[ 2 ] ).validationCompleted(
                                                (UUID)args[ 0 ], null );
                                        SystemWorkPool.getInstance().submit( new Runnable()
                                        {
                                            public void run()
                                            {
                                                ( (TapeMoveListener)args[ 2 ] ).moveSucceeded(
                                                        (UUID)args[ 0 ] );
                                            }
                                        } );
                                        return Boolean.TRUE;
                                    }
                                },
                                MockInvocationHandler.forMethod(
                                        methodIsSlotAvailable, 
                                        new ConstantResponseInvocationHandler( Boolean.FALSE ),
                                        MockInvocationHandler.forMethod(
                                                methodGetTapeLockHolder, 
                                                new InvocationHandler()
                                                {
                                                    public Object invoke(
                                                            Object proxy, Method method, Object[] args )
                                                            throws Throwable
                                                    {
                                                        return tapeLockHolders.get( args[ 0 ] );
                                                    }
                                                },
                                                null ) ) ) ) );

        final EjectTapeProcessor ejector = new EjectTapeProcessor( 
                dbSupport.getServiceManager(), 
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, btih ),
                InterfaceProxyFactory.getProxy( TapeEnvironment.class, btih ),
                10 );
        
        service.update( tape1.setEjectPending( new Date( 1000 ) ), Tape.EJECT_PENDING );
        service.update( tape4.setEjectPending( new Date( 10000 ) ), Tape.EJECT_PENDING );
        service.update( tape3.setEjectPending( new Date( 100000 ) ), Tape.EJECT_PENDING );
        service.update( tape2.setEjectPending( new Date( 1000000 ) ), Tape.EJECT_PENDING );
        
        ejector.schedule();
        TestUtil.sleep( 600 );
        
        assertEquals(
                3,
                btih.getMethodCallCount( methodMoveTape ),
                "Shoulda processed all 3 unlocked tapes."
                );
        assertEquals(
                tape4.getId(),
                btih.getMethodInvokeData( methodMoveTape ).get( 0 ).getArgs().get( 0 ),
                "Shoulda processed tapes in the order eject was requested in."
               );
        assertEquals(
                tape3.getId(),
                btih.getMethodInvokeData( methodMoveTape ).get( 1 ).getArgs().get( 0 ),
                "Shoulda processed tapes in the order eject was requested in."
                 );
        assertEquals(
                tape2.getId(),
                btih.getMethodInvokeData( methodMoveTape ).get( 2 ).getArgs().get( 0 ),
                "Shoulda processed tapes in the order eject was requested in."
                 );
        ejector.shutdown();
    }
    
    @Test
    public void testTapeEjectEventuallySucceedsWhenEeSlotsFilledResultsInEeSlotsFullNotification()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );

        final AtomicInteger numAvailableImportExportSlots = new AtomicInteger( 1 );
        final Map< UUID, TapeMoveListener > moveListeners = new ConcurrentHashMap<>();
        final Method methodIsSlotAvailable =
                ReflectUtil.getMethod( TapeEnvironment.class, "isSlotAvailable" );
        final Method methodGetTaskStateLock =
                ReflectUtil.getMethod( TapeBlobStoreProcessor.class, "getTaskStateLock" );
        final Method methodMoveTape = 
                ReflectUtil.getMethod( TapeEnvironment.class, "moveTapeToSlot" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( 
                MockInvocationHandler.forMethod(
                        methodGetTaskStateLock,
                        new ConstantResponseInvocationHandler( new Object() ), 
                        MockInvocationHandler.forMethod( 
                                methodMoveTape, 
                                new InvocationHandler()
                                {
                                    public Object invoke( Object proxy, Method method, Object[] args )
                                            throws Throwable
                                    {
                                        if ( ElementAddressType.IMPORT_EXPORT != args[ 1 ] )
                                        {
                                            throw new RuntimeException(
                                                    "Shoulda moved tape to an EE slot." );
                                        }
                                        if ( 0 == numAvailableImportExportSlots.get() )
                                        {
                                            throw new RuntimeException( "No slot available." );
                                        }
                                        numAvailableImportExportSlots.decrementAndGet();
                                        moveListeners.put( 
                                                (UUID)args[ 0 ],
                                                (TapeMoveListener)args[ 2 ] );
                                        return Boolean.TRUE;
                                    }
                                },
                                MockInvocationHandler.forMethod(
                                        methodIsSlotAvailable, 
                                        new ConstantResponseInvocationHandler( Boolean.FALSE ),
                                        null ) ) ) );

        final EjectTapeProcessor ejector = new EjectTapeProcessor( 
                dbSupport.getServiceManager(), 
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, btih ),
                InterfaceProxyFactory.getProxy( TapeEnvironment.class, btih ),
                10 );
        
        service.update( tape1.setEjectPending( new Date( 1000 ) ), Tape.EJECT_PENDING );
        service.update( tape2.setEjectPending( new Date( 2000 ) ), Tape.EJECT_PENDING );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Should notta been any notifications yet."
                 );
        ejector.schedule();
        TestUtil.sleep( 150 );

        assertEquals(
                TapeState.NORMAL,
                dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape1.getId() ).getState(),
                "Should notta upgraded tape1's state to pending eject from EE prior to validation success."
               );

        validationCompleted( tape1.getId(), moveListeners, tape1.getId(), null, 2000 );
        moveListeners.get( tape1.getId() ).moveSucceeded( tape1.getId() );
        
        TestUtil.sleep( 150 );

        assertEquals(
                TapePartitionFailureType.TAPE_EJECTION_BY_OPERATOR_REQUIRED,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class )
                        .attain( Require.nothing() ).getType(),
                "Shoulda been a notification generated."
                 );
        assertEquals(
                TapeState.EJECT_FROM_EE_PENDING,
                dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape1.getId() ).getState(),
                "Should upgraded tape1's state to pending eject from EE."
                 );
        assertEquals(
                TapeState.NORMAL,
                dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape2.getId() ).getState(),
                "Shoulda kept tape2 in a state of not yet ready to be ejected."
                 );
        assertNotNull(
                dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape2.getId() )
                        .getEjectPending(),
                "Shoulda kept tape2 in a state of not yet ready to be ejected."
                 );
        ejector.shutdown();
    }
    
    @Test
    public void testTapeEjectEventuallySucceedsWhenEeSlotsNotFullNoMoreEjectsResultsInNotification()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );

        final AtomicInteger numAvailableImportExportSlots = new AtomicInteger( 1 );
        final Map< UUID, TapeMoveListener > moveListeners = new ConcurrentHashMap<>();
        final Method methodIsSlotAvailable =
                ReflectUtil.getMethod( TapeEnvironment.class, "isSlotAvailable" );
        final Method methodGetTaskStateLock =
                ReflectUtil.getMethod( TapeBlobStoreProcessor.class, "getTaskStateLock" );
        final Method methodMoveTape = 
                ReflectUtil.getMethod( TapeEnvironment.class, "moveTapeToSlot" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( 
                MockInvocationHandler.forMethod(
                        methodGetTaskStateLock,
                        new ConstantResponseInvocationHandler( new Object() ), 
                        MockInvocationHandler.forMethod( 
                                methodMoveTape, 
                                new InvocationHandler()
                                {
                                    public Object invoke( Object proxy, Method method, Object[] args )
                                            throws Throwable
                                    {
                                        if ( ElementAddressType.IMPORT_EXPORT != args[ 1 ] )
                                        {
                                            throw new RuntimeException(
                                                    "Shoulda moved tape to an EE slot." );
                                        }
                                        if ( 0 == numAvailableImportExportSlots.get() )
                                        {
                                            throw new RuntimeException( "No slot available." );
                                        }
                                        numAvailableImportExportSlots.decrementAndGet();
                                        moveListeners.put( 
                                                (UUID)args[ 0 ],
                                                (TapeMoveListener)args[ 2 ] );
                                        return Boolean.TRUE;
                                    }
                                },
                                MockInvocationHandler.forMethod(
                                        methodIsSlotAvailable, 
                                        new ConstantResponseInvocationHandler( Boolean.TRUE ),
                                        null ) ) ) );

        final EjectTapeProcessor ejector = new EjectTapeProcessor( 
                dbSupport.getServiceManager(), 
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, btih ),
                InterfaceProxyFactory.getProxy( TapeEnvironment.class, btih ),
                10 );
        
        service.update( tape1.setEjectPending( new Date( 1000 ) ), Tape.EJECT_PENDING );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Should notta been any notifications yet."
                 );
        ejector.schedule();
        
        validationCompleted( tape1.getId(), moveListeners, tape1.getId(), null, 2000 );
        moveListeners.get( tape1.getId() ).moveSucceeded( tape1.getId() );

        assertEquals(
                TapePartitionFailureType.TAPE_EJECTION_BY_OPERATOR_REQUIRED,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class )
                        .attain( Require.nothing() ).getType(),
                "Shoulda been a notification generated."
                 );
        assertEquals(
                TapeState.EJECT_FROM_EE_PENDING,
                dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape1.getId() ).getState(),
                "Should upgraded tape1's state to pending eject from EE."
                 );
        ejector.shutdown();
    }
    
    @Test
    public void testTapeEjectEventuallySucceedsWhenEeSlotsNotFullMoreEjectsInProgressResultsInNoNotification()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );

        final AtomicInteger numAvailableImportExportSlots = new AtomicInteger( 2 );
        final Map< UUID, TapeMoveListener > moveListeners = new ConcurrentHashMap<>();
        final Method methodIsSlotAvailable =
                ReflectUtil.getMethod( TapeEnvironment.class, "isSlotAvailable" );
        final Method methodGetTaskStateLock =
                ReflectUtil.getMethod( TapeBlobStoreProcessor.class, "getTaskStateLock" );
        final Method methodMoveTape = 
                ReflectUtil.getMethod( TapeEnvironment.class, "moveTapeToSlot" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( 
                MockInvocationHandler.forMethod(
                        methodGetTaskStateLock,
                        new ConstantResponseInvocationHandler( new Object() ), 
                        MockInvocationHandler.forMethod( 
                                methodMoveTape, 
                                new InvocationHandler()
                                {
                                    public Object invoke( Object proxy, Method method, Object[] args )
                                            throws Throwable
                                    {
                                        if ( ElementAddressType.IMPORT_EXPORT != args[ 1 ] )
                                        {
                                            throw new RuntimeException(
                                                    "Shoulda moved tape to an EE slot." );
                                        }
                                        if ( 0 == numAvailableImportExportSlots.get() )
                                        {
                                            throw new RuntimeException( "No slot available." );
                                        }
                                        numAvailableImportExportSlots.decrementAndGet();
                                        moveListeners.put( 
                                                (UUID)args[ 0 ],
                                                (TapeMoveListener)args[ 2 ] );
                                        return Boolean.TRUE;
                                    }
                                },
                                MockInvocationHandler.forMethod(
                                        methodIsSlotAvailable, 
                                        new ConstantResponseInvocationHandler( Boolean.TRUE ),
                                        null ) ) ) );

        final EjectTapeProcessor ejector = new EjectTapeProcessor( 
                dbSupport.getServiceManager(), 
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, btih ),
                InterfaceProxyFactory.getProxy( TapeEnvironment.class, btih ),
                10 );
        
        service.update( tape1.setEjectPending( new Date( 1000 ) ), Tape.EJECT_PENDING );
        service.update( tape2.setEjectPending( new Date( 2000 ) ), Tape.EJECT_PENDING );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Should notta been any notifications yet."
                 );
        ejector.schedule();
        
        validationCompleted( tape1.getId(), moveListeners, tape1.getId(), null, 2000 );
        moveListeners.get( tape1.getId() ).moveSucceeded( tape1.getId() );

        assertNull(
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).retrieve(
                        Require.nothing() ),
                "Should notta been any notifications."
                );
        assertEquals(
                TapeState.EJECT_FROM_EE_PENDING,
                dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape1.getId() ).getState(),
                "Should upgraded tape1's state to pending eject from EE."
                 );
        assertEquals(
                TapeState.NORMAL,
                dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape2.getId() ).getState(),
                "Should notta started processing tape 2 yet."
                );
        assertNotNull(
                dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape2.getId() )
                        .getEjectPending(),
                "Should notta started processing tape 2 yet."
                 );
        ejector.shutdown();
    }
    
    @Test
    public void testTapeEjectEventuallySucceedsWhenEeSlotsNotFullMoreEjectsPendingResultsInNoNotification()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );

        final AtomicInteger numAvailableImportExportSlots = new AtomicInteger( 1 );
        final Map< UUID, TapeMoveListener > moveListeners = new ConcurrentHashMap<>();
        final Method methodIsSlotAvailable =
                ReflectUtil.getMethod( TapeEnvironment.class, "isSlotAvailable" );
        final Method methodGetTaskStateLock =
                ReflectUtil.getMethod( TapeBlobStoreProcessor.class, "getTaskStateLock" );
        final Method methodMoveTape = 
                ReflectUtil.getMethod( TapeEnvironment.class, "moveTapeToSlot" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( 
                MockInvocationHandler.forMethod(
                        methodGetTaskStateLock,
                        new ConstantResponseInvocationHandler( new Object() ), 
                        MockInvocationHandler.forMethod( 
                                methodMoveTape, 
                                new InvocationHandler()
                                {
                                    public Object invoke( Object proxy, Method method, Object[] args )
                                            throws Throwable
                                    {
                                        if ( ElementAddressType.IMPORT_EXPORT != args[ 1 ] )
                                        {
                                            throw new RuntimeException(
                                                    "Shoulda moved tape to an EE slot." );
                                        }
                                        if ( 0 == numAvailableImportExportSlots.get() )
                                        {
                                            throw new RuntimeException( "No slot available." );
                                        }
                                        numAvailableImportExportSlots.decrementAndGet();
                                        moveListeners.put( 
                                                (UUID)args[ 0 ],
                                                (TapeMoveListener)args[ 2 ] );
                                        return Boolean.TRUE;
                                    }
                                },
                                MockInvocationHandler.forMethod(
                                        methodIsSlotAvailable, 
                                        new ConstantResponseInvocationHandler( Boolean.TRUE ),
                                        null ) ) ) );

        final EjectTapeProcessor ejector = new EjectTapeProcessor( 
                dbSupport.getServiceManager(), 
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, btih ),
                InterfaceProxyFactory.getProxy( TapeEnvironment.class, btih ),
                10 );
        
        service.update( tape1.setEjectPending( new Date( 1000 ) ), Tape.EJECT_PENDING );
        service.update( tape2.setEjectPending( new Date( 2000 ) ), Tape.EJECT_PENDING );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Should notta been any notifications yet."
                 );
        ejector.schedule();

        validationCompleted( tape1.getId(), moveListeners, tape1.getId(), null, 2000 );
        moveListeners.get( tape1.getId() ).moveSucceeded( tape1.getId() );

        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Should notta been any notifications."
                 );
        assertEquals(
                TapeState.EJECT_FROM_EE_PENDING,
                dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape1.getId() ).getState(),
                "Should upgraded tape1's state to pending eject from EE."
                );
        assertEquals(
                TapeState.NORMAL,
                dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape2.getId() ).getState(),
                "Shoulda kept tape2 in a state of not yet ready to be ejected."
                 );
        assertNotNull(
                dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape2.getId() )
                        .getEjectPending(),
                "Shoulda kept tape2 in a state of not yet ready to be ejected."
                );
        ejector.shutdown();
    }
    
    @Test
    public void testTapeEjectEventuallyFailsDeletesEjectNotificationAndReschedulesTapeForEject()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );

        final AtomicInteger numAvailableImportExportSlots = new AtomicInteger( 2 );
        final Map< UUID, TapeMoveListener > moveListeners = new ConcurrentHashMap<>();
        final Method methodIsSlotAvailable =
                ReflectUtil.getMethod( TapeEnvironment.class, "isSlotAvailable" );
        final Method methodGetTaskStateLock =
                ReflectUtil.getMethod( TapeBlobStoreProcessor.class, "getTaskStateLock" );
        final Method methodMoveTape = 
                ReflectUtil.getMethod( TapeEnvironment.class, "moveTapeToSlot" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( 
                MockInvocationHandler.forMethod(
                        methodGetTaskStateLock,
                        new ConstantResponseInvocationHandler( new Object() ), 
                        MockInvocationHandler.forMethod( 
                                methodMoveTape, 
                                new InvocationHandler()
                                {
                                    public Object invoke( Object proxy, Method method, Object[] args )
                                            throws Throwable
                                    {
                                        if ( ElementAddressType.IMPORT_EXPORT != args[ 1 ] )
                                        {
                                            throw new RuntimeException(
                                                    "Shoulda moved tape to an EE slot." );
                                        }
                                        if ( 0 == numAvailableImportExportSlots.get() )
                                        {
                                            throw new RuntimeException( "No slot available." );
                                        }
                                        numAvailableImportExportSlots.decrementAndGet();
                                        moveListeners.put( 
                                                (UUID)args[ 0 ],
                                                (TapeMoveListener)args[ 2 ] );
                                        return Boolean.TRUE;
                                    }
                                },
                                MockInvocationHandler.forMethod(
                                        methodIsSlotAvailable, 
                                        new ConstantResponseInvocationHandler( Boolean.TRUE ),
                                        null ) ) ) );

        final EjectTapeProcessor ejector = new EjectTapeProcessor( 
                dbSupport.getServiceManager(), 
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, btih ),
                InterfaceProxyFactory.getProxy( TapeEnvironment.class, btih ),
                10 );
        
        service.update( tape1.setEjectPending( new Date( 1000 ) ), Tape.EJECT_PENDING );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Should notta been any notifications yet."
                );
        ejector.schedule();

        validationCompleted( tape1.getId(), moveListeners, tape1.getId(), null, 2000 );
        moveListeners.get( tape1.getId() ).moveSucceeded( tape1.getId() );
        assertEquals(
                TapePartitionFailureType.TAPE_EJECTION_BY_OPERATOR_REQUIRED,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class )
                        .attain( Require.nothing() ).getType(),
                "Shoulda been a notification generated."
                 );

        service.update( tape2.setEjectPending( new Date( 2000 ) ), Tape.EJECT_PENDING );
        validationCompleted( tape2.getId(), moveListeners, tape2.getId(), null, 2000 );
        TestUtil.sleep( 150 );
        assertEquals(
                TapeState.EJECT_TO_EE_IN_PROGRESS,
                dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape2.getId() ).getState(),
                "Shoulda started moving tape2 to eject it."
                );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "The fact that we started another eject in progress means notification shoulda been whacked."
                 );
        validationCompleted( tape1.getId(), moveListeners, tape2.getId(), null, 2000 );
        moveListeners.get( tape2.getId() ).moveFailed( tape2.getId() );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Should notta been any notifications."
                );
        assertEquals(
                TapeState.EJECT_FROM_EE_PENDING,
                dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape1.getId() ).getState(),
                "Should upgraded tape1's state to pending eject from EE."
                 );
        assertEquals(
                TapeState.NORMAL,
                dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape2.getId() ).getState(),
                "Shoulda kept tape2 in a state of not yet ready to be ejected."
                 );
        assertNotNull(
                dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape2.getId() )
                        .getEjectPending(),
                "Shoulda kept tape2 in a state of not yet ready to be ejected."
                );
        ejector.shutdown();
    }
    
    @Test
    public void testWillNotOnlineTapeUntilTapePartitionInStateToAllowIt()
    {
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        
        final TapePartition partition = mockDaoDriver.attainOneAndOnly( TapePartition.class );
        mockDaoDriver.updateBean( partition.setQuiesced( Quiesced.PENDING ), TapePartition.QUIESCED );
        mockDaoDriver.updateBean( partition.setState( TapePartitionState.ERROR ), TapePartition.STATE );

        final AtomicInteger numAvailableImportExportSlots = new AtomicInteger( 1 );
        final Map< UUID, TapeMoveListener > moveListeners = new ConcurrentHashMap<>();
        final Method methodIsSlotAvailable =
                ReflectUtil.getMethod( TapeEnvironment.class, "isSlotAvailable" );
        final Method methodGetTaskStateLock =
                ReflectUtil.getMethod( TapeBlobStoreProcessor.class, "getTaskStateLock" );
        final Method methodMoveTape = 
                ReflectUtil.getMethod( TapeEnvironment.class, "moveTapeToSlot" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( 
                MockInvocationHandler.forMethod(
                        methodGetTaskStateLock,
                        new ConstantResponseInvocationHandler( new Object() ), 
                        MockInvocationHandler.forMethod( 
                                methodMoveTape, 
                                new InvocationHandler()
                                {
                                    public Object invoke( Object proxy, Method method, Object[] args )
                                            throws Throwable
                                    {
                                        if ( ElementAddressType.IMPORT_EXPORT != args[ 1 ] )
                                        {
                                            throw new RuntimeException(
                                                    "Shoulda moved tape to an EE slot." );
                                        }
                                        if ( 0 == numAvailableImportExportSlots.get() )
                                        {
                                            throw new RuntimeException( "No slot available." );
                                        }
                                        numAvailableImportExportSlots.decrementAndGet();
                                        moveListeners.put( 
                                                (UUID)args[ 0 ],
                                                (TapeMoveListener)args[ 2 ] );
                                        return Boolean.TRUE;
                                    }
                                },
                                MockInvocationHandler.forMethod(
                                        methodIsSlotAvailable, 
                                        new ConstantResponseInvocationHandler( Boolean.TRUE ),
                                        null ) ) ) );

        final EjectTapeProcessor ejector = new EjectTapeProcessor( 
                dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, btih ),
                InterfaceProxyFactory.getProxy( TapeEnvironment.class, btih ),
                10 );
        
        service.update( tape1.setEjectPending( new Date( 1000 ) ), Tape.EJECT_PENDING );
        service.update( tape2.setEjectPending( new Date( 2000 ) ), Tape.EJECT_PENDING );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Should notta been any notifications yet."
                );
        ejector.schedule();
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
        
        mockDaoDriver.updateBean( partition.setState( TapePartitionState.ONLINE ), TapePartition.STATE );
        mockDaoDriver.updateBean( partition.setQuiesced( Quiesced.NO ), TapePartition.QUIESCED );
        int i = 0;
        while( 20 > i && moveListeners.isEmpty() )
        {
            TestUtil.sleep( 50 );
            ++i;
        }
        assertFalse(
                moveListeners.isEmpty(),
                "Shoulda been able to start onlining due to good partition's state."
                 );

        validationCompleted( tape1.getId(), moveListeners, tape1.getId(), null, 2000 );
        moveListeners.get( tape1.getId() ).moveSucceeded( tape1.getId() );

        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class ).getCount(),
                "Should notta been any notifications."
                 );
        assertEquals(
                TapeState.EJECT_FROM_EE_PENDING,
                dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape1.getId() ).getState(),
                "Should upgraded tape1's state to pending eject from EE."
                 );
        assertEquals(
                TapeState.NORMAL,
                dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape2.getId() ).getState(),
                "Shoulda kept tape2 in a state of not yet ready to be ejected."
                 );
        assertNotNull(
                dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape2.getId() )
                        .getEjectPending(),
                "Shoulda kept tape2 in a state of not yet ready to be ejected."
                 );
        ejector.shutdown();
    }
    
    
    private void validationCompleted(
            final UUID listenerKey,
            final Map< UUID, TapeMoveListener > moveListeners,
            final UUID tapeId, 
            final RuntimeException failure,
            final int maxWaitInMillis )
    {
        final Duration duration = new Duration();
        do
        {
            if ( moveListeners.containsKey( listenerKey ) )
            {
                moveListeners.get( listenerKey ).validationCompleted( tapeId, failure );
                return;
            }
            TestUtil.sleep( 10 );
        } while ( duration.getElapsedMillis() < maxWaitInMillis );
        
        fail( "Timeout occurred waiting for move listener key " + listenerKey + "." );
    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    @BeforeAll
    public static void resetDb() { dbSupport.reset(); }

    @AfterEach
    public void tearDown() throws InterruptedException {
        if (!SystemWorkPool.getInstance().awaitEmpty(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("System work pool did not empty in a timely manner.");
        }
        dbSupport.reset();
    }
}

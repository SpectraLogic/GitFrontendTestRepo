/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.tape.ElementAddressType;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeBlobStoreProcessor;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeMoveListener;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironment;
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

public final class ForceRemovalTapeProcessor_Test
{

    /* Aped from EjectTapeProcessor_Test.java */
    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new ForceRemovalTapeProcessor(
                        null, 
                        InterfaceProxyFactory.getProxy( TapeEnvironment.class, null),
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
                new ForceRemovalTapeProcessor(
                        new MockBeansServiceManager(), 
                        null,
                        1 );
            }
        } );
    }

    @Test
    public void testHappyConstruction()
    {
        final ForceRemovalTapeProcessor remover = new ForceRemovalTapeProcessor(
                new MockBeansServiceManager(), 
                InterfaceProxyFactory.getProxy( TapeEnvironment.class, null ),
                1 );
        remover.shutdown();
    }

    @Test
    public void testTapesAreRemovedFromRequestingDrives()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape0 = mockDaoDriver.createTape( TapeState.NORMAL );
        final TapePartition partition0 = mockDaoDriver.attainOneAndOnly( TapePartition.class );
        final TapeDrive tapeDrive0 = mockDaoDriver.createTapeDrive( partition0.getId(), "TESTTAPE" );

        tapeDrive0.setTapeId( tape0.getId() );
        tapeDrive0.setForceTapeRemoval( true );

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
                                                
                                                if ( ElementAddressType.STORAGE != args[ 1 ] )
                                                {
                                                    throw new RuntimeException(
                                                            "Shoulda moved tape to a STORAGE slot." );
                                                }
                                                moveListeners.put( 
                                                        (UUID)args[ 0 ],
                                                        (TapeMoveListener)args[ 2 ] );
                                                ( (TapeMoveListener)args[ 2 ] ).validationCompleted(
                                                        (UUID)args[ 0 ], null );
                                                return Boolean.TRUE;
                                            }
                                        },
                                        /* Override isSlotAvailable to always return true */
                                        MockInvocationHandler.forMethod(
                                                methodIsSlotAvailable, 
                                                new ConstantResponseInvocationHandler( Boolean.TRUE ),
                                                null ) ) ) ) );
        //test must be expanded for BLKP-3720
        final ForceRemovalTapeProcessor remover = new ForceRemovalTapeProcessor( 
                dbSupport.getServiceManager(), 
                InterfaceProxyFactory.getProxy( TapeEnvironment.class, btih ),
                10 );
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
// Local Variables:
// indent-tabs-mode: nil
// End:

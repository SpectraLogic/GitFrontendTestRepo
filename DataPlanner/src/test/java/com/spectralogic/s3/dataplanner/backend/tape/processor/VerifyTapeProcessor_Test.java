/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeTaskQueue;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;


import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeBlobStoreProcessor;
import com.spectralogic.s3.dataplanner.backend.tape.task.VerifyTapeTask;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.mock.BasicTestsInvocationHandler.MethodInvokeData;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

public final class VerifyTapeProcessor_Test
{
    @Test
    public void testConstructorNullProcessorNotAllowed()
    {
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new VerifyTapeProcessor(
                        null,
                        dbSupport.getServiceManager(),
                        new MockDiskManager( dbSupport.getServiceManager() ), tapeFailureManagement, 1000 );
            }
        } );
    }
    
    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new VerifyTapeProcessor(
                        InterfaceProxyFactory.getProxy( TapeBlobStoreProcessor.class, null ), 
                        null,
                        new MockDiskManager( dbSupport.getServiceManager() ), tapeFailureManagement, 1000 );
            }
        } );
    }
    
    @Test
    public void testVerifiesScheduledAsTapesEligibleToBeVerified()
    {
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final Tape tape1 = mockDaoDriver.createTape( null, TapeState.NORMAL, TapeType.LTO_CLEANING_TAPE );
        final Tape tape2 = mockDaoDriver.createTape( null, TapeState.EJECTED, TapeType.LTO5 );
        final Tape tape3 = mockDaoDriver.createTape( null, TapeState.NORMAL, TapeType.LTO5 );
        final Tape tape4 = mockDaoDriver.createTape( null, TapeState.NORMAL, TapeType.LTO5 );
        final Tape tape5 = mockDaoDriver.createTape( null, TapeState.BAR_CODE_MISSING, TapeType.LTO5 );
        final Tape tape6 = mockDaoDriver.createTape( null, TapeState.PENDING_INSPECTION, TapeType.LTO5 );
        mockDaoDriver.updateBean(
                tape1.setVerifyPending( BlobStoreTaskPriority.NORMAL ),
                Tape.VERIFY_PENDING );
        mockDaoDriver.updateBean(
                tape2.setVerifyPending( BlobStoreTaskPriority.NORMAL ),
                Tape.VERIFY_PENDING );
        mockDaoDriver.updateBean(
                tape3.setVerifyPending( BlobStoreTaskPriority.HIGH ),
                Tape.VERIFY_PENDING );
        mockDaoDriver.updateBean(
                tape4.setVerifyPending( null ),
                Tape.VERIFY_PENDING );
        mockDaoDriver.updateBean(
                tape5.setVerifyPending( BlobStoreTaskPriority.NORMAL ),
                Tape.VERIFY_PENDING );
        mockDaoDriver.updateBean(
                tape6.setVerifyPending( BlobStoreTaskPriority.BACKGROUND ),
                Tape.VERIFY_PENDING );
        
        final Method mAdd =
                ReflectUtil.getMethod( TapeTaskQueue.class, "addStaticTask" );
        final Method mGet =
                ReflectUtil.getMethod( TapeTaskQueue.class, "get" );
        final Method mTryPriorityUpdate =
                ReflectUtil.getMethod( TapeTaskQueue.class, "tryPriorityUpdate" );
        final Method mGetTaskStateLock = 
                ReflectUtil.getMethod( TapeBlobStoreProcessor.class, "getTaskStateLock" );
        final BasicTestsInvocationHandler btihQueue = new BasicTestsInvocationHandler( null );
        final TapeTaskQueue queue = InterfaceProxyFactory.getProxy( TapeTaskQueue.class, btihQueue );
        
        final BasicTestsInvocationHandler btihProcessor = new BasicTestsInvocationHandler( 
                MockInvocationHandler.forReturnType(
                        TapeTaskQueue.class,
                        new ConstantResponseInvocationHandler( queue ), 
                        MockInvocationHandler.forMethod( 
                                mGetTaskStateLock, 
                                new ConstantResponseInvocationHandler( this ),
                                null ) ) );
        final TapeBlobStoreProcessor bsp = InterfaceProxyFactory.getProxy(
                TapeBlobStoreProcessor.class, 
                btihProcessor );
        
        final VerifyTapeProcessor processor = new VerifyTapeProcessor(
                bsp, 
                dbSupport.getServiceManager(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                tapeFailureManagement, 50 );

        processor.schedule();
        
        final Map< Method, Integer > expectedCalls = new HashMap<>();
        expectedCalls.put( mAdd, Integer.valueOf( 2 ) );
        expectedCalls.put( mTryPriorityUpdate, Integer.valueOf( 2 ) );
        expectedCalls.put( mGet, null );
        btihQueue.eventuallyVerifyMethodInvocations( expectedCalls, 1000 );
        
        TestUtil.sleep( 300 );
        
        assertNull(
                mockDaoDriver.attain( tape1 ).getVerifyPending(),
                "Shoulda cleared verify pending for tapes that can't be verified."
                 );
        assertNull(
                mockDaoDriver.attain( tape2 ).getVerifyPending(),
                "Shoulda cleared verify pending for tapes that can't be verified."
                 );
        assertNotNull(
                mockDaoDriver.attain( tape3 ).getVerifyPending(),
                "Shoulda cleared verify pending for tapes that can't be verified."
                 );
        assertNull(
                mockDaoDriver.attain( tape4 ).getVerifyPending(),
                "Shoulda cleared verify pending for tapes that can't be verified."
                 );
        assertNull(
                mockDaoDriver.attain( tape5 ).getVerifyPending(),
                "Shoulda cleared verify pending for tapes that can't be verified."
                 );
        assertNotNull(
                mockDaoDriver.attain( tape6 ).getVerifyPending(),
                "Shoulda cleared verify pending for tapes that can't be verified."
                 );
        
        final Set< BlobStoreTaskPriority > priorities = new HashSet<>();
        for ( final MethodInvokeData mid : btihQueue.getMethodInvokeData( mAdd ) )
        {
            priorities.add( ( (VerifyTapeTask)mid.getArgs().get( 0 ) ).getPriority() );
        }
        assertEquals(
                CollectionFactory.toSet( BlobStoreTaskPriority.BACKGROUND, BlobStoreTaskPriority.HIGH ),
                priorities,
                "Shoulda used BACKGROUND as default priority and respected explicit priority."
                 );
        
        assertNotNull( processor,"Don't prematurely gc me."  );
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

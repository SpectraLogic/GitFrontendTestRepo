/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main.move.strategy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.tape.ElementAddressType;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironmentManager;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeMoveStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class UnloadTapeFromDriveTapeMoveStrategy_Test
{

    @Test
    public void testConstructorNullTapeDriveNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new UnloadTapeFromDriveTapeMoveStrategy(
                        null,
                        ElementAddressType.IMPORT_EXPORT );
            }
        } );
    }
    
    @Test
    public void testConstructorNullDestinationElementAddressTypeNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new UnloadTapeFromDriveTapeMoveStrategy(
                        BeanFactory.newBean( TapeDrive.class ),
                        null );
            }
        } );
    }
    
    @Test
    public void testSuccessfulMoveResultsInTapeEnvironmentChanges() 
            throws NoSuchMethodException, SecurityException
    {
        final Method methodMoveTapeFromDrive = TapeEnvironmentManager.class.getMethod( 
                "moveTapeFromDrive", TapeDrive.class, ElementAddressType.class );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler(
                MockInvocationHandler.forMethod(
                        methodMoveTapeFromDrive,
                        new ConstantResponseInvocationHandler( Integer.valueOf( 200 ) ),
                        null ) );
        final TapeEnvironmentManager tapeEnvironmentManager = 
                InterfaceProxyFactory.getProxy( TapeEnvironmentManager.class, btih );
        final TapeDrive drive = BeanFactory.newBean( TapeDrive.class );
        drive.setId( UUID.randomUUID() );
        final Tape tape = BeanFactory.newBean( Tape.class );
        tape.setId( UUID.randomUUID() );
        final TapeMoveStrategy strategy = 
                new UnloadTapeFromDriveTapeMoveStrategy( drive, ElementAddressType.IMPORT_EXPORT );
        assertEquals(200,  strategy.getDest(100, tape, tapeEnvironmentManager), "Shoulda moved to element address returned by tape environment manager.");
        strategy.moveSucceeded();
        
        final Map< Method, Integer > expectedCallCounts = new HashMap<>();
        expectedCallCounts.put( methodMoveTapeFromDrive, Integer.valueOf( 1 ) );
        btih.verifyMethodInvocations( expectedCallCounts );

        assertEquals(drive, btih.getMethodInvokeData( methodMoveTapeFromDrive ).get( 0 ).getArgs().get( 0 ), "Shoulda constructed request to tape environment manager correctly.");
        assertEquals(ElementAddressType.IMPORT_EXPORT, btih.getMethodInvokeData( methodMoveTapeFromDrive ).get( 0 ).getArgs().get( 1 ), "Shoulda constructed request to tape environment manager correctly.");
    }
    
    @Test
    public void testMoveFailedResultsInStateRollback() 
            throws NoSuchMethodException, SecurityException
    {
        final Method methodMoveTapeFromDrive = TapeEnvironmentManager.class.getMethod( 
                "moveTapeFromDrive", TapeDrive.class, ElementAddressType.class );
        final Method methodMoveTapeToDrive = TapeEnvironmentManager.class.getMethod( 
                "moveTapeToDrive", UUID.class, TapeDrive.class );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler(
                MockInvocationHandler.forMethod(
                        methodMoveTapeFromDrive,
                        new ConstantResponseInvocationHandler( Integer.valueOf( 200 ) ),
                        null ) );
        final TapeEnvironmentManager tapeEnvironmentManager = 
                InterfaceProxyFactory.getProxy( TapeEnvironmentManager.class, btih );
        final TapeDrive drive = BeanFactory.newBean( TapeDrive.class );
        drive.setId( UUID.randomUUID() );
        final Tape tape = BeanFactory.newBean( Tape.class );
        tape.setId( UUID.randomUUID() );
        final TapeMoveStrategy strategy = 
                new UnloadTapeFromDriveTapeMoveStrategy( drive, ElementAddressType.IMPORT_EXPORT );
        assertEquals(200,  strategy.getDest(100, tape, tapeEnvironmentManager), "Shoulda moved to element address returned by tape environment manager.");
        strategy.moveFailed( tape );
        
        final Map< Method, Integer > expectedCallCounts = new HashMap<>();
        expectedCallCounts.put( methodMoveTapeFromDrive, Integer.valueOf( 1 ) );
        expectedCallCounts.put( methodMoveTapeToDrive, Integer.valueOf( 1 ) );
        btih.verifyMethodInvocations( expectedCallCounts );

        assertEquals(drive, btih.getMethodInvokeData( methodMoveTapeFromDrive ).get( 0 ).getArgs().get( 0 ), "Shoulda constructed request to tape environment manager correctly.");
        assertEquals(ElementAddressType.IMPORT_EXPORT, btih.getMethodInvokeData( methodMoveTapeFromDrive ).get( 0 ).getArgs().get( 1 ), "Shoulda constructed request to tape environment manager correctly.");

        final Object expected = tape.getId();
        assertEquals(expected, btih.getMethodInvokeData( methodMoveTapeToDrive ).get( 0 ).getArgs().get( 0 ), "Shoulda constructed rollback request to tape environment manager correctly.");
        assertEquals(drive, btih.getMethodInvokeData( methodMoveTapeToDrive ).get( 0 ).getArgs().get( 1 ), "Shoulda constructed rollback request to tape environment manager correctly.");
    }
    
    @Test
    public void testGetDestFailedResultsInNoStateRollback() 
            throws NoSuchMethodException, SecurityException
    {
        final Method methodMoveTapeFromDrive = TapeEnvironmentManager.class.getMethod( 
                "moveTapeFromDrive", TapeDrive.class, ElementAddressType.class );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler(
                new InvocationHandler()
                {
                    public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
                    {
                        throw new RuntimeException( "Cannot perform requested operation." );
                    }
                } );
        final TapeEnvironmentManager tapeEnvironmentManager = 
                InterfaceProxyFactory.getProxy( TapeEnvironmentManager.class, btih );
        final TapeDrive drive = BeanFactory.newBean( TapeDrive.class );
        drive.setId( UUID.randomUUID() );
        final Tape tape = BeanFactory.newBean( Tape.class );
        tape.setId( UUID.randomUUID() );
        final TapeMoveStrategy strategy = 
                new UnloadTapeFromDriveTapeMoveStrategy( drive, ElementAddressType.IMPORT_EXPORT );
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                strategy.getDest( 100, tape, tapeEnvironmentManager );
            }
        } );
        strategy.moveFailed( tape );
        
        final Map< Method, Integer > expectedCallCounts = new HashMap<>();
        expectedCallCounts.put( methodMoveTapeFromDrive, Integer.valueOf( 1 ) );
        btih.verifyMethodInvocations( expectedCallCounts );
    }
}

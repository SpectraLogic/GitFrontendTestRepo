/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main.move.strategy;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;



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

public final class LoadTapeIntoDriveTapeMoveStrategy_Test
{

    @Test
    public void testConstructorNullTapeDriveNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new LoadTapeIntoDriveTapeMoveStrategy( null );
            }
        } );
    }
    
    @Test
    public void testSuccessfulMoveResultsInTapeEnvironmentChanges() 
            throws NoSuchMethodException, SecurityException
    {
        final Method methodMoveTapeToDrive = TapeEnvironmentManager.class.getMethod( 
                "moveTapeToDrive", UUID.class, TapeDrive.class );
        final Method methodGetDriveElementAddress = TapeEnvironmentManager.class.getMethod( 
                "getDriveElementAddress", UUID.class );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler(
                MockInvocationHandler.forMethod(
                        methodGetDriveElementAddress,
                        new ConstantResponseInvocationHandler( Integer.valueOf( 200 ) ),
                        null ) );
        final TapeEnvironmentManager tapeEnvironmentManager = 
                InterfaceProxyFactory.getProxy( TapeEnvironmentManager.class, btih );
        final TapeDrive drive = BeanFactory.newBean( TapeDrive.class );
        drive.setId( UUID.randomUUID() );
        final Tape tape = BeanFactory.newBean( Tape.class );
        tape.setId( UUID.randomUUID() );
        final TapeMoveStrategy strategy = 
                new LoadTapeIntoDriveTapeMoveStrategy( drive );
        assertEquals(200,  strategy.getDest(100, tape, tapeEnvironmentManager), "Shoulda moved to element address returned by tape environment manager.");
        strategy.moveSucceeded();
        
        final Map< Method, Integer > expectedCallCounts = new HashMap<>();
        expectedCallCounts.put( methodMoveTapeToDrive, Integer.valueOf( 1 ) );
        expectedCallCounts.put( methodGetDriveElementAddress, Integer.valueOf( 1 ) );
        btih.verifyMethodInvocations( expectedCallCounts );

        final Object expected = tape.getId();
        assertEquals(expected, btih.getMethodInvokeData( methodMoveTapeToDrive ).get( 0 ).getArgs().get( 0 ), "Shoulda constructed request to tape environment manager correctly.");
        assertEquals(drive, btih.getMethodInvokeData( methodMoveTapeToDrive ).get( 0 ).getArgs().get( 1 ), "Shoulda constructed request to tape environment manager correctly.");
    }
    
    @Test
    public void testMoveFailedResultsInStateRollback() throws NoSuchMethodException, SecurityException 
    {
        final Method methodGetDriveElementAddress = TapeEnvironmentManager.class.getMethod( 
                "getDriveElementAddress", UUID.class );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler(
                MockInvocationHandler.forMethod(
                        methodGetDriveElementAddress,
                        new ConstantResponseInvocationHandler( Integer.valueOf( 200 ) ),
                        null ) );
        final TapeEnvironmentManager tapeEnvironmentManager = 
                InterfaceProxyFactory.getProxy( TapeEnvironmentManager.class, btih );
        final TapeDrive drive = BeanFactory.newBean( TapeDrive.class );
        drive.setId( UUID.randomUUID() );
        final Tape tape = BeanFactory.newBean( Tape.class );
        tape.setId( UUID.randomUUID() );
        final TapeMoveStrategy strategy = 
                new LoadTapeIntoDriveTapeMoveStrategy( drive );
        assertEquals(200,  strategy.getDest(100, tape, tapeEnvironmentManager), "Shoulda moved to element address returned by tape environment manager.");
        strategy.moveFailed( tape );
        
        final Map< Method, Integer > expectedCallCounts = new HashMap<>();
        expectedCallCounts.put( methodGetDriveElementAddress, Integer.valueOf( 1 ) );
        btih.verifyMethodInvocations( expectedCallCounts );
    }
}

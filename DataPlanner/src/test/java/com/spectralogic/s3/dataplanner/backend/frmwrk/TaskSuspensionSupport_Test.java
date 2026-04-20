/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.frmwrk;



import com.spectralogic.s3.dataplanner.backend.frmwrk.TaskSuspensionSupport.TaskFailedDueToTooManyRetriesException;
import com.spectralogic.s3.dataplanner.backend.frmwrk.TaskSuspensionSupport.TaskSuspended;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class TaskSuspensionSupport_Test 
{
    @Test
    public void testConstructorMaxRetriesBeforeSuspensionRequiredOfZeroNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new TaskSuspensionSupport( 0, null, 1, 0 );
                }
            } );
    }
    
    @Test
    public void testConstructorTotalMaxRetriesBeforeFailure()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new TaskSuspensionSupport( 2, Integer.valueOf( 1 ), 1, 0 );
                }
            } );
    }
    

    @Test
    public void testConstructorSuspensionDurationOfZeroMillisNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new TaskSuspensionSupport( 1, null, 0, 0 );
                }
            } );
    }
    

    @Test
    public void testConstructorSuspensionStepOfNegativeMillisNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new TaskSuspensionSupport( 1, null, 1, -1 );
                }
            } );
    }
    

    @Test
    public void testHappyConstruction() throws TaskFailedDueToTooManyRetriesException
    {
        new TaskSuspensionSupport( 1, null, 1, 0 ).failureOccurred();
    }
    

    @Test
    public void testStateTransitionsAsExpected() throws TaskFailedDueToTooManyRetriesException
    {
        final TaskSuspensionSupport support = new TaskSuspensionSupport( 4, Integer.valueOf( 9 ), 100, 150 );
        assertEquals(TaskSuspended.NO, support.getState(), "Shoulda reported not suspended originally.");
        support.failureOccurred();
        assertEquals(TaskSuspended.NO, support.getState(), "Should notta suspended task yet.");
        support.failureOccurred();
        assertEquals(TaskSuspended.NO, support.getState(), "Should notta suspended task yet.");
        support.failureOccurred();
        assertEquals(TaskSuspended.NO, support.getState(), "Should notta suspended task yet.");
        support.failureOccurred();
        assertEquals(TaskSuspended.YES, support.getState(), "Shoulda suspended task.");
        TestUtil.sleep( 10 );
        assertEquals(TaskSuspended.YES, support.getState(), "Shoulda suspended task.");
        TestUtil.sleep( 200 );
        assertEquals(TaskSuspended.NO, support.getState(), "Task shoulda been un-suspended.");

        support.failureOccurred();
        assertEquals(TaskSuspended.NO, support.getState(), "Should notta suspended task yet.");
        support.failureOccurred();
        assertEquals(TaskSuspended.NO, support.getState(), "Should notta suspended task yet.");
        support.failureOccurred();
        assertEquals(TaskSuspended.NO, support.getState(), "Should notta suspended task yet.");
        support.failureOccurred();
        assertEquals(TaskSuspended.YES, support.getState(), "Shoulda suspended task.");
        assertEquals(TaskSuspended.YES, support.getState(), "Shoulda suspended task.");
        TestUtil.sleep( 125 );
        assertEquals(TaskSuspended.YES, support.getState(), "Task shoulda still been suspended.");
        TestUtil.sleep( 300 );
        assertEquals(TaskSuspended.NO, support.getState(), "Task shoulda been un-suspended.");

        TestUtil.assertThrows(
                "9th failure shoulda triggered fatal error.",
                TaskFailedDueToTooManyRetriesException.class, 
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        support.failureOccurred();
                    }
                } );
    }
}

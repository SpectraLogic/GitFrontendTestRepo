/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main;

import java.util.UUID;

import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public final class TapePartitionMoveFailureSupport_Test
{
    @Test
    public void testConstructorNegativeSuspensionNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new TapePartitionMoveFailureSupport( -1 );
            }
        } );
        new TapePartitionMoveFailureSupport( 0 );
        new TapePartitionMoveFailureSupport( 1 );
    }
    
    @Test
    public void testMoveFailureOccurredNullPartitionIdNotAllowed()
    {
        final TapePartitionMoveFailureSupport support = new TapePartitionMoveFailureSupport( 0 );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                support.moveFailureOccurred( null );
            }
        } );
    }
    
    @Test
    public void testIsTaskExecutionSuspendedNullPartitionIdNotAllowed()
    {
        final TapePartitionMoveFailureSupport support = new TapePartitionMoveFailureSupport( 100 );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                support.isTaskExecutionSuspended( null );
            }
        } );
    }
    
    @Test
    public void testClearMoveFailureNullPartitionIdNotAllowed()
    {
        final TapePartitionMoveFailureSupport support = new TapePartitionMoveFailureSupport( 100 );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                support.clearMoveFailure( null );
            }
        } );
    }
    
    @Test
    public void testIsTaskExecutionSuspendedReportsSuspensionProperly()
    {
        final UUID partition1 = UUID.randomUUID();
        final UUID partition2 = UUID.randomUUID();
        final UUID partition3 = UUID.randomUUID();
        final UUID partition4 = UUID.randomUUID();
        final UUID partition5 = UUID.randomUUID();
        final TapePartitionMoveFailureSupport support = new TapePartitionMoveFailureSupport( 100 );
        support.moveFailureOccurred( partition2 );
        support.moveFailureOccurred( partition3 );
        support.moveFailureOccurred( partition4 );
        assertFalse(support.isTaskExecutionSuspended( partition1 ), "Should notta reported suspension for partition without move failure.");
        assertTrue(support.isTaskExecutionSuspended( partition2 ), "Shoulda reported suspension for partition with recent move failure.");
        assertTrue(support.isTaskExecutionSuspended( partition3 ), "Shoulda reported suspension for partition with recent move failure.");
        assertTrue(support.isTaskExecutionSuspended( partition4 ), "Shoulda reported suspension for partition with recent move failure.");

        for ( int i = 0; i < 15; ++i )
        {
            TestUtil.sleep( 10 );
            support.moveFailureOccurred( partition3 );
        }

        support.moveFailureOccurred( partition5 );
        TestUtil.sleep( 10 );
        support.moveFailureOccurred( partition4 );
        support.clearMoveFailure( partition5 );

        assertFalse(support.isTaskExecutionSuspended( partition1 ), "Should notta reported suspension for partition without move failure.");
        assertFalse(support.isTaskExecutionSuspended( partition2 ), "Should notta reported suspension for partition with old move failure.");
        assertTrue(support.isTaskExecutionSuspended( partition3 ), "Shoulda reported suspension for partition with recent move failure.");
        assertTrue(support.isTaskExecutionSuspended( partition4 ), "Shoulda reported suspension for partition with recent move failure.");
        assertFalse(support.isTaskExecutionSuspended( partition5 ), "Should notta reported suspension for partition with cleared move failure.");
    }
}

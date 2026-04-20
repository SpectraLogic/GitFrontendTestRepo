/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.frmwrk;

import java.util.UUID;

import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public final class OfflineDurationTracker_Test
{
    @Test
    public void testUpdateNullTargetNotAllowed()
    {
        final OfflineDurationTracker tracker = new OfflineDurationTracker();
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    tracker.update( null, true );
                }
            } );
    }
    
    
    @Test
    public void testGetOfflineDurationNullTargetNotAllowed()
    {
        final OfflineDurationTracker tracker = new OfflineDurationTracker();
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    tracker.getOfflineDuration( null );
                }
            } );
    }
    
    
    @Test
    public void testUpdateDoesSo()
    {
        final UUID id1 = UUID.randomUUID();
        final UUID id2 = UUID.randomUUID();
        final OfflineDurationTracker tracker = new OfflineDurationTracker();
        assertNull(tracker.getOfflineDuration( id1 ), "Should notta been offline.");
        assertNull(tracker.getOfflineDuration( id2 ), "Should notta been offline.");

        tracker.update( id1, true );
        tracker.update( id2, false );

        final String message2 = String.valueOf( tracker.getOfflineDuration(id1));
        assertNotNull(message2,"Shoulda been offline.");
        assertNull(tracker.getOfflineDuration( id2 ), "Should notta been offline.");

        tracker.update( id1, true );
        tracker.update( id2, false );

        final String message1 = String.valueOf(tracker.getOfflineDuration(id1)) ;
        assertNotNull(message1,"Shoulda been offline.");
        assertNull(tracker.getOfflineDuration( id2 ), "Should notta been offline.");

        tracker.update( id1, false );
        tracker.update( id2, true );

        assertNull(tracker.getOfflineDuration( id1 ), "Should notta been offline.");
        final String message = String.valueOf(tracker.getOfflineDuration(id2)) ;
        assertNotNull(message,"Shoulda been offline.");

        TestUtil.sleep( 20 );
        
        tracker.update( id1, false );
        tracker.update( id2, true );
        
        assertTrue(
                10 < tracker.getOfflineDuration( id2 ).getElapsedMillis(),
                "Shoulda reported correct duration."
                 );
    }
}

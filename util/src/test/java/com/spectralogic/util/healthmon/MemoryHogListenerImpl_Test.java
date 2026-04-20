/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.healthmon;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class MemoryHogListenerImpl_Test
{
    @Test
    public void testNegativeThresholdsNotAllowed()
    {
        try
        {
            new MemoryHogListenerImpl( -.00001, 0 );
            fail( "Shoulda thrown exception." ); 
        }
        catch ( final IllegalArgumentException ex )
        {
            assertNotNull( ex,
                    "Shoulda thrown exception." );
        }

        try
        {
            new MemoryHogListenerImpl( 0, -.00001 );
            fail( "Shoulda thrown exception." ); 
        }
        catch ( final IllegalArgumentException ex )
        {
            assertNotNull( ex,
                    "Shoulda thrown exception." );
        }
    }
    
    
    @Test
    public void testThresholdsAbove100PercentNotAllowed()
    {
        try
        {
            new MemoryHogListenerImpl( 0, 1.001 );
            fail( "Shoulda thrown exception." ); 
        }
        catch ( final IllegalArgumentException ex )
        {
            assertNotNull( ex,
                    "Shoulda thrown exception.");
        }
        
        try
        {
            new MemoryHogListenerImpl( 1.001, 1.001 );
            fail( "Shoulda thrown exception." ); 
        }
        catch ( final IllegalArgumentException ex )
        {
            assertNotNull( ex,
                    "Shoulda thrown exception." );
        }
    }
    
    
    @Test
    public void testInfoThresholdGreaterThanWarnThresholdNotAllowed()
    {
        try
        {
            new MemoryHogListenerImpl( .55, .5 );
            fail( "Shoulda thrown exception." ); 
        }
        catch ( final IllegalArgumentException ex )
        {
            assertNotNull( ex,
                    "Shoulda thrown exception." );
        }
    }
    
    
    @Test
    public void testHappyConstruction()
    {
        new MemoryHogListenerImpl( 0, .66 );
        new MemoryHogListenerImpl( .5, .5 );
    }
    
    
    @Test
    public void testLoggingAtInfoLevelWorks()
    {
        final MemoryHogListenerImpl listener = new MemoryHogListenerImpl( 0, 1 );
        listener.monitorMemoryUsage();
    }
    
    
    @Test
    public void testLoggingAtWarnLevelWorks()
    {
        final MemoryHogListenerImpl listener = new MemoryHogListenerImpl( 0, 0 );
        listener.monitorMemoryUsage();
    }
}

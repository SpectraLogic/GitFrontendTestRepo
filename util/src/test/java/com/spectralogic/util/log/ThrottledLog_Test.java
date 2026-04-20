/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.log;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

import com.spectralogic.util.testfrmwrk.TestUtil;

public final class ThrottledLog_Test 
{
    @Test
    public void testThrottlingOfLogCallsHappensAsConfigured()
    {
        final MockLogger backingLog = new MockLogger();
        final ThrottledLog throttledLog = new ThrottledLog( backingLog, 100 );
        
        final RuntimeException ex = new RuntimeException( "hello" );
        assertEquals(0,  backingLog.m_logCalls.size(), "Should notta made any calls yet.");
        throttledLog.info( "hello" );
        assertEquals(1,  backingLog.m_logCalls.size(), "Shoulda made log call.");
        assertEquals(Level.INFO, backingLog.m_logCalls.get( 0 ), "Shoulda made log call.");

        throttledLog.info( "hello again" );
        assertEquals(1,  backingLog.m_logCalls.size(), "Shoulda throttled / consumed log call.");
        throttledLog.info( "hello again", ex );
        assertEquals(1,  backingLog.m_logCalls.size(), "Shoulda throttled / consumed log call.");

        throttledLog.warn( "i really mean it" );
        assertEquals(2,  backingLog.m_logCalls.size(), "Shoulda made log call.");
        assertEquals(Level.WARN, backingLog.m_logCalls.get( 1 ), "Shoulda made log call.");

        throttledLog.warn( "i really mean it" );
        assertEquals(2,  backingLog.m_logCalls.size(), "Shoulda throttled / consumed log call.");
        throttledLog.warn( "i really mean it", ex );
        assertEquals(2,  backingLog.m_logCalls.size(), "Shoulda throttled / consumed log call.");

        throttledLog.error( "i really really mean it" );
        assertEquals(3,  backingLog.m_logCalls.size(), "Shoulda made log call.");
        assertEquals(Level.ERROR, backingLog.m_logCalls.get( 2 ), "Shoulda made log call.");

        throttledLog.error( "i really really mean it" );
        assertEquals(3,  backingLog.m_logCalls.size(), "Shoulda throttled / consumed log call.");
        throttledLog.error( "i really really mean it", ex );
        assertEquals(3,  backingLog.m_logCalls.size(), "Shoulda throttled / consumed log call.");

        throttledLog.info( "hello again" );
        assertEquals(3,  backingLog.m_logCalls.size(), "Shoulda throttled / consumed log call.");

        TestUtil.sleep( 150 );
        
        throttledLog.info( "hello" );
        assertEquals(4,  backingLog.m_logCalls.size(), "Shoulda made log call.");
        assertEquals(Level.INFO, backingLog.m_logCalls.get( 3 ), "Shoulda made log call.");
    }
    

    private final static class MockLogger extends Logger
    {
        private MockLogger()
        {
            super( "MockLogger" );
        }

        @Override
        public void log( final Priority priority, final Object message, final Throwable t )
        {
            m_logCalls.add( priority );
        }
        
        @Override
        public void log( final Priority priority, final Object message )
        {
            m_logCalls.add( priority );
        }

        private final List< Priority > m_logCalls = new ArrayList<>();
    } // end inner class def
}

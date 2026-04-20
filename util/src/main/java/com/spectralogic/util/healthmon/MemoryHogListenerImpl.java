/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.healthmon;

import java.lang.reflect.Method;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.render.BytesRenderer;

public final class MemoryHogListenerImpl implements MemoryHogListener
{
    public MemoryHogListenerImpl(
            final double utilizationThresholdForInfoLogging,
            final double utilizationThresholdForWarnLogging )
    {
        if ( 0 > utilizationThresholdForInfoLogging
                || 0 > utilizationThresholdForWarnLogging )
        {
            throw new IllegalArgumentException(
                    "All thresholds must be positive." ); 
        }
        if ( 1 < utilizationThresholdForInfoLogging
                || 1 < utilizationThresholdForWarnLogging )
        {
            throw new IllegalArgumentException( 
                    "All thresholds must be 100% or lower." ); 
        }
        if ( utilizationThresholdForInfoLogging > utilizationThresholdForWarnLogging )
        {
            throw new IllegalArgumentException(
                    "Warn threshold must be at least the info threshold." ); 
        }
        
        m_utilizationThresholdForInfoLogging = utilizationThresholdForInfoLogging;
        m_utilizationThresholdForWarnLogging = utilizationThresholdForWarnLogging;
    }
    
    
    public void monitorMemoryUsage()
    {
        memoryUsageMonitoringOccurredInternal( true );
    }
    
    
    private void memoryUsageMonitoringOccurredInternal( final boolean retry )
    {
        final long freeBytes;
        final long totalBytes;
        final long usedBytes;
        try
        {
            freeBytes = ( (Long)METHOD_FREE_MEMORY.invoke( Runtime.getRuntime() ) ).longValue();
            totalBytes = ( (Long)METHOD_TOTAL_MEMORY.invoke( Runtime.getRuntime() ) ).longValue();
            usedBytes = totalBytes - freeBytes;
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( "Failed to determine memory usage.", ex );
        }
        
        final double utilization = usedBytes / ( totalBytes * 1.0f );
        if ( retry && utilization > m_utilizationThresholdForWarnLogging )
        {
            System.gc();
            System.gc();
            memoryUsageMonitoringOccurredInternal( false );
            return;
        }
        
        else if ( utilization > m_utilizationThresholdForWarnLogging )
        {
            LOG.warn( getLogMessage( utilization, usedBytes, freeBytes, totalBytes, !retry ) );
        }
        else if ( !retry || utilization > m_utilizationThresholdForInfoLogging )
        {
            LOG.info( getLogMessage( utilization, usedBytes, freeBytes, totalBytes, !retry ) );
        }
    }
    
    
    private String getLogMessage(
            final double utilization,
            final long used,
            final long free, 
            final long total,
            final boolean gcd )
    {
        final BytesRenderer renderer = new BytesRenderer();
        final String suffix = ( gcd ) ? "after a GC" : "without a GC";
        return "Memory utilization is at " 
                + (int) (utilization * 100 ) 
                + "% (" + renderer.render( used ) + " used, " 
                + renderer.render( free ) + " free, " 
                + renderer.render( total ) + " total) " + suffix + ".";
    }
    
    
    private final double m_utilizationThresholdForInfoLogging;
    private final double m_utilizationThresholdForWarnLogging;
    
    private final static Method METHOD_FREE_MEMORY = ReflectUtil.getMethod( Runtime.class, "freeMemory" );
    private final static Method METHOD_TOTAL_MEMORY = ReflectUtil.getMethod( Runtime.class, "totalMemory" );
    private final static Logger LOG = Logger.getLogger( MemoryHogListenerImpl.class );
}

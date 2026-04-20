/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.util.lang;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Duration of time.
 */
public final class Duration
{
    public Duration()
    {
        this( System.nanoTime() );
    }
    
    
    public Duration( final long nanoTimeDurationStartsFrom )
    {
        m_time = new AtomicLong( nanoTimeDurationStartsFrom );
    }
    
    
    public void reset()
    {
        m_time.set( System.nanoTime() );
    }
    
    
    public int getElapsedHours()
    {
        return (int)TimeUnit.HOURS.convert( getElapsedNanos(), TimeUnit.NANOSECONDS );
    }
    
    
    public int getElapsedMinutes()
    {
        return (int)TimeUnit.MINUTES.convert( getElapsedNanos(), TimeUnit.NANOSECONDS );
    }
    
    
    public int getElapsedSeconds()
    {
        return (int)TimeUnit.SECONDS.convert( getElapsedNanos(), TimeUnit.NANOSECONDS );
    }
    
    
    public long getElapsedMillis()
    {
        return TimeUnit.MILLISECONDS.convert( getElapsedNanos(), TimeUnit.NANOSECONDS );
    }
    
    
    public long getElapsedNanos()
    {
        return System.nanoTime() - m_time.get();
    }
    
    
    @Override
    public String toString()
    {
        if ( 2 <= getElapsedHours() )
        {
            return ( getElapsedHours() ) + " hours";
        }
        if ( 9 < getElapsedMinutes() )
        {
            return getElapsedMinutes() + " minutes";
        }
        if ( 9 < getElapsedSeconds() )
        {
            return getElapsedSeconds() + " seconds";
        }
        return getElapsedMillis() + " ms";
    }
    
    
    private final AtomicLong m_time;
}


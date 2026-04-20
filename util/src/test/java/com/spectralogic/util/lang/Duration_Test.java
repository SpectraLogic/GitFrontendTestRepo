/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang;



import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class Duration_Test 
{
    @Test
    public void testElapsedTimeReportedIsCorrect() throws InterruptedException
    {
        final long maxStart = System.nanoTime();
        final Duration duration = new Duration();
        final long minStart = System.nanoTime();
        
        Thread.sleep( 100 );

        final long minEnd = System.nanoTime();
        final int minutes = duration.getElapsedMinutes();
        final int seconds = duration.getElapsedSeconds();
        final long millis = duration.getElapsedMillis();
        final long nanos = duration.getElapsedNanos();
        final long maxEnd = System.nanoTime();
        
        final long minDuration = minEnd - minStart;
        final long maxDuration = maxEnd - maxStart;
        
        assertTrue(
                minDuration <= nanos,
                "Shoulda reported elapsed time accurately.");
        assertTrue(
                maxDuration >= nanos,
                "Shoulda reported elapsed time accurately.");
        
        assertTrue(
                minDuration / 1000 / 1000 <= millis,
                "Shoulda reported elapsed time accurately.");
        assertTrue(
                maxDuration / 1000 / 1000 >= millis,
                "Shoulda reported elapsed time accurately.");
        
        assertTrue(
                minDuration / 1000 / 1000 / 1000 <= seconds,
                "Shoulda reported elapsed time accurately.");
        assertTrue(
                maxDuration / 1000 / 1000 / 1000 >= seconds,
                "Shoulda reported elapsed time accurately.");
        
        assertTrue(
                minDuration / 1000 / 1000 / 1000 / 60 <= minutes,
                "Shoulda reported elapsed time accurately.");
        assertTrue(
                maxDuration / 1000 / 1000 / 1000 / 60 >= minutes,
                "Shoulda reported elapsed time accurately.");
    }
    
    
    @Test
    public void testElapsedTimeReportedIsCorrectWhenReset() throws InterruptedException
    {
        final Duration duration = new Duration();
        Thread.sleep( 100 );
        
        final long maxStart = System.nanoTime();
        duration.reset();
        final long minStart = System.nanoTime();
        
        Thread.sleep( 100 );

        final long minEnd = System.nanoTime();
        final int seconds = duration.getElapsedSeconds();
        final long millis = duration.getElapsedMillis();
        final long nanos = duration.getElapsedNanos();
        final long maxEnd = System.nanoTime();
        
        final long minDuration = minEnd - minStart;
        final long maxDuration = maxEnd - maxStart;
        
        assertTrue(
                minDuration <= nanos,
                "Shoulda reported elapsed time accurately.");
        assertTrue(
                maxDuration >= nanos,
                "Shoulda reported elapsed time accurately.");
        
        assertTrue(
                minDuration / 1000 / 1000 <= millis,
                "Shoulda reported elapsed time accurately.");
        assertTrue(
                maxDuration / 1000 / 1000 >= millis,
                "Shoulda reported elapsed time accurately.");
        
        assertTrue(
                minDuration / 1000 / 1000 / 1000 <= seconds,
                "Shoulda reported elapsed time accurately.");
        assertTrue(
                maxDuration / 1000 / 1000 / 1000 >= seconds,
                "Shoulda reported elapsed time accurately.");
    }
    
    
    @Test
    public void testToStringDoesNotBlowUp()
    {
        assertNotNull( "Shoulda returned a value.", new Duration().toString() );
    }
    
    
    @Test
    public void testDurationHoursToStringConvertsAsExpected()
    {
        final Duration testDuration = new Duration( 
                System.nanoTime() - TimeUnit.NANOSECONDS.convert( 3, TimeUnit.HOURS ) );
        assertEquals(  "3 hours", testDuration.toString() );
    }
    
    
    @Test
    public void testDurationMinutesToStringConvertsAsExpected()
    {
        final Duration testDuration = new Duration( 
                System.nanoTime() - TimeUnit.NANOSECONDS.convert( 42, TimeUnit.MINUTES ) );
        assertEquals(  "42 minutes", testDuration.toString() );
    }
    
    
    @Test
    public void testDurationSecondsToStringConvertsAsExpected()
    {
        final Duration testDuration = new Duration( 
                System.nanoTime() - TimeUnit.NANOSECONDS.convert( 42, TimeUnit.SECONDS ) );
        assertEquals(  "42 seconds", testDuration.toString() );
    }
    
    
    @Test
    public void testDurationMillisToStringConvertsAsExpected()
    {
        final Duration testDuration = new Duration( 
                System.nanoTime() - TimeUnit.NANOSECONDS.convert( 3L, TimeUnit.SECONDS) );
        assertEquals( "3000 ms", testDuration.toString() );
    }
}

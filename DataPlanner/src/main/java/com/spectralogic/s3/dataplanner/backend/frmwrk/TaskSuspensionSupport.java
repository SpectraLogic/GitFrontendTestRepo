/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.frmwrk;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;

public final class TaskSuspensionSupport
{
    /**
     * @param maxRetriesBeforeSuspensionRequired - the number of consecutive retries necessary before a
     * suspension is required
     * @param totalMaxRetries - the total number of retries necessary before we give up executing a task, or
     * null if we are not allowed to give up trying this task
     * @param suspensionDurationInMillis - the duration in millis that a suspension lasts
     * @param suspensionStepInMillis - the number of millis to increase the suspension duration by if 
     * suspension has already occurred once (the suspension step may be applied multiple times; for 
     * example, the first suspension will be for <code>suspensionDurationInMillis</code>, and every
     * subsequent suspension will be for <code>suspensionDurationInMillis + N x suspensionStepInMillis, where 
     * N is at least 1 and no more than the number of suspensions since the first suspension</code>.
     */
    public TaskSuspensionSupport( 
            final int maxRetriesBeforeSuspensionRequired, 
            final Integer totalMaxRetries, 
            final long suspensionDurationInMillis,
            final long suspensionStepInMillis )
    {
        Validations.verifyInRange(
                "Max retries before suspension required", 1, 100, maxRetriesBeforeSuspensionRequired );
        if ( null != totalMaxRetries )
        {
            Validations.verifyInRange(
                    "Total max retries", maxRetriesBeforeSuspensionRequired, 1000, totalMaxRetries );
        }
        Validations.verifyInRange( 
                "Suspension duration in millis", 1, Integer.MAX_VALUE, suspensionDurationInMillis );
        Validations.verifyInRange( 
                "Suspension duration step in millis", 0, Integer.MAX_VALUE, suspensionStepInMillis );
        
        m_maxRetriesBeforeSuspensionRequired = maxRetriesBeforeSuspensionRequired;
        m_totalMaxRetries = totalMaxRetries;
        m_suspensionDurationInMillis = suspensionDurationInMillis;
        m_suspensionStepInMillis = suspensionStepInMillis;
    }
    
    
    public enum TaskSuspended
    {
        NO,
        YES,
    }
    
    
    public final static class TaskFailedDueToTooManyRetriesException extends Exception
    {
        private TaskFailedDueToTooManyRetriesException( final String msg )
        {
            super( msg );
        }
    } // end inner class def
    
    
    synchronized public TaskSuspended getState()
    {
        if ( null != m_durationSuspended 
                && m_durationSuspended.getElapsedMillis() > m_suspensionDurationInMillis )
        {
            m_durationSuspended = null;
        }
        if ( null == m_durationSuspended )
        {
            return TaskSuspended.NO;
        }
        return TaskSuspended.YES;
    }
    
    
    synchronized public void failureOccurred() throws TaskFailedDueToTooManyRetriesException
    {
        ++m_totalNumberOfRetries;
        ++m_numberOfConsecutiveRetries;
        if ( null != m_totalMaxRetries && m_totalNumberOfRetries == m_totalMaxRetries.intValue() )
        {
            throw new TaskFailedDueToTooManyRetriesException(
                    m_totalNumberOfRetries + " retries attempted without success." );
        }
        if ( m_numberOfConsecutiveRetries == m_maxRetriesBeforeSuspensionRequired )
        {
            ++m_numberOfSuspensions;
            if ( 2 == m_numberOfSuspensions || 0 == ( m_numberOfSuspensions % 10 ) )
            {
                m_suspensionDurationInMillis += m_suspensionStepInMillis;
            }
            
            m_numberOfConsecutiveRetries = 0;
            m_durationSuspended = new Duration();
            LOG.info( "Task suspension required for " + new Duration(
                    System.nanoTime() - TimeUnit.NANOSECONDS.convert(
                            m_suspensionDurationInMillis, TimeUnit.MILLISECONDS ) ) + "." );
        }
    }
    

    private int m_numberOfConsecutiveRetries;
    private int m_totalNumberOfRetries;
    private Duration m_durationSuspended;
    private long m_suspensionDurationInMillis;
    private int m_numberOfSuspensions;
    
    private final long m_suspensionStepInMillis;
    private final Integer m_totalMaxRetries;
    private final int m_maxRetriesBeforeSuspensionRequired;
    
    private final static Logger LOG = Logger.getLogger( TaskSuspensionSupport.class );
}

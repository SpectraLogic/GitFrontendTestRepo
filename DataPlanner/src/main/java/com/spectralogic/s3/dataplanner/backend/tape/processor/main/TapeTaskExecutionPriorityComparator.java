/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main;

import java.util.Comparator;

import com.spectralogic.s3.dataplanner.backend.tape.api.LongRunningInterruptableTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeAvailability;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeTask;
import com.spectralogic.util.cache.StaticCache;
import com.spectralogic.util.lang.Validations;

final class TapeTaskExecutionPriorityComparator implements Comparator< TapeTask >
{
    TapeTaskExecutionPriorityComparator( final TapeAvailability tapeAvailability )
    {
        m_tapeAvailability = tapeAvailability;
        m_taskCanUseTapeInDrive = new StaticCache<>(task -> task.canUseTapeAlreadyInDrive(tapeAvailability));
        Validations.verifyNotNull( "Tape availability", m_tapeAvailability );
    }
    
    
    public int compare( final TapeTask task1, final TapeTask task2 )
    {
        // If a task has critical or urgent priority, prefer it
        if ( task1.getPriority().ordinal() != task2.getPriority().ordinal() )
        {
            if (task1.getPriority().isUnconditionallyHigherPriorityThan(task2.getPriority())
                || task2.getPriority().isUnconditionallyHigherPriorityThan(task1.getPriority())) {
                return task1.getPriority().ordinal() - task2.getPriority().ordinal();
            }
        }
        
        /*
         * If there's a tape in the drive, prefer tasks that will operate on that tape first and foremost 
         * since moves are expensive, unless that task is long running and the other is not, in which case,
         * move on to the next check (priority). This means a high priority read task will boot a tape out of
         * the drive even if a medium priority verify could use it, but it won't do the same to a medium priority
         * write that could use it.
         */

        final boolean task1IsLongRunning =
                LongRunningInterruptableTapeTask.class.isAssignableFrom( task1.getClass() );
        final boolean task2IsLongRunning =
                LongRunningInterruptableTapeTask.class.isAssignableFrom( task2.getClass() );
        if ( null != m_tapeAvailability.getTapeInDrive() )
        {
            final boolean task1IsShorter = task2IsLongRunning && !task1IsLongRunning;
            final boolean task2IsShorter = task1IsLongRunning && !task2IsLongRunning;
            final boolean task1CanUseTapeInDrive = m_taskCanUseTapeInDrive.get(task1);
            final boolean task2CanUseTapeInDrive = m_taskCanUseTapeInDrive.get(task2);
            if (task1CanUseTapeInDrive && !task2CanUseTapeInDrive && !task2IsShorter) {
                return -1;
            } else if (task2CanUseTapeInDrive && !task1CanUseTapeInDrive && !task1IsShorter) {
                return 1;
            }
        }

        // If a task has higher priority, prefer it
        if ( task1.getPriority().ordinal() != task2.getPriority().ordinal() )
        {
            return task1.getPriority().ordinal() - task2.getPriority().ordinal();
        }
        
        // If one task is long running and the other is short running, prefer the short running one
        if ( task1IsLongRunning != task2IsLongRunning )
        {
            return ( task2IsLongRunning ) ? -1 : 1;
        }
        
        // By default, as the final tie breaker, pick the task that has been around the longest
        return ( task1.getId() < task2.getId() ) ? -1 : 1;
    }
    
    
    private final TapeAvailability m_tapeAvailability;
    private final StaticCache<TapeTask, Boolean> m_taskCanUseTapeInDrive;
} // end inner class def
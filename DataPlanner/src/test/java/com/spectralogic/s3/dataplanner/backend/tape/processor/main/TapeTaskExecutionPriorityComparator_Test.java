/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;


import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.task.MockTapeAvailability;
import com.spectralogic.s3.dataplanner.backend.tape.task.NoOpTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.task.VerifyTapeTask;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public final class TapeTaskExecutionPriorityComparator_Test
{

    @Test
    public void testConstructorNullTapeAvailabilityNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new TapeTaskExecutionPriorityComparator( null );
            }
        } );
    }
    
    @Test
    public void
    testComparatorPrioritizesTasksByPreferredTapeThenPriorityThenAge()
    {
        final UUID preferredTapeId = UUID.randomUUID();
        final BeansServiceManager serviceManager =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class ).getServiceManager();
        final TapeTask task5 = new NoOpTapeTask( "task5", BlobStoreTaskPriority.BACKGROUND, null, new TapeFailureManagement(serviceManager), serviceManager );
        final TapeTask task4 = new NoOpTapeTask( "task4", BlobStoreTaskPriority.BACKGROUND, null, new TapeFailureManagement(serviceManager), serviceManager );
        final TapeTask task3 = new NoOpTapeTask( "task3", BlobStoreTaskPriority.BACKGROUND, UUID.randomUUID(), new TapeFailureManagement(serviceManager), serviceManager );
        final TapeTask task2 = new NoOpTapeTask( "task2", BlobStoreTaskPriority.HIGH, null, new TapeFailureManagement(serviceManager), serviceManager );
        final TapeTask task1 = new NoOpTapeTask( "task1", BlobStoreTaskPriority.LOW, preferredTapeId, new TapeFailureManagement(serviceManager), serviceManager );

        final List< TapeTask > tasks = CollectionFactory.toList( task1, task2, task5, task4, task3 );
        final List< TapeTask > sortedTasks = new ArrayList<>( tasks );
        int i = 10;
        while ( --i > 0 && tasks.equals( sortedTasks ) )
        {
            Collections.shuffle( sortedTasks );
        }
        assertFalse(tasks.equals( sortedTasks ), "Tasks shoulda started off not same as sorted tasks.");

        Collections.sort(
                sortedTasks,
                new TapeTaskExecutionPriorityComparator(
                        new MockTapeAvailability().setPreferredTape( preferredTapeId ) ) );
        assertTrue(tasks.equals( sortedTasks ), "Tasks shoulda been sorted correctly.");
    }

    @Test
    public void testComparatorPrioritizesTasksByUrgentCriticalFirst()
    {
        final UUID preferredTapeId = UUID.randomUUID();
        final BeansServiceManager serviceManager =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class ).getServiceManager();
        final TapeTask task5 = new NoOpTapeTask( "task5", BlobStoreTaskPriority.NORMAL, null, new TapeFailureManagement(serviceManager), serviceManager );
        final TapeTask task4 = new NoOpTapeTask( "task4", BlobStoreTaskPriority.NORMAL, null, new TapeFailureManagement(serviceManager), serviceManager );
        final TapeTask task3 = new NoOpTapeTask( "task3", BlobStoreTaskPriority.NORMAL, UUID.randomUUID(), new TapeFailureManagement(serviceManager), serviceManager );
        final TapeTask task2 = new NoOpTapeTask( "task2", BlobStoreTaskPriority.CRITICAL, null, new TapeFailureManagement(serviceManager), serviceManager );
        final TapeTask task1 = new NoOpTapeTask( "task1", BlobStoreTaskPriority.HIGH, preferredTapeId, new TapeFailureManagement(serviceManager), serviceManager );

        final List< TapeTask > tasks = CollectionFactory.toList( task2, task1, task5, task4, task3 );
        final List< TapeTask > sortedTasks = new ArrayList<>( tasks );
        int i = 10;
        while ( --i > 0 && tasks.equals( sortedTasks ) )
        {
            Collections.shuffle( sortedTasks );
        }
        assertFalse(tasks.equals( sortedTasks ), "Tasks shoulda started off not same as sorted tasks.");

        Collections.sort(
                sortedTasks,
                new TapeTaskExecutionPriorityComparator(
                        new MockTapeAvailability().setPreferredTape( preferredTapeId ) ) );
        new TapeTaskExecutionPriorityComparator(
                new MockTapeAvailability().setPreferredTape( preferredTapeId ) ).compare( task1, task2 );
        assertTrue(tasks.equals( sortedTasks ), "Tasks shoulda been sorted correctly.");
    }

    @Test
    public void
    testComparatorPrioritizesLongRunningTasksByPriorityThenAbilityToSelectTapeWithoutDaoChangesThenAge()
    {
        final UUID preferredTapeId = UUID.randomUUID();
        final BeansServiceManager serviceManager =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class ).getServiceManager();
        final TapeTask task5 = new NoOpTapeTask( "task5", BlobStoreTaskPriority.NORMAL, null, new TapeFailureManagement(serviceManager), serviceManager );
        final TapeTask task4 = new NoOpTapeTask( "task4", BlobStoreTaskPriority.NORMAL, null, new TapeFailureManagement(serviceManager), serviceManager );
        final TapeTask task3 = new NoOpTapeTask( "task3", BlobStoreTaskPriority.NORMAL, UUID.randomUUID(), new TapeFailureManagement(serviceManager), serviceManager );
        final TapeTask task2 = new NoOpTapeTask( "task2", BlobStoreTaskPriority.CRITICAL, null, new TapeFailureManagement(serviceManager), serviceManager );
        final TapeTask task1 = new VerifyTapeTask( BlobStoreTaskPriority.NORMAL, preferredTapeId, new MockDiskManager( serviceManager ), new TapeFailureManagement(serviceManager), serviceManager );

        final List< TapeTask > tasks = CollectionFactory.toList( task2, task5, task4, task3, task1 );
        final List< TapeTask > sortedTasks = new ArrayList<>( tasks );
        int i = 10;
        while ( --i > 0 && tasks.equals( sortedTasks ) )
        {
            Collections.shuffle( sortedTasks );
        }
        assertFalse(tasks.equals( sortedTasks ), "Tasks shoulda started off not same as sorted tasks.");

        Collections.sort( 
                sortedTasks,
                new TapeTaskExecutionPriorityComparator( 
                        new MockTapeAvailability().setPreferredTape( preferredTapeId ) ) );
        new TapeTaskExecutionPriorityComparator(
                new MockTapeAvailability().setPreferredTape( preferredTapeId ) ).compare( task1, task2 );
        assertTrue(tasks.equals( sortedTasks ), "Tasks shoulda been sorted correctly.");
    }
    
    
    private void setTapeId( final TapeTask task, final UUID tapeId )
    {
        try
        {
            final Method method = ReflectUtil.getMethod( NoOpTapeTask.class.getSuperclass(), "setTapeId" );
            method.setAccessible( true );
            method.invoke( task, tapeId );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }
}

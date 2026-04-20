package com.spectralogic.s3.dataplanner.backend.tape.processor.main;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManagerImpl;
import com.spectralogic.s3.common.platform.api.TapeEjector;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.tape.api.DynamicTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.api.StaticTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeTaskQueue;
import com.spectralogic.s3.dataplanner.backend.tape.task.FormatTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.task.VerifyChunkOnTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.task.WriteChunkToTapeTask;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import org.junit.jupiter.api.*;


import java.util.*;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

public class TapeTaskQueueImpl_Test  {

    private static class MockDequeuedListener implements TapeTaskDequeuedListener {
        @Override
        public void taskDequeued( TapeTask task ) {
            m_dequeuedTasks.add( task );
        }

        private final List<TapeTask> m_dequeuedTasks = new LinkedList<>();
    }

    // Sorts TapeTask from lowest to highest priority
    private static class MockTapeTaskComparator implements Comparator< TapeTask > {
        @Override
        public int compare( TapeTask o1, TapeTask o2 ) {
            if ( o1.getPriority() == o2.getPriority() ) {
                return 0;
            }
            if ( o1.getPriority().isHigherPriorityThan( o2.getPriority() ) ) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    private static class MockTapeTaskPredicate implements Predicate< TapeTask > {
        MockTapeTaskPredicate(final List< StaticTapeTask > tasksToExclude) {
            m_tasksToExclude.addAll( tasksToExclude );
        }

        @Override
        public boolean test( TapeTask tapeTask ) {
            return !m_tasksToExclude.contains(tapeTask);
        }

        private final List< TapeTask > m_tasksToExclude = new LinkedList<>();
    }

   @Test
    public void testStaticNonIoTask() {
        

        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats(tape);

        final StaticTapeTask staticTapeTask =
                new FormatTapeTask( BlobStoreTaskPriority.NORMAL, tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager() );

        final Object lock = new Object();
        final MockDequeuedListener dequeueListener = new MockDequeuedListener();
        final TapeTaskQueueImpl queue = new TapeTaskQueueImpl( dequeueListener, lock );

        queue.addStaticTask( staticTapeTask );
        final TapeTask task = queue.get( tape.getId() ).get(0);
        assertNotNull( task );
        assertEquals( task, staticTapeTask );
        assertTrue( dequeueListener.m_dequeuedTasks.isEmpty() );
        assertTrue( queue.getAllTapeTasks().contains( staticTapeTask ) );
        assertTrue( queue.getChunkIds().isEmpty() );
        assertEquals( queue.size(), 1 );

        assertTrue( queue.remove( staticTapeTask, "testing remove") );
        assertTrue( queue.get( tape.getId() ).isEmpty() );
        assertTrue( dequeueListener.m_dequeuedTasks.contains( staticTapeTask ) );
        assertFalse( queue.getAllTapeTasks().contains( staticTapeTask ) );
        assertTrue( queue.getChunkIds().isEmpty() );
        assertEquals( queue.size(), 0 );
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testStaticIoTask() {
        

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.VERIFY );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId(), blob);
        chunk.setReadFromTapeId( tape.getId() );

        final VerifyChunkOnTapeTask staticIoTask = new VerifyChunkOnTapeTask( BlobStoreTaskPriority.values()[ 0 ],
                List.of(chunk),
                new MockDiskManager( dbSupport.getServiceManager() ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), JobProgressManagerImpl.BufferProgressUpdates.NO ),
                new TapeFailureManagement(dbSupport.getServiceManager()),
                dbSupport.getServiceManager() );

        final Object lock = new Object();
        final MockDequeuedListener dequeueListener = new MockDequeuedListener();
        final TapeTaskQueueImpl queue = new TapeTaskQueueImpl( dequeueListener, lock );

        // add task
        queue.addStaticTask( staticIoTask );
        TapeTask task = queue.get( tape.getId() ).get(0);
        assertNotNull( task );
        assertEquals( task, staticIoTask );
        assertTrue( dequeueListener.m_dequeuedTasks.isEmpty() );
        assertTrue( queue.getAllTapeTasks().contains( staticIoTask ) );
        assertTrue( queue.getChunkIds().contains( chunk.getId() ) );
        assertEquals( queue.size(), 1 );

        // remove task
        assertTrue( queue.remove( staticIoTask, "testing remove" ) );
        assertTrue( queue.get( tape.getId() ).isEmpty() );
        assertTrue( dequeueListener.m_dequeuedTasks.contains( staticIoTask ) );
        assertFalse( queue.getAllTapeTasks().contains( staticIoTask ) );
        assertFalse( queue.getChunkIds().contains( chunk.getId() ) );
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testDynamicIoTask() {
        

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );

        final DataPolicy dataPolicy =
                dbSupport.getServiceManager().getRetriever( DataPolicy.class ).attain( Require.nothing() );
        final TapePartition tapePartition =
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain( Require.nothing() );
        final StorageDomain sd1 =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomain sd2 =
                mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd1.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks( Set.of(chunk) );

        final DynamicTapeTask dynamicTask = new WriteChunkToTapeTask(
                BlobStoreTaskPriority.values()[ 0 ],
                pts,
                InterfaceProxyFactory.getProxy( TapeEjector.class, null ),
                new MockDiskManager( dbSupport.getServiceManager() ),
                new JobProgressManagerImpl( mockDaoDriver.getServiceManager(), JobProgressManagerImpl.BufferProgressUpdates.NO ),
                new TapeFailureManagement(mockDaoDriver.getServiceManager()),
                mockDaoDriver.getServiceManager());

        final Object lock = new Object();
        final MockDequeuedListener dequeueListener = new MockDequeuedListener();
        final TapeTaskQueueImpl queue = new TapeTaskQueueImpl( dequeueListener, lock );

        // add task
        queue.addDynamicTask( dynamicTask );
        assertTrue( queue.get( tape.getId() ).isEmpty() );
        assertTrue( dequeueListener.m_dequeuedTasks.isEmpty() );
        assertTrue( queue.getAllTapeTasks().contains( dynamicTask ) );
        assertTrue( queue.getChunkIds().contains( chunk.getId() ) );
        assertEquals( queue.size(), 1 );

        // remove task
        assertTrue( queue.remove( dynamicTask, "testing remove" ));
        assertTrue( queue.get( tape.getId() ).isEmpty() );
        assertTrue( dequeueListener.m_dequeuedTasks.contains( dynamicTask ) );
        assertFalse( queue.getAllTapeTasks().contains( dynamicTask ) );
        assertTrue( queue.getChunkIds().isEmpty() );
        assertEquals( queue.size(), 0 );
    }

    @Test
    public void testDequeueAggregatedChunkAfterPartialDelete() {
        

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId(), blob);

        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final JobEntry chunk2 = mockDaoDriver.createJobEntry( job2.getId(), blob2 );

        final DataPolicy dataPolicy =
                dbSupport.getServiceManager().getRetriever( DataPolicy.class ).attain( Require.nothing() );
        final TapePartition tapePartition =
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain( Require.nothing() );
        final StorageDomain sd1 =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomain sd2 =
                mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd1.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );

        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks( Set.of(chunk, chunk2) );

        final DynamicTapeTask dynamicTask = new WriteChunkToTapeTask(
                BlobStoreTaskPriority.values()[ 0 ],
                pts,
                InterfaceProxyFactory.getProxy( TapeEjector.class, null ),
                new MockDiskManager( dbSupport.getServiceManager() ),
                new JobProgressManagerImpl( mockDaoDriver.getServiceManager(), JobProgressManagerImpl.BufferProgressUpdates.NO ),
                new TapeFailureManagement(mockDaoDriver.getServiceManager()),
                mockDaoDriver.getServiceManager());

        final Object lock = new Object();
        final MockDequeuedListener dequeueListener = new MockDequeuedListener();
        final TapeTaskQueueImpl queue = new TapeTaskQueueImpl( dequeueListener, lock );

        // add task
        queue.addDynamicTask( dynamicTask );
        assertTrue( queue.get( tape.getId() ).isEmpty() );
        assertTrue( dequeueListener.m_dequeuedTasks.isEmpty() );
        assertTrue( queue.getAllTapeTasks().contains( dynamicTask ) );
        assertTrue( queue.getChunkIds().contains( chunk.getId() ) );
        assertEquals( queue.size(), 1 );

        // delete chunk
        mockDaoDriver.delete(JobEntry.class, chunk2);

        // remove task
        assertTrue( queue.remove( dynamicTask, "testing remove" ));
        assertTrue( queue.get( tape.getId() ).isEmpty() );
        assertTrue( dequeueListener.m_dequeuedTasks.contains( dynamicTask ) );
        assertFalse( queue.getAllTapeTasks().contains( dynamicTask ) );
        assertTrue( queue.getChunkIds().isEmpty() );
        assertEquals( queue.size(), 0 );
    }

    private List< StaticTapeTask > createAndQueueStaticTapeTasks(
            final int count,
            final TapeTaskQueue queue,
            final MockDaoDriver mockDaoDriver,
            final DatabaseSupport dbSupport ) {

        final List< StaticTapeTask > tapeTasks = new LinkedList<>();
        for ( int i = 0; i < count; i++ ) {
            final Tape tape = mockDaoDriver.createTape();
            mockDaoDriver.nullOutCapacityStats( tape );
            final BlobStoreTaskPriority priority;
            switch (i % 3) {
                case 0:
                    priority = BlobStoreTaskPriority.LOW;
                    break;
                case 1:
                    priority = BlobStoreTaskPriority.NORMAL;
                    break;
                default:
                    priority = BlobStoreTaskPriority.HIGH;
            }
            final StaticTapeTask task =
                    new FormatTapeTask( priority, tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager() );
            tapeTasks.add( task );
            queue.addStaticTask( task );
        }
        return tapeTasks;
    }

    private void verifyTasksSortedByPriority( final List< TapeTask > sortedTasks ) {
        BlobStoreTaskPriority prevPriority = BlobStoreTaskPriority.BACKGROUND;
        for ( final TapeTask tapeTask : sortedTasks ) {
            assertTrue(tapeTask.getPriority().isHigherOrEqualPriorityTo( prevPriority ) );
            prevPriority = tapeTask.getPriority();
        }
    }

    @Test
    public void testQueueSorting() {
        

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final Object lock = new Object();
        final MockDequeuedListener dequeueListener = new MockDequeuedListener();
        final TapeTaskQueueImpl queue = new TapeTaskQueueImpl( dequeueListener, lock );

        // create the tasks to be sorted
        final int taskToSortCount = 10;
        final List< StaticTapeTask > sortableTasks = createAndQueueStaticTapeTasks( taskToSortCount, queue, mockDaoDriver, dbSupport );

        final int taskToIgnoreCount = 5;
        final List< StaticTapeTask > ignorableTasks = createAndQueueStaticTapeTasks( taskToIgnoreCount, queue, mockDaoDriver, dbSupport );

        assertEquals( queue.size(), taskToSortCount + taskToIgnoreCount );

        final MockTapeTaskComparator comparator = new MockTapeTaskComparator();
        final MockTapeTaskPredicate predicate = new MockTapeTaskPredicate( ignorableTasks );
        final List< TapeTask > sortedTasks = queue.getSortedPendingTapeTasksFor( comparator, predicate );
        assertEquals( sortedTasks.size(), taskToSortCount );
        sortableTasks.forEach(staticTapeTask -> {
            assertTrue( sortableTasks.contains( staticTapeTask ) );
            assertFalse( ignorableTasks.contains( staticTapeTask ) );
        } );
        verifyTasksSortedByPriority( sortedTasks );

        // update a task to urgent and verify the list is still sorted
        StaticTapeTask taskToUpdate = sortableTasks.get( 0 );
        boolean taskExists = queue.tryPriorityUpdate( taskToUpdate.getTapeId(), StaticTapeTask.class,
                BlobStoreTaskPriority.CRITICAL, false, false );
        assertTrue( taskExists );
        verifyTasksSortedByPriority( queue.getSortedPendingTapeTasksFor( comparator, predicate ) );
        TapeTask updatedTask = queue.get( taskToUpdate.getTapeId() ).get(0);
        assertNotNull(updatedTask);
        assertEquals( updatedTask.getPriority(), BlobStoreTaskPriority.CRITICAL );

        // try to downgrade task priority wih onlyChangePriorityIfIncreasing and verify failure
        taskExists = queue.tryPriorityUpdate( taskToUpdate.getTapeId(), StaticTapeTask.class,
                BlobStoreTaskPriority.BACKGROUND, true, false );
        assertTrue( taskExists );
        verifyTasksSortedByPriority( queue.getSortedPendingTapeTasksFor( comparator, predicate ) );
        updatedTask = queue.get( taskToUpdate.getTapeId() ).get(0);
        assertNotNull(updatedTask);
        assertEquals( updatedTask.getPriority(), BlobStoreTaskPriority.CRITICAL );

        // downgrade task priority to BACKGROUND and verify the list is still sorted
        taskExists = queue.tryPriorityUpdate( taskToUpdate.getTapeId(), StaticTapeTask.class,
                BlobStoreTaskPriority.BACKGROUND, false, false );
        assertTrue( taskExists );
        verifyTasksSortedByPriority( queue.getSortedPendingTapeTasksFor( comparator, predicate ) );
        updatedTask = queue.get( taskToUpdate.getTapeId() ).get(0);
        assertNotNull(updatedTask);
        assertEquals( updatedTask.getPriority(), BlobStoreTaskPriority.BACKGROUND );
    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }
}

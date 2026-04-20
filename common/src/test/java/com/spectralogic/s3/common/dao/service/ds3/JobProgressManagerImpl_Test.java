/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManagerImpl.BufferProgressUpdates;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class JobProgressManagerImpl_Test 
{
    @Test
    public void testBlobLoadedToCacheUpdatesCachedSizeOfJob()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 1 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 10 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 100 );
        mockDaoDriver.createObject( null, "o4", 1000 );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.values()[ 0 ] );
        mockDaoDriver.createJobEntries(job.getId(), CollectionFactory.toSet( b1 ) );
        mockDaoDriver.createJobEntries(job.getId(), CollectionFactory.toSet( b2, b3 ) );
        
        final JobProgressManager manager =
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.YES );
        final JobService service = dbSupport.getServiceManager().getService( JobService.class );
        manager.blobLoadedToCache( job.getId(), b1.getLength() );
        assertEquals(0,  service.attain(job.getId()).getCachedSizeInBytes(), "Should notta updated cached size of job immediately.");
        manager.flush();
        assertEquals(1,  service.attain(job.getId()).getCachedSizeInBytes(), "Shoulda updated cached size of job.");
        manager.blobLoadedToCache( job.getId(), b2.getLength() );
        manager.blobLoadedToCache( job.getId(), b3.getLength() );
        manager.flush();
        assertEquals(111,  service.attain(job.getId()).getCachedSizeInBytes(), "Shoulda updated cached size of job.");
        assertEquals(0,  service.attain(job.getId()).getOriginalSizeInBytes(), "Should notta updated any other properties.");
        assertEquals(0,  service.attain(job.getId()).getCompletedSizeInBytes(), "Should notta updated any other properties.");
    }
    
    
    @Test
    public void testDeleteingJobDoesNotCauseFlushToCrash()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 1 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 10 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 100 );
        mockDaoDriver.createObject( null, "o4", 1000 );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        
        final Job job1 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.createJobEntry(job1.getId(), b1 );
        mockDaoDriver.createJobEntries(job2.getId(), CollectionFactory.toSet( b2, b3 ) );
        
        final JobProgressManager manager =
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.YES );
        final JobService service = dbSupport.getServiceManager().getService( JobService.class );
        manager.blobLoadedToCache( job1.getId(), b1.getLength() );
        manager.blobLoadedToCache( job2.getId(), b2.getLength() );
        assertEquals(0, service.attain(job1.getId()).getCachedSizeInBytes(), "Should notta updated cached size of job immediately.");
        mockDaoDriver.delete( Job.class, job2.getId() );
        manager.flush(); //flush should not crash
        assertEquals(1,  service.attain(job1.getId()).getCachedSizeInBytes(), "Shoulda updated cached size of job.");
        manager.flush(); //flush should not crash
    }
    
    
    @Test
    public void testBlobLoadedToCacheThreadSafe() throws InterruptedException, ExecutionException
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Set< S3Object > objects = new HashSet<>();
        final Set< Blob > blobs = new HashSet<>();
        for ( int i = 0; i < 100; i++ )
        {
        	final S3Object o = mockDaoDriver.createObject( null, "o" + i, 1 );
        	objects.add( o );
        	blobs.add( ( mockDaoDriver.getBlobFor( o.getId() ) ) );
        }
        
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.createJobEntries( job.getId(), blobs );
        
        final JobProgressManager manager =
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.YES );
        final JobService service = dbSupport.getServiceManager().getService( JobService.class );
        final WorkPool wp = WorkPoolFactory.createWorkPool( 20, "TestBlobLoader");
        final Set< Future< ? > > futures = new HashSet<>();
        for ( final Blob b : blobs )
        {
        	futures.add( wp.submit( new Runnable() {
				public void run()
				{
					manager.blobLoadedToCache( job.getId(), b.getLength() );
					
				}
			} ) );
        }
        
        for ( final Future< ? > f : futures )
        {
        	f.get();
        }
        manager.flush();

        assertEquals(100,  service.attain(job.getId()).getCachedSizeInBytes(), "Shoulda updated cached size of job.");
        assertEquals(0,  service.attain(job.getId()).getOriginalSizeInBytes(), "Should notta updated any other properties.");
        assertEquals(0,  service.attain(job.getId()).getCompletedSizeInBytes(), "Should notta updated any other properties.");
    }
    
    
    @Test
    public void testEntryLoadedToCacheUpdatesCachedSizeOfJob()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 1 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 10 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 100 );
        mockDaoDriver.createObject( null, "o4", 1000 );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.values()[ 0 ] );
        final JobEntry entry1 = mockDaoDriver.createJobEntry(job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry(job.getId(), b2 );
        
        final JobProgressManager manager =
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.YES );
        final JobService service = dbSupport.getServiceManager().getService( JobService.class );
        manager.entryLoadedToCache( dbSupport.getServiceManager(), entry1 );
        assertEquals(0,  service.attain(job.getId()).getCachedSizeInBytes(), "Should notta updated cached size of job immediately.");
        manager.flush();
        assertEquals(1,  service.attain(job.getId()).getCachedSizeInBytes(), "Shoulda updated cached size of job.");
        manager.entryLoadedToCache( dbSupport.getServiceManager(), entry2 );
        manager.flush();
        assertEquals(11,  service.attain(job.getId()).getCachedSizeInBytes(), "Shoulda updated cached size of job.");
        assertEquals(0,  service.attain(job.getId()).getOriginalSizeInBytes(), "Should notta updated any other properties.");
        assertEquals(0,  service.attain(job.getId()).getCompletedSizeInBytes(), "Should notta updated any other properties.");
    }
    
    
    @Test
    public void testWorkCompletedUpdatedCompletedSizeOfJob()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 1 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 10 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 100 );
        mockDaoDriver.createObject( null, "o4", 1000 );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.values()[ 0 ] );
        mockDaoDriver.createJobEntries(job.getId(), CollectionFactory.toSet( b1 ) );
        mockDaoDriver.createJobEntries(job.getId(), CollectionFactory.toSet( b2, b3 ) );

        final JobProgressManager manager = 
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.YES );
        final JobService service = dbSupport.getServiceManager().getService( JobService.class );
        manager.workCompleted( job.getId(), 2 );
        assertEquals(0,  service.attain(job.getId()).getCompletedSizeInBytes(), "Should notta updated completed size of job immediately.");
        manager.flush();
        assertEquals(2,  service.attain(job.getId()).getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        manager.workCompleted( job.getId(), 220 );
        manager.flush();
        assertEquals(222,  service.attain(job.getId()).getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        assertEquals(0,  service.attain(job.getId()).getOriginalSizeInBytes(), "Should notta updated any other properties.");
        assertEquals(0,  service.attain(job.getId()).getCachedSizeInBytes(), "Should notta updated any other properties.");
    }
}

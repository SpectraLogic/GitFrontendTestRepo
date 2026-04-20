/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.platform.notification.domain.payload.JobCompletedNotificationPayload;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.MockBeansServiceManager;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class JobCompletedNotificationPayloadGenerator_Test 
{
    @Test
    public void testConstructorNullJobIdNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
 
            public void test()
            {
                new JobCompletedNotificationPayloadGenerator(
                        null,
                        false,
                        new MockBeansServiceManager().getRetriever( S3Object.class ),
                        new MockBeansServiceManager().getRetriever( Blob.class ) );
            }
        } );
    }
    

    @Test
    public void testConstructorNullObjectRetrieverNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {

           public void test()
            {
                new JobCompletedNotificationPayloadGenerator(
                        UUID.randomUUID(),
                        false,
                        null,
                        new MockBeansServiceManager().getRetriever( Blob.class ) );
            }
        } );
    }
    

    @Test
    public void testConstructorNullBlobRetrieverNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {

           public void test()
            {
                new JobCompletedNotificationPayloadGenerator(
                        UUID.randomUUID(),
                        false,
                        new MockBeansServiceManager().getRetriever( S3Object.class ),
                        null );
            }
        } );
    }
    

    @Test
    public void testHappyConstruction()
    {
        new JobCompletedNotificationPayloadGenerator(
                UUID.randomUUID(),
                false,
                new MockBeansServiceManager().getRetriever( S3Object.class ),
                new MockBeansServiceManager().getRetriever( Blob.class ) );
    }
    
    
    @Test
    public void testResponseIsCorrectWhenJobWasNotCancelled()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user =
                BeanFactory.newBean( User.class ).setName( "myUser" )
                .setAuthId( "myAuthId" ).setSecretKey( "mySecretKey" );
        dbSupport.getDataManager().createBean( user );
        
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "myBucket" );
        
        final Job job = BeanFactory.newBean( Job.class )
                .setBucketId( bucket.getId() ).setRequestType( JobRequestType.values()[ 0 ] )
                .setUserId( user.getId() );
        dbSupport.getDataManager().createBean( job );
        
        final NotificationPayloadGenerator generator = new JobCompletedNotificationPayloadGenerator(
                job.getId(),
                false,
                dbSupport.getServiceManager().getRetriever( S3Object.class ),
                dbSupport.getServiceManager().getRetriever( Blob.class ) );
        final JobCompletedNotificationPayload event = 
                (JobCompletedNotificationPayload)generator.generateNotificationPayload();
        NotificationPayloadTracker.register( event );
        assertEquals(false, event.isCancelOccurred(), "Shoulda returned a sensible response.");
        assertEquals(0,  event.getObjectsNotPersisted().length, "Shoulda returned a sensible response.");
    }
    
    
    @Test
    public void testResponseIsCorrectWhenJobWasCancelledAndNoEntriesStillExist()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user =
                BeanFactory.newBean( User.class ).setName( "myUser" )
                .setAuthId( "myAuthId" ).setSecretKey( "mySecretKey" );
        dbSupport.getDataManager().createBean( user );
        
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "myBucket" );
        
        final Job job = BeanFactory.newBean( Job.class )
                .setBucketId( bucket.getId() ).setRequestType( JobRequestType.values()[ 0 ] )
                .setUserId( user.getId() );
        dbSupport.getDataManager().createBean( job );
        
        final NotificationPayloadGenerator generator = new JobCompletedNotificationPayloadGenerator(
                job.getId(),
                true,
                dbSupport.getServiceManager().getRetriever( S3Object.class ),
                dbSupport.getServiceManager().getRetriever( Blob.class ) );
        final JobCompletedNotificationPayload event = 
                (JobCompletedNotificationPayload)generator.generateNotificationPayload();
        NotificationPayloadTracker.register( event );
        assertEquals(true, event.isCancelOccurred(), "Shoulda returned a sensible response.");
        assertEquals(0,  event.getObjectsNotPersisted().length, "Should not have any entries because they'd already be gone when job completed");
    }


    @Test
    public void testResponseIsCorrectWhenJobWasCancelledBasedOnJobEntries()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user =
                BeanFactory.newBean( User.class ).setName( "myUser" )
                        .setAuthId( "myAuthId" ).setSecretKey( "mySecretKey" );
        dbSupport.getDataManager().createBean( user );

        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "myBucket" );

        final Job job = BeanFactory.newBean( Job.class )
                .setBucketId( bucket.getId() ).setRequestType( JobRequestType.values()[ 0 ] )
                .setUserId( user.getId() );
        dbSupport.getDataManager().createBean( job );

        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2", 100 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );
        mockDaoDriver.createObject( bucket.getId(), "o3", 100 );

        mockDaoDriver.createJobEntries(job.getId(), CollectionFactory.toSet( b1, b2 ) );

        final NotificationPayloadGenerator generator = new JobCompletedNotificationPayloadGenerator(
                job.getId(),
                false,
                dbSupport.getServiceManager().getRetriever( S3Object.class ),
                dbSupport.getServiceManager().getRetriever( Blob.class ) );
        final JobCompletedNotificationPayload event =
                (JobCompletedNotificationPayload)generator.generateNotificationPayload();
        NotificationPayloadTracker.register( event );
        assertEquals(true, event.isCancelOccurred(), "Shoulda returned a sensible response.");
        assertEquals(2,  event.getObjectsNotPersisted().length, "Shoulda returned a sensible response.");
    }
}

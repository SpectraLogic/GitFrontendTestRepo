/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.platform.notification.domain.payload.S3ObjectsCachedNotificationPayload;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.MockBeansServiceManager;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import java.util.Set;
import java.util.UUID;

public final class S3ObjectsCachedNotificationPayloadGenerator_Test 
{

    @Test
    public void testHappyConstruction()
    {
        new S3ObjectsCachedNotificationPayloadGenerator(
                UUID.randomUUID(),
                Set.of(BeanFactory.newBean( JobEntry.class )),
                new MockBeansServiceManager().getRetriever( S3Object.class ),
                new MockBeansServiceManager().getRetriever( Blob.class ) );
    }
    

    @Test
    public void testResponseIsCorrect()
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
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3", 100 );
        final Blob b3 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain( 
                Blob.OBJECT_ID, o3.getId() );

        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(job.getId(), CollectionFactory.toSet( b1, b2 ) );
        mockDaoDriver.createJobEntries(job.getId(), CollectionFactory.toSet( b3 ) );
        
        final NotificationPayloadGenerator generator = new S3ObjectsCachedNotificationPayloadGenerator(
                job.getId(),
                chunks,
                dbSupport.getServiceManager().getRetriever( S3Object.class ),
                dbSupport.getServiceManager().getRetriever( Blob.class ) );
        final S3ObjectsCachedNotificationPayload event = 
                (S3ObjectsCachedNotificationPayload)generator.generateNotificationPayload();
        NotificationPayloadTracker.register( event );
        assertEquals(
                job.getId(),
                event.getJobId(),
                "Shoulda returned correct response."
                 );
        assertEquals(
                2,
                event.getObjects().length,
                "Shoulda returned correct response."
                 );
    }
}

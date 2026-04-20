/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.platform.notification.domain.payload.S3ObjectsLostNotificationPayload;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.MockBeansServiceManager;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class S3ObjectsLostNotificationPayloadGenerator_Test 
{
    @Test
    public void testConstructorNullBlobIdsNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new S3ObjectsLostNotificationPayloadGenerator(
                        null, 
                        new MockBeansServiceManager().getRetriever( Bucket.class ),
                        new MockBeansServiceManager().getRetriever( S3Object.class ),
                        new MockBeansServiceManager().getRetriever( Blob.class ) );
            }
        } );
    }
    

    @Test
    public void testConstructorNullBucketRetrieverNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new S3ObjectsLostNotificationPayloadGenerator(
                        CollectionFactory.toSet( UUID.randomUUID() ),
                        null,
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
                new S3ObjectsLostNotificationPayloadGenerator(
                        CollectionFactory.toSet( UUID.randomUUID() ),
                        new MockBeansServiceManager().getRetriever( Bucket.class ),
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
                new S3ObjectsLostNotificationPayloadGenerator(
                        CollectionFactory.toSet( UUID.randomUUID() ),
                        new MockBeansServiceManager().getRetriever( Bucket.class ),
                        new MockBeansServiceManager().getRetriever( S3Object.class ),
                        null );
            }
        } );
    }
    

    @Test
    public void testHappyConstruction()
    {
        new S3ObjectsLostNotificationPayloadGenerator(
                CollectionFactory.toSet( UUID.randomUUID() ),
                new MockBeansServiceManager().getRetriever( Bucket.class ),
                new MockBeansServiceManager().getRetriever( S3Object.class ),
                new MockBeansServiceManager().getRetriever( Blob.class ) );
    }
    
    
    @Test
    public void testResponseIsCorrect()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );

        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain( 
                Blob.OBJECT_ID, o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2", 100 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain( 
                Blob.OBJECT_ID, o2.getId() );
        mockDaoDriver.createObject( bucket.getId(), "o3", 100 );
        
        final NotificationPayloadGenerator generator = new S3ObjectsLostNotificationPayloadGenerator(
                CollectionFactory.toSet( b1.getId(), b2.getId() ),
                dbSupport.getServiceManager().getRetriever( Bucket.class ),
                dbSupport.getServiceManager().getRetriever( S3Object.class ),
                dbSupport.getServiceManager().getRetriever( Blob.class ) );
        final S3ObjectsLostNotificationPayload event = 
                (S3ObjectsLostNotificationPayload)generator.generateNotificationPayload();
        NotificationPayloadTracker.register( event );
        assertEquals(
                2,
                event.getObjects().length,
                "Shoulda returned correct response."
                 );
    }
}

/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class BucketLogicalSizeCacheImpl_Test 
{
    @Test
    public void testConstructorNullBeansRetrieverManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
        public void test()
            {
                new BucketLogicalSizeCacheImpl( null );
            }
        } );
    }
    
    
    @Test
    public void testObjectMetadataCacheWorksWhenNoObjectsInitially()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final UUID bucketId1 = UUID.randomUUID();
        final UUID bucketId2 = UUID.randomUUID();
        
        final BucketLogicalSizeCacheImpl cache =
                new BucketLogicalSizeCacheImpl( dbSupport.getServiceManager() );
        cache.blobCreated( bucketId1, 1000 );
        cache.blobCreated( bucketId1, 0 );
        cache.blobCreated( bucketId1, 1 );
        cache.blobCreated( bucketId1, 2 );
        cache.blobCreated( bucketId1, 3 );
        cache.blobCreated( bucketId1, 10 );
        cache.blobCreated( bucketId1, 20 );
        cache.blobCreated( bucketId1, 30 );
        cache.blobCreated( bucketId1, 75 );
        cache.blobCreated( bucketId1, 19 );
        cache.blobCreated( bucketId1, 6 );
        cache.blobCreated( bucketId1, 200 );
        cache.blobCreated( bucketId1, 300 );
        
        cache.blobCreated( bucketId2, 2000 );
        cache.waitUntilInitialized();
        assertEquals(2000,  cache.getSize(bucketId2), "Shoulda reported correct size.");

        assertEquals(1666,  cache.getSize(bucketId1), "Shoulda reported correct size.");

        cache.blobDeleted( bucketId1, 3 );
        cache.blobDeleted( bucketId1, 10 );
        cache.blobDeleted( bucketId1, 20 );
        cache.blobDeleted( bucketId1, 30 );

        assertEquals(1603,  cache.getSize(bucketId1), "Shoulda reported correct size.");
        assertEquals(3603,  cache.getSize(null), "Shoulda reported correct size.");
    }
    
    
    @Test
    public void testObjectMetadataCacheWorksWhenObjectsInitially()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID bucketId2 = UUID.randomUUID();
        
        final User user = BeanFactory.newBean( User.class )
                .setAuthId( "a" ).setName( "b" ).setSecretKey( "c" );
        dbSupport.getDataManager().createBean( user );
        
        final Bucket b1 = mockDaoDriver.createBucket( user.getId(), "b1" );
        final UUID bucketId1 = b1.getId();
        
        mockDaoDriver.createObject( b1.getId(), "b", 2000 );
        mockDaoDriver.createObject( b1.getId(), "music/raw/avater/d.mp3", 30 );
        
        final BucketLogicalSizeCacheImpl cache = 
                new BucketLogicalSizeCacheImpl( dbSupport.getServiceManager() );
        cache.blobCreated( bucketId1, 1000 );
        cache.blobCreated( bucketId1, 0 );
        cache.blobCreated( bucketId1, 1 );
        cache.blobCreated( bucketId1, 2 );
        cache.blobCreated( bucketId1, 3 );
        cache.blobCreated( bucketId1, 10 );
        cache.blobCreated( bucketId1, 20 );
        cache.blobCreated( bucketId1, 30 );
        cache.blobCreated( bucketId1, 100 );
        cache.blobCreated( bucketId1, 200 );
        cache.blobCreated( bucketId1, 300 );
        
        cache.blobCreated( bucketId2, 2000 );
        cache.waitUntilInitialized();
        assertEquals(2000,  cache.getSize(bucketId2), "Shoulda reported correct size.");
        assertEquals(3696,  cache.getSize(bucketId1), "Shoulda reported correct size.");

        cache.blobDeleted( bucketId1, 3 );
        cache.blobDeleted( bucketId1, 10 );
        cache.blobDeleted( bucketId1, 19 );
        cache.blobDeleted( bucketId1, 1 );
        cache.blobDeleted( bucketId1, 30 );

        assertEquals(3633,  cache.getSize(bucketId1), "Shoulda reported correct size.");
        assertEquals(5633,  cache.getSize(null), "Shoulda reported correct size.");
    }
}

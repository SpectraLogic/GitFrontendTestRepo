/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class BlobServiceImpl_Test 
{
    @Test
    public void testGetSizeInBytesOfBlobIdsReturnsSizeInBytes()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final BeansServiceManager transaction = 
                dbSupport.getServiceManager().startTransaction();
        final S3ObjectService service = transaction.getService( S3ObjectService.class );
        
        final User user =
                BeanFactory.newBean( User.class ).setName( "myUser" )
                .setAuthId( "myAuthId" ).setSecretKey( "mySecretKey" );
        dbSupport.getDataManager().createBean( user );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "myBucket" );
        mockDaoDriver.createBucket( user.getId(), "myBucket2" );
        
        final S3Object o1 = BeanFactory.newBean( S3Object.class ).setBucketId( bucket.getId() )
                .setName( "a" );
        o1.setId( UUID.randomUUID() );
        final Blob b1 = BeanFactory.newBean( Blob.class )
                .setByteOffset( 0 ).setLength( 99 ).setObjectId( o1.getId() );
        final Blob b2 = BeanFactory.newBean( Blob.class )
                .setByteOffset( 99 ).setLength( 900 ).setObjectId( o1.getId() );
        final Set< S3Object > objects = new HashSet<>();
        final Set< Blob > blobs = new HashSet<>();
        for ( int i = 0; i < 100; ++i )
        {
            final S3Object o = BeanFactory.newBean( S3Object.class ).setBucketId( bucket.getId() )
                    .setName( "object" + i );
            o.setId( UUID.randomUUID() );
            objects.add( o );
            blobs.add( BeanFactory.newBean( Blob.class )
                    .setObjectId( o.getId() ).setByteOffset( 0 ).setLength( 3 ) );
        }
        service.create( objects );
        service.create( CollectionFactory.toSet( o1 ) ); 
        transaction.getService( BlobService.class ).create( CollectionFactory.toSet( b1, b2 ) );
        transaction.getService( BlobService.class ).create( blobs );
        transaction.commitTransaction();

        final Object actual = dbSupport.getServiceManager().getService( BlobService.class ).getSizeInBytes(
                BeanUtils.toMap( blobs ).keySet() );
        assertEquals(objects.size() * 3L, actual, "Shoulda returned size in bytes.");
    }
    
    
    @Test
    public void testDeleteSetOfIdsDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 10 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 10 );
        final Blob blob3 = mockDaoDriver.getBlobFor( o3.getId() );
        
        dbSupport.getServiceManager().getService( BlobService.class ).delete( 
                CollectionFactory.toSet( blob1.getId(), blob2.getId() ) );
        final Object expected = blob3.getId();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                        Require.nothing() ).getId(), "Shoulda deleted only the blobs sent to the service to delete.");
    }
}

/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.orm;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class ORM_Test
{
    @Test
    public void testOrmChainingProducesSameResultsAsNestedExistsQueries()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );      
        BeansServiceManager serviceManager = dbSupport.getServiceManager();
        
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "dp" );
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "mybucket" );
        final S3Object obj = mockDaoDriver.createObject( bucket.getId(), "myobject" );
        final Blob blob = mockDaoDriver.getBlobFor( obj.getId() );
        
        final DataPolicy other_dp = mockDaoDriver.createDataPolicy( "other_dp" );
        final Bucket other_bucket = mockDaoDriver.createBucket( null, other_dp.getId(), "other_mybucket" );
        mockDaoDriver.createObject( other_bucket.getId(), "other_myobject" );
        
        DataPolicy dp1 = serviceManager.getRetriever( DataPolicy.class ).attain(
                Require.exists( Bucket.class, Bucket.DATA_POLICY_ID,
                        Require.exists ( S3Object.class, S3Object.BUCKET_ID,
                                Require.exists( Blob.class, Blob.OBJECT_ID,
                                        Require.beanPropertyEquals( Identifiable.ID, blob.getId() ) )
                        ) ) );
        
        DataPolicy dp2 = new BlobRM( blob , serviceManager ).getObject().getBucket().getDataPolicy().unwrap();
        
        assertEquals( dp.getId(), dp1.getId() );
        assertEquals( dp1.getId(), dp2.getId() );
        
                
        Blob blob1 = serviceManager.getRetriever( Blob.class ).attain(
                Require.exists( Blob.OBJECT_ID,
                        Require.exists( S3Object.BUCKET_ID,
                                Require.exists( Bucket.DATA_POLICY_ID,
                                        Require.beanPropertyEquals( Identifiable.ID, dp.getId() ) )
                        ) ) );
        
        Bucket bucket1 = new DataPolicyRM( dp.getId(), serviceManager ).getBuckets().toList().get( 0 );
        S3Object obj1 = new BucketRM( bucket1.getId(), serviceManager ).getS3Objects().toList().get( 0 );
        Blob blob2 = new S3ObjectRM( obj1.getId(), serviceManager ).getBlobs().toList().get( 0 );
        
        assertEquals( blob1.getId(), blob.getId()  );
        assertEquals( blob1.getId(), blob2.getId() );
    }
}

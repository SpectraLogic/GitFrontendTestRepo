/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.s3target;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.S3Connection;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.target.PublicCloudConnectionIntegration_Test;
import com.spectralogic.s3.target.PublicCloudSupport;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * This class is mostly tested via {@link PublicCloudConnectionIntegration_Test}
 */
@Tag( "public-cloud-integration" )
public final class S3ConnectionImpl_Test
{
    @Test
    public void testConnectionFailsIfBadCredentials()
    {
        if ( !PublicCloudSupport.isPublicCloudSupported() )
        {
            return;
        }
        
        final S3Target target = BeanFactory.newBean( S3Target.class );
        target.setAccessKey( PublicCloudSupport.S3_ACCESS_KEY );
        target.setSecretKey( PublicCloudSupport.S3_BAD_SECRET_KEY );
        
        TestUtil.assertThrows( 
                null, 
                S3SdkFailure.valueOf( PublicCloudSupport.S3_403_AUTHENTICATION, 403 ),
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        new S3ConnectionImpl( target , m_dbSupport.getServiceManager() ).shutdown();
                    }
                } );

        target.setSecretKey( PublicCloudSupport.S3_SECRET_KEY );
        target.setHttps(false);
        target.setRegion(S3Region.US_EAST_1);
        target.setDataPathEndPoint(PublicCloudSupport.S3_ENDPOINT);
        new S3ConnectionImpl( target, m_dbSupport.getServiceManager() ).shutdown();
    }
    
    @Test
    public void testStorageClassRespectedIfSuppliedForWriteData()
            throws IOException, InterruptedException, ExecutionException
    {
        if ( !PublicCloudSupport.isPublicCloudSupported() )
        {
            return;
        }

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( m_dbSupport );
        
        final S3Object o = mockDaoDriver.createObject( null, "o1", 0 );
        mockDaoDriver.simulateObjectUploadCompletion( o.getId() );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        blob.setChecksum( "1B2M2Y8AsgTpgAmY7PhCfg==" );
        
        final long maxBlobPartLength = 30;
        final UUID ownerId = UUID.randomUUID();
        final int version = 222;
        final String bn1 = PublicCloudSupport.getTestBucketName();
        final File file = File.createTempFile( getClass().getSimpleName(), "" );
        file.deleteOnExit();

        final PublicCloudBucketInformation cloudBucket =
                BeanFactory.newBean( PublicCloudBucketInformation.class )
                .setOwnerId( ownerId )
                .setVersion( version )
                .setName( bn1 )
                .setLocalBucketName( "local" + bn1 );
        final S3Target target = BeanFactory.newBean( S3Target.class );
        target.setAccessKey( PublicCloudSupport.S3_ACCESS_KEY );
        target.setSecretKey( PublicCloudSupport.S3_SECRET_KEY );
        target.setDataPathEndPoint(PublicCloudSupport.S3_ENDPOINT);
        target.setHttps(false);
        target.setRegion(S3Region.US_EAST_1);
        final S3Connection connection = new S3ConnectionImpl( target, m_dbSupport.getServiceManager() );
        try
        {
            connection.createOrTakeoverBucket( S3InitialDataPlacementPolicy.GLACIER, cloudBucket );
            final long size = m_dbSupport.getServiceManager().getService( S3ObjectService.class )
        			.getSizeInBytes( o.getId() );
            for ( final S3InitialDataPlacementPolicy policy : S3InitialDataPlacementPolicy.values() )
            {
                new BlobWriter(
                        cloudBucket,
                        o,
                        size,
                        blob, 
                        file,
                        maxBlobPartLength, 
                        policy,
                        connection ).run();
            }
            new BlobWriter(
                    cloudBucket,
                    o,
                    size,
                    blob, 
                    file,
                    maxBlobPartLength, 
                    null,
                    connection ).run();
        }
        finally
        {
            connection.deleteBucket( cloudBucket.getName() );
            file.delete();
        }
    }
    
    
    private final static class BlobWriter implements Runnable
    {
        private BlobWriter(
                final PublicCloudBucketInformation cloudBucket,
                final S3Object o,
                final long size,
                final Blob blob,
                final File file,
                final long maxBlobPartLength,
                final Object initialDataPlacement,
                final S3Connection connection )
        {
            m_cloudBucket = cloudBucket;
            m_o = o;
            m_size = size;
            m_blob = blob;
            m_file = file;
            m_maxBlobPartLength = maxBlobPartLength;
            m_initialDataPlacement = initialDataPlacement;
            m_connection = connection;
        }
        
        
        public void run()
        {
            final List< Future< ? > > writeThreads = new ArrayList<>();
            writeThreads.addAll( m_connection.writeBlobToCloud(
                    m_cloudBucket,
                    m_o,
                    m_size,
                    m_blob,
                    1,
                    m_file,
                    new Date(), 
                    new HashSet< S3ObjectProperty >(), 
                    m_maxBlobPartLength, 
                    m_initialDataPlacement ) );
            RuntimeException ex = null;
            for ( final Future< ? > future : writeThreads )
            {
                try
                {
                    future.get( INFINITE_RETRY_THREAD_TIMEOUT, TimeUnit.SECONDS );
                } catch ( Exception e )
                {
                    // Only save the first exception to be thrown after loop
                    if ( null == ex )
                    {
                        ex = new RuntimeException( e );
                    }
                }
            }
            if ( null != ex )
            {
                throw ex;
            }
        }
        
        
        private final PublicCloudBucketInformation m_cloudBucket;
        private final S3Object m_o;
        private final long m_size;
        private final Blob m_blob;
        private final File m_file;
        private final long m_maxBlobPartLength;
        private final Object m_initialDataPlacement;
        private final S3Connection m_connection;
        private final static int INFINITE_RETRY_THREAD_TIMEOUT = 30;

    } // end inner class def
    final DatabaseSupport m_dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
}

package com.spectralogic.s3.target.testrunners;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.rpc.target.BucketOnPublicCloud;
import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.target.PublicCloudSupport;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.s3.target.PublicCloudConnectionIntegration_Test.getBucketName;
import static com.spectralogic.s3.target.PublicCloudConnectionIntegration_Test.getTestRequestPayload;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class BlobsAndBlobPartsTestRunner  extends BaseTestRunner {
    private final static Logger LOG = Logger.getLogger( BlobsAndBlobPartsTestRunner.class );
    private final static int INFINITE_RETRY_THREAD_TIMEOUT_FOR_SUCCESS = 60;

    public BlobsAndBlobPartsTestRunner(
            final DatabaseSupport dbSupport,
            final ConnectionCreator<?> cc)
    {
        super( dbSupport, cc );
    }


    @Override
    protected void runTest(
            final DatabaseSupport dbSupport,
            final PublicCloudConnection connection ) throws IOException
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final List<Future< ? >> writeThreads = new ArrayList<>();
        final Bucket bucket = mockDaoDriver.createBucket( null, null, getBucketName() );
        final UUID ownerId = UUID.randomUUID();
        final String cloudBucketNamePrefix = PublicCloudSupport.getTestBucketName();
        final int numBlobs = 1;

        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "mbpo", -1 );

        final PublicCloudBucketInformation cloudBucket =
                BeanFactory.newBean( PublicCloudBucketInformation.class )
                        .setLocalBucketName( bucket.getName() )
                        .setName( cloudBucketNamePrefix + bucket.getName() )
                        .setOwnerId( ownerId )
                        .setVersion( 1 );
        connection.createOrTakeoverBucket( null, cloudBucket );
        final File file =
                File.createTempFile( getClass().getSimpleName(), UUID.randomUUID().toString() );
        file.deleteOnExit();
        try
        {
            final FileOutputStream fout = new FileOutputStream( file );
            IOUtils.write( getTestRequestPayload(), fout );
            fout.close();
            final long blobLength = file.length();

            final List<Blob> blobs = mockDaoDriver.createBlobs( o.getId(), numBlobs, blobLength );
            mockDaoDriver.simulateObjectUploadCompletion( o.getId() );
            mockDaoDriver.attainAndUpdate( Blob.class, blobs.get( 0 ) );
            final long size =
                    dbSupport.getServiceManager().getService( S3ObjectService.class ).getSizeInBytes( o.getId() );
            writeThreads.addAll( connection.writeBlobToCloud(
                    cloudBucket,
                    o,
                    size,
                    blobs.get( 0 ).setChecksum( "8Dn/GQtZ78RR+slAJGUjuA==" ),
                    blobs.size(),
                    file,
                    new Date(),
                    new HashSet<S3ObjectProperty>(),
                    blobLength,
                    null ) );
            RuntimeException ex = null;
            for ( final Future< ? > future : writeThreads )
            {
                try
                {
                    future.get( INFINITE_RETRY_THREAD_TIMEOUT_FOR_SUCCESS, TimeUnit.SECONDS );
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

            final BucketOnPublicCloud boc = connection.discoverContents( cloudBucket, null );
            if ( boc.getFailures().size() != 0 )
            {
                for ( final Set< Exception > exceptions : boc.getFailures().values() )
                {
                    for ( final Exception e : exceptions )
                    {
                        throw new RuntimeException( "Shoulda reported no failures.", e );
                    }
                }
                fail( "Shoulda had no failures, but produced " + boc.getFailures().size() +
                        "failures with no associated exceptions" );
            }
            assertEquals(bucket.getName(), boc.getBucketName(), "Shoulda reported contents correctly.");
            assertEquals(1, boc.getObjects().length, "Shoulda reported contents correctly.");
            assertEquals(o.getName(), boc.getObjects()[ 0 ].getObjectName(), "Shoulda reported contents correctly.");
            assertEquals(1, boc.getObjects()[0].getBlobs().length, "Shoulda reported contents correctly.");
        }
        finally
        {
            try
            {
                connection.deleteBucket( cloudBucketNamePrefix + bucket.getName() );
            }
            catch ( final Exception ex )
            {
                LOG.warn( "Failed to cleanup bucket: " + cloudBucketNamePrefix + bucket.getName(), ex );
            }
            file.delete();
        }
    }
}

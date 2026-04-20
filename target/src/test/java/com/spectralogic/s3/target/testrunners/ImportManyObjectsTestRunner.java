package com.spectralogic.s3.target.testrunners;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.dao.service.ds3.BlobService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectMetadataKeyValue;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.target.BucketOnPublicCloud;
import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.target.PublicCloudSupport;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.security.ChecksumType;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class ImportManyObjectsTestRunner extends BaseTestRunner {
    private final static Logger LOG = Logger.getLogger( ImportManyObjectsTestRunner.class );
    private final static int INFINITE_RETRY_THREAD_TIMEOUT_FOR_SUCCESS = 60;

    public ImportManyObjectsTestRunner(
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
        final Bucket bucket = mockDaoDriver.createBucket( null, null, getBucketName() );
        final UUID ownerId = UUID.randomUUID();
        final String cloudBucketNamePrefix = PublicCloudSupport.getTestBucketName();
        final int numObjects = 140;

        final String objectNamePrefix = "oscal";
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
            int expectedBlobTotalCount = 0;
            final FileOutputStream fout = new FileOutputStream( file );
            IOUtils.write( getTestRequestPayload(), fout );
            fout.close();
            final long blobLength = file.length();

            final List<S3Object> objects = new ArrayList<>();
            final Map< UUID, List<Blob> > blobs = new HashMap<>();
            for ( int i = 0; i < numObjects; ++i )
            {
                final UUID oid = UUID.randomUUID();
                objects.add( (S3Object)BeanFactory.newBean( S3Object.class )
                        .setBucketId( bucket.getId() )
                        .setCreationDate( new Date( Long.valueOf( i ) ) )
                        .setName( objectNamePrefix + i )
                        .setType( S3ObjectType.DATA )
                        .setId( oid ) );
                blobs.put( oid, new ArrayList< Blob >() );
                for ( int j = 0; j < Math.max( 1, i / 9 ); ++j )
                {
                    ++expectedBlobTotalCount;
                    blobs.get( oid ).add( BeanFactory.newBean( Blob.class )
                            .setByteOffset( j * blobLength )
                            .setLength( blobLength )
                            .setObjectId( oid )
                            .setChecksum( "8Dn/GQtZ78RR+slAJGUjuA==" )
                            .setChecksumType( ChecksumType.MD5 ) );
                }
            }

            final Duration duration = new Duration();
            final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
            try
            {
                transaction.getService( S3ObjectService.class ).create( new HashSet<>( objects ) );
                final Set< Blob > allBlobs = new HashSet<>();
                for ( final S3Object o : objects )
                {
                    allBlobs.addAll( blobs.get( o.getId() ) );
                }
                transaction.getService( BlobService.class ).create( allBlobs );
                transaction.commitTransaction();
            }
            finally
            {
                transaction.closeTransaction();
            }
            LOG.info( "Created local objects and blobs in " + duration + "." );
            duration.reset();

            int objectNum = 0;
            int blobNum = 0;
            final List<Future< ? >> writeThreads = new ArrayList<>();
            RuntimeException ex = null;
            for ( final S3Object o : objects )
            {
                for ( final Blob blob : blobs.get( o.getId() ) )
                {
                    final long size = dbSupport.getServiceManager().getService( S3ObjectService.class )
                            .getSizeInBytes( o.getId() );
                    writeThreads.addAll( connection.writeBlobToCloud(
                            cloudBucket,
                            o,
                            size,
                            blob,
                            blobs.get( o.getId() ).size(),
                            file,
                            new Date( Long.valueOf( objectNum ) ),
                            new HashSet<S3ObjectProperty>(),
                            getTestRequestPayload().length,
                            null ) );
                    blobNum++;
                }
                for ( final Future< ? > future : writeThreads )
                {
                    try
                    {
                        future.get( INFINITE_RETRY_THREAD_TIMEOUT_FOR_SUCCESS, TimeUnit.SECONDS );
                    } catch ( Exception e )
                    {
                        // Only save the first exception to be thrown after loopresults
                        if ( null == ex )
                        {
                            ex = new RuntimeException( e );
                        }
                    }
                }

                objectNum++;
            }

            if ( null != ex )
            {
                throw ex;
            }

            duration.reset();

            final List<BucketOnPublicCloud> results = new ArrayList<>();
            String marker = null;
            do
            {
                results.add( connection.discoverContents( cloudBucket, marker ) );
                marker = results.get( results.size() - 1 ).getNextMarker();
            } while ( null != marker );
            LOG.info( "Discovered " + objectNum + " cloud objects and " + blobNum + " cloud blobs in "
                    + duration + "." );

            int actualBlobTotalCount = 0;
            final Map< Integer, Integer > blobsPerObject = new HashMap<>();
            for ( final BucketOnPublicCloud boc : results )
            {
                assertEquals(bucket.getName(), boc.getBucketName(), "Shoulda returned correct discovery info.");
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
                for ( final S3ObjectOnMedia oom : boc.getObjects() )
                {
                    final Integer key = Integer.valueOf(
                            oom.getObjectName().substring( objectNamePrefix.length() ) );
                    final int value = oom.getBlobs().length;
                    actualBlobTotalCount += value;
                    if ( !blobsPerObject.containsKey( key ) )
                    {
                        blobsPerObject.put( key, Integer.valueOf( 0 ) );
                    }
                    blobsPerObject.put(
                            key,
                            Integer.valueOf( blobsPerObject.get( key ).intValue() + value ) );
                }
            }
            assertTrue(1 < results.size(), "Shoulda been at least 2 pages of results.");
            assertEquals(objectNum, blobsPerObject.size(), "Shoulda returned correct discovery info for objects.");
            assertEquals(expectedBlobTotalCount, actualBlobTotalCount, "Shoulda returned correct discovery info for blobs.");

            for ( final BucketOnPublicCloud boc : results )
            {
                for ( final S3ObjectOnMedia oom : boc.getObjects() )
                {
                    final Map< String, String > actualMetadata = new HashMap<>();
                    for ( final S3ObjectMetadataKeyValue kv : oom.getMetadata() )
                    {
                        actualMetadata.put( kv.getKey(), kv.getValue() );
                    }

                    final Integer key = Integer.valueOf(
                            oom.getObjectName().substring( objectNamePrefix.length() ) );
                    final Map< String, String > expectedMetadata = new HashMap<>();
                    expectedMetadata.put(
                            KeyValueObservable.TOTAL_BLOB_COUNT,
                            String.valueOf( blobsPerObject.get( key ) ) );
                    expectedMetadata.put(
                            KeyValueObservable.CREATION_DATE,
                            String.valueOf( key ) );
                    assertEquals(expectedMetadata, actualMetadata, "Metadata should have been as expected for object.");
                }
            }
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

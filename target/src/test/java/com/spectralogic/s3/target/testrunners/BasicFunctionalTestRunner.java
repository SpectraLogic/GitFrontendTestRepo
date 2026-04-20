package com.spectralogic.s3.target.testrunners;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.rpc.tape.domain.BlobOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.target.BucketOnPublicCloud;
import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.target.PublicCloudSupport;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.s3.target.PublicCloudConnectionIntegration_Test.getBucketName;
import static com.spectralogic.s3.target.PublicCloudConnectionIntegration_Test.getTestRequestPayload;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class BasicFunctionalTestRunner extends BaseTestRunner {
    private final static Logger LOG = Logger.getLogger( BasicFunctionalTestRunner.class );
    private final static int INFINITE_RETRY_THREAD_TIMEOUT_FOR_SUCCESS = 60;
    private final static int INFINITE_RETRY_THREAD_TIMEOUT_FOR_FAILURES = 5;

    public BasicFunctionalTestRunner(
            final DatabaseSupport dbSupport,
            final ConnectionCreator<?> cc)
    {
        super( dbSupport, cc );
    }


    @Override
    protected void runTest(
            final DatabaseSupport dbSupport,
            final PublicCloudConnection connection ) throws Exception
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final List<Future< ? >> writeThreads = new ArrayList<>();
        final long maxBlobPartLength = 30;
        final UUID ownerId = UUID.randomUUID();
        final int version = 222;
        final String bn1 = PublicCloudSupport.getTestBucketName();
        final File fileToRead =
                File.createTempFile( getClass().getSimpleName(), UUID.randomUUID().toString() );
        final File fileToWrite =
                File.createTempFile( getClass().getSimpleName(), UUID.randomUUID().toString() );
        fileToWrite.delete();
        fileToRead.deleteOnExit();
        fileToWrite.deleteOnExit();
        try
        {
            String o2Name = "";
            for ( int i = 0; i < 9000; ++i )
            {
                o2Name += "a";
            }
            final Date[] objectCreationDate = new Date [] { new Date() };
            final Bucket bucket = mockDaoDriver.createBucket( null, null, getBucketName() );
            final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1func", -1 );
            final S3ObjectOnMedia oom1 = BeanFactory.newBean( S3ObjectOnMedia.class );
            BeanCopier.copy( oom1, o1 );
            final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), o2Name, 0 );
            final List<Blob> blobs =
                    mockDaoDriver.createBlobs( o1.getId(), 3, 0, getTestRequestPayload().length );
            final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
            final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3func", -1 );
            final List< Blob > blobs3 =
                    mockDaoDriver.createBlobs( o3.getId(), 3, 0, getTestRequestPayload().length );
            for ( final Blob blob : blobs )
            {
                mockDaoDriver.attainAndUpdate( Blob.class, blob );
                mockDaoDriver.updateBean(
                        blob.setChecksum( "8Dn/GQtZ78RR+slAJGUjuA==" )
                                .setChecksumType( ChecksumType.MD5 ),
                        ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
            }
            for ( final Blob blob : blobs3 )
            {
                mockDaoDriver.attainAndUpdate( Blob.class, blob );
                mockDaoDriver.updateBean(
                        blob.setChecksum( "8Dn/GQtZ78RR+slAJGUjuA==" )
                                .setChecksumType( ChecksumType.MD5 ),
                        ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
            }
            mockDaoDriver.attainAndUpdate( Blob.class, blob2 );
            mockDaoDriver.updateBean(
                    blob2.setChecksum( "1B2M2Y8AsgTpgAmY7PhCfg==" )
                            .setChecksumType( ChecksumType.MD5 ),
                    ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
            final FileOutputStream fout = new FileOutputStream( fileToRead );
            IOUtils.write( getTestRequestPayload(), fout );
            fout.close();

            final Map< String, String > propertiesMapping = new HashMap<>();
            propertiesMapping.put( "key1", getClass().getName() );
            propertiesMapping.put( "key2", getClass().getSimpleName() );
            mockDaoDriver.createObjectProperties( o1.getId(), propertiesMapping );
            final PublicCloudBucketInformation cloudBucket =
                    BeanFactory.newBean( PublicCloudBucketInformation.class )
                            .setOwnerId( ownerId )
                            .setVersion( version )
                            .setName( bn1 )
                            .setLocalBucketName( "local" + bn1 );

            TestUtil.assertThrows(
                    "Shoulda failed since bucket not created yet",
                    RuntimeException.class,
                    new TestUtil.BlastContainer()
                    {
                        public void test() throws Throwable
                        {
                            final Duration duration = new Duration();
                            final long size = dbSupport.getServiceManager().getService( S3ObjectService.class )
                                    .getSizeInBytes( o1.getId() );
                            writeThreads.addAll( connection.writeBlobToCloud(
                                    cloudBucket,
                                    o1,
                                    size,
                                    blobs.get( 0 ),
                                    blobs.size(),
                                    fileToRead,
                                    objectCreationDate[ 0 ],
                                    dbSupport.getServiceManager().getRetriever(
                                            S3ObjectProperty.class ).retrieveAll().toSet(),
                                    blobs.get( 0 ).getLength(),
                                    null ) );
                            RuntimeException ex = null;
                            for ( final Future< ? > future : writeThreads )
                            {
                                try
                                {
                                    future.get( INFINITE_RETRY_THREAD_TIMEOUT_FOR_FAILURES, TimeUnit.SECONDS );
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
                    } );

            writeThreads.clear();
            connection.createOrTakeoverBucket( null, cloudBucket );
            final long size = dbSupport.getServiceManager().getService( S3ObjectService.class )
                    .getSizeInBytes( o1.getId() );
            writeThreads.addAll( connection.writeBlobToCloud(
                    cloudBucket,
                    o1,
                    size,
                    blobs.get( 0 ),
                    blobs.size(),
                    fileToRead,
                    null,
                    dbSupport.getServiceManager().getRetriever(
                            S3ObjectProperty.class ).retrieveAll().toSet(),
                    blobs.get( 0 ).getLength(),
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

            writeThreads.clear();
            writeThreads.addAll( connection.writeBlobToCloud(
                    cloudBucket,
                    o1,
                    size,
                    blobs.get( 0 ),
                    blobs.size(),
                    fileToRead,
                    objectCreationDate[ 0 ],
                    dbSupport.getServiceManager().getRetriever(
                            S3ObjectProperty.class ).retrieveAll().toSet(),
                    blobs.get( 0 ).getLength(),
                    null ) );

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

            writeThreads.clear();
            writeThreads.addAll( connection.writeBlobToCloud(
                    cloudBucket,
                    o1,
                    size,
                    blobs.get( 0 ),
                    blobs.size(),
                    fileToRead,
                    objectCreationDate[ 0 ],
                    dbSupport.getServiceManager().getRetriever(
                            S3ObjectProperty.class ).retrieveAll().toSet(),
                    blobs.get( 0 ).getLength(),
                    null ) );

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

            writeThreads.clear();
            writeThreads.addAll( connection.writeBlobToCloud(
                    cloudBucket,
                    o1,
                    size,
                    //TODO: Previously this code was using an incorrect checksum, but is not written to expect an exception. Investigate.
                    //blobs.get( 0 ).setChecksum( "9Dn/GQtZ78RR+slAJGUjuA==" ),
                    blobs.get( 0 ).setChecksum( "8Dn/GQtZ78RR+slAJGUjuA==" ),
                    blobs.size(),
                    fileToRead,
                    objectCreationDate[ 0 ],
                    dbSupport.getServiceManager().getRetriever(
                            S3ObjectProperty.class ).retrieveAll().toSet(),
                    blobs.get( 0 ).getLength(),
                    null ) );

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

            writeThreads.clear();
            writeThreads.addAll( connection.writeBlobToCloud(
                    cloudBucket,
                    o1,
                    size,
                    blobs.get( 1 ),
                    blobs.size(),
                    fileToRead,
                    objectCreationDate[ 0 ],
                    dbSupport.getServiceManager().getRetriever(
                            S3ObjectProperty.class ).retrieveAll().toSet(),
                    blobs.get( 1 ).getLength(),
                    null ) );

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

            writeThreads.clear();
            blobs.get( 2 ).setChecksum( "9Dn/GQtZ78RR+slAJGUjuA==" )
                    .setChecksumType( ChecksumType.SHA_512 );
            writeThreads.addAll( connection.writeBlobToCloud(
                    cloudBucket,
                    o1,
                    size,
                    blobs.get( 2 ),
                    blobs.size(),
                    fileToRead,
                    objectCreationDate[ 0 ],
                    dbSupport.getServiceManager().getRetriever(
                            S3ObjectProperty.class ).retrieveAll().toSet(),
                    blobs.get( 2 ).getLength(),
                    null ) );

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

            assertFalse(fileToWrite.exists(), "Should notta existed initially.");

            writeThreads.clear();
            writeThreads.addAll( connection.readBlobFromCloud( cloudBucket, o1, blobs.get( 0 ), fileToWrite ) );
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

            assertTrue(fileToWrite.exists(), "Shoulda loaded file from cloud.");

            final FileInputStream fin = new FileInputStream( fileToWrite );
            final byte [] retrievedData = IOUtils.toByteArray( fin );
            fin.close();

            assertEquals("This is the request payload that we are verifying the checksum for.", new String( retrievedData, "UTF-8" ), "Shoulda reconstructed data correctly.");

            final long originalBlobLength = blobs.get( 0 ).getLength();
            TestUtil.assertThrows(
                    "Shoulda failed since blob length mismatch compared to cloud",
                    RuntimeException.class,
                    new TestUtil.BlastContainer()
                    {
                        public void test() throws Throwable
                        {
                            final Duration duration = new Duration();
                            writeThreads.clear();
                            writeThreads.addAll( connection.readBlobFromCloud(
                                    cloudBucket,
                                    o1,
                                    blobs.get( 0 ).setLength( originalBlobLength - 1 ),
                                    fileToWrite ) );
                            RuntimeException ex = null;
                            for ( final Future< ? > future : writeThreads )
                            {
                                try
                                {
                                    future.get( INFINITE_RETRY_THREAD_TIMEOUT_FOR_FAILURES, TimeUnit.SECONDS );
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
                    } );
            TestUtil.assertThrows(
                    "Shoulda failed since blob length mismatch compared to cloud",
                    RuntimeException.class,
                    new TestUtil.BlastContainer()
                    {
                        public void test() throws Throwable
                        {
                            final Duration duration = new Duration();
                            writeThreads.clear();
                            writeThreads.addAll( connection.readBlobFromCloud(
                                    cloudBucket,
                                    o1,
                                    blobs.get( 0 ).setLength( originalBlobLength + 1 ),
                                    fileToWrite ) );
                            RuntimeException ex = null;
                            for ( final Future< ? > future : writeThreads )
                            {
                                try
                                {
                                    future.get( INFINITE_RETRY_THREAD_TIMEOUT_FOR_FAILURES, TimeUnit.SECONDS );
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
                    } );

            // The below lines up with a blob part boundary, which is why it doesn't throw
            writeThreads.clear();
            writeThreads.addAll( connection.readBlobFromCloud(
                    cloudBucket,
                    o1,
                    blobs.get( 0 ).setLength( originalBlobLength ),
                    fileToWrite ) );

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

            fileToWrite.delete();
            fileToWrite.createNewFile();

            writeThreads.clear();
            final long size2 = dbSupport.getServiceManager().getService( S3ObjectService.class )
                    .getSizeInBytes( o2.getId() );
            writeThreads.addAll( connection.writeBlobToCloud(
                    cloudBucket,
                    o2,
                    size2,
                    blob2,
                    1,
                    fileToWrite,
                    objectCreationDate[ 0 ],
                    new HashSet< S3ObjectProperty >(),
                    maxBlobPartLength,
                    null ) );

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

            fileToWrite.delete();
            writeThreads.clear();
            writeThreads.addAll( connection.readBlobFromCloud(
                    cloudBucket,
                    o2,
                    blob2,
                    fileToWrite ) );

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
            assertTrue(fileToWrite.exists(), "Shoulda restored file.");
            assertEquals(0, fileToWrite.length(), "Shoulda restored file.");

            final BucketOnPublicCloud contents =
                    connection.discoverContents( cloudBucket, null );
            if ( contents.getFailures().size() != 0 )
            {
                for ( final Set<Exception> exceptions : contents.getFailures().values() )
                {
                    for ( final Exception e : exceptions )
                    {
                        throw new RuntimeException( "Shoulda reported no failures.", e );
                    }
                }
                fail( "Shoulda had no failures, but produced " + contents.getFailures().size() +
                        "failures with no associated exceptions" );
            }
            assertNull(contents.getNextMarker(), "Shoulda fit all results into single page.");
            assertEquals("local" + bn1, contents.getBucketName(), "Shoulda populated correct cloud bucket name");
            assertEquals(2, contents.getObjects().length, "Shoulda reported both objects written to cloud");
            assertEquals(
                    CollectionFactory.toSet( o1.getId(), o2.getId() ),
                    CollectionFactory.toSet(
                                contents.getObjects()[ 0 ].getId(),
                                contents.getObjects()[ 1 ].getId() ),
                    "Shoulda reported contents from cloud accurately.");
            assertEquals(
                    CollectionFactory.toSet( o1.getName(), o2.getName() ),
                    CollectionFactory.toSet(
                                contents.getObjects()[ 0 ].getObjectName(),
                                contents.getObjects()[ 1 ].getObjectName() ),
                    "Shoulda reported contents from cloud accurately.");
            assertEquals(
                    CollectionFactory.toSet( Integer.valueOf( 2 ), Integer.valueOf( 4 ) ),
                    CollectionFactory.toSet(
                                Integer.valueOf( contents.getObjects()[ 0 ].getMetadata().length ),
                                Integer.valueOf( contents.getObjects()[ 1 ].getMetadata().length ) ),
                    "Shoulda reported contents from cloud accurately.");
            assertEquals(
                    CollectionFactory.toSet( Integer.valueOf( 1 ), Integer.valueOf( 3 ) ),
                    CollectionFactory.toSet(
                                Integer.valueOf( contents.getObjects()[ 0 ].getBlobs().length ),
                                Integer.valueOf( contents.getObjects()[ 1 ].getBlobs().length ) ),
                    "Shoulda reported contents from cloud accurately.");

            final Set<BlobOnMedia> boms = new HashSet<>();
            boms.addAll( CollectionFactory.toSet( contents.getObjects()[ 0 ].getBlobs() ) );
            boms.addAll( CollectionFactory.toSet( contents.getObjects()[ 1 ].getBlobs() ) );
            assertEquals(4, boms.size(), "Shoulda been 4 boms.");
            assertEquals(
                    CollectionFactory.toSet(
                            blob2.getId(),
                            blobs.get( 0 ).getId(),
                            blobs.get( 1 ).getId(),
                            blobs.get( 2 ).getId() ),
                    BeanUtils.extractPropertyValues( boms, Identifiable.ID ),
                    "Shoulda reported contents from cloud accurately.");
            assertEquals(
                    CollectionFactory.toSet( Long.valueOf( 0 ), Long.valueOf( 67 ) ),
                    BeanUtils.extractPropertyValues( boms, BlobOnMedia.LENGTH ),
                    "Shoulda reported contents from cloud accurately.");
            assertEquals(
                    CollectionFactory.toSet( Long.valueOf( 0 ) ),
                    BeanUtils.extractPropertyValues( boms, BlobOnMedia.OFFSET ),
                    "Shoulda reported contents from cloud accurately.");
            assertEquals(
                    CollectionFactory.toSet( ChecksumType.SHA_512, ChecksumType.MD5 ),
                    BeanUtils.extractPropertyValues( boms, ChecksumObservable.CHECKSUM_TYPE ),
                    "Shoulda reported contents from cloud accurately.");
            assertEquals(
                    CollectionFactory.toSet( Long.valueOf( 0 ) ),
                    BeanUtils.extractPropertyValues( boms, BlobOnMedia.OFFSET ),
                    "Shoulda reported contents from cloud accurately.");

            writeThreads.clear();
            final long size3 = dbSupport.getServiceManager().getService( S3ObjectService.class )
                    .getSizeInBytes( o3.getId() );
            writeThreads.addAll( connection.writeBlobToCloud(
                    cloudBucket,
                    o3,
                    size3,
                    blobs3.get( 0 ),
                    blobs.size(),
                    fileToRead,
                    null,
                    dbSupport.getServiceManager().getRetriever(
                            S3ObjectProperty.class ).retrieveAll().toSet(),
                    blobs3.get( 0 ).getLength(),
                    null ) );

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

            writeThreads.clear();
            writeThreads.addAll( connection.readBlobFromCloud(
                    cloudBucket,
                    o1,
                    blobs.get( 0 ),
                    fileToWrite ) );

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
            connection.delete( cloudBucket, CollectionFactory.toSet( oom1 ) );
            TestUtil.sleep( 1000 );
            TestUtil.assertThrows(
                    "Should notta been able to recall deleted cloud object.",
                    RuntimeException.class,
                    new TestUtil.BlastContainer()
                    {
                        public void test() throws Throwable
                        {
                            final Duration duration = new Duration();
                            writeThreads.clear();
                            writeThreads.addAll( connection.readBlobFromCloud(
                                    cloudBucket,
                                    o1,
                                    blobs.get( 0 ),
                                    fileToWrite ) );
                            RuntimeException ex = null;
                            for ( final Future< ? > future : writeThreads )
                            {
                                try
                                {
                                    future.get( INFINITE_RETRY_THREAD_TIMEOUT_FOR_FAILURES, TimeUnit.SECONDS );
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
                    } );
            writeThreads.clear();
            writeThreads.addAll( connection.readBlobFromCloud(
                    cloudBucket,
                    o3,
                    blobs3.get( 0 ),
                    fileToWrite ) );

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
        }
        finally
        {
            try
            {
                connection.deleteBucket( bn1 );
            }
            catch ( final Exception ex )
            {
                LOG.warn( "Failed to cleanup bucket: " + bn1, ex );
            }
            fileToRead.delete();
            fileToWrite.delete();
            connection.shutdown();
        }
    }
}

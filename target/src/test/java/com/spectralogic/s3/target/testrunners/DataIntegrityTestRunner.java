package com.spectralogic.s3.target.testrunners;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.target.PublicCloudSupport;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.s3.target.PublicCloudConnectionIntegration_Test.getBucketName;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DataIntegrityTestRunner extends BaseTestRunner {
    private final static Logger LOG = Logger.getLogger( DataIntegrityTestRunner.class );
    private final static int INFINITE_RETRY_THREAD_TIMEOUT_FOR_SUCCESS = 60;
    private final static int INFINITE_RETRY_THREAD_TIMEOUT_FOR_FAILURES = 5;
    public DataIntegrityTestRunner(
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
        final UUID ownerId = UUID.randomUUID();
        final int version = 222;
        final String bn1 = PublicCloudSupport.getTestBucketName();
        final File file =
                File.createTempFile( getClass().getSimpleName(), UUID.randomUUID().toString() );
        final File badFile =
                File.createTempFile( getClass().getSimpleName(), UUID.randomUUID().toString() );
        file.deleteOnExit();
        badFile.deleteOnExit();
        try
        {
            final Bucket bucket = mockDaoDriver.createBucket( null, null, getBucketName() );
            final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1", 1000 );
            final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
            mockDaoDriver.attainAndUpdate( Blob.class, blob );
            mockDaoDriver.updateBean(
                    blob.setChecksum( "2YXMb+EElVbEBf1D0wkWjg==" )
                            .setChecksumType( ChecksumType.MD5 ),
                    ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
            FileOutputStream fout = new FileOutputStream( file );
            final byte [] buffer = new byte[ (int)( blob.getLength() ) ];
            for ( int i = 0; i < buffer.length; ++i )
            {
                buffer[ i ] = (byte)( i % 100 );
            }
            for ( int i = 0; i < blob.getLength() / buffer.length; ++i )
            {
                fout.write( buffer );
            }

            fout.close();

            final Map< String, String > propertiesMapping = new HashMap<>();
            propertiesMapping.put( "key1", getClass().getName() );
            propertiesMapping.put( "key2", getClass().getSimpleName() );
            mockDaoDriver.createObjectProperties( o.getId(), propertiesMapping );
            final PublicCloudBucketInformation cloudBucket =
                    BeanFactory.newBean( PublicCloudBucketInformation.class )
                            .setOwnerId( ownerId )
                            .setVersion( version )
                            .setName( bn1 )
                            .setLocalBucketName( "local" + bn1 );

            connection.createOrTakeoverBucket( null, cloudBucket );
            final long size = dbSupport.getServiceManager().getService( S3ObjectService.class )
                    .getSizeInBytes( o.getId() );
            writeThreads.addAll( connection.writeBlobToCloud(
                    cloudBucket,
                    o,
                    size,
                    blob,
                    1,
                    file,
                    null,
                    dbSupport.getServiceManager().getRetriever(
                            S3ObjectProperty.class ).retrieveAll().toSet(),
                    blob.getLength(),
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
            TestUtil.assertThrows(
                    "Shoulda failed since blob checksum mismatch to actual data sent",
                    RuntimeException.class,
                    new TestUtil.BlastContainer()
                    {
                        public void test() throws Throwable
                        {
                            final Duration duration = new Duration();
                            final long size = dbSupport.getServiceManager().getService( S3ObjectService.class )
                                    .getSizeInBytes( o.getId() );
                            writeThreads.addAll( connection.writeBlobToCloud(
                                    cloudBucket,
                                    o,
                                    size,
                                    blob.setChecksum( "8Dn/GQtZ78RR+slAJGUjuA==" ),
                                    1,
                                    file,
                                    null,
                                    dbSupport.getServiceManager().getRetriever(
                                            S3ObjectProperty.class ).retrieveAll().toSet(),
                                    Integer.MAX_VALUE,
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
            assertTrue(file.delete(), "Shoulda been able to delete file.");
            assertFalse(file.exists(), "Shoulda been able to delete file.");

            writeThreads.clear();
            TestUtil.assertThrows(
                    "Shoulda failed since blob checksum mismatch to actual data sent",
                    RuntimeException.class,
                    new TestUtil.BlastContainer()
                    {
                        public void test() throws Throwable
                        {
                            final Duration duration = new Duration();
                            writeThreads.clear();
                            writeThreads.addAll( connection.readBlobFromCloud( cloudBucket, o, blob, badFile ) );
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

            blob.setChecksum( "2YXMb+EElVbEBf1D0wkWjg==" );
            fout = new FileOutputStream( file );
            for ( int i = 0; i < blob.getLength() / buffer.length; ++i )
            {
                fout.write( buffer );
            }
            fout.close();

            writeThreads.clear();
            writeThreads.addAll( connection.writeBlobToCloud(
                    cloudBucket,
                    o,
                    size,
                    blob,
                    1,
                    file,
                    null,
                    dbSupport.getServiceManager().getRetriever(
                            S3ObjectProperty.class ).retrieveAll().toSet(),
                    Integer.MAX_VALUE,
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
            writeThreads.addAll( connection.readBlobFromCloud( cloudBucket, o, blob, file ) );

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

            blob.setChecksum( "invalid" );
            blob.setChecksumType( ChecksumType.CRC_32 );
            writeThreads.clear();
            writeThreads.addAll( connection.writeBlobToCloud(
                    cloudBucket,
                    o,
                    size,
                    blob,
                    1,
                    file,
                    null,
                    dbSupport.getServiceManager().getRetriever(
                            S3ObjectProperty.class ).retrieveAll().toSet(),
                    Integer.MAX_VALUE,
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
            writeThreads.addAll( connection.readBlobFromCloud( cloudBucket, o, blob, file ) );

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

            assertTrue(file.exists(), "Shoulda loaded file from cloud.");
            assertEquals(blob.getLength(), file.length(), "Shoulda loaded file from cloud.");
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
            file.delete();
            connection.shutdown();
        }
    }
}

package com.spectralogic.s3.target.testrunners;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.target.BlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.target.PublicCloudSupport;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.service.api.RetrieveBeansResult;
import com.spectralogic.util.exception.BlobReadFailedException;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.s3.target.PublicCloudConnectionIntegration_Test.getBucketName;
import static com.spectralogic.s3.target.PublicCloudConnectionIntegration_Test.getTestRequestPayload;
import static org.junit.jupiter.api.Assertions.*;


public class SuspectAzureBlobTestRunner extends BaseTestRunner {
    private final static Logger LOG = Logger.getLogger( SuspectAzureBlobTestRunner.class );
    private final static int INFINITE_RETRY_THREAD_TIMEOUT_FOR_SUCCESS = 60;
    private ConnectionCreator<?> cc;
    public SuspectAzureBlobTestRunner(
            final DatabaseSupport dbSupport,
            final ConnectionCreator<?> cc)
    {
        super( dbSupport, cc );
        this.cc = cc;
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
            final Bucket bucket = mockDaoDriver.createBucket( null, null,  getBucketName() );
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
            final S3ObjectOnMedia oom2 = BeanFactory.newBean( S3ObjectOnMedia.class );
            BeanCopier.copy( oom2, o1 );
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

               /* final Map< String, String > propertiesMapping2 = new HashMap<>();
                propertiesMapping2.put( "key1", getClass().getName() );
                propertiesMapping2.put( "key2", getClass().getSimpleName() );
                mockDaoDriver.createObjectProperties( o2.getId(), propertiesMapping2 );*/
            final PublicCloudBucketInformation cloudBucket =
                    BeanFactory.newBean( PublicCloudBucketInformation.class )
                            .setOwnerId( ownerId )
                            .setVersion( version )
                            .setName( bn1 )
                            .setLocalBucketName( "local" + bn1 );

            connection.createOrTakeoverBucket( null, cloudBucket );
            final long size = dbSupport.getServiceManager().getService( S3ObjectService.class )
                    .getSizeInBytes( o1.getId() );

            for (Blob blob: blobs) {
                writeThreads.addAll( connection.writeBlobToCloud(
                        cloudBucket,
                        o1,
                        size,
                        blob,
                        blobs.size(),
                        fileToRead,
                        new Date(),
                        dbSupport.getServiceManager().getRetriever(
                                S3ObjectProperty.class ).retrieveAll().toSet(),
                        blob.getLength(),
                        null ) );
            }



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

            RetrieveBeansResult<AzureTarget> azureTargets = dbSupport.getServiceManager().getRetriever(AzureTarget.class).retrieveAll();
            final BlobAzureTarget blobAzureTarget = BeanFactory.newBean( BlobAzureTarget.class );
            blobAzureTarget.setBlobId( blobs.get(0).getId() );
            blobAzureTarget.setTargetId( azureTargets.getFirst().getId() );
            dbSupport.getServiceManager().getCreator( BlobAzureTarget.class ).create( blobAzureTarget );


            connection.delete( cloudBucket, CollectionFactory.toSet( oom2 ) );
            final RuntimeException thrown = assertThrows( RuntimeException.class, () -> {
                writeThreads.addAll( connection.readBlobFromCloud( cloudBucket, o1, blobs.get(0), fileToWrite ) );

            } );



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
            connection.shutdown();
        }
    }
}


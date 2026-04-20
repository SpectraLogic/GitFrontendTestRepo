package com.spectralogic.s3.target.testrunners;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.target.BlobS3Target;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.rpc.tape.domain.BlobOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.target.PublicCloudSupport;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.service.api.RetrieveBeansResult;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.TestUtil;
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

public class SuspectS3NativeBlobReadTestRunner extends BaseTestRunner {
    private final static Logger LOG = Logger.getLogger( SuspectS3NativeBlobReadTestRunner.class );
    private final static int INFINITE_RETRY_THREAD_TIMEOUT_FOR_SUCCESS = 60;
    private ConnectionCreator<?> cc;
    public SuspectS3NativeBlobReadTestRunner(
            final DatabaseSupport dbSupport,
            final ConnectionCreator<?> cc)
    {
        super( dbSupport, cc );
        this.cc = cc;
    }

    @Override
    protected void runTest(
            final DatabaseSupport dbSupport,
            final PublicCloudConnection connection ) throws Exception {

        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);

        final List<Future<?>> writeThreads = new ArrayList<>();
        final UUID ownerId = UUID.randomUUID();
        final int version = 222;
        final String bn1 = PublicCloudSupport.getTestBucketName();
        final File file =
                File.createTempFile(getClass().getSimpleName(), UUID.randomUUID().toString());
        final File badFile =
                File.createTempFile(getClass().getSimpleName(), UUID.randomUUID().toString());
        file.deleteOnExit();
        badFile.deleteOnExit();
        try {
            final Bucket bucket = mockDaoDriver.createBucket(null, null, getBucketName());
            final S3Object o = mockDaoDriver.createObject(bucket.getId(), "o1", 1000);
            final Blob blob = mockDaoDriver.getBlobFor(o.getId());
            mockDaoDriver.attainAndUpdate(Blob.class, blob);
            mockDaoDriver.updateBean(
                    blob.setChecksum("2YXMb+EElVbEBf1D0wkWjg==")
                            .setChecksumType(ChecksumType.MD5),
                    ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE);
            final S3Object o2 = mockDaoDriver.createObject(bucket.getId(), "o2", 1000);
            final Blob blob2 = mockDaoDriver.getBlobFor(o2.getId());
            mockDaoDriver.attainAndUpdate(Blob.class, blob);

            mockDaoDriver.attainAndUpdate(Blob.class, blob2);
            mockDaoDriver.updateBean(
                    blob2.setChecksum("2YXMb+EElVbEBf1D0wkWjg==")
                            .setChecksumType(ChecksumType.MD5),
                    ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE);

            FileOutputStream fout = new FileOutputStream(file);
            final byte[] buffer = new byte[(int) (blob.getLength())];
            for (int i = 0; i < buffer.length; ++i) {
                buffer[i] = (byte) (i % 100);
            }
            for (int i = 0; i < blob.getLength() / buffer.length; ++i) {
                fout.write(buffer);
            }

            fout.close();


             fout = new FileOutputStream(badFile);
            final byte[] buffer2 = new byte[(int) (blob.getLength())];
            for (int i = 0; i < buffer.length; ++i) {
                buffer2[i] = (byte) (i % 100);
            }
            for (int i = 0; i < blob.getLength() / buffer2.length; ++i) {
                fout.write(buffer2);
            }

            fout.close();

            final Map<String, String> propertiesMapping = new HashMap<>();
            propertiesMapping.put("key1", getClass().getName());
            propertiesMapping.put("key1", getClass().getName());
            propertiesMapping.put(S3ObjectOnMedia.OBJECT_NAME,"Myname");
            mockDaoDriver.createObjectProperties(o.getId(), propertiesMapping);

            //mockDaoDriver.createObjectProperties(o2.getId(), propertiesMapping);
            final S3ObjectOnMedia oom1 = BeanFactory.newBean( S3ObjectOnMedia.class );
            BeanCopier.copy( oom1, o );
            oom1.setObjectName( o.getName() );

            //final S3ObjectOnMedia oom2 = BeanFactory.newBean( S3ObjectOnMedia.class );
            //BeanCopier.copy( oom2, o2 );
            final PublicCloudBucketInformation cloudBucket =
                    BeanFactory.newBean(PublicCloudBucketInformation.class)
                            .setOwnerId(ownerId)
                            .setVersion(version)
                            .setName(bn1)
                            .setLocalBucketName("local" + bn1);

            connection.createOrTakeoverBucket(null, cloudBucket);
            final long size = dbSupport.getServiceManager().getService(S3ObjectService.class)
                    .getSizeInBytes(o.getId());
            writeThreads.addAll(connection.writeBlobToCloud(
                    cloudBucket,
                    o,
                    size,
                    blob,
                    1,
                    file,
                    null,
                    dbSupport.getServiceManager().getRetriever(
                            S3ObjectProperty.class).retrieveAll().toSet(),
                    blob.getLength(),
                    null));
            final long size2 = dbSupport.getServiceManager().getService(S3ObjectService.class)
                    .getSizeInBytes(o2.getId());
            writeThreads.addAll(connection.writeBlobToCloud(
                    cloudBucket,
                    o2,
                    size2,
                    blob2,
                    1,
                    badFile,
                    null,
                    dbSupport.getServiceManager().getRetriever(
                            S3ObjectProperty.class).retrieveAll().toSet(),
                    blob2.getLength(),
                    null));
            RuntimeException ex = null;
            for (final Future<?> future : writeThreads) {
                try {
                    future.get(INFINITE_RETRY_THREAD_TIMEOUT_FOR_SUCCESS, TimeUnit.SECONDS);
                } catch (Exception e) {
                    // Only save the first exception to be thrown after loop
                    if (null == ex) {
                        ex = new RuntimeException(e);
                    }
                }
            }
            if (null != ex) {
                throw ex;
            }



            writeThreads.clear();

            RetrieveBeansResult<S3Target> s3Targets = dbSupport.getServiceManager().getRetriever(S3Target.class).retrieveAll();
            final BlobS3Target blobS3Target = BeanFactory.newBean( BlobS3Target.class );
            blobS3Target.setBlobId( blob2.getId() );
            blobS3Target.setTargetId( s3Targets.getFirst().getId() );
            dbSupport.getServiceManager().getCreator( BlobS3Target.class ).create( blobS3Target );

            connection.delete( cloudBucket, CollectionFactory.toSet( oom1 ) );

            writeThreads.addAll(connection.readBlobFromCloud(cloudBucket, o, blob, file));

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
            assertNotNull(ex);
           


        } finally {
            try {
                connection.deleteBucket(bn1);
            } catch (final Exception ex) {
                LOG.warn("Failed to cleanup bucket: " + bn1, ex);
            }
            file.delete();
            connection.shutdown();
        }
    }
}


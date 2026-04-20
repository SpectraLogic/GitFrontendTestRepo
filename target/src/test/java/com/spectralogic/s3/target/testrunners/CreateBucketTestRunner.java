package com.spectralogic.s3.target.testrunners;

import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.s3.target.PublicCloudSupport;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.apache.log4j.Logger;

import java.util.UUID;

public class CreateBucketTestRunner extends BaseTestRunner {
    private final static Logger LOG = Logger.getLogger( CreateBucketTestRunner.class );

    public CreateBucketTestRunner(
            final DatabaseSupport dbSupport,
            final ConnectionCreator<?> cc)
    {
        super( dbSupport, cc );
    }


    @Override
    protected void runTest( final DatabaseSupport dbSupport, final PublicCloudConnection connection )
    {
        final UUID ownerId = UUID.randomUUID();
        final int version = 222;
        final String bn1 = PublicCloudSupport.getTestBucketName();
        boolean success = false;
        try
        {
            final PublicCloudBucketInformation cloudBucket =
                    BeanFactory.newBean( PublicCloudBucketInformation.class )
                            .setOwnerId( ownerId )
                            .setVersion( version );
            TestUtil.assertThrows( null, RuntimeException.class, new TestUtil.BlastContainer()
            {
                public void test() throws Throwable
                {
                    connection.createOrTakeoverBucket( null, cloudBucket );
                }
            } );

            cloudBucket.setName( bn1 ).setLocalBucketName( "local" + bn1 );
            connection.createOrTakeoverBucket( null, cloudBucket );

            TestUtil.assertThrows( null, RuntimeException.class, new TestUtil.BlastContainer()
            {
                public void test() throws Throwable
                {
                    connection.createOrTakeoverBucket( null, cloudBucket );
                }
            } );

            connection.deleteBucket( bn1 );
            success = true;
        }
        finally
        {
            if ( !success )
            {
                try
                {
                    connection.deleteBucket( bn1 );
                }
                catch ( final Exception ex )
                {
                    LOG.warn( "Failed to cleanup bucket: " + bn1, ex );
                }
            }
            connection.shutdown();
        }
    }

}

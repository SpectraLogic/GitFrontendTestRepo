package com.spectralogic.s3.target.testrunners;

import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.s3.target.PublicCloudSupport;
import com.spectralogic.s3.target.frmwrk.TestablePublicCloudConnection;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import org.apache.log4j.Logger;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class GetBucketContractTestRunner extends BaseTestRunner {
    private final static Logger LOG = Logger.getLogger( GetBucketContractTestRunner.class );
    public GetBucketContractTestRunner(
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
        final UUID ownerId = UUID.randomUUID();
        final int version = 222;
        final String bn1 = PublicCloudSupport.getTestBucketName();
        final String bn2 = PublicCloudSupport.getTestBucketName();
        final String bn3 = PublicCloudSupport.getTestBucketName();
        try
        {
            connection.createOrTakeoverBucket( null, BeanFactory.newBean( PublicCloudBucketInformation.class )
                    .setOwnerId( ownerId )
                    .setVersion( version )
                    .setName( bn1 )
                    .setLocalBucketName( "local" + bn1 ) );
            ( (TestablePublicCloudConnection)connection ).createGenericBucket( bn2 );

            final PublicCloudBucketInformation cloudBucket1 =
                    connection.getExistingBucketInformation( bn1 );
            assertNotNull(cloudBucket1, "Shoulda returned bucket info per contract.");
            assertEquals(bn1, cloudBucket1.getName(), "Shoulda returned bucket info per contract.");
            assertEquals(ownerId, cloudBucket1.getOwnerId(), "Shoulda returned bucket info per contract.");
            assertEquals(version, cloudBucket1.getVersion(), "Shoulda returned bucket info per contract.");

            final PublicCloudBucketInformation cloudBucket2 =
                    connection.getExistingBucketInformation( bn2 );
            assertNotNull(cloudBucket2, "Shoulda returned bucket info per contract.");
            assertEquals(bn2, cloudBucket2.getName(), "Shoulda returned bucket info per contract.");
            assertNull(cloudBucket2.getOwnerId(), "Shoulda returned bucket info per contract.");
            assertEquals(0, cloudBucket2.getVersion(), "Shoulda returned bucket info per contract.");

            final PublicCloudBucketInformation cloudBucket3 =
                    connection.getExistingBucketInformation( bn3 );
            assertNull(cloudBucket3, "Shoulda returned bucket info per contract.");
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
            try
            {
                connection.deleteBucket( bn2 );
            }
            catch ( final Exception ex )
            {
                LOG.warn( "Failed to cleanup bucket: " + bn2, ex );
            }
            connection.shutdown();
        }
    }
}

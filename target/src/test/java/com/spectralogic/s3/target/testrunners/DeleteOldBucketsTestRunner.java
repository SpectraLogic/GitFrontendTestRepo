package com.spectralogic.s3.target.testrunners;

import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.s3.target.PublicCloudConnectionIntegration_Test;
import com.spectralogic.s3.target.PublicCloudSupport;
import com.spectralogic.s3.target.frmwrk.TestablePublicCloudConnection;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import org.apache.log4j.Logger;

public class DeleteOldBucketsTestRunner extends BaseTestRunner {
    public DeleteOldBucketsTestRunner(
            final DatabaseSupport dbSupport,
            final ConnectionCreator<?> cc)
    {
        super( dbSupport, cc );
    }


    @Override
    protected void runTest( final DatabaseSupport dbSupport, final PublicCloudConnection connection )
    {
        try
        {
            ( (TestablePublicCloudConnection)connection )
                    .deleteOldBuckets( 1000 * 60 * 5, PublicCloudSupport.getTestBucketPrefix() );
        }
        finally
        {
            connection.shutdown();
        }
    }

}

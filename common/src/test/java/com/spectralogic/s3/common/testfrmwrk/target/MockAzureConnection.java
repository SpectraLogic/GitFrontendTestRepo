/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.testfrmwrk.target;

import java.util.List;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.rpc.target.AzureConnection;
import com.spectralogic.s3.common.rpc.target.AzureConnectionFactory;
import com.spectralogic.s3.common.rpc.target.BucketOnPublicCloud;
import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

public final class MockAzureConnection 
    extends MockPublicCloudConnection< AzureConnectionFactory > 
    implements AzureConnection
{
    public MockAzureConnection( 
            final BeansServiceManager serviceManager,
            final List< BucketOnPublicCloud > segments )
    {
        super( AzureConnectionFactory.class, serviceManager, segments );
    }
    
    
    public static List< BucketOnPublicCloud > createDiscoverableSegments( 
            final DatabaseSupport dbSupport,
            final AzureTarget target,
            final int objectCount )
    {
        return MockPublicCloudConnection.createDiscoverableSegments( dbSupport, target, objectCount );
    }

    @Override
    public boolean isBlobAvailableOnCloud(PublicCloudBucketInformation cloudBucket, S3Object object, Blob blob) {
        return false;
    }
}

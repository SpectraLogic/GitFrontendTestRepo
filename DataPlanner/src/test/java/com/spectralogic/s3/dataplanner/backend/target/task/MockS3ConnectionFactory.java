/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.rpc.target.S3Connection;
import com.spectralogic.s3.common.rpc.target.S3ConnectionFactory;

public final class MockS3ConnectionFactory
    extends BaseMockPublicCloudConnectionFactory< S3Connection, S3Target > 
    implements S3ConnectionFactory
{
    public MockS3ConnectionFactory()
    {
        super( S3Connection.class );
    }
}

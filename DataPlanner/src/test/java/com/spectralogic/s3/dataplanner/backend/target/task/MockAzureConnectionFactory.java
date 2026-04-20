/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.rpc.target.AzureConnection;
import com.spectralogic.s3.common.rpc.target.AzureConnectionFactory;

public final class MockAzureConnectionFactory 
    extends BaseMockPublicCloudConnectionFactory< AzureConnection, AzureTarget > 
    implements AzureConnectionFactory
{
    public MockAzureConnectionFactory()
    {
        super( AzureConnection.class );
    }
}

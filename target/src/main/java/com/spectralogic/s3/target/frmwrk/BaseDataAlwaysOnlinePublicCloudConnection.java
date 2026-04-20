/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.frmwrk;

import com.spectralogic.s3.common.rpc.target.DataAlwaysOnlinePublicCloudConnection;

public abstract class BaseDataAlwaysOnlinePublicCloudConnection 
    extends BasePublicCloudConnection
    implements DataAlwaysOnlinePublicCloudConnection
{
    @Override
    final protected boolean isBlobPartReadyToBeReadFromCloud(
            final String cloudBucketName,
            final String cloudKeyBlobPart )
    {
        throw new UnsupportedOperationException( 
                "Asking if a blob part is ready to be read from the cloud is non-sensical for "
                + getClass().getSimpleName() + "." );
    }
    
    
    @Override
    final protected void beginStagingBlobPartToRead( 
            final String cloudBucketName,
            final String cloudKeyBlobPart,
            final int stagedDataExpirationInDays )
    {
        throw new UnsupportedOperationException( 
                "Asking to begin staging blob parts is non-sensical for " 
                + getClass().getSimpleName() + "." );
    }
}

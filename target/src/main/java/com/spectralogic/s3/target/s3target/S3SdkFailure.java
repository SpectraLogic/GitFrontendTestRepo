/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.s3target;

import com.spectralogic.s3.target.frmwrk.TargetSdkFailure;

public final class S3SdkFailure extends TargetSdkFailure
{
    private S3SdkFailure( String stringCode, final int intCode )
    {
        super( stringCode, intCode );
    }
    
    
    public static S3SdkFailure valueOf( final String errorCode, final int code )
    {
        return TargetSdkFailure.valueOf( S3SdkFailure.class, errorCode, code );
    }
}

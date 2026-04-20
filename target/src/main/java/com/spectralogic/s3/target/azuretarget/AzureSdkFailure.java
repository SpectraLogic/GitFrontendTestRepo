/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.azuretarget;

import com.spectralogic.s3.target.frmwrk.TargetSdkFailure;

public final class AzureSdkFailure extends TargetSdkFailure
{
    private AzureSdkFailure( String stringCode, final int intCode )
    {
        super( stringCode, intCode );
    }
    
    
    public static AzureSdkFailure valueOf( final String errorCode, final int code )
    {
        return TargetSdkFailure.valueOf( AzureSdkFailure.class, errorCode, code );
    }
}

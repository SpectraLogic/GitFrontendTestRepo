/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.ds3target;

import com.spectralogic.s3.target.frmwrk.TargetSdkFailure;

public final class Ds3SdkFailure extends TargetSdkFailure
{
    private Ds3SdkFailure( String stringCode, final int intCode )
    {
        super( stringCode, intCode );
    }
    
    
    public static Ds3SdkFailure valueOf( final int code )
    {
        return TargetSdkFailure.valueOf( Ds3SdkFailure.class, null, code );
    }
}
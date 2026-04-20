/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.simulator.domain;

import com.spectralogic.util.bean.lang.DefaultBooleanValue;


public interface OnlineObservable< T >
{
    String ONLINE = "online";
    
    @DefaultBooleanValue( true )
    boolean isOnline();
    
    T setOnline( final boolean value );
}

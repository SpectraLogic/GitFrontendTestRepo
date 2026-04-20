/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface MarshalableBean extends SimpleBeanSafeToProxy
{
    String NAME = "name";
    
    String getName();
    
    void setName( final String value );
    
    
    String INT = "int";
    
    int getInt();
    
    void setInt( final int value );
    
    
    String PAYLOAD = "payload";
    
    @Optional
    String getPayload();
    
    void setPayload( final String value );
}

/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import java.util.UUID;

import com.spectralogic.util.bean.lang.ConcreteImplementation;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.CustomMarshaledName;

@ConcreteImplementation( UserApiBeanImpl.class )
public interface UserApiBean extends SimpleBeanSafeToProxy
{
    String ID = "id";
    
    @CustomMarshaledName( "iD" )
    UUID getId();
    
    public UserApiBean setId( final UUID value );
    
       
    String DISPLAY_NAME = "displayName";
    
    String getDisplayName();
    
    public UserApiBean setDisplayName( final String value );
}

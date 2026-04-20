/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import java.util.Date;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface BucketApiBean extends SimpleBeanSafeToProxy
{
    String NAME = "name";
    
    String getName();
    
    void setName( final String name );
    
    
    String CREATION_DATE = "creationDate";
    
    @Optional
    Date getCreationDate();
    
    void setCreationDate( final Date value );
}

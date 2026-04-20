/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.util.bean.lang.ConcreteImplementation;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;


@ConcreteImplementation( CacheEntryInformationImpl.class )
public interface CacheEntryInformation extends SimpleBeanSafeToProxy
{
    String BLOB = "blob";
    
    @Optional
    Blob getBlob();
    
    void setBlob( final Blob value );
    
    
    String STATE = "state";
    
    CacheEntryState getState();
    
    void setState( final CacheEntryState value );
}

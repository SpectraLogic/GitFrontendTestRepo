/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.spectrads3;

import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.util.bean.lang.ConcreteImplementation;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

@ConcreteImplementation( BlobPersistenceImpl.class )
public interface BlobPersistence 
    extends SimpleBeanSafeToProxy, Identifiable, ChecksumObservable< BlobPersistence >
{
    String AVAILABLE_ON_POOL_NOW = "availableOnPoolNow";
    
    boolean isAvailableOnPoolNow();
    
    void setAvailableOnPoolNow( final boolean value );
    
    
    String AVAILABLE_ON_TAPE_NOW = "availableOnTapeNow";
    
    boolean isAvailableOnTapeNow();
    
    void setAvailableOnTapeNow( final boolean value );
}

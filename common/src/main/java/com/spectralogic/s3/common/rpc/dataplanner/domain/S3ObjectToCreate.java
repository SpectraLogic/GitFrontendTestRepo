/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import com.spectralogic.util.bean.lang.ConcreteImplementation;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

@ConcreteImplementation( S3ObjectToCreateImpl.class )
public interface S3ObjectToCreate extends SimpleBeanSafeToProxy
{
    String NAME = "name";
    
    String getName();
    
    S3ObjectToCreate setName( final String value );
    
    
    String SIZE_IN_BYTES = "sizeInBytes";
    
    /**
     * @return 0 if there is no data to the object, or greater than 0 if there is data to the object
     */
    long getSizeInBytes();
    
    S3ObjectToCreate setSizeInBytes( final long value );
}

/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape.domain;

import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface S3ObjectMetadataKeyValue 
    extends KeyValueObservable< S3ObjectMetadataKeyValue >, SimpleBeanSafeToProxy
{
    // empty
}

/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import com.spectralogic.util.bean.lang.DefaultLongValue;

public interface PublicCloudDataReplicationRule
    < T extends DataReplicationRule< T > > extends DataReplicationRule< T >
{
    String MAX_BLOB_PART_SIZE_IN_BYTES = "maxBlobPartSizeInBytes";
    
    @DefaultLongValue( 1024L * 1024 * 1024 )
    long getMaxBlobPartSizeInBytes();
    
    T setMaxBlobPartSizeInBytes( final long value );
}

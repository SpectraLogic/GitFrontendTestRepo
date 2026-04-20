/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.CustomMarshaledName;
import com.spectralogic.util.marshal.CustomMarshaledTypeName;
import com.spectralogic.util.marshal.CustomMarshaledName.CollectionNameRenderingMode;

@CustomMarshaledTypeName( "ListAllMyBucketsResult" )
public interface BucketsApiBean extends SimpleBeanSafeToProxy
{
    String OWNER = "owner";
    
    UserApiBean getOwner();
    
    void setOwner( final UserApiBean owner );
    
    
    String BUCKETS = "buckets";
    
    @CustomMarshaledName(
            value = "Bucket", 
            collectionValue = "Buckets", 
            collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    BucketApiBean [] getBuckets();
    
    void setBuckets( final BucketApiBean [] buckets );
}

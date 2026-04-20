/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.marshal.CustomMarshaledName;
import com.spectralogic.util.marshal.CustomMarshaledName.CollectionNameRenderingMode;
import com.spectralogic.util.marshal.CustomMarshaledTypeName;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;

@CustomMarshaledTypeName( "ListBucketResult" )
public interface BucketObjectsApiBean extends BucketApiBean
{
    String COMMON_PREFIXES = "commonPrefixes";
    
    @CustomMarshaledName( 
            value = "Prefix", 
            collectionValue = "CommonPrefixes", 
            collectionValueRenderingMode = CollectionNameRenderingMode.BLOCK_FOR_EVERY_ELEMENT )
    String [] getCommonPrefixes();
    
    void setCommonPrefixes( final String [] value );
    
    
    String PREFIX = "prefix";
    
    @Optional
    String getPrefix();
    
    void setPrefix( final String value );
    
    
    String DELIMITER = "delimiter";

    @Optional
    String getDelimiter();
    
    void setDelimiter( final String value );

    
    String MARKER = "marker";

    @Optional
    String getMarker();
    
    void setMarker( final String value );

    
    String NEXT_MARKER = "nextMarker";

    String getNextMarker();
    
    void setNextMarker( final String value );
    
    
    String TRUNCATED = "truncated";
    
    @CustomMarshaledName( "IsTruncated" )
    boolean isTruncated();
    
    void setTruncated( final boolean value );
    
    
    String MAX_KEYS = "maxKeys";
    int DEFAULT_MAX_KEYS = 1000;
    
    int getMaxKeys();
    
    void setMaxKeys( final int value );
    
    
    String OBJECTS = "objects";
    
    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    @CustomMarshaledName( value = "Contents" )
    S3ObjectApiBean [] getObjects();
    
    void setObjects( final S3ObjectApiBean [] objects );
    
    
    String VERSIONED_OBJECTS = "versionedObjects";
    
    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    @CustomMarshaledName( value = "Version" )
    S3ObjectApiBean [] getVersionedObjects();
    
    void setVersionedObjects( final S3ObjectApiBean [] objects );
}

/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import java.util.UUID;

import com.spectralogic.s3.common.platform.domain.BlobApiBean;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.CustomMarshaledName;
import com.spectralogic.util.marshal.CustomMarshaledTypeName;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;
import com.spectralogic.util.marshal.MarshalXmlAsAttribute;

@CustomMarshaledTypeName( "Objects" )
public interface JobChunkApiBean extends SimpleBeanSafeToProxy
{
    String CHUNK_ID = "chunkId";
    
    @MarshalXmlAsAttribute
    UUID getChunkId();
    
    void setChunkId( final UUID value );
    
    
    String CHUNK_NUMBER = "chunkNumber";
    
    @MarshalXmlAsAttribute
    int getChunkNumber();
    
    void setChunkNumber( final int value );
    
    
    String NODE_ID = "nodeId";
    
    @Optional
    @MarshalXmlAsAttribute
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    UUID getNodeId();
    
    void setNodeId( final UUID value );
    
    
    String OBJECTS = "objects";
            
    @CustomMarshaledName( "object" )
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    BlobApiBean [] getObjects();
    
    void setObjects( final BlobApiBean [] value );
}

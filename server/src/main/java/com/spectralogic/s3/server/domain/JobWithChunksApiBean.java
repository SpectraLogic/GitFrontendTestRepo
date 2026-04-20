/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import com.spectralogic.util.marshal.CustomMarshaledTypeName;


@CustomMarshaledTypeName( "MasterObjectList" )
public interface JobWithChunksApiBean extends JobApiBean
{
    String OBJECTS = "objects";
    
    JobChunkApiBean [] getObjects();
    
    void setObjects( final JobChunkApiBean [] value );
}

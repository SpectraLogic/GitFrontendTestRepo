/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.domain;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface JobToReplicate extends SimpleBeanSafeToProxy, Identifiable, NameObservable< JobToReplicate >
{
    String CHUNKS = "chunks";
    
    JobChunkToReplicate [] getChunks();
    
    void setChunks( final JobChunkToReplicate [] value );
    
    
    String OBJECTS = "objects";
    
    S3Object [] getObjects();
    
    void setObjects( final S3Object [] value );
    
    
    String BLOBS = "blobs";
    
    Blob [] getBlobs();
    
    void setBlobs( final Blob [] value );
}

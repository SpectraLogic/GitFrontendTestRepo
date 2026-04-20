/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.domain;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.bean.lang.SortBy;

public interface JobChunkToReplicate extends BlobObservable<JobChunkToReplicate>, SimpleBeanSafeToProxy, Identifiable
{
    String CHUNK_NUMBER = "chunkNumber";
    
    @SortBy
    int getChunkNumber();
    
    JobChunkToReplicate setChunkNumber( final int value );
    
    
    //This field is used only when location replicating from a BP to the same BP. This is only done in IOM jobs where
    //we are replicating a local PUT job that mimics a local GET job. This field is used to make sure the node ID on
    //the PUT chunk matches the node ID on the GET chunk at the time of creation.
    String ORIGINAL_CHUNK_ID = "originalChunkId";
    
    @Optional
    UUID getOriginalChunkId();
    
    JobChunkToReplicate setOriginalChunkId( final UUID value );
}

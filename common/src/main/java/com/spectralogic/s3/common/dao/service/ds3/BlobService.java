/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface BlobService 
    extends BeansRetriever< Blob >, BeanUpdater< Blob >, BeanDeleter
{
    void create( final Set< Blob > blobs );
    
    
    void delete( final Set< UUID > blobIds );
    
    
    long getSizeInBytes( final Set< UUID > blobIds );
}

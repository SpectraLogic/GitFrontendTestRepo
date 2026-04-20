/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.servlet.http.HttpServletResponse;

import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface S3ObjectPropertyService extends BeansRetriever< S3ObjectProperty >
{
    void createProperties( final UUID objectId, final List< S3ObjectProperty > properties );
    
    
    void create( final Set< S3ObjectProperty > properties );
    
    
    void populateAllHttpHeaders( final UUID blobId, final HttpServletResponse response );
    
    
    void populateObjectHttpHeaders( final UUID objectId, final HttpServletResponse response );
    
    
    void deleteTemporaryCreationDates( final Set< UUID > objectIds );
}

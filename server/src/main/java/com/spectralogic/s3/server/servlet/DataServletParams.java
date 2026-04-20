/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.servlet;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.server.request.RequestCompletedListener;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.io.lang.ByteRanges;

interface DataServletParams extends ChecksumObservable< DataServletParams >, SimpleBeanSafeToProxy
{
    String CACHE_FILE = "cacheFile";
    
    String getCacheFile();
    
    DataServletParams setCacheFile( final String value );


    String FILE_SOURCE = "fileSource";

    String getFileSource();

    DataServletParams setFileSource( final String value );
    
    
    String JOB_ID = "jobId";
    
    UUID getJobId();
    
    DataServletParams setJobId( final UUID value );
    
    
    String BLOB_ID = "blobId";
    
    UUID getBlobId();
    
    DataServletParams setBlobId( final UUID value );
    
    
    String BLOB_OFFSET = "blobOffset";
    
    long getBlobOffset();
    
    DataServletParams setBlobOffset( final long value );
    
    
    String BYTE_RANGES = "byteRanges";
    
    ByteRanges getByteRanges();
    
    DataServletParams setByteRanges( final ByteRanges value );
    
    
    String RESOURCE = "resource";
    
    DataPlannerResource getResource();
    
    DataServletParams setResource( final DataPlannerResource value );
    
    
    String REQUEST_COMPLETED_LISTENER = "requestCompletedListener";
    
    RequestCompletedListener< String > getRequestCompletedListener();
    
    DataServletParams setRequestCompletedListener( final RequestCompletedListener< String > value );
    
    
    String RESPONSE_TYPE = "responseType";
    
    String getResponseType();
    
    DataServletParams setResponseType( final String value );
}
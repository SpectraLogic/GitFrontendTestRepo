/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.frmwrk;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.io.ThreadedDataMover.BytesReadListener;
import com.spectralogic.util.lang.Duration;

public final class IoInProgressNotifier implements BytesReadListener
{
    public IoInProgressNotifier( 
            final UUID jobId, 
            final UUID blobId,
            final DataPlannerResource dataPlanneResource,
            final BeansRetriever< Blob > blobRetriever )
    {
        m_jobId = jobId;
        m_blobId = blobId;
        m_dataPlannerResource = dataPlanneResource;
        m_blobRetriever = blobRetriever;
    }
    
    
    public IoInProgressNotifier( 
            final UUID jobId, 
            final UUID blobId,
            final DataPlannerResource resource )
    {
        m_jobId = jobId;
        m_blobId = blobId;
        m_dataPlannerResource = resource;
        m_blobRetriever = null;
    }
    
    
    public void bytesRead( final int numberOfBytes )
    {
        if ( 0 < m_durationSinceLastActivityRpcCall.getElapsedMinutes() )
        {
            if ( null != m_blobRetriever )
            {
                ensureBlobStillExists();
            }
            m_durationSinceLastActivityRpcCall = new Duration();
            m_dataPlannerResource.jobStillActive( m_jobId, m_blobId );
        }
    }
    
    
    public void ensureBlobStillExists()
    {
        if ( null == m_blobRetriever.retrieve( m_blobId ) )
        {
            throw new S3RestException( 
                    GenericFailure.GONE, 
                    "The blob being transmitted was deleted while in transmission." );
        }
    }
    
    
    private volatile Duration m_durationSinceLastActivityRpcCall = new Duration();
    
    private final UUID m_jobId;
    private final UUID m_blobId;
    private final BeansRetriever< Blob > m_blobRetriever;
    private final DataPlannerResource m_dataPlannerResource;
}
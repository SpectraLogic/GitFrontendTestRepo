/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import com.spectralogic.s3.common.dao.domain.target.SuspectBlobAzureTarget;
import com.spectralogic.s3.common.dao.service.target.BlobAzureTargetService;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class MarkSuspectBlobAzureTargetsAsDegradedRequestHandler
    extends BaseMarkSuspectBlobTargetsAsDegraded< SuspectBlobAzureTarget, BlobAzureTargetService >
{
    public MarkSuspectBlobAzureTargetsAsDegradedRequestHandler()
    {
        super( SuspectBlobAzureTarget.class,
                BlobAzureTargetService.class, 
                RestDomainType.SUSPECT_BLOB_AZURE_TARGET );
    }
}

/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import com.spectralogic.s3.common.dao.domain.target.SuspectBlobS3Target;
import com.spectralogic.s3.common.dao.service.target.BlobS3TargetService;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class MarkSuspectBlobS3TargetsAsDegradedRequestHandler
    extends BaseMarkSuspectBlobTargetsAsDegraded< SuspectBlobS3Target, BlobS3TargetService >
{
    public MarkSuspectBlobS3TargetsAsDegradedRequestHandler()
    {
        super( SuspectBlobS3Target.class,
                BlobS3TargetService.class, 
                RestDomainType.SUSPECT_BLOB_S3_TARGET );
    }
}

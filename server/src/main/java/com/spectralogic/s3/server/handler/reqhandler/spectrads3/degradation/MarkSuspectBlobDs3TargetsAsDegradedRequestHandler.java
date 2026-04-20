/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import com.spectralogic.s3.common.dao.domain.target.SuspectBlobDs3Target;
import com.spectralogic.s3.common.dao.service.target.BlobDs3TargetService;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class MarkSuspectBlobDs3TargetsAsDegradedRequestHandler 
    extends BaseMarkSuspectBlobTargetsAsDegraded< SuspectBlobDs3Target, BlobDs3TargetService >
{
    public MarkSuspectBlobDs3TargetsAsDegradedRequestHandler()
    {
        super( SuspectBlobDs3Target.class,
                BlobDs3TargetService.class, 
                RestDomainType.SUSPECT_BLOB_DS3_TARGET );
    }
}

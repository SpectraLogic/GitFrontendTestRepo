/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class CancelActiveJobRequestHandler extends CancelJobRequestHandler
{
    public CancelActiveJobRequestHandler()
    {
        super( RestDomainType.ACTIVE_JOB );
    }
}

/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.s3;

import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.shared.BaseModifyAllTargetsRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class ModifyAllS3TargetsRequestHandler
    extends BaseModifyAllTargetsRequestHandler< S3Target >
{
    public ModifyAllS3TargetsRequestHandler()
    {
        super( S3Target.class, RestDomainType.S3_TARGET );
    }
}

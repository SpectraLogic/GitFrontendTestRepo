/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.ds3;

import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.shared.BaseModifyAllTargetsRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class ModifyAllDs3TargetsRequestHandler 
    extends BaseModifyAllTargetsRequestHandler< Ds3Target >
{
    public ModifyAllDs3TargetsRequestHandler()
    {
        super( Ds3Target.class, RestDomainType.DS3_TARGET );
    }
}
